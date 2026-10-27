//! The server's end of the template socket: the frames each side sends, and the connection that
//! carries them.
//!
//! The socket carries two families of messages, one per direction: the server's commands to the
//! template ([`TemplateCommand`], only encoded here) and the template's reports to the server
//! ([`TemplateReport`], only decoded here; python/mipyre/_process_lane/template.py is the other
//! side). Each is one frame: a 4-byte little-endian length, then that many bytes, the first of
//! which is the tag. The template is another process that ran the app's imports, so its frames
//! are read as untrusted: a length past [`MAX_FRAME_BYTES`] is refused from the header alone,
//! before anything is buffered for it.

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::time::Instant;

use rustix::io::Errno;

use super::lane_failure::LaneIndex;
use super::pid::Pid;
use crate::sync::Mutex;
use mipyre_shm::channel::Attempt;

/// The longest frame either side sends, its tag included. The template cuts a traceback to fit.
pub(super) const MAX_FRAME_BYTES: u32 = 64 * 1024;

/// The length that starts every frame.
const HEADER_BYTES: usize = 4;

/// The longest body a command frame holds after its tag.
pub(super) const MAX_BODY_BYTES: usize = MAX_FRAME_BYTES as usize - 1;

/// The tags of both directions, numbered in one space so a frame read the wrong way round is
/// never taken for another.
mod tag {
    pub(super) const SETUP: u8 = 1;
    pub(super) const LANE: u8 = 2;
    pub(super) const START: u8 = 3;
    pub(super) const FORKED: u8 = 4;
    pub(super) const EXITED: u8 = 5;
    pub(super) const IMPORT_FAILED: u8 = 6;
    pub(super) const LANE_FDS_MISSING: u8 = 7;
    pub(super) const RUN: u8 = 8;
}

/// What the server tells the template.
#[derive(Debug)]
pub(super) enum TemplateCommand<'a> {
    /// What a worker must import and how the lanes are laid out, as JSON.
    Setup(CommandBody<'a>),
    /// The shared memory and the wake-pipe write end of lane `lane`, attached to the frame.
    Lane { lane: LaneIndex, fds: [BorrowedFd<'a>; 2] },
    /// Start a worker for lane `lane` as start attempt `attempt`: the worker reports its start in its channel under
    /// it, and the answer carries it back.
    Start { lane: LaneIndex, attempt: Attempt },
    /// The command line of the run whose workers come next, as `CommandLine.encode()` JSON.
    Run(CommandBody<'a>),
}

/// The body of a setup or run command. It fits a frame after its tag, because only `new` makes
/// one.
#[derive(Clone, Copy, Debug)]
pub(super) struct CommandBody<'a>(&'a [u8]);

impl<'a> CommandBody<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Result<Self, BodyTooLong> {
        body_fits(bytes.len())?;
        Ok(CommandBody(bytes))
    }
}

/// A command body of `bytes` bytes, more than [`MAX_BODY_BYTES`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BodyTooLong {
    pub(super) bytes: usize,
}

fn body_fits(bytes: usize) -> Result<(), BodyTooLong> {
    if bytes <= MAX_BODY_BYTES {
        Ok(())
    } else {
        Err(BodyTooLong { bytes })
    }
}

impl TemplateCommand<'_> {
    /// The command's name, for an error that says which frame did not go.
    pub(super) fn name(&self) -> &'static str {
        match self {
            TemplateCommand::Setup(_) => "setup",
            TemplateCommand::Lane { .. } => "lane",
            TemplateCommand::Start { .. } => "start",
            TemplateCommand::Run(_) => "run",
        }
    }

    /// The command's frame.
    pub(super) fn frame(&self) -> Vec<u8> {
        let (tag, body) = match self {
            TemplateCommand::Setup(json) => (tag::SETUP, json.0.to_vec()),
            TemplateCommand::Lane { lane, .. } => (tag::LANE, wire_lane(*lane).to_le_bytes().to_vec()),
            TemplateCommand::Start { lane, attempt } => (
                tag::START,
                [wire_lane(*lane).to_le_bytes(), attempt.get().to_le_bytes()].concat(),
            ),
            TemplateCommand::Run(json) => (tag::RUN, json.0.to_vec()),
        };
        framed(tag, &body)
    }

    /// The fds that ride on the frame's first byte.
    pub(super) fn fds(&self) -> &[BorrowedFd<'_>] {
        match self {
            TemplateCommand::Lane { fds, .. } => fds,
            TemplateCommand::Setup(_) | TemplateCommand::Start { .. } | TemplateCommand::Run(_) => &[],
        }
    }
}

/// A lane's number on the socket. The server has far fewer lanes than `u32` counts.
fn wire_lane(lane: LaneIndex) -> u32 {
    lane.0 as u32
}

fn framed(tag: u8, body: &[u8]) -> Vec<u8> {
    let len = 1 + body.len() as u32;
    let mut frame = Vec::with_capacity(HEADER_BYTES + len as usize);
    frame.extend_from_slice(&len.to_le_bytes());
    frame.push(tag);
    frame.extend_from_slice(body);
    frame
}

/// What the template tells the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TemplateReport {
    /// The answer to [`TemplateCommand::Start`]: the worker it started, or why it could not.
    Forked {
        lane: LaneIndex,
        attempt: Attempt,
        result: Result<Pid, Errno>,
    },
    /// The template reaped worker `pid` of `lane` with wait status `status`.
    Exited { lane: LaneIndex, pid: Pid, status: i32 },
    /// With start="fork" the template could not import the app; this is the traceback, cut
    /// to fit a frame. The template ends after it.
    ImportFailed(String),
    /// A [`TemplateCommand::Lane`] frame came with `got` fds, not two. The template ends after it.
    LaneFdsMissing { lane: LaneIndex, got: u32 },
}

/// Why bytes from the template are no frame the server can take.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TemplateFrameError {
    #[error("a frame of {len} bytes, past the {max}-byte limit")]
    TooLong { len: u32, max: u32 },
    #[error("a frame of 0 bytes, which has no tag")]
    Untagged,
    #[error("a frame with tag {0}, which the template never sends")]
    UnknownTag(u8),
    #[error("a frame with tag {tag} and a {len}-byte body, which takes {want}")]
    BodyLength { tag: u8, len: usize, want: usize },
    #[error("a report for lane {lane}, but the server has {lanes}")]
    LaneOutOfRange { lane: u32, lanes: usize },
    #[error("a fork answer of {raw}: neither a pid nor a negated errno")]
    NeitherPidNorErrno { raw: i32 },
    #[error("an exit report for pid {raw}, which names no process")]
    NoProcess { raw: i32 },
}

/// Decodes the first frame in `buf`, from a template serving `lanes` lanes: the report and the
/// bytes it took, `None` while the frame is not whole yet. The length is judged as soon as the
/// header is in, so a frame past [`MAX_FRAME_BYTES`] fails before its body is waited for.
pub(super) fn decode_template_report(
    buf: &[u8],
    lanes: usize,
) -> Result<Option<(TemplateReport, usize)>, TemplateFrameError> {
    let Some((&header, rest)) = buf.split_first_chunk::<HEADER_BYTES>() else {
        return Ok(None);
    };
    let len = u32::from_le_bytes(header);
    if len > MAX_FRAME_BYTES {
        return Err(TemplateFrameError::TooLong {
            len,
            max: MAX_FRAME_BYTES,
        });
    }
    let Some(frame) = rest.get(..len as usize) else {
        return Ok(None);
    };
    let (&tag, body) = frame.split_first().ok_or(TemplateFrameError::Untagged)?;
    let lane = |raw: u32| match usize::try_from(raw) {
        Ok(i) if i < lanes => Ok(LaneIndex(i)),
        _ => Err(TemplateFrameError::LaneOutOfRange { lane: raw, lanes }),
    };
    let report = match tag {
        tag::FORKED => {
            let [l, a, r] = words(tag, body)?;
            let raw = r as i32;
            let result = match (Pid::new(raw), raw.checked_neg()) {
                (Some(pid), _) => Ok(pid),
                (None, Some(errno)) if errno > 0 => Err(Errno::from_raw_os_error(errno)),
                (None, _) => return Err(TemplateFrameError::NeitherPidNorErrno { raw }),
            };
            TemplateReport::Forked {
                lane: lane(l)?,
                attempt: Attempt::new(a),
                result,
            }
        }
        tag::EXITED => {
            let [l, p, s] = words(tag, body)?;
            let pid = Pid::new(p as i32).ok_or(TemplateFrameError::NoProcess { raw: p as i32 })?;
            TemplateReport::Exited {
                lane: lane(l)?,
                pid,
                status: s as i32,
            }
        }
        tag::IMPORT_FAILED => TemplateReport::ImportFailed(String::from_utf8_lossy(body).into_owned()),
        tag::LANE_FDS_MISSING => {
            let [l, got] = words(tag, body)?;
            TemplateReport::LaneFdsMissing { lane: lane(l)?, got }
        }
        other => return Err(TemplateFrameError::UnknownTag(other)),
    };
    Ok(Some((report, HEADER_BYTES + len as usize)))
}

/// The `N` little-endian 4-byte words that make a body of tag `tag`.
fn words<const N: usize>(tag: u8, body: &[u8]) -> Result<[u32; N], TemplateFrameError> {
    let mismatch = || TemplateFrameError::BodyLength {
        tag,
        len: body.len(),
        want: 4 * N,
    };
    let (chunks, []) = body.as_chunks::<4>() else {
        return Err(mismatch());
    };
    let words: &[[u8; 4]; N] = chunks.try_into().map_err(|_| mismatch())?;
    Ok(words.map(u32::from_le_bytes))
}

/// How the connection to the template ended: a value, not an error of its own. The first end
/// seen is kept, and every later use of the connection gets it.
#[derive(Clone, Debug)]
pub enum LinkEnd {
    Closed,
    RecvFailed(Errno),
    /// The template sent bytes that are no frame; `prefix` is the first of them (at most 64).
    BadFrame {
        error: TemplateFrameError,
        prefix: Vec<u8>,
    },
    /// The template could not import the app (start="fork"), said so with this traceback
    /// ([`TemplateReport::ImportFailed`]), and ended.
    ImportFailed(String),
    /// The template got `got` fds on lane `lane`'s frame, not two
    /// ([`TemplateReport::LaneFdsMissing`]), and ended.
    LaneFdsMissing {
        lane: LaneIndex,
        got: u32,
    },
}

impl std::fmt::Display for LinkEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkEnd::Closed => write!(f, "the template closed the connection"),
            LinkEnd::RecvFailed(errno) => write!(f, "reading from the template: {errno}"),
            LinkEnd::BadFrame { error, prefix } => {
                write!(f, "the template sent {error} (it began ")?;
                prefix.iter().try_for_each(|b| write!(f, "{b:02x}"))?;
                write!(f, ")")
            }
            LinkEnd::ImportFailed(traceback) => write!(f, "the template could not import the app:\n{traceback}"),
            LinkEnd::LaneFdsMissing { lane, got } => write!(f, "the template got {got} fds on lane {lane}'s frame"),
        }
    }
}

/// How many bytes of a bad frame its [`LinkEnd::BadFrame`] keeps.
const BAD_FRAME_PREFIX: usize = 64;

/// A command frame that did not reach the template.
#[derive(Debug, thiserror::Error)]
#[error("sending the {frame} frame to template pid {template}: {source}")]
pub struct TemplateSendError {
    pub(crate) frame: &'static str,
    pub(crate) template: Pid,
    pub(crate) source: std::io::Error,
}

/// The wait status of the last worker the template reported reaped, per lane. Filled from the
/// template's [`TemplateReport::Exited`] frames, by whoever reads the socket; read by pid, so a
/// report about another worker of the lane is never taken for this one.
pub(super) struct ExitReports(Mutex<Vec<Option<(Pid, i32)>>>);

impl ExitReports {
    pub(super) fn new(lanes: usize) -> Self {
        ExitReports(Mutex::new(vec![None; lanes]))
    }

    /// How many lanes it keeps a report for: the lanes a report may name.
    pub(super) fn lanes(&self) -> usize {
        self.0.lock().len()
    }

    pub(super) fn record(&self, lane: LaneIndex, pid: Pid, status: i32) {
        if let Some(cell) = self.0.lock().get_mut(lane.0) {
            *cell = Some((pid, status));
        }
    }

    /// The wait status the template reported for worker `pid` of `lane`, once it has.
    pub(super) fn status_of(&self, lane: LaneIndex, pid: Pid) -> Option<i32> {
        match self.0.lock().get(lane.0) {
            Some(Some((reaped, status))) if *reaped == pid => Some(*status),
            _ => None,
        }
    }
}

/// The server's side of the template socket: the socket, the bytes read from it that make no
/// whole frame yet, and how the connection ended once it has.
/// Whoever holds it is the only reader and writer of the socket.
pub(super) struct TemplateLink {
    template: Pid,
    sock: OwnedFd,
    read: Vec<u8>,
    ended: Option<LinkEnd>,
    /// Counts the fork requests that gave up before their answer came: each answer still on its
    /// way names a worker nobody takes, which only a read of the socket ends.
    late_answers: usize,
    /// Says whether a thread waits on the socket for those answers.
    watched: bool,
}

/// Says whether someone must read the socket though no fork request waits on it: the answers owed
/// to requests that gave up name workers that live until they are read.
pub(super) fn awaits_late_answers(late_answers: usize, ended: bool) -> bool {
    late_answers > 0 && !ended
}

/// How much one `recv` from the template asks for.
const RECV_BYTES: usize = 4096;

impl TemplateLink {
    pub(super) fn new(template: Pid, sock: OwnedFd) -> Self {
        TemplateLink {
            template,
            sock,
            read: Vec::new(),
            ended: None,
            late_answers: 0,
            watched: false,
        }
    }

    /// Owes an answer to a fork request that gave up waiting for it.
    pub(super) fn give_up_answer(&mut self) {
        self.late_answers = self.late_answers.saturating_add(1);
    }

    /// Takes one answer to a request that gave up as come.
    pub(super) fn late_answer_came(&mut self) {
        self.late_answers = self.late_answers.saturating_sub(1);
    }

    /// Says whether a watch on the socket must start now for the late answers, and takes it as
    /// started when it must.
    pub(super) fn start_watch(&mut self) -> bool {
        let start = !self.watched && awaits_late_answers(self.late_answers, self.ended.is_some());
        self.watched |= start;
        start
    }

    /// Says whether the watch is done, every late answer read or the connection ended, and takes
    /// it as ended when it is.
    pub(super) fn end_watch(&mut self) -> bool {
        let done = !awaits_late_answers(self.late_answers, self.ended.is_some());
        if done {
            self.watched = false;
        }
        done
    }

    /// Takes the watch as ended though answers are still owed: its thread could not wait. The
    /// next read of the socket, or the next request's timeout, reads them.
    pub(super) fn drop_watch(&mut self) {
        self.watched = false;
    }

    /// How the connection ended, once it has.
    pub(super) fn ended(&self) -> Option<&LinkEnd> {
        self.ended.as_ref()
    }

    /// Returns the socket, for a wait that wakes when a report comes; the reports are still read
    /// through [`TemplateLink::next_report`].
    pub(super) fn socket(&self) -> BorrowedFd<'_> {
        self.sock.as_fd()
    }

    /// Sends `command` whole, its fds on its first byte.
    pub(super) fn send(&self, command: &TemplateCommand<'_>) -> Result<(), TemplateSendError> {
        let frame = command.frame();
        send_with_fds(self.sock.as_fd(), &frame, command.fds()).map_err(|source| TemplateSendError {
            frame: command.name(),
            template: self.template,
            source,
        })
    }

    /// Keeps `end` as how the connection ended, and says so once: nothing more comes from the
    /// template, and its next fork request fails with it.
    pub(super) fn end(&mut self, end: LinkEnd) {
        tracing::error!(
            target: "mipyre::server",
            template = self.template.as_raw(),
            "the connection to the process-lane template ended: {end}"
        );
        self.ended = Some(end);
    }

    /// Returns the next report from a template serving `lanes` lanes. With a `deadline` it waits
    /// for one until then (`None` once it passes); without one it takes only what the socket
    /// already holds (`None` when that is no whole frame). Frames read before the connection
    /// ended are all returned before its end is.
    pub(super) fn next_report(
        &mut self,
        lanes: usize,
        deadline: Option<Instant>,
    ) -> Result<Option<TemplateReport>, LinkEnd> {
        loop {
            match decode_template_report(&self.read, lanes) {
                Ok(Some((report, used))) => {
                    self.read.drain(..used);
                    return Ok(Some(report));
                }
                Ok(None) => {}
                Err(error) => {
                    let prefix = self.read[..self.read.len().min(BAD_FRAME_PREFIX)].to_vec();
                    self.read.clear();
                    self.end(LinkEnd::BadFrame { error, prefix });
                }
            }
            if let Some(end) = &self.ended {
                return Err(end.clone());
            }
            if let Some(deadline) = deadline {
                match readable_by(self.sock.as_fd(), deadline) {
                    Ok(true) => {}
                    Ok(false) => return Ok(None),
                    Err(Errno::INTR) => continue,
                    Err(errno) => {
                        self.end(LinkEnd::RecvFailed(errno));
                        continue;
                    }
                }
            }
            let at = self.read.len();
            self.read.resize(at + RECV_BYTES, 0);
            let got = rustix::net::recv(
                self.sock.as_fd(),
                &mut self.read[at..],
                rustix::net::RecvFlags::DONTWAIT,
            );
            self.read.truncate(at + got.map_or(0, |(n, _)| n));
            match got {
                Ok((0, _)) => self.end(LinkEnd::Closed),
                Ok(_) | Err(Errno::INTR) => {}
                Err(Errno::AGAIN) if deadline.is_none() => return Ok(None),
                // Readable, then nothing to read: another wait.
                Err(Errno::AGAIN) => {}
                Err(errno) => self.end(LinkEnd::RecvFailed(errno)),
            }
        }
    }
}

/// Waits until `fd` has bytes to read (or its peer closed it), and says whether it does before
/// `deadline`; at or past `deadline` it only looks.
fn readable_by(fd: BorrowedFd<'_>, deadline: Instant) -> Result<bool, Errno> {
    use rustix::event::{PollFd, PollFlags, Timespec};
    let left = deadline.saturating_duration_since(Instant::now());
    let timeout = Timespec {
        tv_sec: i64::try_from(left.as_secs()).unwrap_or(i64::MAX),
        tv_nsec: left.subsec_nanos().into(),
    };
    let mut fds = [PollFd::new(&fd, PollFlags::IN)];
    Ok(rustix::event::poll(&mut fds, Some(&timeout))? > 0)
}

/// Why a whole message could not be moved.
#[derive(Debug)]
pub(super) enum TransferError {
    /// A call moved nothing: the peer closed the connection (a `recv`), or would take no more
    /// (a `send`).
    Closed,
    /// A call failed with this error, other than an interrupt.
    CallFailed(std::io::Error),
}

/// Moves a message of `len` bytes by calling `step(offset)` until all of it moved: a short
/// transfer continues from where it stopped, an interrupted call (EINTR) is made again.
pub(super) fn transfer_all(
    len: usize,
    mut step: impl FnMut(usize) -> std::io::Result<usize>,
) -> Result<(), TransferError> {
    let mut done = 0;
    while done < len {
        match step(done) {
            Ok(0) => return Err(TransferError::Closed),
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(TransferError::CallFailed(e)),
        }
    }
    Ok(())
}

/// Sends all of `data` with `fds` attached as `SCM_RIGHTS` (see [`send_attached`]).
pub(super) fn send_with_fds(sock: BorrowedFd<'_>, data: &[u8], fds: &[BorrowedFd<'_>]) -> std::io::Result<()> {
    send_attached(
        data.len(),
        || sendmsg_with_fds(sock, data, fds),
        |at| Ok(rustix::net::send(sock, &data[at..], rustix::net::SendFlags::empty())?),
    )
}

/// Sends a message of `len` bytes whose fds ride on its first byte: `with_fds` (from the start,
/// the fds attached) until a call moves something, then `plain(offset)` for the rest. An
/// interrupted call is made again (one that moved nothing carried no fds, so they are sent
/// again with it); a call that moves nothing of what is left is `WriteZero`, as `write_all`
/// says it.
pub(super) fn send_attached(
    len: usize,
    mut with_fds: impl FnMut() -> std::io::Result<usize>,
    mut plain: impl FnMut(usize) -> std::io::Result<usize>,
) -> std::io::Result<()> {
    transfer_all(len, |at| if at == 0 { with_fds() } else { plain(at) }).map_err(|cut| match cut {
        TransferError::Closed => std::io::ErrorKind::WriteZero.into(),
        TransferError::CallFailed(e) => e,
    })
}

/// One `sendmsg` of `data` with `fds` attached as `SCM_RIGHTS`: the bytes it moved, or its
/// error.
fn sendmsg_with_fds(sock: BorrowedFd<'_>, data: &[u8], fds: &[BorrowedFd<'_>]) -> std::io::Result<usize> {
    use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage};
    if fds.is_empty() {
        return Ok(rustix::net::send(sock, data, rustix::net::SendFlags::empty())?);
    }
    let iov = [std::io::IoSlice::new(data)];
    let mut space = vec![std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(fds.len()))];
    let mut control = SendAncillaryBuffer::new(&mut space);
    if !control.push(SendAncillaryMessage::ScmRights(fds)) {
        return Err(std::io::Error::other(format!(
            "{} fds did not fit the control buffer sized for them",
            fds.len()
        )));
    }
    Ok(rustix::net::sendmsg(
        sock,
        &iov,
        &mut control,
        rustix::net::SendFlags::empty(),
    )?)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The socket is watched only while an answer is owed to a request that gave up and the
    /// connection is still there to bring it.
    #[test]
    fn late_answers_are_awaited_only_while_owed_on_an_open_connection() {
        assert!(!awaits_late_answers(0, false));
        assert!(awaits_late_answers(1, false));
        assert!(!awaits_late_answers(1, true));
    }

    /// A report as the template encodes it (`report_frame` in template.py).
    pub(in super::super) fn report_frame(report: &TemplateReport) -> Vec<u8> {
        let lane = |l: LaneIndex| (l.0 as u32).to_le_bytes();
        match report {
            TemplateReport::Forked {
                lane: l,
                attempt,
                result,
            } => {
                let raw = match result {
                    Ok(pid) => pid.as_raw(),
                    Err(errno) => -errno.raw_os_error(),
                };
                framed(
                    tag::FORKED,
                    &[lane(*l), attempt.get().to_le_bytes(), raw.to_le_bytes()].concat(),
                )
            }
            TemplateReport::Exited { lane: l, pid, status } => framed(
                tag::EXITED,
                &[lane(*l), pid.as_raw().to_le_bytes(), status.to_le_bytes()].concat(),
            ),
            TemplateReport::ImportFailed(text) => framed(tag::IMPORT_FAILED, text.as_bytes()),
            TemplateReport::LaneFdsMissing { lane: l, got } => {
                framed(tag::LANE_FDS_MISSING, &[lane(*l), got.to_le_bytes()].concat())
            }
        }
    }

    fn exited(lane: usize) -> TemplateReport {
        TemplateReport::Exited {
            lane: LaneIndex(lane),
            pid: Pid::new(4321).unwrap(),
            status: 9,
        }
    }

    /// A whole frame decodes, one byte short of it is not whole yet.
    #[test]
    fn a_whole_frame_decodes_and_one_byte_short_waits() {
        let frame = report_frame(&exited(1));
        assert_eq!(decode_template_report(&frame, 2), Ok(Some((exited(1), frame.len()))));
        assert_eq!(decode_template_report(&frame[..frame.len() - 1], 2), Ok(None));
        assert_eq!(decode_template_report(&frame[..3], 2), Ok(None));
    }

    /// An exit report for the last lane is taken; one for the lane past it is refused (it
    /// changes no lane: the caller records nothing).
    #[test]
    fn a_report_for_the_last_lane_is_taken_and_one_past_it_refused() {
        assert_eq!(
            decode_template_report(&report_frame(&exited(1)), 2).map(|r| r.map(|(r, _)| r)),
            Ok(Some(exited(1)))
        );
        assert_eq!(
            decode_template_report(&report_frame(&exited(2)), 2),
            Err(TemplateFrameError::LaneOutOfRange { lane: 2, lanes: 2 })
        );
    }

    /// A tag the template never sends is refused, as is a frame with no tag at all.
    #[test]
    fn an_unknown_tag_and_an_empty_frame_are_refused() {
        assert_eq!(
            decode_template_report(&framed(tag::START, &[0; 8]), 2),
            Err(TemplateFrameError::UnknownTag(tag::START))
        );
        assert_eq!(
            decode_template_report(&framed(99, &[]), 2),
            Err(TemplateFrameError::UnknownTag(99))
        );
        assert_eq!(
            decode_template_report(&0u32.to_le_bytes(), 2),
            Err(TemplateFrameError::Untagged)
        );
    }

    /// A frame of exactly the limit waits for its body; one byte more is refused from the header
    /// alone.
    #[test]
    fn the_length_limit_is_judged_from_the_header() {
        assert_eq!(decode_template_report(&MAX_FRAME_BYTES.to_le_bytes(), 2), Ok(None));
        assert_eq!(
            decode_template_report(&(MAX_FRAME_BYTES + 1).to_le_bytes(), 2),
            Err(TemplateFrameError::TooLong {
                len: MAX_FRAME_BYTES + 1,
                max: MAX_FRAME_BYTES
            })
        );
        let mut whole = framed(tag::IMPORT_FAILED, &vec![b'x'; MAX_FRAME_BYTES as usize - 1]);
        assert!(
            matches!(decode_template_report(&whole, 2), Ok(Some((TemplateReport::ImportFailed(t), _))) if t.len() == MAX_FRAME_BYTES as usize - 1)
        );
        whole.pop();
        assert_eq!(decode_template_report(&whole, 2), Ok(None));
    }

    #[test]
    fn a_command_body_fits_up_to_the_frame_less_its_tag() {
        assert_eq!(body_fits(MAX_FRAME_BYTES as usize - 1), Ok(()));
        assert_eq!(
            body_fits(MAX_FRAME_BYTES as usize),
            Err(BodyTooLong {
                bytes: MAX_FRAME_BYTES as usize
            })
        );
    }

    /// The template's answer to a fork request is read as untrusted: a pid is a pid, a negated
    /// errno is the fork's own error with its errno kept, and anything else (0, `i32::MIN`, whose
    /// negation overflows) is said as what it was.
    #[test]
    fn the_template_fork_answer_keeps_the_errno_and_refuses_the_rest() {
        let answer = |raw: i32| {
            framed(
                tag::FORKED,
                &[0u32.to_le_bytes(), 5u32.to_le_bytes(), raw.to_le_bytes()].concat(),
            )
        };
        let result = |raw| match decode_template_report(&answer(raw), 1) {
            Ok(Some((TemplateReport::Forked { result, .. }, _))) => Ok(result),
            other => Err(other),
        };
        assert_eq!(result(42), Ok(Ok(Pid::new(42).unwrap())));
        assert_eq!(result(-libc::EAGAIN), Ok(Err(Errno::AGAIN)));
        for raw in [0, i32::MIN] {
            assert_eq!(result(raw), Err(Err(TemplateFrameError::NeitherPidNorErrno { raw })));
        }
    }

    /// A body that is not the tag's layout is refused with its length.
    #[test]
    fn a_body_of_the_wrong_length_is_refused() {
        assert_eq!(
            decode_template_report(&framed(tag::EXITED, &[0; 11]), 2),
            Err(TemplateFrameError::BodyLength {
                tag: tag::EXITED,
                len: 11,
                want: 12
            })
        );
    }

    /// A traceback that is not UTF-8 still arrives, its bad bytes replaced.
    #[test]
    fn a_traceback_that_is_not_utf8_still_arrives() {
        let got = decode_template_report(&framed(tag::IMPORT_FAILED, b"bad \xff byte"), 1);
        assert_eq!(
            got.map(|r| r.map(|(r, _)| r)),
            Ok(Some(TemplateReport::ImportFailed("bad \u{fffd} byte".into())))
        );
    }

    fn any_report(lanes: usize) -> impl Strategy<Value = TemplateReport> {
        let lane = (0..lanes).prop_map(LaneIndex);
        let pid = (1..=i32::MAX).prop_map(|raw| Pid::new(raw).unwrap());
        prop_oneof![
            (
                lane.clone(),
                any::<u32>(),
                prop_oneof![
                    pid.clone().prop_map(Ok),
                    (1..=4095i32).prop_map(|e| Err(Errno::from_raw_os_error(e))),
                ]
            )
                .prop_map(|(lane, a, result)| TemplateReport::Forked {
                    lane,
                    attempt: Attempt::new(a),
                    result
                }),
            (lane.clone(), pid, any::<i32>()).prop_map(|(lane, pid, status)| TemplateReport::Exited {
                lane,
                pid,
                status
            }),
            "\\PC{0,200}".prop_map(TemplateReport::ImportFailed),
            (lane, any::<u32>()).prop_map(|(lane, got)| TemplateReport::LaneFdsMissing { lane, got }),
        ]
    }

    /// Decodes `stream` frame by frame, its bytes handed over in pieces of the given sizes, as a
    /// socket hands them: the reports, then the error the stream ends in, if any.
    fn decode_in_pieces(
        stream: &[u8],
        pieces: &[usize],
        lanes: usize,
    ) -> (Vec<TemplateReport>, Option<TemplateFrameError>) {
        let mut buf = Vec::new();
        let mut reports = Vec::new();
        let mut fed = 0;
        let mut sizes = pieces.iter().copied().cycle();
        loop {
            match decode_template_report(&buf, lanes) {
                Ok(Some((report, used))) => {
                    reports.push(report);
                    buf.drain(..used);
                }
                Err(e) => return (reports, Some(e)),
                Ok(None) if fed == stream.len() => return (reports, None),
                Ok(None) => {
                    let n = sizes.next().unwrap_or(1).max(1).min(stream.len() - fed);
                    buf.extend_from_slice(&stream[fed..fed + n]);
                    fed += n;
                }
            }
        }
    }

    proptest! {
        /// Every report the template can send decodes back to itself.
        #[test]
        fn a_report_decodes_to_what_was_encoded(report in any_report(8)) {
            let frame = report_frame(&report);
            prop_assert_eq!(decode_template_report(&frame, 8), Ok(Some((report, frame.len()))));
        }

        /// However the socket cuts a stream of frames, the same reports come out in the same
        /// order, and a stream ending in a bad frame fails at the same point.
        #[test]
        fn a_stream_cut_anywhere_decodes_the_same(
            reports in proptest::collection::vec(any_report(4), 0..8),
            tail in proptest::collection::vec(any::<u8>(), 0..16),
            pieces in proptest::collection::vec(1usize..40, 1..10),
        ) {
            let mut stream: Vec<u8> = reports.iter().flat_map(report_frame).collect();
            stream.extend_from_slice(&tail);
            let whole = decode_in_pieces(&stream, &[stream.len().max(1)], 4);
            prop_assert_eq!(decode_in_pieces(&stream, &pieces, 4), whole.clone());
            prop_assert_eq!(&whole.0[..reports.len()], &reports[..]);
        }

        /// Any bytes at all decode to a report, wait, or fail: never a panic, never a frame past
        /// the bytes given.
        #[test]
        fn any_bytes_decode_without_panic(bytes in proptest::collection::vec(any::<u8>(), 0..300), lanes in 1usize..5) {
            if let Ok(Some((_, used))) = decode_template_report(&bytes, lanes) {
                prop_assert!(used <= bytes.len());
            }
        }
    }
}
