//! The server's side of the template: setting it up, telling it each run, and asking it for workers.

use std::os::fd::{BorrowedFd, OwnedFd};
use std::time::{Duration, Instant};

use super::core::WorkerTag;
use super::lane_failure::LaneIndex;
use super::pid::Pid;
use super::run::{LaneStartError, ServerLanes};
use super::template::{
    CommandBody, ExitReports, LinkEnd, TemplateCommand, TemplateLink, TemplateReport, TemplateSendError, MAX_BODY_BYTES,
};
use super::worker_process::WorkerProcess;
use crate::sync::Mutex;
use mipyre_shm::channel::Attempt;

/// A setup that did not reach the template.
#[derive(Debug, thiserror::Error)]
pub enum TemplateSetupError {
    #[error(
        "the process-lane setup is {bytes} bytes of JSON, and the template takes at most {max}: \
         its sys.path is the usual cause",
        max = MAX_BODY_BYTES
    )]
    TooLong { bytes: usize },
    #[error("no template process to set up")]
    NoTemplate,
    #[error(transparent)]
    Send(#[from] TemplateSendError),
}

/// A run's command line that did not reach the template.
#[derive(Debug, thiserror::Error)]
pub enum TemplateRunError {
    #[error(
        "the run's command line is {bytes} bytes of JSON, and the process-lane template takes at most {max}: \
         pass less on the command line (a file the app reads, the environment)",
        max = MAX_BODY_BYTES
    )]
    CommandLineTooLong { bytes: usize },
    #[error("no template process to tell the run's command line")]
    NoTemplate,
    #[error(transparent)]
    Send(#[from] TemplateSendError),
}

/// Why the template gave no new worker for a lane.
#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    #[error("no template process (the app was not prepared with runtime='process' routes)")]
    NoTemplate,
    #[error(transparent)]
    Send(TemplateSendError),
    #[error("reading the forked pid from template pid {template}: {source}")]
    Receive { template: Pid, source: std::io::Error },
    #[error("reading the forked pid from template pid {template}: the template closed the connection")]
    TemplateClosed { template: Pid },
    #[error("template pid {template} could not fork: {source}")]
    Fork { template: Pid, source: std::io::Error },
    /// The template did not answer within `waited`; whoever reads its late answer (a thread
    /// that waits for it, or a later exchange) ends the worker it names.
    #[error("template pid {template} did not answer a fork request within {waited:?}")]
    TimedOut { template: Pid, waited: Duration },
    /// With start="fork" the template could not import the app; `traceback` is its own report.
    #[error("template pid {template} could not import the app:\n{traceback}")]
    ImportFailed { template: Pid, traceback: String },
    /// The template got `got` fds on lane `lane`'s frame, not two (it may be out of fds: it holds
    /// two per lane).
    #[error("template pid {template} got {got} of the {want} fds of lane {lane}", want = LANE_FDS)]
    LaneFdsMissing { template: Pid, lane: LaneIndex, got: u32 },
    /// The template broke the socket's protocol; the connection is no use any more.
    #[error("template pid {template} broke the socket protocol: {end}")]
    ProtocolViolated { template: Pid, end: LinkEnd },
    /// The template named a process it did not start; the server never takes it as a worker,
    /// so it is never killed on the template's word.
    #[error("template pid {template} answered pid {pid} to a fork request, but its parent is pid {parent}")]
    NotTemplateChild { template: Pid, pid: Pid, parent: i32 },
    #[error("reading the parent of pid {pid}, which template pid {template} answered to a fork request: {source}")]
    ParentUnread {
        template: Pid,
        pid: Pid,
        source: std::io::Error,
    },
    /// No pidfd could be opened for the template's child `pid` (Linux; macOS opens none), so it
    /// was killed: a worker the server cannot signal safely is not taken.
    #[error("opening a pidfd for pid {pid}, which template pid {template} forked, failed, so it was killed: {source}")]
    PidfdUnopened {
        template: Pid,
        pid: Pid,
        source: std::io::Error,
    },
    /// The blocking task that asked for the fork ended without its answer (it panicked, or
    /// the runtime was shutting down).
    #[error("the fork task ended without an answer: {0}")]
    Task(#[from] tokio::task::JoinError),
}

/// The process that forks the lane workers, and the server's side of the socket to it.
pub(super) struct Template {
    pid: Pid,
    /// Held by whoever reads or writes the socket: a fork exchange for as long as it waits for
    /// its answer, a look for exit reports only when nobody holds it (it never waits for it).
    pub(crate) link: Mutex<TemplateLink>,
}

/// How many fds a [`TemplateCommand::Lane`] frame carries: the shared memory and the lane's
/// wake-pipe write end.
const LANE_FDS: u32 = 2;

impl Template {
    /// Sends the template its setup: what a worker imports and how a lane's channel is laid out.
    pub(super) fn send_setup(&self, message: CommandBody<'_>) -> Result<(), TemplateSendError> {
        self.link.lock().send(&TemplateCommand::Setup(message))
    }

    /// Sends the template lane `lane`'s channel for this run: its shared memory and the write
    /// end of its wake pipe, which the template hands to each worker it starts for the
    /// lane, in place of the last run's.
    pub(crate) fn send_lane(
        &self,
        lane: LaneIndex,
        fds: [BorrowedFd<'_>; LANE_FDS as usize],
        exits: &ExitReports,
    ) -> Result<(), ForkError> {
        let mut link = self.link.lock();
        if let Some(end) = link.ended() {
            return Err(fork_error(self.pid, end.clone()));
        }
        link.send(&TemplateCommand::Lane { lane, fds })
            .map_err(|cut| self.cut_short(&mut link, cut, exits))
    }

    /// Says what a command that did not reach the template met: a template that ended because it
    /// cannot start workers said why first, and that, not the send it made fail, is the cause.
    fn cut_short(&self, link: &mut TemplateLink, cut: TemplateSendError, exits: &ExitReports) -> ForkError {
        self.take_ready(link, exits);
        match link.ended() {
            Some(said @ (LinkEnd::ImportFailed(_) | LinkEnd::LaneFdsMissing { .. })) => {
                fork_error(self.pid, said.clone())
            }
            _ => ForkError::Send(cut),
        }
    }

    /// Tells the template the command line of the run whose workers it starts next: the workers
    /// of one app's runs come from one template, and each run's flag values are its own.
    pub(super) fn send_run(&self, command_line: CommandBody<'_>) -> Result<(), TemplateSendError> {
        self.link.lock().send(&TemplateCommand::Run(command_line))
    }

    /// Asks for a worker on `lane` and returns it once the template's answer to this
    /// request comes, taking the other reports that come before it (`exits` gets the exit
    /// reports). Fails when `within` passes first or when the connection ends.
    pub(crate) fn fork(
        &self,
        lane: LaneIndex,
        attempt: Attempt,
        within: Duration,
        exits: &ExitReports,
    ) -> Result<WorkerProcess, ForkError> {
        let template = self.pid;
        let mut link = self.link.lock();
        if let Some(end) = link.ended() {
            return Err(fork_error(template, end.clone()));
        }
        if let Err(cut) = link.send(&TemplateCommand::Start { lane, attempt }) {
            return Err(self.cut_short(&mut link, cut, exits));
        }
        let deadline = Instant::now() + within;
        loop {
            match link.next_report(exits.lanes(), Some(deadline)) {
                Ok(Some(TemplateReport::Forked {
                    attempt: answered,
                    result,
                    ..
                })) if answered == attempt => {
                    // What else is already there goes now, so nobody reads it later.
                    self.take_ready(&mut link, exits);
                    drop(link);
                    let pid = result.map_err(|errno| ForkError::Fork {
                        template,
                        source: errno.into(),
                    })?;
                    return self.child(WorkerTag { lane, attempt, pid });
                }
                Ok(Some(report)) => self.take(&mut link, report, exits),
                Ok(None) => {
                    link.give_up_answer();
                    return Err(ForkError::TimedOut {
                        template,
                        waited: within,
                    });
                }
                Err(end) => return Err(fork_error(template, end)),
            }
        }
    }

    /// Takes the reports already waiting on the socket, unless someone else is reading it. A
    /// connection that ended stays ended: the next fork request fails with how.
    fn read_ready(&self, exits: &ExitReports) {
        if let Some(mut link) = self.link.try_lock() {
            self.take_ready(&mut link, exits);
        }
    }

    pub(crate) fn take_ready(&self, link: &mut TemplateLink, exits: &ExitReports) {
        while let Ok(Some(report)) = link.next_report(exits.lanes(), None) {
            self.take(link, report, exits);
        }
    }

    /// Takes one report that is not the answer a fork request waits for. A template that cannot
    /// start workers says why as its last frame: that is how the connection ended, and every
    /// fork request from then on fails with it.
    fn take(&self, link: &mut TemplateLink, report: TemplateReport, exits: &ExitReports) {
        match report {
            TemplateReport::Exited { lane, pid, status } => exits.record(lane, pid, status),
            TemplateReport::Forked { lane, attempt, result } => {
                link.late_answer_came();
                self.end_late(lane, attempt.get(), result);
            }
            TemplateReport::ImportFailed(traceback) => link.end(LinkEnd::ImportFailed(traceback)),
            TemplateReport::LaneFdsMissing { lane, got } => link.end(LinkEnd::LaneFdsMissing { lane, got }),
        }
    }

    /// Takes `pid`, which the template answered to the fork request `tag`, as a worker once
    /// the kernel says the template is its parent.
    fn child(&self, tag: WorkerTag) -> Result<WorkerProcess, ForkError> {
        WorkerProcess::open(self.pid, tag)
    }

    /// Ends the worker a late answer names (a request whose spawn already gave up), when the
    /// template is its parent; a late fork failure only gets logged.
    fn end_late(&self, lane: LaneIndex, attempt: u32, result: Result<Pid, rustix::io::Errno>) {
        let template = self.pid;
        let worker = result
            .map_err(|errno| ForkError::Fork {
                template,
                source: errno.into(),
            })
            .and_then(|pid| {
                self.child(WorkerTag {
                    lane,
                    attempt: Attempt::new(attempt),
                    pid,
                })
            });
        match worker {
            Ok(worker) => {
                tracing::warn!(
                    target: "mipyre::server",
                    lane = lane.0,
                    attempt,
                    pid = worker.pid().as_raw(),
                    template = template.as_raw(),
                    "the template answered a fork request after its deadline: its worker is killed"
                );
                worker.kill();
            }
            Err(error) => tracing::warn!(
                target: "mipyre::server",
                lane = lane.0,
                attempt,
                template = template.as_raw(),
                %error,
                "the template answered a fork request after its deadline with no worker of its own"
            ),
        }
    }
}

/// Says why a run's start failed at lane `lane`'s fork (or its lane frame): the template's own
/// report when it could not start workers, else the fork's error.
pub(crate) fn fork_failed(lane: LaneIndex, fork: ForkError) -> LaneStartError {
    match fork {
        ForkError::ImportFailed { traceback, .. } => LaneStartError::TemplateFailed {
            lane,
            failure: traceback,
        },
        ForkError::LaneFdsMissing { lane, got, .. } => LaneStartError::LaneFds {
            lane,
            got,
            want: LANE_FDS,
        },
        source => LaneStartError::Fork { lane, source },
    }
}

/// What a fork request says when the connection to the template ended.
fn fork_error(template: Pid, end: LinkEnd) -> ForkError {
    match end {
        LinkEnd::Closed => ForkError::TemplateClosed { template },
        LinkEnd::RecvFailed(errno) => ForkError::Receive {
            template,
            source: errno.into(),
        },
        end @ LinkEnd::BadFrame { .. } => ForkError::ProtocolViolated { template, end },
        LinkEnd::ImportFailed(traceback) => ForkError::ImportFailed { template, traceback },
        LinkEnd::LaneFdsMissing { lane, got } => ForkError::LaneFdsMissing { template, lane, got },
    }
}

/// How long a fork request waits for the template's answer, which comes once one `fork` or
/// `posix_spawn` returns. It only guards against a template that is alive but stuck (a dead one
/// closes its socket), while every other lane's fork request waits behind it. A fork's time
/// grows with the template's heap: the slowest of 500 forks of an 8 GiB heap took 279 ms on
/// Linux, about 35 ms per GiB, so 3 s covers a heap as large as a 64 GiB machine's memory.
pub(crate) const FORK_ANSWER_TIMEOUT: Duration = Duration::from_secs(3);

impl ServerLanes {
    /// Takes `pid` as the template, `sock` as the socket to it. The lanes have one template: a
    /// second is refused, its pid given back (its socket is closed).
    pub fn attach_template(&self, pid: Pid, sock: OwnedFd) -> Result<(), Pid> {
        self.template
            .set(Template {
                pid,
                link: Mutex::new(TemplateLink::new(pid, sock)),
            })
            .map_err(|refused| refused.pid)
    }

    pub fn template_pid(&self) -> Option<Pid> {
        self.template.get().map(|t| t.pid)
    }

    pub(super) fn template(&self) -> Option<&Template> {
        self.template.get()
    }

    /// Sends the template `message`: what a worker imports and how a lane's channel is laid out. Each run then sends
    /// each lane's channel.
    ///
    /// # Errors
    ///
    /// Refuses a message longer than a template frame holds before anything is sent, and fails when the lanes have no
    /// template yet or the message does not reach it.
    pub fn setup_template(&self, message: &[u8]) -> Result<(), TemplateSetupError> {
        let body = CommandBody::new(message).map_err(|e| TemplateSetupError::TooLong { bytes: e.bytes })?;
        let template = self.template().ok_or(TemplateSetupError::NoTemplate)?;
        Ok(template.send_setup(body)?)
    }

    /// Tells the template `command_line`, the one of the run whose workers it starts next.
    ///
    /// # Errors
    ///
    /// Refuses a command line longer than a template frame holds before anything is sent, and fails when the lanes
    /// have no template yet or the command line does not reach it.
    pub fn tell_template_run(&self, command_line: &str) -> Result<(), TemplateRunError> {
        let body = CommandBody::new(command_line.as_bytes())
            .map_err(|e| TemplateRunError::CommandLineTooLong { bytes: e.bytes })?;
        let template = self.template().ok_or(TemplateRunError::NoTemplate)?;
        Ok(template.send_run(body)?)
    }

    /// Takes the template's reports already waiting on its socket, unless a fork exchange is
    /// reading it (and takes them itself).
    pub(crate) fn read_template_reports(&self) {
        if let Some(template) = self.template.get() {
            template.read_ready(&self.exits);
        }
    }
}
