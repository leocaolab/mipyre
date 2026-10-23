//! Sends a file the cache does not keep from disk, a chunk at a time as the connection takes it,
//! so an answer holds at most one chunk of it however large the file and however slow the client.

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use std::fs::File;
use std::future::Future;
use std::hash::{DefaultHasher, Hasher};
use std::io::{self, Read};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::{JoinError, JoinHandle};

use crate::sync::{AtomicU64, Ordering};
use mipyre_util::clock;

use super::degraded::{self, StaticDegradation};
use super::keyed_log_throttle::HeldLines;

/// Reads a file in chunks of 64 KiB, the write-coalescing limit, so a chunk goes to the socket
/// without being copied into the coalescing buffer.
pub(super) const CHUNK: usize = crate::plan::coalesce::COALESCE_LIMIT;

/// Counts the bytes read from disk by answers being sent and not yet written out or dropped.
#[derive(Debug)]
pub(super) struct HeldBytes(AtomicU64);

impl HeldBytes {
    pub(super) fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub(super) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Holds a chunk's bytes inside the `Bytes` handed to the connection, and hands them back to the
/// count when the connection drops the last reference.
struct HeldChunk {
    bytes: Vec<u8>,
    held: &'static HeldBytes,
}

impl HeldChunk {
    fn into_bytes(bytes: Vec<u8>, held: &'static HeldBytes) -> Bytes {
        held.0.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Bytes::from_owner(Self { bytes, held })
    }
}

impl AsRef<[u8]> for HeldChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for HeldChunk {
    fn drop(&mut self) {
        self.held.0.fetch_sub(self.bytes.len() as u64, Ordering::Relaxed);
    }
}

/// Hashes a representation's bytes into the digest its `ETag` is made of; feeding the bytes in
/// pieces gives the digest of the whole.
#[derive(Clone, Default)]
pub(super) struct Digest(DefaultHasher);

impl Digest {
    pub(super) fn of(bytes: &[u8]) -> u64 {
        let mut digest = Self::default();
        digest.feed(bytes);
        digest.finish()
    }

    pub(super) fn feed(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }

    pub(super) fn finish(&self) -> u64 {
        self.0.finish()
    }
}

/// Follows what was read against the length and digest the answer's head announced.
struct SendCheck {
    len: u64,
    sent: u64,
    announced: u64,
    digest: Digest,
}

/// What a chunk read from disk means for the answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChunkVerdict {
    /// More of the file is to come.
    More,
    /// It ends the file, and the whole file has the announced digest.
    Last,
    /// It ends the file, and the bytes are not the ones the head announced.
    Changed,
    /// The file ended before the announced length.
    EndedEarly,
}

impl SendCheck {
    /// Returns how many bytes the next read asks for; it is called only while bytes remain.
    fn next_read(&self) -> u64 {
        (self.len - self.sent).min(CHUNK as u64)
    }

    /// Takes a chunk of at most [`Self::next_read`] bytes. The last chunk is judged before it is
    /// sent, so a file changed while it was sent never reaches the client whole.
    fn on_chunk(&mut self, chunk: &[u8]) -> ChunkVerdict {
        if chunk.is_empty() {
            return ChunkVerdict::EndedEarly;
        }
        self.digest.feed(chunk);
        self.sent += chunk.len() as u64;
        if self.sent < self.len {
            ChunkVerdict::More
        } else if self.digest.finish() == self.announced {
            ChunkVerdict::Last
        } else {
            ChunkVerdict::Changed
        }
    }
}

/// Why an answer was cut off before its last byte.
#[derive(Debug, thiserror::Error)]
enum ChunkError {
    #[error("the file changed while it was sent")]
    Changed,
    #[error("the file ended before the length its answer announced")]
    EndedEarly,
    #[error(transparent)]
    Unreadable(io::Error),
    #[error("the read did not finish: {0}")]
    Unfinished(JoinError),
}

/// Ends the connection, so the client sees a body shorter than its `Content-Length`, never wrong
/// bytes passed off as the file.
#[derive(Debug, thiserror::Error)]
#[error("the answer of {path:?} was cut off after {sent} of {len} bytes: {cause}")]
pub struct FileSendError {
    path: PathBuf,
    sent: u64,
    len: u64,
    #[source]
    cause: ChunkError,
}

impl FileSendError {
    /// Counts the cut answer and logs it, throttled per file.
    pub fn report(&self) {
        degraded::count(StaticDegradation::AnswerCut);
        match super::CUT_THROTTLE.admit(self.path.as_path(), clock::now()) {
            Some(HeldLines::OfKey(held)) => tracing::warn!(error = %self, held, "static answer cut off"),
            Some(HeldLines::OfUnrememberedKeys(held)) => {
                tracing::warn!(error = %self, held_among_other_paths = held, "static answer cut off")
            }
            None => tracing::debug!(error = %self, "static answer cut off"),
        }
    }
}

/// The file between reads, or the read the blocking pool is doing; a read takes the file and
/// hands it back.
enum Disk {
    Idle(File),
    Reading(JoinHandle<(File, io::Result<Vec<u8>>)>),
    Done,
}

/// Streams a file of known length and digest from disk, reading the next chunk only when the
/// connection asks for it, so a client that reads slowly makes it read the disk slowly.
pub struct FileBody(Box<Transfer>);

struct Transfer {
    path: PathBuf,
    disk: Disk,
    check: SendCheck,
    held: &'static HeldBytes,
}

impl FileBody {
    /// Sends `len` bytes of `file` from where it stands; they must hash to `announced`. `len` is
    /// not zero: an empty file is answered with an empty whole body.
    pub(super) fn new(file: File, path: PathBuf, len: u64, announced: u64, held: &'static HeldBytes) -> Self {
        Self(Box::new(Transfer {
            path,
            disk: Disk::Idle(file),
            check: SendCheck {
                len,
                sent: 0,
                announced,
                digest: Digest::default(),
            },
            held,
        }))
    }
}

/// Reads up to `want` bytes on the blocking pool; fewer means the file ended.
fn read_chunk(file: File, want: u64) -> (File, io::Result<Vec<u8>>) {
    let mut chunk = Vec::with_capacity(want as usize);
    let read = (&file).take(want).read_to_end(&mut chunk);
    (file, read.map(|_| chunk))
}

impl Transfer {
    /// Ends the answer; the state is already `Done`, since every poll takes the disk out first.
    fn cut(&mut self, cause: ChunkError) -> FileSendError {
        FileSendError {
            path: std::mem::take(&mut self.path),
            sent: self.check.sent,
            len: self.check.len,
            cause,
        }
    }
}

impl Body for FileBody {
    type Data = Bytes;
    type Error = FileSendError;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, FileSendError>>> {
        let transfer = &mut *self.get_mut().0;
        loop {
            match std::mem::replace(&mut transfer.disk, Disk::Done) {
                Disk::Done => return Poll::Ready(None),
                Disk::Idle(file) => {
                    let want = transfer.check.next_read();
                    transfer.disk = Disk::Reading(tokio::task::spawn_blocking(move || read_chunk(file, want)));
                }
                Disk::Reading(mut read) => {
                    let Poll::Ready(finished) = Pin::new(&mut read).poll(cx) else {
                        transfer.disk = Disk::Reading(read);
                        return Poll::Pending;
                    };
                    let (file, chunk) = match finished {
                        Ok((file, Ok(chunk))) => (file, chunk),
                        Ok((_, Err(source))) => {
                            return Poll::Ready(Some(Err(transfer.cut(ChunkError::Unreadable(source)))))
                        }
                        Err(join) => return Poll::Ready(Some(Err(transfer.cut(ChunkError::Unfinished(join))))),
                    };
                    match transfer.check.on_chunk(&chunk) {
                        ChunkVerdict::More => transfer.disk = Disk::Idle(file),
                        ChunkVerdict::Last => {}
                        ChunkVerdict::Changed => return Poll::Ready(Some(Err(transfer.cut(ChunkError::Changed)))),
                        ChunkVerdict::EndedEarly => {
                            return Poll::Ready(Some(Err(transfer.cut(ChunkError::EndedEarly))))
                        }
                    }
                    return Poll::Ready(Some(Ok(Frame::data(HeldChunk::into_bytes(chunk, transfer.held)))));
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.0.disk, Disk::Done)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.check.len - self.0.check.sent)
    }
}

/// The length and digest of a file's bytes as one read from start to end saw them.
#[derive(Clone, Copy, Debug)]
pub(super) struct Fingerprint {
    pub(super) len: u64,
    pub(super) digest: u64,
}

/// Reads `file` from its start to its end with one buffer, at most `cap` bytes, and leaves it
/// rewound for sending; `None` means it grew past `cap`.
pub(super) fn fingerprint(mut file: &File, cap: u64) -> io::Result<Option<Fingerprint>> {
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Digest::default();
    let mut buf = vec![0u8; CHUNK];
    let mut len = 0u64;
    loop {
        let got = match file.read(&mut buf) {
            Ok(0) => break,
            Ok(got) => got,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        len += got as u64;
        if len > cap {
            return Ok(None);
        }
        digest.feed(&buf[..got]);
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(Some(Fingerprint {
        len,
        digest: digest.finish(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(len: u64, file: &[u8]) -> SendCheck {
        SendCheck {
            len,
            sent: 0,
            announced: Digest::of(file),
            digest: Digest::default(),
        }
    }

    #[test]
    fn a_digest_fed_in_pieces_is_the_digest_of_the_whole() {
        let whole: Vec<u8> = (0..=255u8).cycle().take(3 * CHUNK + 17).collect();
        let mut pieces = Digest::default();
        for piece in whole.chunks(CHUNK) {
            pieces.feed(piece);
        }
        assert_eq!(pieces.finish(), Digest::of(&whole));
    }

    #[test]
    fn the_announced_bytes_end_with_the_last_chunk() {
        let file = vec![1u8; CHUNK + 1];
        let mut check = check(file.len() as u64, &file);
        assert_eq!(check.next_read(), CHUNK as u64);
        assert_eq!(check.on_chunk(&file[..CHUNK]), ChunkVerdict::More);
        assert_eq!(check.next_read(), 1);
        assert_eq!(check.on_chunk(&file[CHUNK..]), ChunkVerdict::Last);
    }

    /// One byte changed in the first chunk is caught at the last one, before it is sent.
    #[test]
    fn a_file_changed_while_sent_is_caught_before_its_last_chunk_goes() {
        let file = vec![1u8; CHUNK + 1];
        let mut check = check(file.len() as u64, &file);
        let mut changed = file.clone();
        changed[0] = 2;
        assert_eq!(check.on_chunk(&changed[..CHUNK]), ChunkVerdict::More);
        assert_eq!(check.on_chunk(&changed[CHUNK..]), ChunkVerdict::Changed);
    }

    #[test]
    fn a_file_cut_short_ends_early() {
        let file = vec![1u8; 10];
        let mut check = check(10, &file);
        assert_eq!(check.on_chunk(&file[..9]), ChunkVerdict::More);
        assert_eq!(check.next_read(), 1);
        assert_eq!(check.on_chunk(&[]), ChunkVerdict::EndedEarly);
    }
}
