//! Writes the log to stderr from one thread and lets `flush_logs` wait until earlier lines are out.
//!
//! Every formatted line goes into one FIFO channel, so a line reaches stderr some time after the
//! call that logged it. A fence on the same channel passes after everything sent before it, so
//! whoever waits on it knows those lines are out. A full channel drops a line, because a log never
//! holds a request up, while a fence waits for room.

use std::io::{self, Write};
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, TrySendError};
use crate::sync::{AtomicU64, Condvar, Mutex, Ordering};

/// The channel holds this many lines before a new one is dropped, as tracing-appender did.
const CHANNEL_LINES: usize = 128_000;

enum Message {
    Line(Vec<u8>),
    /// A fence passes once everything before it is written and flushed.
    Fence(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "the log writer thread has stopped; lines logged before the flush may not have been written"
)]
pub(crate) struct FenceError;

/// The fmt layer clones this for every event and writes one `Line` through it.
#[derive(Clone)]
pub(super) struct LogChannel {
    tx: Sender<Message>,
    fences: Arc<Fences>,
}

#[derive(Default)]
struct Fences {
    next: AtomicU64,
    state: Mutex<FenceProgress>,
    changed: Condvar,
}

#[derive(Default)]
struct FenceProgress {
    ticket: u64,
    writer_stopped: bool,
}

impl Fences {
    fn pass(&self, ticket: u64) {
        let mut state = self.state.lock();
        state.ticket = state.ticket.max(ticket);
        self.changed.notify_all();
    }

    fn writer_stopped(&self) {
        self.state.lock().writer_stopped = true;
        self.changed.notify_all();
    }

    /// Blocks until the writer thread has passed fence `ticket` or a later one, and fails once
    /// the thread has stopped.
    fn wait(&self, ticket: u64) -> Result<(), FenceError> {
        let mut state = self.state.lock();
        loop {
            if state.ticket >= ticket {
                return Ok(());
            }
            if state.writer_stopped {
                return Err(FenceError);
            }
            self.changed.wait(&mut state);
        }
    }
}

/// Starts a writer thread that writes to `out` and returns its channel and handle.
pub(super) fn log_channel<W: Write + Send + 'static>(
    out: W,
) -> io::Result<(LogChannel, std::thread::JoinHandle<()>)> {
    let (tx, rx) = crossbeam_channel::bounded(CHANNEL_LINES);
    let fences = Arc::new(Fences::default());
    let writer_fences = Arc::clone(&fences);
    let thread = std::thread::Builder::new()
        .name("mipyre-log".to_string())
        .spawn(move || write_out(rx, out, &writer_fences))?;
    Ok((LogChannel { tx, fences }, thread))
}

/// Writes every message in order until the last sender is gone. A line that cannot be written has
/// nowhere else to be reported, so the thread goes on with the next. However the thread ends, a
/// panic included, a fence still waited on fails instead of waiting forever.
fn write_out<W: Write>(rx: Receiver<Message>, mut out: W, fences: &Fences) {
    /// Marks the writer stopped when the thread leaves, unwinding or not.
    struct Stopped<'a>(&'a Fences);
    impl Drop for Stopped<'_> {
        fn drop(&mut self) {
            self.0.writer_stopped();
        }
    }
    let _stopped = Stopped(fences);
    for message in rx {
        match message {
            Message::Line(line) => {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "stderr refusing a line has no other place to be reported: the log is that place"
                )]
                let _ = out.write_all(&line);
            }
            Message::Fence(ticket) => {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "as for a line: the fence still passes, the lines before it were handed to stderr"
                )]
                let _ = out.flush();
                fences.pass(ticket);
            }
        }
    }
}

impl LogChannel {
    /// Blocks until every line sent before the call is written and `out` is flushed, and fails
    /// once the writer thread has stopped. Tickets only grow and one is drawn (`SeqCst`) after
    /// everything the caller logged before, so a later ticket passing means an earlier one's lines
    /// are out too, whichever fence reached the channel first.
    pub(super) fn fence(&self) -> Result<(), FenceError> {
        let ticket = self.fences.next.fetch_add(1, Ordering::SeqCst) + 1;
        // A full channel drops lines but never a fence, so this waits for room.
        self.tx.send(Message::Fence(ticket)).map_err(|_| FenceError)?;
        self.fences.wait(ticket)
    }
}

impl Write for LogChannel {
    /// Takes one formatted line and drops it when the channel is full, so a request never waits
    /// for the log.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.tx.try_send(Message::Line(buf.to_vec())) {
            Ok(()) | Err(TrySendError::Full(_)) => Ok(buf.len()),
            Err(TrySendError::Disconnected(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the log writer thread has stopped",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogChannel {
    type Writer = LogChannel;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<u8>>>);

    impl Write for Recorder {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_lines_before_a_fence_are_out_when_it_returns_and_it_is_not_written() {
        let out = Recorder::default();
        let (mut channel, _thread) = log_channel(out.clone()).unwrap();
        const LINES: usize = 1000;
        for i in 0..LINES {
            channel.write_all(format!("line {i}\n").as_bytes()).unwrap();
        }
        channel.fence().unwrap();
        let written = String::from_utf8(out.0.lock().clone()).unwrap();
        assert_eq!(written.lines().count(), LINES);
        assert!(written.ends_with(&format!("line {}\n", LINES - 1)));
    }

    #[test]
    fn a_line_that_looks_like_anything_is_written_as_it_is() {
        let out = Recorder::default();
        let (mut channel, _thread) = log_channel(out.clone()).unwrap();
        let odd = b"\0mipyre-log-fence\0\x07\0\0\0\0\0\0\0";
        channel.write_all(odd).unwrap();
        channel.fence().unwrap();
        assert_eq!(&*out.0.lock(), odd);
    }

    #[test]
    fn a_later_fence_covers_an_earlier_one_and_an_earlier_does_not_cover_a_later() {
        let fences = Fences::default();
        fences.pass(3);
        assert_eq!(fences.wait(2), Ok(()));
        assert_eq!(fences.wait(3), Ok(()));
        // A fence that arrives after a later one does not move it back.
        fences.pass(1);
        assert_eq!(fences.state.lock().ticket, 3);
    }

    /// The writer panics on a line here, and afterwards a line fails too.
    #[test]
    fn a_stopped_writer_fails_the_fence() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                panic!("the writer breaks");
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (mut channel, thread) = log_channel(Broken).unwrap();
        channel.write_all(b"x\n").unwrap();
        assert!(thread.join().is_err(), "the writer thread panicked on the line");
        assert_eq!(channel.fence(), Err(FenceError));
        assert_eq!(
            channel.write(b"x\n").unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn a_full_channel_drops_a_line_and_does_not_wait() {
        let (tx, _rx) = crossbeam_channel::bounded(1);
        let mut channel = LogChannel { tx, fences: Arc::default() };
        channel.write_all(b"kept\n").unwrap();
        channel.write_all(b"dropped\n").unwrap();
    }
}
