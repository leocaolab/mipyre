//! Writes the log to stderr from one thread and lets `flush_logs` wait until earlier lines are out.
//!
//! Every formatted line goes into one FIFO queue, so a line reaches stderr some time after the
//! call that logged it. A fence on the same queue passes after everything sent before it, so
//! whoever waits on it knows those lines are out. A full queue drops a line, because a log never
//! holds a request up, while a fence waits for room. A batch stderr refuses (EPIPE, a closed or
//! full disk) is dropped too. Dropped lines are counted by cause, and the writer warns of each
//! cause at most once a minute, a refusal with stderr's own error. The queue outlives each writer thread: the thread is
//! stopped before a fork and another started after, and a line queued in between is written by
//! the next one.

use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use crate::sync::{AtomicU64, Condvar, Mutex, Ordering};
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError};
use mipyre_util::throttle::{ThrottlePeriod, ThrottleState};

/// The channel holds this many lines before a new one is dropped, as tracing-appender did.
const CHANNEL_LINES: usize = 128_000;

/// The writer gathers lines up to this many bytes, the default size of a Linux pipe's buffer,
/// before one write.
const BATCH_BYTES: usize = 64 * 1024;

const DROP_WARNING_PERIOD: ThrottlePeriod = ThrottlePeriod::new(Duration::from_secs(60));

enum Message {
    Line(Vec<u8>),
    /// A fence passes once everything before it is written and flushed.
    Fence(u64),
    /// Ends the writer thread once everything before it is written and flushed.
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the log writer thread has stopped; lines logged before the flush may not have been written")]
pub(crate) struct FenceError;

/// The fmt layer clones this for every event and writes one `Line` through it.
#[derive(Clone)]
pub(super) struct LogChannel {
    tx: Sender<Message>,
    ledger: Arc<Ledger>,
}

/// What the loggers and the writer thread of one queue both keep. It is one allocation so that
/// cloning a channel for each event bumps one count.
#[derive(Default)]
struct Ledger {
    fences: Fences,
    queue_full: AtomicU64,
    write_failed: AtomicU64,
}

/// The lines the log dropped so far, by cause.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LogDrops {
    /// A line counts here when the queue was full because the writer could not keep up.
    pub(crate) queue_full: u64,
    /// A line counts here when stderr refused the batch it was in; a batch refused part way is
    /// counted whole.
    pub(crate) write_failed: u64,
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
    fn writer_started(&self) {
        self.state.lock().writer_stopped = false;
    }

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

/// The log's one queue, which every writer thread reads in turn.
pub(super) struct LogQueue {
    channel: LogChannel,
    rx: Receiver<Message>,
}

impl LogQueue {
    pub(super) fn new() -> Self {
        let (tx, rx) = crossbeam_channel::bounded(CHANNEL_LINES);
        LogQueue {
            channel: LogChannel {
                tx,
                ledger: Arc::new(Ledger::default()),
            },
            rx,
        }
    }

    pub(super) fn channel(&self) -> &LogChannel {
        &self.channel
    }

    /// Starts a writer thread that writes this queue to `out`, and returns once it runs: the
    /// process's threads (`/proc/self/task`, which a fork checks and tests read) list it by its
    /// name from then on.
    pub(super) fn start_writer<W: Write + Send + 'static>(&self, out: W) -> io::Result<LogWriter> {
        let rx = self.rx.clone();
        let ledger = Arc::clone(&self.channel.ledger);
        ledger.fences.writer_started();
        let running = Arc::new(std::sync::Barrier::new(2));
        let started = Arc::clone(&running);
        let thread = std::thread::Builder::new()
            .name(WRITER_THREAD.to_string())
            .spawn(move || {
                started.wait();
                write_out(rx, out, &ledger);
            })?;
        running.wait();
        Ok(LogWriter {
            tx: self.channel.tx.clone(),
            thread,
        })
    }
}

/// The thread that writes the log, one at a time per queue.
pub(super) struct LogWriter {
    tx: Sender<Message>,
    thread: std::thread::JoinHandle<()>,
}

impl LogWriter {
    /// Writes out every line queued before the call, then ends the thread and waits for it. A line
    /// queued after it waits for the next writer, so the caller first points the log elsewhere.
    ///
    /// # Errors
    ///
    /// Fails when the thread had already stopped (it panicked on a line).
    pub(super) fn stop(self) -> Result<(), FenceError> {
        let sent = self.tx.send(Message::Stop);
        let joined = self.thread.join();
        match (sent, joined) {
            (Ok(()), Ok(())) => Ok(()),
            _ => Err(FenceError),
        }
    }
}

/// The writer thread's name, by which a process's threads are told apart.
pub(crate) const WRITER_THREAD: &str = "mipyre-log";

/// Writes every message in order until a `Stop` (or the last sender is gone), gathering the lines
/// that are waiting into one write. A batch that cannot be written is counted as dropped and the
/// thread goes on with the next. However the thread ends, a panic included, a fence still
/// waited on fails instead of waiting forever.
fn write_out<W: Write>(rx: Receiver<Message>, mut out: W, ledger: &Ledger) {
    /// Marks the writer stopped when the thread leaves, unwinding or not.
    struct Stopped<'a>(&'a Fences);
    impl Drop for Stopped<'_> {
        fn drop(&mut self) {
            self.0.writer_stopped();
        }
    }
    let _stopped = Stopped(&ledger.fences);
    let mut batch = Batch::default();
    let mut notices = DropNotices::default();
    loop {
        let message = match rx.try_recv() {
            Ok(message) => message,
            Err(TryRecvError::Empty) => {
                notices.write(&mut out, &mut batch, ledger);
                notices.warn(ledger);
                match rx.recv() {
                    Ok(message) => message,
                    Err(_) => break,
                }
            }
            Err(TryRecvError::Disconnected) => break,
        };
        match message {
            Message::Line(line) => {
                batch.push(&line);
                if batch.bytes.len() >= BATCH_BYTES {
                    notices.write(&mut out, &mut batch, ledger);
                    notices.warn(ledger);
                }
            }
            Message::Fence(ticket) => {
                notices.write(&mut out, &mut batch, ledger);
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "as for a batch: the fence still passes, the lines before it were handed to stderr"
                )]
                let _ = out.flush();
                ledger.fences.pass(ticket);
            }
            Message::Stop => break,
        }
    }
    notices.write(&mut out, &mut batch, ledger);
    #[expect(
        clippy::let_underscore_must_use,
        reason = "as for a fence: the lines before it were handed to stderr"
    )]
    let _ = out.flush();
}

/// The lines gathered for one write.
struct Batch {
    bytes: Vec<u8>,
    lines: u64,
}

impl Default for Batch {
    fn default() -> Self {
        Batch {
            bytes: Vec::with_capacity(BATCH_BYTES),
            lines: 0,
        }
    }
}

impl Batch {
    fn push(&mut self, line: &[u8]) {
        self.bytes.extend_from_slice(line);
        self.lines += 1;
    }

    /// Writes the batch to `out` and empties it, and returns stderr's error when it refused the
    /// batch.
    fn write_to<W: Write>(&mut self, out: &mut W) -> Result<(), io::Error> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        let written = out.write_all(&self.bytes);
        self.bytes.clear();
        self.lines = 0;
        written
    }
}

/// What the writer has yet to warn of: the drops of each cause, and stderr's last refusal.
#[derive(Default)]
struct DropNotices {
    queue_full: DropNotice,
    write_failed: DropNotice,
    last_write_error: Option<io::Error>,
}

impl DropNotices {
    /// Writes `batch` to `out`; a batch stderr refuses is counted as dropped and its error kept
    /// for the warning, since the log is the only place it could be reported.
    fn write<W: Write>(&mut self, out: &mut W, batch: &mut Batch, ledger: &Ledger) {
        let lines = batch.lines;
        if let Err(error) = batch.write_to(out) {
            ledger.write_failed.fetch_add(lines, Ordering::Relaxed);
            self.last_write_error = Some(error);
        }
    }

    fn warn(&mut self, ledger: &Ledger) {
        let now = mipyre_util::clock::now();
        let queue_full = ledger.queue_full.load(Ordering::Relaxed);
        if let Some(since) = self.queue_full.admit(queue_full, now) {
            tracing::warn!(
                target: "mipyre::server",
                dropped = since,
                dropped_total = queue_full,
                "the log could not keep up and dropped lines; they are counted in log_lines_dropped"
            );
        }
        let write_failed = ledger.write_failed.load(Ordering::Relaxed);
        let Some(error) = &self.last_write_error else {
            return;
        };
        if let Some(since) = self.write_failed.admit(write_failed, now) {
            tracing::warn!(
                target: "mipyre::server",
                dropped = since,
                dropped_total = write_failed,
                %error,
                "the log could not write lines to stderr and dropped them; they are counted in log_lines_dropped"
            );
        }
    }
}

/// Decides when the writer warns of lines the full queue dropped: once new ones were dropped, and
/// at most once a period.
#[derive(Default)]
struct DropNotice {
    reported: u64,
    throttle: ThrottleState,
}

impl DropNotice {
    /// Returns how many lines were dropped since the last warning when one is due now, given the
    /// queue's total dropped so far.
    fn admit(&mut self, dropped: u64, now: Duration) -> Option<u64> {
        if dropped <= self.reported {
            return None;
        }
        self.throttle.admit(now, DROP_WARNING_PERIOD)?;
        let since = dropped - self.reported;
        self.reported = dropped;
        Some(since)
    }
}

impl LogChannel {
    /// Blocks until every line sent before the call is written and `out` is flushed, and fails
    /// once the writer thread has stopped. Tickets only grow and one is drawn (`SeqCst`) after
    /// everything the caller logged before, so a later ticket passing means an earlier one's lines
    /// are out too, whichever fence reached the channel first.
    pub(super) fn fence(&self) -> Result<(), FenceError> {
        let ticket = self.ledger.fences.next.fetch_add(1, Ordering::SeqCst) + 1;
        // A full channel drops lines but never a fence, so this waits for room.
        self.tx.send(Message::Fence(ticket)).map_err(|_| FenceError)?;
        self.ledger.fences.wait(ticket)
    }

    /// Returns how many lines the log dropped so far, by cause.
    pub(super) fn dropped(&self) -> LogDrops {
        LogDrops {
            queue_full: self.ledger.queue_full.load(Ordering::Relaxed),
            write_failed: self.ledger.write_failed.load(Ordering::Relaxed),
        }
    }
}

impl Write for LogChannel {
    /// Takes one formatted line and drops it, counted, when the channel is full, so a request
    /// never waits for the log.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.tx.try_send(Message::Line(buf.to_vec())) {
            Ok(()) => Ok(buf.len()),
            Err(TrySendError::Full(_)) => {
                self.ledger.queue_full.fetch_add(1, Ordering::Relaxed);
                Ok(buf.len())
            }
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
        let queue = LogQueue::new();
        let _writer = queue.start_writer(out.clone()).unwrap();
        let mut channel = queue.channel().clone();
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
        let queue = LogQueue::new();
        let _writer = queue.start_writer(out.clone()).unwrap();
        let mut channel = queue.channel().clone();
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
        let queue = LogQueue::new();
        let LogWriter { thread, .. } = queue.start_writer(Broken).unwrap();
        let mut channel = queue.channel().clone();
        channel.write_all(b"x\n").unwrap();
        assert!(thread.join().is_err(), "the writer thread panicked on the line");
        assert_eq!(channel.fence(), Err(FenceError));
    }

    #[test]
    fn a_stopped_writer_wrote_every_line_before_the_stop_and_the_next_writes_the_rest() {
        let out = Recorder::default();
        let queue = LogQueue::new();
        let writer = queue.start_writer(out.clone()).unwrap();
        let mut channel = queue.channel().clone();
        channel.write_all(b"before\n").unwrap();
        writer.stop().unwrap();
        assert_eq!(&*out.0.lock(), b"before\n");
        // No thread writes now: a fence fails, and a line waits in the queue for the next writer.
        assert_eq!(channel.fence(), Err(FenceError));
        channel.write_all(b"between\n").unwrap();
        let _next = queue.start_writer(out.clone()).unwrap();
        channel.fence().unwrap();
        assert_eq!(&*out.0.lock(), b"before\nbetween\n");
    }

    #[test]
    fn a_writer_that_already_stopped_fails_its_stop() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                panic!("the writer breaks");
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let queue = LogQueue::new();
        let writer = queue.start_writer(Broken).unwrap();
        queue.channel().clone().write_all(b"x\n").unwrap();
        assert_eq!(writer.stop(), Err(FenceError));
    }

    #[test]
    fn a_full_channel_drops_a_line_and_does_not_wait() {
        let (tx, _rx) = crossbeam_channel::bounded(1);
        let mut channel = LogChannel {
            tx,
            ledger: Arc::default(),
        };
        channel.write_all(b"kept\n").unwrap();
        channel.write_all(b"dropped\n").unwrap();
        assert_eq!(channel.dropped().queue_full, 1);
    }

    #[derive(Clone, Default)]
    struct Writes(Arc<Mutex<Vec<Vec<u8>>>>);

    impl Write for Writes {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().push(buf.to_vec());
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_lines_waiting_when_the_writer_wakes_go_out_in_one_write() {
        let out = Writes::default();
        let queue = LogQueue::new();
        let mut channel = queue.channel().clone();
        const LINES: usize = 1000;
        for i in 0..LINES {
            channel.write_all(format!("line {i}\n").as_bytes()).unwrap();
        }
        let _writer = queue.start_writer(out.clone()).unwrap();
        channel.fence().unwrap();
        let writes = out.0.lock();
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].split(|&b| b == b'\n').filter(|l| !l.is_empty()).count(),
            LINES
        );
    }

    #[test]
    fn a_batch_past_its_size_is_written_without_waiting_for_the_queue_to_empty() {
        let out = Writes::default();
        let queue = LogQueue::new();
        let mut channel = queue.channel().clone();
        let mut line = vec![b'x'; 1023];
        line.push(b'\n');
        const LINES: usize = 3 * BATCH_BYTES / 1024;
        for _ in 0..LINES {
            channel.write_all(&line).unwrap();
        }
        let _writer = queue.start_writer(out.clone()).unwrap();
        channel.fence().unwrap();
        let sizes: Vec<usize> = out.0.lock().iter().map(Vec::len).collect();
        assert_eq!(sizes, [BATCH_BYTES; 3]);
    }

    /// Holds every write until the test opens it, and says when the writer is inside one.
    #[derive(Clone)]
    struct Stuck {
        entered: crossbeam_channel::Sender<()>,
        open: crossbeam_channel::Receiver<()>,
        lines: Arc<AtomicU64>,
    }

    impl Write for Stuck {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            #[expect(clippy::let_underscore_must_use, reason = "the test may have stopped listening")]
            let _ = self.entered.try_send(());
            #[expect(
                clippy::let_underscore_must_use,
                reason = "the test opens the gate by dropping its end"
            )]
            let _ = self.open.recv();
            let lines = buf.iter().filter(|&&b| b == b'\n').count();
            self.lines.fetch_add(lines as u64, Ordering::Relaxed);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_writer_that_cannot_write_makes_the_queue_drop_and_count_without_holding_the_logger() {
        let (entered_tx, entered) = crossbeam_channel::bounded(1);
        let (open, open_rx) = crossbeam_channel::unbounded();
        let lines = Arc::new(AtomicU64::new(0));
        let out = Stuck {
            entered: entered_tx,
            open: open_rx,
            lines: Arc::clone(&lines),
        };
        let queue = LogQueue::new();
        let _writer = queue.start_writer(out).unwrap();
        let mut channel = queue.channel().clone();

        channel.write_all(b"first\n").unwrap();
        entered.recv().unwrap();
        const OVER: usize = 7;
        for _ in 0..CHANNEL_LINES + OVER {
            channel.write_all(b"line\n").unwrap();
        }
        assert_eq!(channel.dropped().queue_full, OVER as u64);

        drop(open);
        channel.fence().unwrap();
        let sent = 1 + CHANNEL_LINES + OVER;
        assert_eq!(lines.load(Ordering::Relaxed), (sent - OVER) as u64);
        assert_eq!(channel.dropped().queue_full, OVER as u64);
    }

    /// Refuses its first `refusals` writes as a closed pipe does, then takes every write.
    #[derive(Clone)]
    struct PipeWithoutReader {
        refusals: Arc<AtomicU64>,
        taken: Writes,
    }

    impl Write for PipeWithoutReader {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.refusals.load(Ordering::Relaxed) > 0 {
                self.refusals.fetch_sub(1, Ordering::Relaxed);
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
            self.taken.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_batch_stderr_refuses_is_counted_line_by_line_and_the_next_batch_is_written() {
        let out = PipeWithoutReader {
            refusals: Arc::new(AtomicU64::new(1)),
            taken: Writes::default(),
        };
        let queue = LogQueue::new();
        let mut channel = queue.channel().clone();
        const LINES: usize = 5;
        for _ in 0..LINES {
            channel.write_all(b"refused\n").unwrap();
        }
        let _writer = queue.start_writer(out.clone()).unwrap();
        channel.fence().unwrap();
        assert_eq!(
            channel.dropped(),
            LogDrops {
                queue_full: 0,
                write_failed: LINES as u64
            }
        );

        channel.write_all(b"taken\n").unwrap();
        channel.fence().unwrap();
        assert_eq!(*out.taken.0.lock(), [b"taken\n".to_vec()]);
        assert_eq!(channel.dropped().write_failed, LINES as u64);
    }

    #[test]
    fn a_refused_batch_is_warned_of_with_stderrs_own_error() {
        let ledger = Ledger::default();
        let mut notices = DropNotices::default();
        let mut batch = Batch::default();
        batch.push(b"a\n");
        batch.push(b"b\n");
        let mut out = PipeWithoutReader {
            refusals: Arc::new(AtomicU64::new(1)),
            taken: Writes::default(),
        };
        notices.write(&mut out, &mut batch, &ledger);
        assert_eq!(ledger.write_failed.load(Ordering::Relaxed), 2);

        let log = mipyre_runtime::testkit::captured_log(|| notices.warn(&ledger));
        assert!(log.contains("could not write lines to stderr"), "{log}");
        assert!(
            log.contains(&io::Error::from(io::ErrorKind::BrokenPipe).to_string()),
            "{log}"
        );
        assert!(log.contains("\"dropped\":2"), "{log}");
        assert!(!log.contains("could not keep up"), "{log}");
    }

    #[test]
    fn a_written_batch_is_neither_counted_nor_warned_of() {
        let ledger = Ledger::default();
        let mut notices = DropNotices::default();
        let mut batch = Batch::default();
        batch.push(b"a\n");
        notices.write(&mut Writes::default(), &mut batch, &ledger);
        assert_eq!(ledger.write_failed.load(Ordering::Relaxed), 0);
        let log = mipyre_runtime::testkit::captured_log(|| notices.warn(&ledger));
        assert!(log.is_empty(), "{log}");
    }

    #[test]
    fn a_drop_is_warned_of_once_a_period_with_every_line_dropped_since_the_last_warning() {
        let at = Duration::from_secs;
        let mut notice = DropNotice::default();
        assert_eq!(notice.admit(0, at(100)), None);
        assert_eq!(notice.admit(5, at(100)), Some(5));
        assert_eq!(notice.admit(9, at(159)), None);
        assert_eq!(notice.admit(12, at(160)), Some(7));
        assert_eq!(notice.admit(12, at(500)), None);
    }
}
