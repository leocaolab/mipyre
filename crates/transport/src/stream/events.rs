//! Counts what happened to byte streams, per kind, and throttles each kind's log line to one a
//! minute.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// The peer reset, aborted or closed a stream that was being served.
    PeerGone,
    /// The peer stopped reading, so the stream is closed.
    WriteStalled,
    /// An accepted connection could not be registered with the runtime of the thread that serves
    /// it, so it is dropped.
    RegisterFailed,
    /// The peer kept sending through a lingering close's whole limit, so the stream was closed
    /// while it still sent.
    LingerCutShort,
}

const LOG_PERIOD: Duration = Duration::from_secs(60);

/// Holds a value alone on a 128-byte line, which covers x86's paired 64-byte lines and Apple's
/// line size, so one kind's writes on one core do not stall another kind's on another. It is
/// `mipyre_cpu::CacheLine` again, which this package may not depend on, so each events module pads
/// its own.
#[repr(align(128))]
struct OwnLine<T>(T);

/// Holds one value per kind, so a kind added without a field does not compile. The counts, which
/// telemetry reads, and the log throttles, which are logging policy, are two of these, kept apart.
struct StreamEventTable<T> {
    peer_gone: T,
    write_stalled: T,
    register_failed: T,
    linger_cut_short: T,
}

impl<T> StreamEventTable<T> {
    fn of(&self, event: StreamEvent) -> &T {
        match event {
            StreamEvent::PeerGone => &self.peer_gone,
            StreamEvent::WriteStalled => &self.write_stalled,
            StreamEvent::RegisterFailed => &self.register_failed,
            StreamEvent::LingerCutShort => &self.linger_cut_short,
        }
    }
}

impl StreamEventTable<OwnLine<AtomicU64>> {
    const fn counts() -> Self {
        StreamEventTable {
            peer_gone: OwnLine(AtomicU64::new(0)),
            write_stalled: OwnLine(AtomicU64::new(0)),
            register_failed: OwnLine(AtomicU64::new(0)),
            linger_cut_short: OwnLine(AtomicU64::new(0)),
        }
    }
}

impl StreamEventTable<OwnLine<LogThrottle>> {
    const fn throttles() -> Self {
        StreamEventTable {
            peer_gone: OwnLine(LogThrottle::new(LOG_PERIOD)),
            write_stalled: OwnLine(LogThrottle::new(LOG_PERIOD)),
            register_failed: OwnLine(LogThrottle::new(LOG_PERIOD)),
            linger_cut_short: OwnLine(LogThrottle::new(LOG_PERIOD)),
        }
    }
}

static STREAM_EVENT_COUNTS: StreamEventTable<OwnLine<AtomicU64>> = StreamEventTable::counts();
static STREAM_EVENT_THROTTLES: StreamEventTable<OwnLine<LogThrottle>> = StreamEventTable::throttles();

/// Counts one `event` without deciding its log line.
pub fn count(event: StreamEvent) {
    STREAM_EVENT_COUNTS.of(event).0.fetch_add(1, Ordering::Relaxed);
}

/// Counts one `event` and returns, once a minute per kind, how many lines were held back since
/// the last one went out.
pub fn degrade(event: StreamEvent) -> Option<u64> {
    count(event);
    admit(event)
}

/// Decides the log line as [`degrade`] does, for an event already counted.
pub fn admit(event: StreamEvent) -> Option<u64> {
    STREAM_EVENT_THROTTLES.of(event).0.admit(clock::now())
}

pub fn total(event: StreamEvent) -> u64 {
    STREAM_EVENT_COUNTS.of(event).0.load(Ordering::Relaxed)
}
