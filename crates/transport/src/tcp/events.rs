//! Counts what TCP gave up on, per kind, and throttles each kind's log line to one a minute.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpEvent {
    /// An accept failed for a reason other than resource exhaustion or the peer going away.
    AcceptFailed,
    /// An accept failed for lack of file descriptors, sockets or kernel buffers.
    AcceptExhausted,
    /// The peer reset or aborted a connection while it waited in the accept queue.
    PeerGoneInQueue,
    /// The connection keeps delayed ACKs.
    QuickackFailed,
    /// The listener goes without the cold-connect mitigation.
    DeferAcceptFailed,
    /// The connection keeps Nagle's algorithm.
    NodelayFailed,
}

const LOG_PERIOD: Duration = Duration::from_secs(60);

/// Holds a value alone on a 128-byte line, which covers x86's paired 64-byte lines and Apple's
/// line size, so one kind's writes on one core do not stall another kind's on another. It is
/// `mipyre_cpu::CacheLine` again, which this package may not depend on; where the one definition
/// lives is b/633's question, and until then each events module pads its own.
#[repr(align(128))]
struct OwnLine<T>(T);

/// Holds one value per kind, so a kind added without a field does not compile. The counts, which
/// telemetry reads, and the log throttles, which are logging policy, are two of these, kept apart.
struct TcpEventTable<T> {
    accept_failed: T,
    accept_exhausted: T,
    peer_gone_in_queue: T,
    quickack_failed: T,
    defer_accept_failed: T,
    nodelay_failed: T,
}

impl<T> TcpEventTable<T> {
    fn of(&self, event: TcpEvent) -> &T {
        match event {
            TcpEvent::AcceptFailed => &self.accept_failed,
            TcpEvent::AcceptExhausted => &self.accept_exhausted,
            TcpEvent::PeerGoneInQueue => &self.peer_gone_in_queue,
            TcpEvent::QuickackFailed => &self.quickack_failed,
            TcpEvent::DeferAcceptFailed => &self.defer_accept_failed,
            TcpEvent::NodelayFailed => &self.nodelay_failed,
        }
    }
}

impl TcpEventTable<OwnLine<AtomicU64>> {
    const fn counts() -> Self {
        TcpEventTable {
            accept_failed: OwnLine(AtomicU64::new(0)),
            accept_exhausted: OwnLine(AtomicU64::new(0)),
            peer_gone_in_queue: OwnLine(AtomicU64::new(0)),
            quickack_failed: OwnLine(AtomicU64::new(0)),
            defer_accept_failed: OwnLine(AtomicU64::new(0)),
            nodelay_failed: OwnLine(AtomicU64::new(0)),
        }
    }
}

impl TcpEventTable<OwnLine<LogThrottle>> {
    const fn throttles() -> Self {
        TcpEventTable {
            accept_failed: OwnLine(LogThrottle::new(LOG_PERIOD)),
            accept_exhausted: OwnLine(LogThrottle::new(LOG_PERIOD)),
            peer_gone_in_queue: OwnLine(LogThrottle::new(LOG_PERIOD)),
            quickack_failed: OwnLine(LogThrottle::new(LOG_PERIOD)),
            defer_accept_failed: OwnLine(LogThrottle::new(LOG_PERIOD)),
            nodelay_failed: OwnLine(LogThrottle::new(LOG_PERIOD)),
        }
    }
}

static TCP_EVENT_COUNTS: TcpEventTable<OwnLine<AtomicU64>> = TcpEventTable::counts();
static TCP_EVENT_THROTTLES: TcpEventTable<OwnLine<LogThrottle>> = TcpEventTable::throttles();

/// Counts one `event` without deciding its log line.
pub fn count(event: TcpEvent) {
    TCP_EVENT_COUNTS.of(event).0.fetch_add(1, Ordering::Relaxed);
}

/// Counts one `event` and returns, once a minute per kind, how many lines were held back since
/// the last one went out.
pub fn degrade(event: TcpEvent) -> Option<u64> {
    count(event);
    admit(event)
}

/// Decides the log line as [`degrade`] does, for an event already counted.
pub fn admit(event: TcpEvent) -> Option<u64> {
    TCP_EVENT_THROTTLES.of(event).0.admit(clock::now())
}

pub fn total(event: TcpEvent) -> u64 {
    TCP_EVENT_COUNTS.of(event).0.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Holds a fresh count and throttle per kind, decided at the time a test hands in, as the
    /// statics are by [`degrade`].
    struct Tables {
        counts: TcpEventTable<OwnLine<AtomicU64>>,
        throttles: TcpEventTable<OwnLine<LogThrottle>>,
    }

    impl Tables {
        fn new() -> Self {
            Tables {
                counts: TcpEventTable::counts(),
                throttles: TcpEventTable::throttles(),
            }
        }

        fn degrade(&self, event: TcpEvent, now: Duration) -> Option<u64> {
            self.counts.of(event).0.fetch_add(1, Ordering::Relaxed);
            self.throttles.of(event).0.admit(now)
        }

        fn total(&self, event: TcpEvent) -> u64 {
            self.counts.of(event).0.load(Ordering::Relaxed)
        }
    }

    const ALL: [TcpEvent; 6] = [
        TcpEvent::AcceptFailed,
        TcpEvent::AcceptExhausted,
        TcpEvent::PeerGoneInQueue,
        TcpEvent::QuickackFailed,
        TcpEvent::DeferAcceptFailed,
        TcpEvent::NodelayFailed,
    ];

    fn at(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    #[test]
    fn each_event_is_counted_and_one_line_a_period_goes_out() {
        let counts = Tables::new();
        let event = TcpEvent::DeferAcceptFailed;
        let lines = (0..64).filter(|_| counts.degrade(event, at(0)).is_some()).count();
        assert_eq!((counts.total(event), lines), (64, 1));
        let just_before = LOG_PERIOD - Duration::from_nanos(1);
        assert_eq!(counts.degrade(event, just_before), None, "the period is not over");
        assert_eq!(
            counts.degrade(event, LOG_PERIOD),
            Some(64),
            "the next line carries the 64 held"
        );
        assert_eq!(counts.total(event), 66);
    }

    #[test]
    fn kinds_count_and_throttle_apart() {
        let counts = Tables::new();
        for event in ALL {
            assert_eq!(counts.degrade(event, at(0)), Some(0), "{event:?}");
        }
        assert_eq!(counts.degrade(TcpEvent::NodelayFailed, at(1)), None);
        assert_eq!(counts.total(TcpEvent::NodelayFailed), 2);
        assert_eq!(counts.total(TcpEvent::QuickackFailed), 1);
    }
}
