//! Counts the TLS handshakes that failed and throttles their log line to one a minute.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsEvent {
    HandshakeFailed,
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
struct TlsEventTable<T> {
    handshake_failed: T,
}

impl<T> TlsEventTable<T> {
    fn of(&self, event: TlsEvent) -> &T {
        match event {
            TlsEvent::HandshakeFailed => &self.handshake_failed,
        }
    }
}

impl TlsEventTable<OwnLine<AtomicU64>> {
    const fn counts() -> Self {
        TlsEventTable {
            handshake_failed: OwnLine(AtomicU64::new(0)),
        }
    }
}

impl TlsEventTable<OwnLine<LogThrottle>> {
    const fn throttles() -> Self {
        TlsEventTable {
            handshake_failed: OwnLine(LogThrottle::new(LOG_PERIOD)),
        }
    }
}

static TLS_EVENT_COUNTS: TlsEventTable<OwnLine<AtomicU64>> = TlsEventTable::counts();
static TLS_EVENT_THROTTLES: TlsEventTable<OwnLine<LogThrottle>> = TlsEventTable::throttles();

/// Counts one `event` without deciding its log line.
fn count(event: TlsEvent) {
    TLS_EVENT_COUNTS.of(event).0.fetch_add(1, Ordering::Relaxed);
}

/// Counts one `event` and returns, once a minute per kind, how many lines were held back since
/// the last one went out.
pub fn degrade(event: TlsEvent) -> Option<u64> {
    count(event);
    admit(event)
}

/// Decides the log line as [`degrade`] does, for an event already counted.
fn admit(event: TlsEvent) -> Option<u64> {
    TLS_EVENT_THROTTLES.of(event).0.admit(clock::now())
}

pub fn total(event: TlsEvent) -> u64 {
    TLS_EVENT_COUNTS.of(event).0.load(Ordering::Relaxed)
}
