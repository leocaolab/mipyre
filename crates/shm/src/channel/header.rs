//! Lays out the channel's header page: the ring's counters and sleep flags, the report on the current start
//! attempt, and how the peer waited. Each word has one writer, named beside it.

use mipyre_cpu::CacheLine;

use crate::plan::geometry::HEADER_PAGE;
use crate::sync::{AtomicU32, AtomicU64, Ordering::Relaxed};

/// Bytes the note a peer that could not start leaves (its traceback) may take; the note follows the header in the
/// header page.
pub const NOTE_BYTES: u32 = 8 << 10;

/// The header page's words; the type is unnameable outside the crate. All of them are atomics, and zero is each one's starting value, so a fresh zeroed
/// memory holds a header without being written.
#[repr(C)]
pub struct ChannelHeader {
    /// Written by the host: one past the last request it published.
    pub(crate) req_head: CacheLine<AtomicU32>,
    /// Written by the peer: `k + 1` once it began request `k`.
    pub(crate) taken: CacheLine<AtomicU32>,
    /// Written by the peer, and by the host when it takes over from a dead one: one past the last answer published.
    pub(crate) resp_head: CacheLine<AtomicU32>,
    /// Written by the peer: 1 while it sleeps, or is about to, on `req_head`.
    pub(crate) peer_sleeping: CacheLine<AtomicU32>,
    /// Written by the host: 1 while it sleeps, or is about to, on `resp_head`.
    pub(crate) host_sleeping: CacheLine<AtomicU32>,
    pub(crate) start: StartReportArea,
    pub(crate) peer_waits: CacheLine<WaitCounts>,
}

/// Where the start note's bytes begin in the header page: right after the header, word-aligned.
pub(crate) const NOTE_AT: usize = std::mem::size_of::<ChannelHeader>().next_multiple_of(8);

const _: () = assert!(NOTE_AT + NOTE_BYTES as usize <= HEADER_PAGE);

/// The report on one start attempt: the host picks the attempt's number and clears the rest before it starts a
/// peer, and the peer of that attempt alone writes `state` (and on a failure first `note_len`).
#[repr(C)]
pub struct StartReportArea {
    pub(crate) attempt: CacheLine<AtomicU32>,
    pub(crate) state: CacheLine<AtomicU32>,
    pub(crate) note_len: CacheLine<AtomicU32>,
}

/// Counts how the peer waited for requests; it has one writer, and the host reads it only for stats.
#[repr(C)]
#[derive(Default)]
pub struct WaitCounts {
    /// One per request the peer did not find at once.
    pub(crate) waits: OwnCounter,
    pub(crate) spin_rounds: OwnCounter,
    pub(crate) parks: OwnCounter,
    pub(crate) wakes: OwnCounter,
}

/// What [`WaitCounts`] held when it was read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WaitTotals {
    pub waits: u64,
    pub spin_rounds: u64,
    pub parks: u64,
    pub wakes: u64,
}

/// Holds a counter with one writer, which adds with a plain load and store instead of a read-modify-write; others
/// only read it.
#[derive(Default)]
#[repr(transparent)]
pub struct OwnCounter(AtomicU64);

impl OwnCounter {
    /// Adds `n` with a plain load and store; only the counter's one writer calls it.
    #[inline]
    pub(crate) fn add(&self, n: u64) {
        self.0.store(self.0.load(Relaxed).wrapping_add(n), Relaxed);
    }

    pub fn read(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

impl WaitCounts {
    pub fn read(&self) -> WaitTotals {
        WaitTotals {
            waits: self.waits.read(),
            spin_rounds: self.spin_rounds.read(),
            parks: self.parks.read(),
            wakes: self.wakes.read(),
        }
    }
}

#[cfg(shm_model)]
impl ChannelHeader {
    /// Returns a header of zeroes, the state a fresh memory starts in; Loom's atomics have no `const` constructor
    /// and cannot be laid over bytes.
    pub(crate) fn new() -> Self {
        let word = || CacheLine(AtomicU32::new(0));
        ChannelHeader {
            req_head: word(),
            taken: word(),
            resp_head: word(),
            peer_sleeping: word(),
            host_sleeping: word(),
            start: StartReportArea {
                attempt: word(),
                state: word(),
                note_len: word(),
            },
            peer_waits: CacheLine(WaitCounts {
                waits: OwnCounter(AtomicU64::new(0)),
                spin_rounds: OwnCounter(AtomicU64::new(0)),
                parks: OwnCounter(AtomicU64::new(0)),
                wakes: OwnCounter(AtomicU64::new(0)),
            }),
        }
    }
}
