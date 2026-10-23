//! The channel itself, generic over the memory it lives in: it knows no OS, so it builds and runs on heap memory
//! under Miri and on the model's memory under Loom exactly as on a shared mapping.

mod ends;
pub(crate) mod header;
pub(crate) mod memory;
#[cfg(test)]
#[cfg(not(shm_model))]
mod ring_model;
mod slot;
#[cfg(test)]
#[cfg(not(shm_model))]
mod slot_fuzz;

pub use self::ends::{
    sleep_all, wake_all, Attempt, DeadPeerAnswer, DeadPeerError, HostEnd, PeerEnd, PeerFault, SleepVerdict,
    StartOutcome, Wait,
};
pub use self::header::{WaitTotals, NOTE_BYTES};
#[cfg(not(shm_model))]
pub use self::memory::HeapMemory;
pub use self::memory::{Memory, MessageView};
#[cfg(shm_model)]
pub use self::memory::{ModelMemory, MODEL_SLOT};
pub use self::slot::{PeerRequest, ReadyAnswers, RequestSlot, SlotRoomError, SlotWriter};

/// One channel between a host and a peer, for one run: every run makes a new one, so its memory starts zeroed and
/// no peer of an earlier run can write into it.
pub struct Channel<M: Memory> {
    memory: M,
}

impl<M: Memory> Channel<M> {
    /// Returns the channel over `memory`, which was made for one geometry and is zeroed, so nothing needs clearing.
    pub fn new(memory: M) -> Self {
        Channel { memory }
    }

    /// Returns how the peer waited, as it counted; it counts over this channel's run only.
    pub fn peer_waits(&self) -> WaitTotals {
        self.memory.header().peer_waits.read()
    }
}
