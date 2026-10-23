//! A shared-memory channel between two processes: a host that sends requests and a peer that answers them, over
//! two rings that share one slot index.
//!
//! [`plan`] holds the channel's decisions as pure functions of the values its counters and slot headers held. The
//! peer runs user code and C extensions, so every value it wrote is believed only once it fits what the host itself
//! sent and read. [`protocol`] keeps the two words' wake-up protocol and defines the ports for waking and sleeping;
//! [`channel`] is the channel over a [`channel::Memory`], knowing no OS; [`adapter`] implements the ports over
//! the OS, whose calls are all in one private module.

#[cfg(not(shm_model))]
pub mod adapter;
pub mod channel;
// The models and the kernel they sleep in exist only for the model tests: Loom's, and Shuttle's (b/744).
#[cfg(test)]
#[cfg(all(shm_model, not(shm_shuttle)))]
mod loom_model;
#[cfg(test)]
#[cfg(shm_model)]
mod model;
#[cfg(not(shm_model))]
mod os;
pub mod plan;
pub mod protocol;
#[cfg(test)]
#[cfg(all(shm_model, shm_shuttle))]
mod shuttle_model;
mod sync;

#[cfg(not(shm_model))]
pub use os::{Syscall, SyscallError};
