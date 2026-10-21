//! A shared-memory channel between two processes: a host that sends requests and a peer that answers them, over
//! two rings that share one slot index.
//!
//! [`plan`] holds the channel's decisions as pure functions of the values its counters and slot headers held. The
//! peer runs user code and C extensions, so every value it wrote is believed only once it fits what the host itself
//! sent and read.

pub mod plan;
