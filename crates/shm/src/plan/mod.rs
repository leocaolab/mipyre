//! Makes the channel's decisions as pure functions: they take the values the counters and slot headers held, never
//! the memory, and read no clock.
//!
//! The ones that read what the peer wrote return `Result`: a value no correct peer writes is a fact they report,
//! with the values it was measured against, not a failure of the function.
#![forbid(unsafe_code)]
#![deny(unreachable_pub, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod geometry;
pub mod ring;
pub mod slot_len;
pub mod spin_window;
pub mod start;
pub mod wakes;
