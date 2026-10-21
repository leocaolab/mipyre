//! Carries pyre's connections, whatever is spoken over them: TCP listeners and accepts ([`tcp`]),
//! TLS over any byte stream ([`tls`]), connections made in this process ([`memory`]), what holds
//! for any byte stream ([`stream`]), and how a server takes its connections ([`driver`],
//! [`endpoint`]).
//!
//! [`tcp`] and [`tls`] use only [`stream`]; [`endpoint`] is the one place they meet.

pub mod driver;
pub mod endpoint;
pub mod memory;
pub mod stream;
mod sync;
pub mod tcp;
pub mod tls;
