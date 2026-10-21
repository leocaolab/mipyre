//! Binds TCP listening sockets, or takes the ones systemd handed in, and accepts connections
//! from them.

pub mod events;
pub mod listener;
pub(crate) mod loops;
mod os;
pub use os::passed;
