//! Implements the channel's ports over the OS: a shared mapping as its [`Memory`](crate::channel::Memory),
//! a futex and a pipe as its doorbells, a futex wait as its parker. It is the only part of the crate that uses
//! `os.rs`.

pub mod doorbell;
pub mod mapped;
pub mod parker;
