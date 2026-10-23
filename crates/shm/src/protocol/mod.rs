//! Keeps the protocol between two atomic words: one side publishes a counter and wakes the other only when it said
//! it sleeps; the other says it sleeps and looks at the counter once more before it does (Dekker), so no wake is
//! lost and a busy channel never enters the kernel. Waking and sleeping are effects, so they are ports
//! ([`doorbell::Doorbell`], [`doorbell::Parker`]) that the adapters implement.
#![forbid(unsafe_code)]

pub mod doorbell;
