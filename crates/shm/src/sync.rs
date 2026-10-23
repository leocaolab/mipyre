//! Names the atomics, the fence, the spin hint and the slot cell the channel is made of: std's in a normal build,
//! Loom's under `--cfg shm_model`, where the models run the same protocol over every interleaving. Only the names
//! switch; nothing wraps the real ones, so a build pays nothing for them.
//!
//! This is the one module that names an atomic type itself (dev/archcheck.toml E-L1; the package's clippy.toml
//! disallows std's and Loom's alike, so the expectation below holds under both builds): the atomics are type
//! aliases here, and every other module names the alias, never the type behind it.
//!
//! Under `--cfg shm_model --cfg shm_shuttle` the names are Shuttle's instead (b/744): the same models' memory and
//! kernel, run under seeded random schedules at a scale Loom cannot enumerate. Shuttle checks no cell (it explores
//! schedules, not data races, and runs every atomic as `SeqCst`), so the slot cell there is `channel/memory.rs`'s.
//!
//! The switch is this package's own `shm_model`, not the workspace's `sync_model`: under the model the channel's
//! header is made of Loom's atomics, which cannot lie over a shared mapping, so the package drops its OS half
//! (`os.rs`, `adapter/`). The engine's own Loom builds (`--cfg sync_model`) build this package as shipped, with the
//! OS half its process lanes run on.
//!
//! The slot cell and the lock the model's kernel sleeps under exist only under the model: a normal build reaches a
//! slot's bytes through the memory's base pointer, which Loom cannot see, so the model's memory keeps each slot in a
//! cell Loom checks instead.
#![expect(
    clippy::disallowed_types,
    reason = "this module is where the channel's atomics are named"
)]

#[cfg(not(shm_model))]
pub(crate) use std::hint::spin_loop;
#[cfg(not(shm_model))]
pub(crate) use std::sync::atomic::fence;
/// The word the ports take; it is public because an engine's own parker names it through `protocol::doorbell`.
#[cfg(not(shm_model))]
pub type AtomicU32 = std::sync::atomic::AtomicU32;
#[cfg(not(shm_model))]
pub(crate) type AtomicU64 = std::sync::atomic::AtomicU64;

#[cfg(all(shm_model, not(shm_shuttle)))]
#[expect(
    clippy::unsafe_removed_from_name,
    reason = "the cell is named for what it holds; its calls are safe, only the pointers they hand out are not"
)]
pub(crate) use loom::cell::{ConstPtr as SlotRead, UnsafeCell as SlotCell};
#[cfg(all(shm_model, not(shm_shuttle)))]
pub(crate) use loom::hint::spin_loop;
#[cfg(all(shm_model, not(shm_shuttle)))]
pub(crate) use loom::sync::atomic::fence;
// The lock the models' kernel (`model.rs`) sleeps under.
#[cfg(test)]
#[cfg(all(shm_model, not(shm_shuttle)))]
pub(crate) use loom::sync::{Condvar, Mutex};
/// The word the ports take, Loom's under the model.
#[cfg(all(shm_model, not(shm_shuttle)))]
pub type AtomicU32 = loom::sync::atomic::AtomicU32;
#[cfg(all(shm_model, not(shm_shuttle)))]
pub(crate) type AtomicU64 = loom::sync::atomic::AtomicU64;

#[cfg(all(shm_model, shm_shuttle))]
pub(crate) use shuttle::hint::spin_loop;
#[cfg(all(shm_model, shm_shuttle))]
pub(crate) use shuttle::sync::atomic::fence;
#[cfg(test)]
#[cfg(all(shm_model, shm_shuttle))]
pub(crate) use shuttle::sync::{Condvar, Mutex};
/// The word the ports take, Shuttle's under its models.
#[cfg(all(shm_model, shm_shuttle))]
pub type AtomicU32 = shuttle::sync::atomic::AtomicU32;
#[cfg(all(shm_model, shm_shuttle))]
pub(crate) type AtomicU64 = shuttle::sync::atomic::AtomicU64;

pub(crate) use std::sync::atomic::Ordering;
