//! Runs a caller's future on one pinned OS thread per core, each with its own current-thread
//! runtime and `LocalSet`, and gives each thread its own view of the server's stop.
//!
//! A connection on such a thread never leaves it (`spawn_local`), so its future needs no `Send`
//! and nothing on the request path is written by two threads; [`LocalStop`] keeps the stop
//! signal off that path too.

mod stop;
mod sync;
mod tasks;
mod threads;

pub use stop::{LocalStop, StopWaiter};
pub use tasks::{IdleWaiter, LocalTasks};
pub use threads::{
    block_on, block_until_cancelled, join_until, pin_and_raise_qos, serve_on_this_thread, spawn, spawn_in_run,
    spawn_per_core, spawn_stop_watcher, AffinityOutcome, Cancellable, CoreSlot, Leftover, OsSpawner, OsThread, RunStop,
    ShutdownDeadline, SpawnError, SpawnFailure, Spawner, StopCause, StopWatcher, StopWatcherError, ThreadError,
    ThreadFailure, ThreadHandle, ThreadHooks, ThreadJoinError, ThreadLeftover, ThreadResults, ThreadStop,
};
