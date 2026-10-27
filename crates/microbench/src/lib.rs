//! What every Rust microbench of the workspace runs on (`[[bench]]` targets, `harness = false`).
//!
//! A per-call bench names its cases and hands them to [`run`]:
//!
//!   BENCH_N=10000000 cargo bench -p mipyre_http --bench body -- read_body_ready
//!
//! `BENCH_N` is the number of calls, and naming no case runs them all. `--list` prints the case
//! names one per line, so a runner can find them and count one case per process; a name selects
//! exactly one case, and a name that selects none is an error.
//!
//! The bench binary chooses its allocator, so it declares the counting one itself:
//!
//! ```text
//! #[global_allocator]
//! static GLOBAL: AllocationCounter<System> = AllocationCounter(System);
//! ```
//!
//! and [`run`] stops when the global allocator is not an [`AllocationCounter`] one, since every
//! `ALLOCATIONS_PER_CALL` it printed would be 0.

mod counting;
mod harness;
mod measure;
mod must;

pub use counting::{AllocationCounter, ALLOCATIONS};
pub use harness::{allocations_counted, calls, run, Case, DEFAULT_CALLS};
pub use measure::measure;
pub use must::Must;
