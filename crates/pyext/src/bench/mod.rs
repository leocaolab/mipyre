//! Runs the in-process benchmarks; it exists only in `--features bench` builds.

mod alloc;
mod client;
mod measure;
mod run;

pub(crate) use self::alloc::AllocationCounter;
pub(crate) use self::client::BENCH_REQUEST;
pub(crate) use self::measure::ProfileDepth;
pub(crate) use self::run::{run_inmem_bench, run_inmem_profile, run_loopback_bench, BenchWorkers};
