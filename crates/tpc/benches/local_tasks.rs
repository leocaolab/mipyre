//! Measures a connection-sized task (spawn, run, end) in `LocalTasks`, on a current-thread
//! runtime and `LocalSet`.
//!
//! With the case `local_task`, it runs `BENCH_N` tasks for `benchmarks/bench_micro_count.py` to
//! count instructions per task. With no case, `cargo bench -p mipyre_tpc --bench local_tasks`
//! prints ns per task beside tokio-util's `TaskTracker` doing the same.

use std::time::Instant;

use mipyre_microbench::calls;
use mipyre_tpc::LocalTasks;

fn main() {
    // `benchmarks/bench_micro_count.py` reads the case names from `--list`.
    if std::env::args().any(|a| a == "--list") {
        println!("local_task");
        return;
    }
    let rt = match tokio::runtime::Builder::new_current_thread().enable_time().build() {
        Ok(rt) => rt,
        Err(e) => panic!("no runtime for the bench: {e}"),
    };
    match std::env::args().nth(1).as_deref() {
        Some("local_task") => {
            tokio::task::LocalSet::new().block_on(&rt, local_tasks(calls(1_000_000)));
        }
        Some(other) => panic!("no case {other}; the one case is local_task"),
        None => tokio::task::LocalSet::new().block_on(&rt, against_task_tracker()),
    }
}

/// Spawns `n` tasks one after another, each ended before the next starts.
async fn local_tasks(n: u64) {
    let mut tasks = LocalTasks::new();
    for _ in 0..n {
        tasks.spawn_local(async {});
        tokio::task::yield_now().await;
    }
    tasks.idle().await;
}

async fn against_task_tracker() {
    let n: u64 = 1_000_000;
    let start = Instant::now();
    local_tasks(n).await;
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    println!("local_tasks: {ns:.1} ns/task ({n} tasks)");

    let tracker = tokio_util::task::TaskTracker::new();
    let start = Instant::now();
    for _ in 0..n {
        tracker.spawn_local(async {});
        tokio::task::yield_now().await;
    }
    tracker.close();
    tracker.wait().await;
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    println!("task_tracker: {ns:.1} ns/task ({n} tasks)");
}
