//! Counts the instructions of a request's calls into a sub-interpreter: one unit of work, one
//! GIL round trip, and the GIL save/restore pair a park does.
//!
//! A unit includes a GIL round trip, so the unit's own cost is the difference of the two.
//! `work_unit_begin_end` also prints `ALLOCATIONS_IN_LOOP=<n>`. Run them with:
//!
//!   BENCH_N=2000000 cargo bench -p mipyre_subinterp --bench interp -- work_unit_begin_end

use std::alloc::System;
use std::hint::black_box;
use std::sync::atomic::Ordering;

use mipyre_microbench::{run, AllocationCounter, Case, Must, ALLOCATIONS};
use pyo3::prelude::*;

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

fn work_unit_begin_end(n: u64) {
    Python::initialize();
    let serve = std::thread::spawn(move || {
        let mut worker = mipyre_subinterp::SubInterpreter::<Nothing>::create_on_this_thread::<
            mipyre_subinterp::NewInterpreterError,
        >("bench".into(), std::convert::identity, |_py| Ok(Nothing))
        .must("a sub-interpreter on its serving thread");
        mipyre_subinterp::keep_attached_until_idle();
        // The first unit allocates the slot, so it runs outside the counted loop.
        worker.with_unit(|_held, unit, _py| unit.end()).must("no panic");
        let before = ALLOCATIONS.load(Ordering::Relaxed);
        for _ in 0..n {
            worker
                .with_unit(|_held, unit, _py| {
                    black_box(unit.target());
                    unit.end();
                })
                .must("no panic");
        }
        let after = ALLOCATIONS.load(Ordering::Relaxed);
        eprintln!("ALLOCATIONS_IN_LOOP={}", after - before);
        worker.end_on_own_thread();
    });
    if serve.join().is_err() {
        eprintln!("the worker thread panicked");
        std::process::exit(1)
    }
}

fn gil_save_restore_pair(n: u64) {
    Python::initialize();
    Python::attach(|_py| {
        for _ in 0..n {
            // SAFETY: The calling thread holds the main interpreter's thread state, and the
            // same state is restored right after it is saved.
            unsafe {
                let saved = pyo3::ffi::PyEval_SaveThread();
                pyo3::ffi::PyEval_RestoreThread(saved);
            }
        }
    });
}

struct Nothing;

impl mipyre_subinterp::Releasable for Nothing {
    fn release(self, _py: Python<'_>) {}
}

fn with_gil_round_trip(n: u64) {
    Python::initialize();
    let serve = std::thread::spawn(move || {
        let mut worker = mipyre_subinterp::SubInterpreter::<Nothing>::create_on_this_thread::<
            mipyre_subinterp::NewInterpreterError,
        >("bench".into(), std::convert::identity, |_py| Ok(Nothing))
        .must("a sub-interpreter on its serving thread");
        mipyre_subinterp::keep_attached_until_idle();
        for _ in 0..n {
            worker.with_gil(|_held, _py| black_box(())).must("no panic");
        }
        worker.end_on_own_thread();
    });
    if serve.join().is_err() {
        eprintln!("the worker thread panicked");
        std::process::exit(1)
    }
}

fn main() {
    let cases: [Case; 3] = [
        ("work_unit_begin_end", work_unit_begin_end),
        ("gil_save_restore_pair", gil_save_restore_pair),
        ("with_gil_round_trip", with_gil_round_trip),
    ];
    run(&cases);
}
