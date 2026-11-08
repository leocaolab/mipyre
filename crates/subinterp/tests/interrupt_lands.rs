//! An interruption set while a unit waits outside Python lands in the unit's next Python code.
//!
//! An integration test because the first Python a fresh interpreter runs must be the unit's: no
//! other test may have used this interpreter, or PyO3's per-interpreter set-up, first.

use std::sync::mpsc;

use mipyre_subinterp::{
    InterruptTarget, Interruption, NewInterpreterError, Releasable, SubInterpreter, PYTHON_THREAD_STACK,
};
use pyo3::prelude::*;

struct Nothing;

impl Releasable for Nothing {
    fn release(self, _py: Python<'_>) {}
}

/// Spins in bytecode for far longer than the test takes, so only the interruption ends it.
const SPIN: &std::ffi::CStr = c"import time\nt = time.monotonic()\nwhile time.monotonic() - t < 600: pass\n";

#[test]
fn an_interruption_set_while_the_unit_waits_in_rust_lands_in_its_next_python() {
    Python::initialize();
    let (target_out, target_in) = mpsc::channel::<InterruptTarget>();
    let (fired, has_fired) = mpsc::channel::<()>();
    let serve = std::thread::Builder::new()
        .stack_size(PYTHON_THREAD_STACK)
        .spawn(move || {
            let mut worker = SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(
                std::convert::identity,
                |_py| Ok(Nothing),
            )
            .expect("a sub-interpreter");
            worker
                .with_unit(|_, unit, py| {
                    target_out.send(unit.target()).expect("the test listens");
                    py.detach(move || has_fired.recv().expect("the test fires"));
                    let ran = py.run(SPIN, None, None);
                    let interrupt = py
                        .import("builtins")
                        .and_then(|builtins| builtins.getattr("KeyboardInterrupt"))
                        .expect("builtins has KeyboardInterrupt");
                    let raised = ran.is_err_and(|e| e.is_instance(py, &interrupt));
                    (raised, unit.end().interrupted)
                })
                .expect("no panic")
        })
        .expect("spawn the worker thread");

    let target = target_in.recv().expect("the unit began");
    assert_eq!(target.interrupt(), Ok(Interruption::Set));
    fired.send(()).expect("the unit waits");

    let (raised, interrupted) = serve.join().expect("the worker thread ends");
    assert!(raised, "the unit's next Python did not raise the interruption");
    assert!(interrupted, "the unit's end did not report the interruption");
}
