//! Attaches a Rust thread to the interpreter it names.
//!
//! Once more than one interpreter has run the engine, the PyO3 fork refuses a bare
//! `Python::attach` (and a `Py<T>` drop) on a thread with no thread state, because landing in
//! main by accident would run another interpreter's objects under the wrong GIL.
//!
//! No attach, `clone_ref` or `Py<T>` drop of a main-interpreter object may happen on a thread
//! bound to a sub-interpreter. A value holding such objects may pass through that thread only
//! if its last owner drops it on main, attached.

use crate::ThreadStateCreationFailure;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::InterpreterHandle;

/// Returns whether the calling thread is attached to the main interpreter.
pub fn on_main(py: Python<'_>) -> bool {
    InterpreterHandle::current(py).is_main()
}

/// Runs `f` attached to the main interpreter.
///
/// A thread with no thread state keeps one main thread state for its whole life, so a pool
/// thread does not create and destroy one per call.
///
/// # Panics
///
/// Panics if the calling thread is bound to a sub-interpreter.
pub fn with_main<F, R>(f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    let main = InterpreterHandle::main();
    if InterpreterHandle::bound().is_none() {
        // During thread-local teardown the slot may be gone, and creation fails when out of
        // memory; either way `with_interpreter` attaches with a thread state for this call only.
        match THREAD_MAIN_TSTATE.try_with(|slot| slot.ensure(main)) {
            Ok(Ok(())) | Err(_) => {}
            Ok(Err(e)) => tracing::warn!(
                "could not create this thread's main-interpreter thread state: {e}; attaching \
                 with a thread state per call instead"
            ),
        }
    }
    with_interpreter(main, f)
}

/// Runs `f` attached to `interp`, from any thread.
///
/// A thread with no thread state gets a temporary one for `interp`. A thread that has one never
/// goes through `InterpreterHandle::attach`, whose fast path would run `f` without the GIL on a
/// thread that is bound but detached.
///
/// # Panics
///
/// Panics if the thread is bound to a different interpreter, or if a thread state other than
/// the thread's own is current.
pub fn with_interpreter<F, R>(interp: InterpreterHandle, f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    // A TPC thread may have kept its worker's thread state current between requests.
    crate::detach_for_idle();
    // SAFETY: This is always safe to call and returns null when the thread has no thread state.
    let bound = unsafe { ffi::PyGILState_GetThisThreadState() };
    // SAFETY: This is always safe to call and returns null when no thread state is current.
    let current = unsafe { ffi::PyThreadState_GetUnchecked() };
    // `PyGILState_Ensure` would restore the bound thread state on top of another current one,
    // which CPython treats as fatal without saying why.
    assert!(
        current.is_null() || current == bound,
        "with_interpreter(interpreter {}) while another thread state is current on this thread \
         (a sub-interpreter's init or handler running on it); attach from a thread that has \
         none",
        interp.id(),
    );
    let Some(bound_interp) = InterpreterHandle::bound() else {
        return interp.attach(f);
    };
    assert!(
        bound_interp == interp,
        "with_interpreter(interpreter {}) on a thread bound to interpreter {}",
        interp.id(),
        bound_interp.id(),
    );
    Python::attach(f)
}

struct MainThreadState(std::cell::Cell<Option<std::ptr::NonNull<ffi::PyThreadState>>>);

thread_local! {
    static THREAD_MAIN_TSTATE: MainThreadState =
        const { MainThreadState(std::cell::Cell::new(None)) };
}

impl MainThreadState {
    /// Creates this thread's main thread state unless it has one; the thread must have no thread
    /// state, so the new one becomes the one `PyGILState_Ensure` re-attaches.
    fn ensure(&self, main: InterpreterHandle) -> Result<(), ThreadStateCreationFailure> {
        if self.0.get().is_some() {
            return Ok(());
        }
        let tstate = main.new_thread_state().ok_or(ThreadStateCreationFailure)?;
        self.0.set(Some(tstate));
        Ok(())
    }
}

impl Drop for MainThreadState {
    fn drop(&mut self) {
        let Some(tstate) = self.0.get() else {
            return;
        };
        let tstate = tstate.as_ptr();
        // SAFETY: This is always safe to call.
        if unsafe { ffi::Py_IsInitialized() } == 0 {
            return;
        }
        // SAFETY: `tstate` was created on this thread and is detached, because every attach on
        // it went through `PyGILState_Ensure`/`Release`. Clear needs it current with the GIL,
        // and DeleteCurrent releases both.
        unsafe {
            ffi::PyEval_RestoreThread(tstate);
            ffi::PyThreadState_Clear(tstate);
            ffi::PyThreadState_DeleteCurrent();
        }
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    //! Counts a worker's thread states exactly before and after visits from other threads.
    //!
    //! A thread state leaked per cross-thread attach is raw malloc, invisible to the Python heap,
    //! but it stays in the interpreter's list until the interpreter ends.

    use super::*;
    use crate::{NewInterpreterError, Releasable, SubInterpreter};
    use std::sync::Barrier;

    struct Interp(InterpreterHandle);

    impl Releasable for Interp {
        fn release(self, _py: Python<'_>) {}
    }

    /// A barrier releases the visitors together, as request threads arrive together.
    const VISITORS: usize = 4;
    const VISITS: usize = 200;

    fn thread_states(_py: Python<'_>) -> usize {
        let mut n = 0;
        // SAFETY: `py` proves the interpreter is alive and its GIL held, and the parked visitors
        // make or free no thread state meanwhile.
        unsafe {
            let mut t = ffi::PyInterpreterState_ThreadHead(ffi::PyInterpreterState_Get());
            while !t.is_null() {
                n += 1;
                t = ffi::PyThreadState_Next(t);
            }
        }
        n
    }

    /// Returns how many thread states `VISITORS` threads, each calling `visit` `visits` times,
    /// leave in a worker's interpreter.
    ///
    /// Each visitor hands its results to `give_back` on its own thread after the count, so no
    /// visitor thread state is left when `Py_EndInterpreter` runs.
    fn left_behind<K: 'static>(
        visits: usize,
        visit: fn(InterpreterHandle) -> K,
        give_back: fn(Vec<K>),
    ) -> isize {
        Python::initialize();
        crate::python_thread("left-behind-worker".into())
            .spawn(move || {
                let mut worker =
                    SubInterpreter::<Interp>::create_on_this_thread::<NewInterpreterError>(
                        "left-behind".into(), std::convert::identity,
                        |py| Ok(Interp(InterpreterHandle::current(py))),
                    )
                .expect("a sub-interpreter on its serving thread");
                let interp = worker.held().0;
                let count = |worker: &mut SubInterpreter<Interp>| {
                    worker.with_gil(|_, py| thread_states(py)).expect("no panic")
                };
                let before = count(&mut worker);
                let arrive = Barrier::new(VISITORS);
                let counted = Barrier::new(VISITORS + 1);
                let after = std::thread::scope(|s| {
                    for i in 0..VISITORS {
                        let (arrive, counted) = (&arrive, &counted);
                        crate::python_thread(format!("visitor-{i}"))
                            .spawn_scoped(s, move || {
                                arrive.wait();
                                let kept: Vec<K> = (0..visits).map(|_| visit(interp)).collect();
                                counted.wait(); // visited
                                counted.wait(); // counted
                                give_back(kept);
                            })
                            .expect("spawn a visitor");
                    }
                    counted.wait();
                    let after = count(&mut worker);
                    counted.wait();
                    after
                });
                worker.end_on_own_thread();
                after as isize - before as isize
            })
            .expect("spawn the worker thread")
            .join()
            .expect("the worker thread ends")
    }

    fn visit(interp: InterpreterHandle) {
        with_interpreter(interp, |py| {
            assert_eq!(InterpreterHandle::current(py), interp);
        });
    }

    /// Keeps the thread state it made, as the leak did, so the count has a leak to see.
    fn leaking_visit(interp: InterpreterHandle) -> std::ptr::NonNull<ffi::PyThreadState> {
        interp.new_thread_state().expect("a thread state")
    }

    fn free_kept(kept: Vec<std::ptr::NonNull<ffi::PyThreadState>>) {
        for tstate in kept {
            // SAFETY: The thread state was made on this thread for a live interpreter and was
            // never current; Clear needs it current, and DeleteCurrent releases the GIL.
            unsafe {
                ffi::PyEval_RestoreThread(tstate.as_ptr());
                ffi::PyThreadState_Clear(tstate.as_ptr());
                ffi::PyThreadState_DeleteCurrent();
            }
        }
    }

    #[test]
    fn visits_from_other_threads_leave_no_thread_state() {
        assert_eq!(left_behind(VISITS, visit, drop), 0);
    }

    #[test]
    fn twice_the_visits_leave_the_same() {
        assert_eq!(
            left_behind(2 * VISITS, visit, drop),
            left_behind(VISITS, visit, drop)
        );
    }

    #[test]
    fn the_count_sees_one_kept_thread_state_per_visit() {
        let once = left_behind(VISITS, leaking_visit, free_kept);
        assert_eq!(once, (VISITORS * VISITS) as isize);
        assert_eq!(left_behind(2 * VISITS, leaking_visit, free_kept), 2 * once);
    }
}
