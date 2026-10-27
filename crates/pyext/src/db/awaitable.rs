//! Hands a DB-runtime result to an asyncio future on the caller's loop, for the `*_async` methods.

use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::InterpreterHandle;

use mipyre_subinterp::attach::{with_interpreter, with_main};

use mipyre_pg::{DbRuntime, TaskError};

use super::error::DbError;

/// Runs `fut` on the DB runtime `rt` and returns an asyncio future on the caller's running loop that
/// completes with `convert(result)`.
///
/// The query holds `rt` until its result is delivered, so it is answered even when the handle lets
/// go of the pool meanwhile (a run sealing binds the handle to the run's pool).
///
/// The result is delivered by attaching to the interpreter captured here, because the PyO3 fork
/// refuses a bare attach from a runtime thread once more than one interpreter has run the
/// engine; that also rules out `pyo3-async-runtimes`. Delivery runs on the blocking pool, so a
/// busy GIL never stalls a runtime thread. The future is set from its own loop and only if it is
/// not done, cancelling it aborts the query, and a panic in the query or in `convert` completes
/// it with an error. A result for a closed loop is dropped and logged.
///
/// # Errors
///
/// Returns the Python error when there is no running loop.
pub(super) fn await_on_loop<'py, T, F, C>(
    py: Python<'py>,
    rt: &Arc<DbRuntime>,
    fut: F,
    convert: C,
) -> PyResult<Bound<'py, PyAny>>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, DbError>> + Send + 'static,
    C: for<'a> FnOnce(Python<'a>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
    let py_fut = event_loop.call_method0("create_future")?;
    let delivery = LoopFuture {
        interp: InterpreterHandle::current(py),
        target: Some((event_loop.unbind(), py_fut.clone().unbind())),
    };

    let query = rt.spawn(fut);
    let abort = query.abort_handle();
    rt.spawn(async move {
        let result = match query.await {
            Ok(done) => done,
            Err(join_error) => Err(DbError::from(TaskError::from(join_error))),
        };
        // `delivery` is moved into the closure, so its Drop completes the future even when the
        // closure panics or never runs because the runtime shut down.
        let delivered = tokio::task::spawn_blocking(move || {
            delivery.complete(move |py| match result {
                Ok(value) => convert(py, value),
                Err(e) => Err(e.into_pyerr(py)),
            });
        })
        .await;
        if let Err(e) = delivered {
            tracing::error!(target: "mipyre::server", error = %e, "delivering a query result failed");
        }
    });
    py_fut.call_method1("add_done_callback", (QueryAbortHook(abort),))?;
    Ok(py_fut)
}

/// Holds the asyncio loop and future one `await_on_loop` call delivers to, and their interpreter.
struct LoopFuture {
    interp: InterpreterHandle,
    /// Holds `(loop, future)` until the first completion takes it.
    target: Option<(Py<PyAny>, Py<PyAny>)>,
}

impl LoopFuture {
    /// Sets the future to `outcome` from its loop unless it is already done, and releases the
    /// loop and future while attached to their interpreter.
    fn complete<O>(mut self, outcome: O)
    where
        O: for<'a> FnOnce(Python<'a>) -> PyResult<Py<PyAny>>,
    {
        self.complete_with(outcome);
    }

    fn complete_with<O>(&mut self, outcome: O)
    where
        O: for<'a> FnOnce(Python<'a>) -> PyResult<Py<PyAny>>,
    {
        let Some((event_loop, py_fut)) = self.target.take() else {
            return;
        };
        // SAFETY: `Py_IsInitialized` and `Py_IsFinalizing` may be called at any time, without an
        // attached thread state.
        if unsafe { pyo3::ffi::Py_IsInitialized() == 0 || pyo3::ffi::Py_IsFinalizing() != 0 } {
            // The objects are leaked because there is no interpreter left to release them into.
            std::mem::forget((event_loop, py_fut));
            return;
        }
        let interp = self.interp;
        let run = move |py: Python<'_>| {
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| outcome(py))).unwrap_or_else(|payload| {
                    Err(PyRuntimeError::new_err(format!(
                        "converting the database result to Python panicked: {}",
                        mipyre_util::panic_message(&*payload)
                    )))
                });
            let (ok, value) = match outcome {
                Ok(v) => (true, v),
                Err(e) => (false, e.into_value(py).into_any()),
            };
            let sent = resolver(py).and_then(|resolve| {
                event_loop
                    .bind(py)
                    .call_method1("call_soon_threadsafe", (resolve, py_fut.bind(py), ok, value))
                    .map(drop)
            });
            if let Err(e) = sent {
                crate::telemetry::count_db_result_undeliverable();
                // A closed loop raises `RuntimeError` and its warning is rate-limited, because a
                // lost answer is a failure to see but every query in flight hits it at shutdown.
                // Any other failure, a subclass of `RuntimeError` such as `RecursionError`
                // included, is unexpected and warns every time.
                if e.get_type(py).is(py.get_type::<PyRuntimeError>()) {
                    if let Some(held) = crate::telemetry::admit_closed_loop_warning() {
                        tracing::warn!(
                            target: "mipyre::server",
                            error = %e,
                            held,
                            "async database result dropped: the asyncio loop can't be reached (it is closed)"
                        );
                    }
                } else {
                    tracing::warn!(
                        target: "mipyre::server",
                        error = %e,
                        "async database result dropped: the asyncio loop can't be reached"
                    );
                }
            }
            drop(event_loop);
            drop(py_fut);
        };
        if interp.is_main() {
            with_main(run);
        } else {
            with_interpreter(interp, run);
        }
    }
}

impl Drop for LoopFuture {
    /// Completes the future with an error, which happens only when the runtime shut down before
    /// the delivering task finished.
    fn drop(&mut self) {
        self.complete_with(|_py| {
            Err(PyRuntimeError::new_err(
                "mipyre-db task ended without a result: the runtime shut down",
            ))
        });
    }
}

/// Caches `resolve(fut, ok, value)`, which sets `fut` unless it is already done; with the PyO3
/// fork `PyOnceLock` is per interpreter, so it is compiled once per interpreter.
static RESOLVE: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

fn resolver(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    RESOLVE
        .get_or_try_init(py, || {
            let module = pyo3::types::PyModule::from_code(
                py,
                c"def resolve(fut, ok, value):\n    if fut.done():\n        return\n    if ok:\n        fut.set_result(value)\n    else:\n        fut.set_exception(value)\n",
                c"mipyre_db_async",
                c"mipyre_db_async",
            )?;
            Ok::<_, PyErr>(module.getattr("resolve")?.unbind())
        })
        .map(|f| f.bind(py).clone())
}

/// Aborts the DB task when its asyncio future is cancelled.
#[pyclass(frozen)]
struct QueryAbortHook(tokio::task::AbortHandle);

#[pymethods]
impl QueryAbortHook {
    fn __call__(&self, fut: &Bound<'_, PyAny>) -> PyResult<()> {
        if fut.call_method0("cancelled")?.is_truthy()? {
            self.0.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use mipyre_pg::DbRuntime;
    use mipyre_subinterp::PYTHON_THREAD_STACK;

    #[test]
    fn the_db_runtime_starts_its_threads_with_python_stacks() {
        mipyre_runtime::testkit::thread_stack::with_exact_stacks(
            concat!(module_path!(), "::the_db_runtime_starts_its_threads_with_python_stacks"),
            || {
                let python = mipyre_runtime::testkit::thread_stack::of_a_thread_with(PYTHON_THREAD_STACK);
                let rt = DbRuntime::build(Some(NonZeroUsize::MIN), PYTHON_THREAD_STACK).expect("the DB runtime builds");
                let blocking = rt
                    .run(async { tokio::task::spawn_blocking(mipyre_runtime::testkit::thread_stack::current).await })
                    .expect("the DB runtime runs the task")
                    .expect("on the blocking pool");
                assert_eq!(blocking, python);
            },
        );
    }
}
