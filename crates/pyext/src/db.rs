//! Gives Python handlers Postgres pools backed by sqlx: a server run's, one per DSN, or a
//! handle's own outside any run (the pools and their binding are
//! `mipyre_pg`'s).
//!
//! `PgPool.connect()` only records what it asked for. A run sealing on the main interpreter
//! opens one pool per DSN from every such call, refusing at once every setting two calls name
//! differently, and every worker's `connect()` to that DSN binds to the run's pool (`ValueError`
//! when it names a setting the pool does not have). A handle no run holds opens its own pool on
//! its first query, and a failed connection raises there.
//!
//! Each run's pools run on a `mipyre-db` tokio runtime of their own, not the server's, so DB I/O
//! stays off the accept loop. Sync calls wait for it on a std channel with the GIL released,
//! never with `Runtime::block_on`, which panics on a thread already inside a Tokio context such
//! as a TPC worker. The `*_async` methods work on the main interpreter only.
//!
//! Columns of a type outside the decoded set reach Python as their raw binary wire bytes.

mod awaitable;
mod cell;
mod cursor;
mod error;
mod kind;
mod module_key;
mod numeric;
mod param;
mod py_types;
mod query;

use std::num::NonZeroUsize;
use std::time::Duration;

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

use std::sync::Arc;

use mipyre_pg::{Dsn, Expiry, OpenPool, PgHandle, PoolRequest};
use mipyre_subinterp::PYTHON_THREAD_STACK;

use cursor::PgCursor;
use error::DbError;
use mipyre_runtime::EngineModule;
use query::{FetchMode, SqlQuery};

pub(crate) use self::module_key::{module_key_of_globals, module_key_of_module, module_key_of_path};

/// Adds the DB classes and exceptions to the engine module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PgPool>()?;
    m.add_class::<PgCursor>()?;
    error::register(m)
}

/// Runs the statement on the DB runtime while this thread waits with the GIL released.
fn run_blocking(
    py: Python<'_>,
    handle: &PgHandle,
    fetch: FetchMode,
    sql: String,
    params: Vec<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    refuse_in_lane_worker()?;
    let stmt = SqlQuery::from_py(py, sql, &params)?;
    let outcome = py
        .detach(|| {
            let open = handle.open(PYTHON_THREAD_STACK).map_err(DbError::from)?;
            open.runtime
                .run(query::run(open.pool, stmt, fetch))
                .map_err(DbError::from)?
        })
        .map_err(|e| e.into_pyerr(py))?;
    outcome.into_py(py)
}

/// The pool a query of `handle` runs on now, opened if it is the handle's own and not yet open.
fn open(py: Python<'_>, handle: &PgHandle) -> PyResult<OpenPool> {
    py.detach(|| handle.open(PYTHON_THREAD_STACK))
        .map_err(|e| DbError::from(e).into_pyerr(py))
}

/// Runs the statement on the DB runtime and returns an asyncio future for its result.
fn run_awaitable<'py>(
    py: Python<'py>,
    handle: &PgHandle,
    fetch: FetchMode,
    sql: String,
    params: Vec<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    refuse_async_in_worker(py, fetch)?;
    refuse_in_lane_worker()?;
    let stmt = SqlQuery::from_py(py, sql, &params)?;
    let OpenPool { pool, runtime } = open(py, handle)?;
    awaitable::await_on_loop(
        py,
        &runtime,
        query::run(pool, stmt, fetch),
        |py, outcome: query::QueryAnswer| outcome.into_py(py),
    )
}

/// Is a handle to a Postgres pool: in a server run, the run's pool for its DSN, shared by every
/// handle to that DSN; outside any run, its own.
#[pyclass(frozen, module = "mipyre.engine")]
pub(crate) struct PgPool {
    handle: Arc<PgHandle>,
}

#[pymethods]
impl PgPool {
    /// Return a handle to `dsn`'s pool, asking for these settings; it connects nothing yet.
    ///
    /// On the main interpreter the next server run opens one pool per DSN from every handle
    /// still held, and refuses to start, naming each, when two calls for one DSN name different
    /// values of a setting (a setting left out takes whatever another call named). A worker's
    /// call binds to its run's pool for the DSN, and raises ValueError when it names a setting
    /// that pool does not have. Outside any run, the handle opens a pool of its own on its first
    /// query (raising ConnectionError there if it cannot) and closes it when dropped.
    /// `max_connections` and `acquire_timeout_secs` default to 10 and 30, and
    /// `max_lifetime_secs` and `idle_timeout_secs` default to 1800 and 300, with 0 meaning
    /// never. `runtime_threads` sizes the runtime the run's queries run on, defaulting to one
    /// thread per 4 CPUs and at least 2. `ping_on_acquire` tests each connection with a round
    /// trip before handing it out.
    #[classmethod]
    #[pyo3(signature = (
        dsn,
        max_connections = None,
        acquire_timeout_secs = None,
        runtime_threads = None,
        max_lifetime_secs = None,
        idle_timeout_secs = None,
        ping_on_acquire = None,
    ))]
    #[expect(
        clippy::too_many_arguments,
        reason = "one parameter per Python keyword argument of `PgPool.connect()`"
    )]
    fn connect(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        dsn: String,
        max_connections: Option<u32>,
        acquire_timeout_secs: Option<u64>,
        runtime_threads: Option<usize>,
        max_lifetime_secs: Option<u64>,
        idle_timeout_secs: Option<u64>,
        ping_on_acquire: Option<bool>,
    ) -> PyResult<Self> {
        let runtime_threads = runtime_threads
            .map(|n| {
                NonZeroUsize::new(n)
                    .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("runtime_threads must be at least 1, got 0"))
            })
            .transpose()?;
        refuse_in_lane_worker()?;
        let request = PoolRequest {
            dsn: Dsn::new(dsn),
            max_connections,
            acquire_timeout: acquire_timeout_secs.map(Duration::from_secs),
            runtime_threads,
            max_lifetime: max_lifetime_secs.map(Expiry::from_secs),
            idle_timeout: idle_timeout_secs.map(Expiry::from_secs),
            ping_on_acquire,
        };
        // The module whose code is calling: a run takes only its own app's modules' handles.
        let caller = py
            .import("sys")?
            .call_method1("_getframe", (0,))?
            .getattr("f_globals")?;
        let owner = module_key_of_globals(caller.cast::<pyo3::types::PyDict>()?)?;
        let handle = PgHandle::new(request, owner);
        EngineModule::with(py, |module| match module.run_pools() {
            // A worker of a run that has pools binds to them now.
            Some(run) => run.adopt(&handle).map_err(DbError::from),
            None => {
                if mipyre_subinterp::attach::on_main(py) {
                    module.record_pg(&handle);
                }
                Ok(())
            }
        })?
        .map_err(|e| e.into_pyerr(py))?;
        Ok(PgPool { handle })
    }

    /// Fetch the first row as a dict, or None when there is none; extra rows are ignored.
    #[pyo3(signature = (sql, *params))]
    fn fetch_one(&self, py: Python<'_>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.handle, FetchMode::One, sql, params)
    }

    /// Fetch all matching rows into a list of dicts.
    #[pyo3(signature = (sql, *params))]
    fn fetch_all(&self, py: Python<'_>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.handle, FetchMode::All, sql, params)
    }

    /// Fetch the first column of the first row, raising if there is no row; SQL NULL is None.
    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar(&self, py: Python<'_>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.handle, FetchMode::Scalar, sql, params)
    }

    /// Execute a statement and return the number of rows it affected.
    #[pyo3(signature = (sql, *params))]
    fn execute(&self, py: Python<'_>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.handle, FetchMode::Execute, sql, params)
    }

    /// Return a `PgCursor` that yields rows one at a time in bounded memory, for result sets
    /// too large for `fetch_all`.
    ///
    /// ```python
    /// for row in pool.fetch_iter("SELECT * FROM users"):
    ///     process(row)
    /// # Cursor drops on loop exit → the driver task stops; a cursor left before the end
    /// # closes its connection (the pool opens a new one), one read to the end returns it.
    /// ```
    #[pyo3(signature = (sql, *params))]
    fn fetch_iter(&self, py: Python<'_>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<PgCursor> {
        refuse_in_lane_worker()?;
        let stmt = SqlQuery::from_py(py, sql, &params)?;
        Ok(PgCursor::open(open(py, &self.handle)?, stmt))
    }

    // The async variants are separate methods, so one call never returns a dict in one place
    // and a coroutine in another. They work on the main interpreter only.

    #[pyo3(signature = (sql, *params))]
    fn fetch_one_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, &self.handle, FetchMode::One, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_all_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, &self.handle, FetchMode::All, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, &self.handle, FetchMode::Scalar, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn execute_async<'py>(&self, py: Python<'py>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, &self.handle, FetchMode::Execute, sql, params)
    }
}

/// Refuses `PgPool` in a process-lane worker, a process apart from the server's pool.
fn refuse_in_lane_worker() -> PyResult<()> {
    mipyre_process::refuse_in_lane_worker("PgPool")
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Refuses `*_async` outside the main interpreter with a `NotImplementedError` that names the
/// sync method and `runtime="main"`.
fn refuse_async_in_worker(py: Python<'_>, fetch: FetchMode) -> PyResult<()> {
    if mipyre_subinterp::attach::on_main(py) {
        return Ok(());
    }
    let name = fetch.name();
    Err(PyNotImplementedError::new_err(format!(
        "{name}_async is not available in sub-interpreter workers; \
         use {name} (sync) or route with runtime='main'"
    )))
}
