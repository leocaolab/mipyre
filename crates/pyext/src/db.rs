//! Gives Python handlers Postgres pools backed by sqlx, one pool per DSN per process.
//!
//! Every handler that connects to one DSN, on any interpreter, shares its pool, though each
//! worker runs the app script's `connect()`. A later `connect()` to an open DSN reuses its
//! pool only when every setting it names matches, and raises `ValueError` naming the one that
//! differs.
//!
//! Every pool runs on one dedicated tokio runtime, not the server's, so DB I/O stays off the
//! accept loop. Sync calls wait for it on a std channel with the GIL released, never with
//! `Runtime::block_on`, which panics on a thread already inside a Tokio context such as a TPC
//! worker. The `*_async` methods work on the main interpreter only.
//!
//! Columns of a type outside the decoded set reach Python as their raw binary wire bytes.

mod awaitable;
mod build_slot;
mod cell;
mod cursor;
mod dsn;
mod error;
mod kind;
mod numeric;
mod param;
mod pool;
mod py_types;
mod query;
mod runtime;

use std::num::NonZeroUsize;
use std::time::Duration;

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

use cursor::PgCursor;
use dsn::Dsn;
use error::DbError;
use pool::{connect_pool, Expiry, PoolRequest};
use query::{FetchMode, SqlQuery};

/// Adds the DB classes and exceptions to the engine module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PgPool>()?;
    m.add_class::<PgCursor>()?;
    error::register(m)
}

/// Runs the statement on the DB runtime while this thread waits with the GIL released.
fn run_blocking(
    py: Python<'_>,
    pool: &sqlx::PgPool,
    fetch: FetchMode,
    sql: String,
    params: Vec<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    refuse_in_lane_worker()?;
    let stmt = SqlQuery::from_py(py, sql, &params)?;
    let outcome = py
        .detach(|| runtime::run_on_db_rt(query::run(pool.clone(), stmt, fetch)))
        .map_err(DbError::from)
        .and_then(|done| done)
        .map_err(|e| e.into_pyerr(py))?;
    outcome.into_py(py)
}

/// Runs the statement on the DB runtime and returns an asyncio future for its result.
fn run_awaitable<'py>(
    py: Python<'py>,
    pool: &sqlx::PgPool,
    fetch: FetchMode,
    sql: String,
    params: Vec<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    refuse_async_in_worker(py, fetch)?;
    refuse_in_lane_worker()?;
    let stmt = SqlQuery::from_py(py, sql, &params)?;
    awaitable::await_on_loop(py, query::run(pool.clone(), stmt, fetch), |py, outcome: query::QueryAnswer| {
        outcome.into_py(py)
    })
}

/// Is a handle to a process-wide Postgres pool; every handle to one DSN is the same pool.
#[pyclass(frozen, module = "mipyre.engine")]
pub(crate) struct PgPool {
    pool: sqlx::PgPool,
}

#[pymethods]
impl PgPool {
    /// Open `dsn`'s pool, or return a handle to the one already open for it.
    ///
    /// A later call for the same DSN that names a setting its pool does not have raises
    /// ValueError; a setting left out matches whatever the pool has. On the first call
    /// `max_connections` and `acquire_timeout_secs` default to 10 and 30, and
    /// `max_lifetime_secs` and `idle_timeout_secs` default to 1800 and 300, with 0 meaning
    /// never. `runtime_threads` sizes the runtime every query runs on, defaulting to one thread
    /// per 4 CPUs and at least 2, and the first call fixes it. `ping_on_acquire` tests each
    /// connection with a round trip before handing it out.
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
                NonZeroUsize::new(n).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "runtime_threads must be at least 1, got 0",
                    )
                })
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
        let pool = connect_pool(py, request).map_err(|e| e.into_pyerr(py))?;
        Ok(PgPool { pool })
    }

    /// Fetch the first row as a dict, or None when there is none; extra rows are ignored.
    #[pyo3(signature = (sql, *params))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.pool, FetchMode::One, sql, params)
    }

    /// Fetch all matching rows into a list of dicts.
    #[pyo3(signature = (sql, *params))]
    fn fetch_all(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.pool, FetchMode::All, sql, params)
    }

    /// Fetch the first column of the first row, raising if there is no row; SQL NULL is None.
    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.pool, FetchMode::Scalar, sql, params)
    }

    /// Execute a statement and return the number of rows it affected.
    #[pyo3(signature = (sql, *params))]
    fn execute(&self, py: Python<'_>, sql: String, params: Vec<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        run_blocking(py, &self.pool, FetchMode::Execute, sql, params)
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
    fn fetch_iter(
        &self,
        py: Python<'_>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<PgCursor> {
        refuse_in_lane_worker()?;
        let stmt = SqlQuery::from_py(py, sql, &params)?;
        PgCursor::open(self.pool.clone(), stmt).map_err(|e| DbError::from(e).into_pyerr(py))
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
        run_awaitable(py, &self.pool, FetchMode::One, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_all_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, &self.pool, FetchMode::All, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn fetch_scalar_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, &self.pool, FetchMode::Scalar, sql, params)
    }

    #[pyo3(signature = (sql, *params))]
    fn execute_async<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        run_awaitable(py, &self.pool, FetchMode::Execute, sql, params)
    }
}

/// Refuses `PgPool` in a process-lane worker, which has only a forked copy of the server's pool.
fn refuse_in_lane_worker() -> PyResult<()> {
    crate::lane::refuse_in_lane_worker("PgPool")
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
