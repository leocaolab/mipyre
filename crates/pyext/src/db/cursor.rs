//! Streams a result set to Python row by row through a bounded channel, as `PgPool.fetch_iter`.

use futures_util::TryStreamExt;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use sqlx::postgres::PgRow;

use super::cell;
use mipyre_pg::{DbRuntime, OpenPool, TaskError};

use super::error::DbError;
use super::query::{bind, QueryMethod, SqlQuery};

/// Bounds the rows in flight to a `PgCursor` while prefetching enough to hide the server round
/// trip from a fast consumer.
const CURSOR_CAPACITY: usize = 8;

/// Carries a row, or the error the driver stopped on as its last message; the channel closing
/// is the end of the stream.
type CursorItem = Result<PgRow, DbError>;

/// Runs the statement and sends its rows to the cursor, then an error if one occurred.
async fn drive_cursor(pool: sqlx::PgPool, stmt: SqlQuery, tx: tokio::sync::mpsc::Sender<CursorItem>) {
    if let Err(e) = stream_rows(&pool, stmt, &tx).await {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a closed channel means the cursor was dropped, and nobody wants the error"
        )]
        let _ = tx.send(Err(e)).await;
    }
}

async fn stream_rows(
    pool: &sqlx::PgPool,
    stmt: SqlQuery,
    tx: &tokio::sync::mpsc::Sender<CursorItem>,
) -> Result<(), DbError> {
    let op = QueryMethod::Cursor;
    let query_error = |source| DbError::QueryFailed { op, source };
    let mut conn = pool.acquire().await.map_err(query_error)?;
    let query = bind(&mut conn, op, stmt.sql, stmt.params).await?;
    let streamed = {
        let mut rows = query.fetch(&mut *conn);
        loop {
            match rows.try_next().await {
                Ok(Some(row)) => {
                    if tx.send(Ok(row)).await.is_err() {
                        break Ok(StreamEnd::Abandoned);
                    }
                }
                Ok(None) => break Ok(StreamEnd::AllRead),
                Err(e) => break Err(e),
            }
        }
    };
    match ResultEnd::of(&streamed).release() {
        Release::ToPool => {}
        // sqlx closes the connection from its own task and keeps the pool's permit until it
        // has, so the pool never exceeds `max_connections`.
        Release::Close => conn.close_on_drop(),
    }
    drop(conn);
    streamed.map(drop).map_err(query_error)
}

/// Says how the loop over a result's rows stopped without an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamEnd {
    AllRead,
    /// Means the cursor was dropped before the stream ended.
    Abandoned,
}

/// Says where a streamed result stands on its connection when the connection is let go. The
/// server never says how many rows a result has, so only whether the end was reached is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResultEnd {
    /// Means the server sent every row and its ReadyForQuery is next.
    AllRead,
    /// Means the server ended the statement with its error and its ReadyForQuery is next.
    ServerError,
    /// Means the stream failed on the client side, so what is still on the wire is unknown.
    StreamFault,
    /// Means the cursor was dropped early, so the server may still send any number of rows.
    Abandoned,
}

impl ResultEnd {
    fn of(streamed: &Result<StreamEnd, sqlx::Error>) -> Self {
        match streamed {
            Ok(StreamEnd::AllRead) => Self::AllRead,
            Ok(StreamEnd::Abandoned) => Self::Abandoned,
            Err(sqlx::Error::Database(_)) => Self::ServerError,
            Err(_) => Self::StreamFault,
        }
    }

    /// Decides whether the connection goes back to the pool. sqlx readies a returned connection
    /// by reading whatever the server still sends, which took 1.9 s for 10 million rows and
    /// never ends for an endless result while holding a pool slot, so only a connection with
    /// nothing but ReadyForQuery left goes back. Closing it ends that backend on its next write.
    fn release(self) -> Release {
        match self {
            Self::AllRead | Self::ServerError => Release::ToPool,
            Self::StreamFault | Self::Abandoned => Release::Close,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Release {
    ToPool,
    Close,
}

/// Iterates a result set row by row, as returned by `PgPool.fetch_iter(sql, ...)`.
///
/// It holds a bounded number of rows at a time, where `fetch_all` holds every row twice during
/// conversion. A slow consumer applies backpressure to the query. Dropping the cursor before
/// the end closes its connection instead of returning it to the pool, because returning it
/// would hold a pool slot until the server had sent the rest of the result.
///
/// Threads sharing a cursor wait on its lock with the GIL released, and the holder never needs
/// the GIL, so the lock and the GIL cannot deadlock.
#[pyclass(module = "mipyre.engine")]
pub(super) struct PgCursor {
    rows: crate::sync::Mutex<CursorRows>,
}

/// The runtime the cursor's query streams on, which waiting for its next batch runs on too.
struct CursorRuntime(std::sync::Arc<DbRuntime>);

/// Holds a cursor's row channel and the messages read from it but not yet taken.
struct CursorRows {
    runtime: CursorRuntime,
    /// Is `None` once the stream has ended, whether the channel closed or waiting on it failed.
    rx: Option<tokio::sync::mpsc::Receiver<CursorItem>>,
    buf: std::collections::VecDeque<CursorItem>,
}

impl CursorRows {
    /// Returns the next message, waiting for the next batch if none is buffered; call it with
    /// the GIL released. It returns `None` at the end of the stream.
    ///
    /// # Errors
    ///
    /// Returns [`TaskError`] when the wait fails, which also ends the stream because the
    /// receiver went with it.
    fn next(&mut self) -> Result<Option<CursorItem>, TaskError> {
        if let Some(msg) = self.buf.pop_front() {
            return Ok(Some(msg));
        }
        let Some(rx) = self.rx.take() else {
            return Ok(None);
        };
        let (rx, batch) = self.runtime.0.recv_batch(rx, CURSOR_CAPACITY)?;
        // An empty batch means the channel closed, so the receiver is let go.
        if !batch.is_empty() {
            self.rx = Some(rx);
        }
        self.buf.extend(batch);
        Ok(self.buf.pop_front())
    }
}

impl PgCursor {
    /// Starts streaming `stmt`'s rows from `open`'s pool on its runtime.
    pub(super) fn open(open: OpenPool, stmt: SqlQuery) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(CURSOR_CAPACITY);
        open.runtime.spawn(drive_cursor(open.pool, stmt, tx));
        Self {
            rows: crate::sync::Mutex::new(CursorRows {
                runtime: CursorRuntime(open.runtime),
                rx: Some(rx),
                buf: std::collections::VecDeque::new(),
            }),
        }
    }

    /// Returns the next message, taking a buffered row without releasing the GIL when the lock
    /// is free and otherwise waiting with the GIL released.
    ///
    /// # Errors
    ///
    /// Returns [`TaskError`] when waiting for the channel fails.
    fn next_message(&self, py: Python<'_>) -> Result<Option<CursorItem>, TaskError> {
        if let Some(mut rows) = self.rows.try_lock() {
            if let Some(msg) = rows.buf.pop_front() {
                return Ok(Some(msg));
            }
        }
        py.detach(|| self.rows.lock().next())
    }
}

#[pymethods]
impl PgCursor {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Return the next row as a dict; raise `StopIteration` at the end and `DatabaseError` on a
    /// database error.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        match self.next_message(py).map_err(|e| DbError::from(e).into_pyerr(py))? {
            Some(Ok(row)) => cell::row_to_dict(py, &row),
            Some(Err(e)) => Err(e.into_pyerr(py)),
            None => Err(PyStopIteration::new_err(py.None())),
        }
    }

    /// Return every remaining row as a list of dicts, as `fetch_all` does.
    fn to_list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        loop {
            match self.__next__(py) {
                Ok(row) => list.append(row)?,
                Err(e) if e.is_instance_of::<PyStopIteration>(py) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(list)
    }
}

/// A streamed result's connection goes back to the pool only when the result reached its end.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_result_read_to_its_end_returns_the_connection() {
        let end = ResultEnd::of(&Ok(StreamEnd::AllRead));
        assert_eq!(end, ResultEnd::AllRead);
        assert_eq!(end.release(), Release::ToPool);
    }

    #[test]
    fn a_cursor_dropped_before_the_end_closes_the_connection() {
        let end = ResultEnd::of(&Ok(StreamEnd::Abandoned));
        assert_eq!(end, ResultEnd::Abandoned);
        assert_eq!(end.release(), Release::Close);
    }

    #[test]
    fn a_server_error_returns_the_connection() {
        assert_eq!(ResultEnd::ServerError.release(), Release::ToPool);
    }

    #[test]
    fn a_client_side_stream_failure_closes_the_connection() {
        let io = Err(sqlx::Error::Io(std::io::Error::other("connection reset")));
        assert_eq!(ResultEnd::of(&io), ResultEnd::StreamFault);
        let protocol = Err(sqlx::Error::Protocol("unexpected message".into()));
        assert_eq!(ResultEnd::of(&protocol), ResultEnd::StreamFault);
        assert_eq!(ResultEnd::StreamFault.release(), Release::Close);
    }
}
