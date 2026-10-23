//! Prepares and binds one statement, and runs the queries whose answer comes back whole.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;
use sqlx::postgres::{PgArguments, PgConnection, PgRow};
use sqlx::{AssertSqlSafe, Either, Executor, Postgres, SqlSafeStr as _, SqlStr, Statement as _};

use super::cell;
use super::error::DbError;
use super::param::{self, PyParam};

/// Owns a statement and its arguments so the query can run on the DB runtime.
pub(super) struct SqlQuery {
    pub(super) sql: SqlStr,
    pub(super) params: Vec<PyParam>,
}

impl SqlQuery {
    pub(super) fn from_py(py: Python<'_>, sql: String, params: &[Py<PyAny>]) -> PyResult<Self> {
        let params = (1..)
            .zip(params)
            .map(|(index, p)| PyParam::extract(p.bind(py), index))
            .collect::<Result<_, _>>()
            .map_err(|e| DbError::from(e).into_pyerr(py))?;
        // The statement is the handler author's text; values reach the server only as bound
        // parameters. Arc-backed, so handing it to both prepare and the query is a refcount bump.
        let sql = AssertSqlSafe(Arc::<str>::from(sql)).into_sql_str();
        Ok(Self { sql, params })
    }
}

/// Is the `PgPool` method a statement ran through; its failures start with the method's name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QueryMethod {
    /// `fetch_one`, `fetch_all`, `fetch_scalar` or `execute`, whose answer comes back whole.
    Answer(FetchMode),
    /// `fetch_iter`, whose rows come back one at a time through a `PgCursor`.
    Cursor,
}

impl std::fmt::Display for QueryMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Answer(fetch) => fetch.name(),
            Self::Cursor => "fetch_iter",
        })
    }
}

/// Is the shape of a whole answer: one row, every row, one value, or the rows affected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FetchMode {
    One,
    All,
    Scalar,
    Execute,
}

impl FetchMode {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::One => "fetch_one",
            Self::All => "fetch_all",
            Self::Scalar => "fetch_scalar",
            Self::Execute => "execute",
        }
    }
}

/// Prepares `sql` on `conn` (cached per connection after the first time) and encodes
/// `params` as the parameter types the server declared for it.
pub(super) async fn bind(
    conn: &mut PgConnection,
    op: QueryMethod,
    sql: SqlStr,
    params: Vec<PyParam>,
) -> Result<sqlx::query::Query<'static, Postgres, PgArguments>, DbError> {
    let prepared = conn
        .prepare(sql.clone())
        .await
        .map_err(|source| DbError::QueryFailed { op, source })?;
    // Postgres always describes parameter types; without them nothing can be encoded.
    let declared = match prepared.parameters() {
        Some(Either::Left(types)) => types,
        Some(Either::Right(count)) => return Err(DbError::Undescribed { op, count: Some(count) }),
        None => return Err(DbError::Undescribed { op, count: None }),
    };
    let args = param::encode_args(declared, params)?;
    Ok(sqlx::query_with(sql, args))
}

pub(super) enum QueryAnswer {
    Row(Option<PgRow>),
    Rows(Vec<PgRow>),
    Scalar(PgRow),
    Affected(u64),
}

impl QueryAnswer {
    pub(super) fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self {
            Self::Row(Some(row)) => cell::row_to_dict(py, &row)?.into_any(),
            Self::Row(None) => py.None(),
            Self::Rows(rows) => {
                let list = PyList::empty(py);
                for row in &rows {
                    list.append(cell::row_to_dict(py, row)?)?;
                }
                list.into_any().unbind()
            }
            Self::Scalar(row) => cell::column_value(py, &row, 0)?,
            Self::Affected(n) => n.into_pyobject(py)?.into_any().unbind(),
        })
    }
}

pub(super) async fn run(pool: sqlx::PgPool, stmt: SqlQuery, fetch: FetchMode) -> Result<QueryAnswer, DbError> {
    let op = QueryMethod::Answer(fetch);
    let query_error = |source| DbError::QueryFailed { op, source };
    let mut conn = pool.acquire().await.map_err(query_error)?;
    let query = bind(&mut conn, op, stmt.sql, stmt.params).await?;
    match fetch {
        FetchMode::One => query.fetch_optional(&mut *conn).await.map(QueryAnswer::Row),
        FetchMode::All => query.fetch_all(&mut *conn).await.map(QueryAnswer::Rows),
        FetchMode::Scalar => query.fetch_one(&mut *conn).await.map(QueryAnswer::Scalar),
        FetchMode::Execute => query
            .execute(&mut *conn)
            .await
            .map(|done| QueryAnswer::Affected(done.rows_affected())),
    }
    .map_err(query_error)
}
