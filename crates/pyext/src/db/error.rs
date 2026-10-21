//! Defines the Postgres layer's typed failures and the Python exceptions they become.
//!
//! A failed query raises `DatabaseError` (a `RuntimeError`) with the server's SQLSTATE in
//! `.sqlstate`, or `None` when the server was never reached. Integrity violations (class 23)
//! raise `IntegrityError` and duplicate keys (23505) its subclass `UniqueViolation`, so a handler
//! can answer 409 without parsing message text. A value its parameter can't take raises
//! `ParamError`, a subclass of both `TypeError` and `ValueError`, before the query is sent, so a
//! handler can answer 422 without catching every `TypeError`. A failed `PgPool.connect()` raises
//! `ConnectionError`, also with `.sqlstate`.

use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PerInterpreterCell;
use pyo3::types::{PyTuple, PyType};

use super::numeric::NumericError;
use super::pool::PoolSettingDifferences;
use super::query::QueryMethod;

pyo3::create_exception!(
    mipyre.engine,
    DatabaseError,
    PyRuntimeError,
    "A query failed. `sqlstate` is the server's SQLSTATE code, or None when the server never reported one."
);
pyo3::create_exception!(
    mipyre.engine,
    IntegrityError,
    DatabaseError,
    "An integrity constraint was violated (SQLSTATE class 23)."
);
pyo3::create_exception!(
    mipyre.engine,
    UniqueViolation,
    IntegrityError,
    "A unique or primary-key constraint was violated (SQLSTATE 23505)."
);

const INTEGRITY_CLASS: &str = "23";
const UNIQUE_VIOLATION: &str = "23505";

#[derive(Debug, thiserror::Error)]
pub(crate) enum DbError {
    #[error(transparent)]
    SettingsDiffer(#[from] PoolSettingDifferences),
    #[error("PgPool connect: {0}")]
    ConnectFailed(#[source] sqlx::Error),
    #[error("{op}: {source}")]
    QueryFailed {
        op: QueryMethod,
        #[source]
        source: sqlx::Error,
    },
    /// Postgres always describes the parameters' types, so this means the server or driver
    /// broke that promise; `count` is the parameter count it did give, if any.
    #[error(
        "{op}: the server did not describe the statement's parameter types (count: {count:?})"
    )]
    Undescribed {
        op: QueryMethod,
        count: Option<usize>,
    },
    #[error(transparent)]
    Param(#[from] ParamError),
    #[error(transparent)]
    Task(#[from] TaskError),
}

impl DbError {
    /// Returns the SQLSTATE the server reported, or `None` if the failure never reached it.
    pub(crate) fn sqlstate(&self) -> Option<String> {
        match self {
            Self::ConnectFailed(source) | Self::QueryFailed { source, .. } => source
                .as_database_error()
                .and_then(|e| e.code())
                .map(|code| code.into_owned()),
            _ => None,
        }
    }

    pub(crate) fn into_pyerr(self, py: Python<'_>) -> PyErr {
        let message = self.to_string();
        match self {
            Self::Task(_) => PyRuntimeError::new_err(message),
            Self::SettingsDiffer(_) => PyValueError::new_err(message),
            // The SQLSTATE is set when the server answered (28P01 bad password, 3D000 no such
            // database) and is `None` when it was never reached (refused, DNS, TLS).
            Self::ConnectFailed(_) => {
                with_sqlstate(py, PyConnectionError::new_err(message), self.sqlstate())
            }
            // The statement's arity is the caller's code, not a value, so it is no ParamError.
            Self::Param(ParamError::WrongCount { .. }) => PyTypeError::new_err(message),
            // If the class cannot be made, the error is still raised as a TypeError with the
            // parameter's own text, and the failure to make the class is its cause.
            Self::Param(_) => match param_error_type(py) {
                Ok(ty) => PyErr::from_type(ty.clone(), message),
                Err(class_failed) => {
                    let err = PyTypeError::new_err(message);
                    err.set_cause(py, Some(class_failed));
                    err
                }
            },
            Self::QueryFailed { .. } => database_error(py, message, self.sqlstate()),
            Self::Undescribed { .. } => database_error(py, message, None),
        }
    }
}

/// Builds the `DatabaseError` subclass that `sqlstate` selects, with `.sqlstate` set.
fn database_error(py: Python<'_>, message: String, sqlstate: Option<String>) -> PyErr {
    let err = match sqlstate.as_deref() {
        Some(UNIQUE_VIOLATION) => UniqueViolation::new_err(message),
        Some(code) if code.starts_with(INTEGRITY_CLASS) => IntegrityError::new_err(message),
        _ => DatabaseError::new_err(message),
    };
    with_sqlstate(py, err, sqlstate)
}

fn with_sqlstate(py: Python<'_>, err: PyErr, sqlstate: Option<String>) -> PyErr {
    match err.value(py).setattr("sqlstate", sqlstate) {
        Ok(()) => err,
        Err(set_failed) => {
            set_failed.set_cause(py, Some(err));
            set_failed
        }
    }
}

/// Returns this interpreter's `mipyre.engine.ParamError`, built by hand because
/// `create_exception!` takes one base and the class subclasses both `TypeError` and
/// `ValueError`, so code written against either base still catches it.
fn param_error_type(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static TYPE_OBJECT: PerInterpreterCell<Py<PyType>> = PerInterpreterCell::new();
    let ty = TYPE_OBJECT.get_or_try_init(py, || {
        let bases = PyTuple::new(
            py,
            [py.get_type::<PyTypeError>(), py.get_type::<PyValueError>()],
        )?;
        // SAFETY: The thread is attached, both strings are NUL-terminated, and `bases` is a
        // tuple of exception classes; the call returns a new reference or NULL with an
        // exception set, which `from_owned_ptr_or_err` takes.
        unsafe {
            Bound::from_owned_ptr_or_err(
                py,
                pyo3::ffi::PyErr_NewExceptionWithDoc(
                    c"mipyre.engine.ParamError".as_ptr(),
                    c"A value a statement parameter can't take (wrong type, out of range, not \
                      encodable), refused before the query is sent. Both a TypeError and a \
                      ValueError."
                        .as_ptr(),
                    bases.as_ptr(),
                    std::ptr::null_mut(),
                ),
            )
        }
        .and_then(|t| Ok(t.cast_into::<PyType>()?.unbind()))
    })?;
    Ok(ty.bind(py))
}

/// Adds the exception classes to the engine module and sets `sqlstate = None` on the base
/// class, so an instance raised by Python code also has the attribute.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let base = py.get_type::<DatabaseError>();
    base.setattr("sqlstate", py.None())?;
    m.add("DatabaseError", base)?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("UniqueViolation", py.get_type::<UniqueViolation>())?;
    m.add("ParamError", param_error_type(py)?)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParamError {
    #[error("the statement takes {expected} parameter(s), {given} given")]
    WrongCount { expected: usize, given: usize },

    #[error("parameter ${index} is {pg_type}; a Python {given} cannot be sent as {pg_type}")]
    TypeNotAccepted {
        index: usize,
        pg_type: String,
        given: &'static str,
    },
    #[error("parameter ${index}: {problem}")]
    ValueDoesNotFit {
        index: usize,
        problem: ParamValueError,
    },
    #[error("parameter ${index}: {source}")]
    NotEncodable {
        index: usize,
        #[source]
        source: sqlx::error::BoxDynError,
    },
    #[error(
        "parameter ${index}: unsupported parameter type {python_type} (supported: int, \
         float, str, bool, bytes, None, dict, list, datetime.date, datetime.datetime, \
         uuid.UUID, decimal.Decimal)"
    )]
    Unsupported { index: usize, python_type: String },
    /// Reading the value out of Python raised, as for a `str` with a lone surrogate or a
    /// `dict` that isn't JSON.
    #[error("parameter ${index}: {source}")]
    Unreadable {
        index: usize,
        #[source]
        source: PyErr,
    },
}

/// The value's Python type is accepted, but the value still does not fit its parameter.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ParamValueError {
    /// `value` is the text Python shows for it, so an int of any size fits.
    #[error("{value} is out of range for {pg_type}")]
    OutOfRange {
        value: String,
        pg_type: &'static str,
    },
    #[error(transparent)]
    Numeric(#[from] NumericError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error("mipyre-db task panicked: {0}")]
    Panicked(String),
    #[error("mipyre-db task was cancelled")]
    Cancelled,
    #[error("mipyre-db runtime shut down before the task finished")]
    RuntimeGone,
    /// The OS refused to build the runtime (threads or fds exhausted). Nothing ran, and the
    /// next query tries to build it again.
    #[error("mipyre-db runtime could not be built: {0}")]
    RuntimeNotBuilt(#[source] std::io::Error),
}

impl From<tokio::task::JoinError> for TaskError {
    fn from(e: tokio::task::JoinError) -> Self {
        match e.try_into_panic() {
            Ok(payload) => Self::Panicked(mipyre_tpc::panic_message(&*payload)),
            Err(_cancelled) => Self::Cancelled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::FetchMode;

    /// A full pool times out (sqlx's `PoolTimedOut`) before the server sees the query, so it is
    /// a `DatabaseError` named for the call with `sqlstate` None, not a `ConnectionError` (the
    /// pool is connected) nor one of the integrity classes.
    #[test]
    fn a_pool_timeout_is_a_database_error_without_sqlstate() {
        let timed_out = DbError::QueryFailed {
            op: QueryMethod::Answer(FetchMode::Scalar),
            source: sqlx::Error::PoolTimedOut,
        };
        assert_eq!(timed_out.sqlstate(), None);
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let err = timed_out.into_pyerr(py);
            assert!(err.is_instance_of::<DatabaseError>(py), "{err}");
            assert!(!err.is_instance_of::<IntegrityError>(py), "{err}");
            assert!(!err.is_instance_of::<PyConnectionError>(py), "{err}");
            let value = err.value(py);
            assert!(value.getattr("sqlstate").unwrap().is_none());
            let text = value.to_string();
            assert!(text.starts_with("fetch_scalar: "), "{text}");
            assert!(text.contains("timed out"), "{text}");
        });
    }
}
