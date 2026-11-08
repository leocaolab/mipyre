//! `GrpcError`: what a gRPC method's handler raises to end the call with a status of its choosing.

use pyo3::exceptions::{PyException, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;

use mipyre_http::plan::grpc::GrpcCode;

/// The failure a gRPC handler chose: a code other than `OK` and the details the caller reads as
/// `grpc-message`. The (code, details) pair is grpcio's `context.abort(code, details)`.
#[pyclass(module = "mipyre.engine", extends = PyException, subclass, frozen, name = "GrpcError")]
pub struct GrpcError {
    code: GrpcCode,
    details: Option<String>,
}

#[pymethods]
impl GrpcError {
    #[new]
    #[pyo3(signature = (code, details = None))]
    fn new(code: i64, details: Option<String>) -> PyResult<Self> {
        let code = failure_code(code).ok_or_else(|| {
            PyValueError::new_err(format!(
                "GrpcError code must be a gRPC status code other than OK (1..=16), got {code}"
            ))
        })?;
        Ok(GrpcError { code, details })
    }

    /// `__new__` made it whole; this takes the same arguments, because `BaseException`'s own
    /// `__init__` refuses keyword arguments, and sets `args` to `(code, details)`.
    #[pyo3(signature = (code, details = None))]
    fn __init__(slf: &Bound<'_, Self>, code: &Bound<'_, PyAny>, details: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        let _ = (code, details);
        let me = slf.get();
        slf.setattr(intern!(slf.py(), "args"), (me.code.number(), me.details.as_deref()))
    }

    #[getter]
    fn code(&self) -> u8 {
        self.code.number()
    }

    #[getter]
    fn details(&self) -> Option<&str> {
        self.details.as_deref()
    }

    fn __str__(&self) -> String {
        match &self.details {
            Some(details) => format!("{}: {details}", self.code.name()),
            None => self.code.name().to_string(),
        }
    }

    fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        let me = slf.get();
        let details = match &me.details {
            Some(details) => format!("{details:?}"),
            None => "None".to_string(),
        };
        Ok(format!("{}({}, {details})", slf.get_type().name()?, me.code.name()))
    }
}

impl GrpcError {
    pub(crate) fn status(&self) -> (GrpcCode, Option<String>) {
        (self.code, self.details.clone())
    }
}

/// Returns the code numbered `number` when a handler may end a call with it: any code but `OK`,
/// which is the status of a call that returned its reply.
fn failure_code(number: i64) -> Option<GrpcCode> {
    GrpcCode::of(number).filter(|code| *code != GrpcCode::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handler_ends_a_call_with_any_code_but_ok() {
        assert_eq!(failure_code(0), None);
        assert_eq!(failure_code(1), Some(GrpcCode::Cancelled));
        assert_eq!(failure_code(16), Some(GrpcCode::Unauthenticated));
        assert_eq!(failure_code(17), None);
    }
}
