//! `HTTPException`: a failure whose raiser chose what the other side is told, answered with its
//! status, its headers and `{"detail": …}` wherever the request runs.
//!
//! The engine both raises it (a body `req.json()` cannot decode) and answers it, so it is defined
//! here, beside the dispatcher that recognizes it, as `BodyReadError` is.

use bytes::Bytes;
use hyper::header::{HeaderName, HeaderValue};
use hyper::StatusCode;
use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyString};

use mipyre_http::plan::response::{HandlerResponse, HeaderLines, JSON, MAX_HANDLER_LINES};

/// What its raiser chose to tell the other side: a 4xx or 5xx status, any JSON-encodable
/// `detail` and response headers. The FastAPI signature and attribute names, so a handler
/// written for one raises it unchanged.
#[pyclass(module = "mipyre.engine", extends = PyException, subclass, frozen, name = "HTTPException")]
pub(crate) struct HttpException {
    /// Within 400..=599: `new` refuses any other.
    status: StatusCode,
    detail: Py<PyAny>,
    headers: Vec<(HeaderName, HeaderValue)>,
}

#[pymethods]
impl HttpException {
    /// `detail=None` is the status's standard phrase (`http.HTTPStatus(status_code).phrase`,
    /// as Starlette does), or stays `None` for a status that has none.
    #[new]
    #[pyo3(signature = (status_code, detail = None, headers = None))]
    fn new(
        py: Python<'_>,
        status_code: i64,
        detail: Option<Py<PyAny>>,
        headers: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let status = error_status(status_code).ok_or_else(|| {
            PyValueError::new_err(format!(
                "HTTPException status_code must be a 4xx or 5xx status (400..=599), got {status_code}"
            ))
        })?;
        let detail = match detail {
            Some(detail) => detail,
            None => standard_phrase(py, status)?,
        };
        let headers = match headers {
            Some(given) => header_lines(given)?,
            None => Vec::new(),
        };
        Ok(HttpException {
            status,
            detail,
            headers,
        })
    }

    /// `__new__` made it whole; this takes the same arguments, because `BaseException`'s own
    /// `__init__` refuses keyword arguments, and sets `args` to `(status_code, detail)` as
    /// Starlette's has them.
    #[pyo3(signature = (status_code, detail = None, headers = None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        status_code: &Bound<'_, PyAny>,
        detail: Option<&Bound<'_, PyAny>>,
        headers: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (status_code, detail, headers);
        slf.get().set_args(slf.as_any())
    }

    #[getter]
    fn status_code(&self) -> u16 {
        self.status.as_u16()
    }

    #[getter]
    fn detail(&self, py: Python<'_>) -> Py<PyAny> {
        self.detail.clone_ref(py)
    }

    /// A new `dict` on every read, so changing it changes no answer.
    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, value) in &self.headers {
            dict.set_item(name.as_str(), String::from_utf8_lossy(value.as_bytes()))?;
        }
        Ok(dict)
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!("{}: {}", self.status.as_u16(), self.detail.bind(py).str()?))
    }

    fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        let me = slf.get();
        Ok(format!(
            "{}(status_code={}, detail={})",
            slf.get_type().name()?,
            me.status.as_u16(),
            me.detail.bind(slf.py()).repr()?
        ))
    }
}

impl HttpException {
    /// Raises a 400 whose detail is `reason`, caused by `cause` when there is one: a body the
    /// client sent that the handler's `req.json()` / `req.text()` cannot read.
    pub(crate) fn bad_request(py: Python<'_>, reason: String, cause: Option<PyErr>) -> PyErr {
        let raised = Bound::new(
            py,
            HttpException {
                status: StatusCode::BAD_REQUEST,
                detail: PyString::new(py, &reason).into_any().unbind(),
                headers: Vec::new(),
            },
        );
        let err = match raised.and_then(|raised| {
            raised.get().set_args(raised.as_any())?;
            Ok(raised)
        }) {
            Ok(raised) => PyErr::from_value(raised.into_any()),
            Err(failed) => failed,
        };
        err.set_cause(py, cause);
        err
    }

    fn set_args(&self, me: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = me.py();
        me.setattr(intern!(py, "args"), (self.status.as_u16(), self.detail.bind(py)))
    }

    /// The answer this exception asks for, its body encoded once here; a `detail` that is not
    /// JSON raises `TypeError`, caused by this exception, so its traceback shows where it was
    /// raised.
    pub(crate) fn answer(&self, py: Python<'_>, raised: &PyErr) -> PyResult<HandlerResponse> {
        let body = PyDict::new(py);
        body.set_item(intern!(py, "detail"), self.detail.bind(py))?;
        let encoded = crate::exchange::json::serializer(py)?
            .dumps(py, body.as_any())
            .map_err(|e| {
                let err = PyTypeError::new_err(format!(
                    "the detail of HTTPException({}) could not be encoded as JSON: {}",
                    self.status.as_u16(),
                    e.value(py)
                ));
                err.set_cause(py, Some(raised.clone_ref(py)));
                err
            })?;
        let body = Bytes::copy_from_slice(encoded.cast::<PyBytes>()?.as_bytes());
        let mut lines = HeaderLines::with_capacity(self.headers.len());
        let mut content_type = JSON;
        for (name, value) in &self.headers {
            if let Some(given) = lines.append(name.clone(), value.clone()) {
                content_type = given;
            }
        }
        Ok(HandlerResponse::new(self.status, body, content_type, lines))
    }
}

fn error_status(code: i64) -> Option<StatusCode> {
    let code = u16::try_from(code).ok().filter(|c| (400..=599).contains(c))?;
    StatusCode::from_u16(code).ok()
}

fn standard_phrase(py: Python<'_>, status: StatusCode) -> PyResult<Py<PyAny>> {
    let known = py
        .import(intern!(py, "http"))?
        .getattr(intern!(py, "HTTPStatus"))?
        .call1((status.as_u16(),));
    match known {
        Ok(known) => Ok(known.getattr(intern!(py, "phrase"))?.unbind()),
        // `HTTPStatus` raises ValueError for a status it has no name for (499): that status has
        // no standard phrase, and the detail is the `None` it was given.
        Err(e) if e.is_instance_of::<PyValueError>(py) => Ok(py.None()),
        Err(e) => Err(e),
    }
}

/// Checks each line where it is given: a name that is not a token, or a value with CR, LF or NUL
/// (response splitting), raises `ValueError` naming it.
fn header_lines(given: &Bound<'_, PyAny>) -> PyResult<Vec<(HeaderName, HeaderValue)>> {
    let mut lines = Vec::new();
    for item in given.call_method0(intern!(given.py(), "items"))?.try_iter()? {
        let (name, value): (String, String) = item?.extract()?;
        let name_ok = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| PyValueError::new_err(format!("HTTPException header name {name:?} is not an HTTP token")))?;
        let value_ok = HeaderValue::from_bytes(value.as_bytes()).map_err(|_| {
            PyValueError::new_err(format!(
                "HTTPException header {name:?} has a value with a character a header cannot carry \
                 (CR, LF, NUL or another control character): {value:?}"
            ))
        })?;
        if lines.len() == MAX_HANDLER_LINES {
            return Err(PyValueError::new_err(format!(
                "HTTPException headers: more than the {MAX_HANDLER_LINES} lines a response carries"
            )));
        }
        lines.push((name_ok, value_ok));
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_4xx_or_5xx_is_an_error_status() {
        for code in [400, 404, 499, 500, 599] {
            assert_eq!(error_status(code).map(|s| s.as_u16()), Some(code as u16), "{code}");
        }
        for code in [-1, 0, 200, 399, 600, 65536, i64::MAX] {
            assert_eq!(error_status(code), None, "{code}");
        }
    }
}
