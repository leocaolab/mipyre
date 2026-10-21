//! Defines the Python `Response` and converts its header lines to and from Python.

use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::StatusCode;
use pyo3::exceptions::{PyTypeError, PyValueError};
use mipyre_http::plan::response::HeaderLines;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid HTTP status {0}")]
pub(crate) struct InvalidStatus(pub(crate) u16);

impl From<InvalidStatus> for PyErr {
    fn from(e: InvalidStatus) -> PyErr {
        PyValueError::new_err(e.to_string())
    }
}

pub(crate) fn http_status(code: u16) -> Result<StatusCode, InvalidStatus> {
    StatusCode::from_u16(code).map_err(|_| InvalidStatus(code))
}

/// Reads `{name: str | list[str]}` as header lines, one line per list item.
///
/// # Errors
///
/// Raises `TypeError` for a non-`str` name or value and `ValueError` for an invalid one, each
/// naming the header.
pub(crate) fn response_headers_from_py(dict: &Bound<'_, PyDict>) -> PyResult<HeaderMap> {
    let mut map = HeaderMap::with_capacity(dict.len());
    each_header_line(dict, |name, value| {
        map.append(name, value);
    })?;
    Ok(map)
}

/// A `content-type` given among a `Response`'s `headers=` is the type the response goes out with.
#[derive(Clone, Debug)]
pub(crate) struct HeaderType {
    pub(crate) value: HeaderValue,
    /// This counts the other header names before it, so `Response.headers` shows it in its place.
    at: usize,
}

/// Sets the first `content-type` apart while the other lines keep the order given.
fn response_lines_from_py(dict: &Bound<'_, PyDict>) -> PyResult<(HeaderLines, Option<HeaderType>)> {
    let mut lines = HeaderLines::with_capacity(dict.len());
    let mut header_type = None;
    each_header_line(dict, |name, value| {
        if let Some(value) = lines.append(name, value) {
            let at = lines.as_map().keys_len();
            header_type.get_or_insert(HeaderType { value, at });
        }
    })?;
    Ok((lines, header_type))
}

fn each_header_line(
    dict: &Bound<'_, PyDict>,
    mut put: impl FnMut(HeaderName, HeaderValue),
) -> PyResult<()> {
    for (key, value) in dict.iter() {
        let key = key.cast::<PyString>().map_err(|_| {
            PyTypeError::new_err(format!(
                "response header name must be str, got {}",
                type_name(&key)
            ))
        })?;
        let key = key.to_str()?;
        let name = header_name(key)?;
        for item in header_items(key, &value)? {
            put(name.clone(), header_value(key, str_item(key, &item)?)?);
        }
    }
    Ok(())
}

pub(crate) fn response_headers_to_py<'py>(
    map: &HeaderMap,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyDict>> {
    lines_to_py(map, None, py)
}

fn lines_to_py<'py>(
    map: &HeaderMap,
    header_type: Option<&HeaderType>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let put_type = |at: usize| -> PyResult<()> {
        match header_type {
            Some(t) if t.at == at => dict.set_item("content-type", header_text(&t.value)?),
            _ => Ok(()),
        }
    };
    for (at, name) in map.keys().enumerate() {
        put_type(at)?;
        let lines = map
            .get_all(name)
            .iter()
            .map(header_text)
            .collect::<PyResult<Vec<_>>>()?;
        match lines.as_slice() {
            [one] => dict.set_item(name.as_str(), *one)?,
            many => dict.set_item(name.as_str(), many)?,
        }
    }
    put_type(map.keys_len())?;
    Ok(dict)
}

fn header_items<'py>(key: &str, value: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if let Ok(list) = value.cast::<PyList>() {
        return Ok(list.iter().collect());
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return Ok(tuple.iter().collect());
    }
    str_item(key, value)?;
    Ok(vec![value.clone()])
}

fn str_item<'a>(key: &str, item: &'a Bound<'_, PyAny>) -> PyResult<&'a str> {
    let text = item.cast::<PyString>().map_err(|_| {
        PyTypeError::new_err(format!(
            "response header {key:?}: value must be str or a list of str, got {}",
            type_name(item)
        ))
    })?;
    text.to_str()
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    match obj.get_type().name() {
        Ok(name) => name.to_string(),
        Err(e) => format!("<type name unavailable: {e}>"),
    }
}

/// Raises `ValueError` naming an invalid header name.
pub(crate) fn header_name(name: &str) -> PyResult<HeaderName> {
    HeaderName::from_bytes(name.as_bytes())
        .map_err(|e| PyValueError::new_err(format!("invalid header name {name:?}: {e}")))
}

/// Raises `ValueError` naming the header for a value with CR, LF, NUL or another control byte.
pub(crate) fn header_value(name: &str, value: &str) -> PyResult<HeaderValue> {
    HeaderValue::from_str(value).map_err(|e| {
        PyValueError::new_err(format!("header {name:?}: invalid value {value:?}: {e}"))
    })
}

/// These content types are kept as static values, so a `Response` with one copies nothing and
/// every clone of it is free.
const COMMON_CONTENT_TYPES: &[&str] = &[
    "text/plain",
    "text/plain; charset=utf-8",
    "text/html",
    "text/html; charset=utf-8",
    "text/css",
    "text/csv",
    "text/javascript",
    "text/event-stream",
    "text/xml",
    "application/json",
    "application/javascript",
    "application/octet-stream",
    "application/xml",
    "application/x-www-form-urlencoded",
    "application/msgpack",
    "application/x-msgpack",
    "image/png",
    "image/jpeg",
    "image/svg+xml",
];

fn content_type_value(ct: &str) -> PyResult<HeaderValue> {
    match COMMON_CONTENT_TYPES.iter().find(|common| **common == ct) {
        Some(common) => Ok(HeaderValue::from_static(common)),
        None => header_value("content_type", ct),
    }
}

/// Prefers a `Content-Type` in `headers` over `given`, and is what `content_type` reads back, so
/// Python sees the type that is sent.
pub(crate) fn sent_content_type<'a>(
    headers: &'a HeaderMap,
    given: Option<&'a HeaderValue>,
) -> Option<&'a HeaderValue> {
    headers.get(hyper::header::CONTENT_TYPE).or(given)
}

pub(crate) fn header_text(value: &HeaderValue) -> PyResult<&str> {
    std::str::from_utf8(value.as_bytes())
        .map_err(|e| PyValueError::new_err(format!("header value is not UTF-8: {e}")))
}

#[pyclass(frozen, name = "Response", module = "mipyre.engine")]
pub(crate) struct Response {
    #[pyo3(get)]
    pub(crate) body: Py<PyAny>,
    pub(crate) status: StatusCode,
    /// The type is `None` when it comes from the body, and `header_type` overrides it.
    pub(crate) content_type: Option<HeaderValue>,
    pub(crate) header_type: Option<HeaderType>,
    /// These are every header line except the `Content-Type`, in the order given.
    pub(crate) headers: HeaderLines,
}

impl Response {
    /// Returns `None` when the type comes from the body.
    pub(crate) fn sent_content_type(&self) -> Option<&HeaderValue> {
        match &self.header_type {
            Some(t) => Some(&t.value),
            None => self.content_type.as_ref(),
        }
    }
}

#[pymethods]
impl Response {
    /// Creates a response; `headers` maps a name to a `str`, or to a list of `str` for a header
    /// sent on several lines such as `Set-Cookie`, and a `Content-Type` or `Server` in it
    /// replaces the default.
    #[new]
    #[pyo3(signature = (body, status_code=200, content_type=None, headers=None))]
    fn new(
        body: Py<PyAny>,
        status_code: u16,
        content_type: Option<&str>,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let status = http_status(status_code)?;
        let content_type = content_type.map(content_type_value).transpose()?;
        let (headers, header_type) = headers
            .map(response_lines_from_py)
            .transpose()?
            .unwrap_or_default();
        Ok(Response {
            body,
            status,
            content_type,
            header_type,
            headers,
        })
    }

    #[getter]
    fn status_code(&self) -> u16 {
        self.status.as_u16()
    }

    /// Returns a `Content-Type` from `headers`, else `content_type`, or `None` when the type comes from the body.
    #[getter]
    fn content_type(&self) -> PyResult<Option<&str>> {
        self.sent_content_type().map(header_text).transpose()
    }

    /// Returns `{name: str}`, or a list for a header on several lines, with names lower-cased as on the wire.
    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        lines_to_py(self.headers.as_map(), self.header_type.as_ref(), py)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_status_is_refused_where_python_hands_it_over() {
        assert_eq!(http_status(1000), Err(InvalidStatus(1000)));
        assert!(InvalidStatus(1000)
            .to_string()
            .contains("invalid HTTP status 1000"));
        assert_eq!(http_status(201).unwrap(), StatusCode::CREATED);
    }
}
