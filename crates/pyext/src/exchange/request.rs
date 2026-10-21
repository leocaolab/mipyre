//! Defines the Python `Request` a handler receives and `Headers`, its read-only view of the header fields.

use std::borrow::Cow;
use std::collections::HashMap;
use std::net::IpAddr;

use mipyre_http::io::body::BodyReceiver;
use mipyre_transport::driver::Peer;

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};

use super::response::{header_name, header_value};
use mipyre_http::io::request_id::RequestId;

#[pyclass(frozen, name = "Request", module = "mipyre.engine")]
pub(crate) struct Request {
    /// The head keeps hyper's parts as they were parsed; Python reads the headers through the
    /// `Headers` view, which converts only what it is asked for.
    pub(crate) head: crate::exchange::RequestHead,
    pub(crate) body: RequestBody,
}

/// A streamed body's receiver sits behind a lock because it is taken once from a `frozen`
/// pyclass that Python threads may share; the lock is taken once per request, uncontended.
pub(crate) enum RequestBody {
    Buffered(Bytes),
    /// The body is already the Python `bytes` that `req.body` hands out, because a process-lane
    /// worker copies it out of its slot once.
    Received(Py<PyBytes>),
    Streamed(crate::sync::Mutex<Option<BodyReceiver>>),
}

#[derive(Debug, thiserror::Error)]
#[error("req.stream was already taken: a streamed body is read through one stream")]
pub(crate) struct BodyStreamAlreadyTaken;

impl RequestBody {
    pub(crate) fn streamed(rx: BodyReceiver) -> Self {
        RequestBody::Streamed(crate::sync::Mutex::new(Some(rx)))
    }

    /// Returns `None` for a streamed body, which is read through its stream.
    pub(crate) fn bytes<'a>(&'a self, py: Python<'a>) -> Option<&'a [u8]> {
        match self {
            RequestBody::Buffered(bytes) => Some(bytes),
            RequestBody::Received(bytes) => Some(bytes.as_bytes(py)),
            RequestBody::Streamed(_) => None,
        }
    }

    /// Returns `None` for a buffered body, and the receiver only the first time for a streamed one.
    pub(crate) fn take_stream(&self) -> Result<Option<BodyReceiver>, BodyStreamAlreadyTaken> {
        match self {
            RequestBody::Buffered(_) | RequestBody::Received(_) => Ok(None),
            RequestBody::Streamed(slot) => slot.lock().take().map(Some).ok_or(BodyStreamAlreadyTaken),
        }
    }
}

const STREAMED_BODY: &str =
    "this route's body is streamed (stream=True): read it through req.stream, not req.body";

impl Request {
    fn query_str(&self) -> &str {
        self.head.parts.uri.query().unwrap_or("")
    }

    fn buffered_body<'a>(&'a self, py: Python<'a>) -> PyResult<&'a [u8]> {
        self.body
            .bytes(py)
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(STREAMED_BODY))
    }
}

#[pymethods]
impl Request {
    /// Creates a request the way the server does, with a fresh `request_id`.
    ///
    /// Raises `ValueError` naming whatever is invalid: a method, path or query that cannot form
    /// a request line, a header that is not a valid field, or a `client_ip` that is not an IP
    /// address.
    #[new]
    fn py_new(
        method: &str,
        path: &str,
        params: HashMap<String, String>,
        query: &str,
        body_bytes: Vec<u8>,
        headers: HashMap<String, String>,
        client_ip: &str,
    ) -> PyResult<Self> {
        let headers = headers
            .iter()
            .map(|(name, value)| Ok((header_name(name)?, header_value(name, value)?)))
            .collect::<PyResult<HeaderMap>>()?;
        let client_ip_addr = client_ip.parse::<IpAddr>().map_err(|e| {
            PyValueError::new_err(format!("client_ip {client_ip:?} is not an IP address: {e}"))
        })?;
        let target = if query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query}")
        };
        let (mut parts, ()) = hyper::Request::builder()
            .method(method)
            .uri(&target)
            .body(())
            .map_err(|e| {
                PyValueError::new_err(format!(
                    "method {method:?} with target {target:?} is not a valid request line: {e}"
                ))
            })?
            .into_parts();
        parts.headers = headers;
        let head = crate::exchange::request_head::RequestHead {
            parts,
            params: params.into_iter().collect(),
            client: Peer::Ip(client_ip_addr),
            request_id: RequestId::mint(),
        };
        Ok(Request::new(
            head,
            RequestBody::Buffered(Bytes::from(body_bytes)),
        ))
    }

    #[getter]
    fn method(&self) -> &str {
        self.head.parts.method.as_str()
    }

    #[getter]
    fn path(&self) -> &str {
        self.head.parts.uri.path()
    }

    /// Returns the raw query string without the `?`, or `""` when there is none.
    #[getter]
    fn query(&self) -> &str {
        self.query_str()
    }

    /// Returns the `host[:port]` the request was sent to, preferring the target's authority over
    /// `Host` as RFC 9112 §3.2.2 requires, or `None` with neither.
    #[getter]
    fn authority(&self) -> Option<Cow<'_, str>> {
        match self.head.parts.uri.authority() {
            Some(authority) => Some(Cow::Borrowed(authority.as_str())),
            None => self.head.parts.headers.get(hyper::header::HOST).map(field_text),
        }
    }

    /// Builds a new `dict` of the path params on every access.
    #[getter]
    fn params<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, value) in &self.head.params {
            dict.set_item(name, value)?;
        }
        Ok(dict)
    }

    /// Returns a read-only, case-insensitive view of the headers that copies nothing.
    #[getter]
    fn headers(slf: &Bound<'_, Self>) -> PyResult<Py<RequestHeaders>> {
        Py::new(
            slf.py(),
            RequestHeaders {
                req: slf.clone().unbind(),
            },
        )
    }

    /// Returns `None` for an in-memory connection, such as `TestClient`'s default, which has no peer address.
    #[getter]
    fn client_ip(&self) -> Option<String> {
        match self.head.client {
            Peer::Ip(ip) => Some(ip.to_string()),
            Peer::InMemory => None,
        }
    }

    /// Returns the id a 5xx body reports and the error log line carries: the client's own when
    /// the app enabled request ids and the client sent one, else one the server minted.
    #[getter]
    fn request_id(&self) -> String {
        self.head.request_id.to_string()
    }

    /// Raises `RuntimeError` on a `stream=True` route rather than hand back an empty body that
    /// was never sent.
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        match &self.body {
            RequestBody::Received(bytes) => Ok(bytes.bind(py).clone()),
            _ => Ok(PyBytes::new(py, self.buffered_body(py)?)),
        }
    }

    /// Returns the body's chunk iterator on a `stream=True` route and `None` on any other.
    ///
    /// A second access raises `RuntimeError`, so a hook that took the stream cannot silently leave
    /// the handler none.
    ///
    /// ```python
    /// for chunk in req.stream:
    ///     process(chunk)
    /// ```
    #[getter]
    fn stream(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<Py<crate::exchange::body_stream::BodyStream>>> {
        let rx = self
            .body
            .take_stream()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        rx.map(|rx| Py::new(py, crate::exchange::body_stream::BodyStream::new(rx)))
            .transpose()
    }

    /// Raises `ValueError` if the body is not UTF-8 and `RuntimeError` on a streamed body.
    fn text<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyString>> {
        let s = std::str::from_utf8(self.buffered_body(py)?)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(pyo3::types::PyString::new(py, s))
    }

    /// Decodes the body as JSON, keeping an integer of any size exact; raises `ValueError` if it
    /// is not JSON and `RuntimeError` on a streamed body.
    fn json<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        crate::exchange::json::loads(py, self.buffered_body(py)?)
    }

    /// Returns the first value on duplicate keys, as `query_params` does.
    fn query_param(&self, key: &str) -> Option<String> {
        form_urlencoded::parse(self.query_str().as_bytes())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
    }

    /// Builds a new `dict` on every access, keeping the first value on duplicate keys.
    #[getter(query_params)]
    fn py_query_params<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in form_urlencoded::parse(self.query_str().as_bytes()) {
            let key = PyString::new(py, &key);
            // SAFETY: The thread is attached and `dict`, `key` and the value are live objects.
            let set = unsafe {
                let value = PyString::new(py, &value);
                ffi::PyDict_SetDefaultRef(
                    dict.as_ptr(),
                    key.as_ptr(),
                    value.as_ptr(),
                    std::ptr::null_mut(),
                )
            };
            if set < 0 {
                return Err(PyErr::fetch(py));
            }
        }
        Ok(dict)
    }

    /// Builds a new `dict[str, list[str]]` that keeps every value of a duplicate key.
    fn query_params_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in form_urlencoded::parse(self.query_str().as_bytes()) {
            match dict.get_item(&*key)? {
                Some(values) => values.cast::<PyList>()?.append(&*value)?,
                None => dict.set_item(&*key, PyList::new(py, [&*value])?)?,
            }
        }
        Ok(dict)
    }

    // The buffer protocol lets `memoryview(req)` read the body in place, where `req.body`
    // copies; the view holds a strong reference to `req`, so the body outlives it.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: std::ffi::c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(pyo3::exceptions::PyBufferError::new_err("NULL view"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            // The view is zeroed so a caller that ignores the error sees nulls, not stack garbage.
            std::ptr::write_bytes(view, 0, 1);
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "mipyre Request body is readonly",
            ));
        }

        // The frozen instance owns the bytes at a stable address, and the reference stored in
        // `view.obj` below keeps it alive, so the pointer is valid for the view's life.
        let Some(data) = slf.get().body.bytes(slf.py()) else {
            std::ptr::write_bytes(view, 0, 1);
            return Err(pyo3::exceptions::PyBufferError::new_err(STREAMED_BODY));
        };
        (*view).buf = data.as_ptr() as *mut std::ffi::c_void;
        (*view).len = data.len() as ffi::Py_ssize_t;
        (*view).itemsize = 1;
        (*view).readonly = 1;
        (*view).ndim = 1;
        (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
            c"B".as_ptr() as *mut std::ffi::c_char
        } else {
            std::ptr::null_mut()
        };
        // CPython lets a 1-D buffer's shape and strides point at the view's own `len` and `itemsize`.
        (*view).shape = if (flags & ffi::PyBUF_ND) == ffi::PyBUF_ND {
            &mut (*view).len
        } else {
            std::ptr::null_mut()
        };
        (*view).strides = if (flags & ffi::PyBUF_STRIDES) == ffi::PyBUF_STRIDES {
            &mut (*view).itemsize
        } else {
            std::ptr::null_mut()
        };
        (*view).suboffsets = std::ptr::null_mut();
        (*view).internal = std::ptr::null_mut();
        (*view).obj = slf.into_ptr();
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        // Nothing is freed here, because `PyBuffer_Release` already drops the reference in `view.obj`.
    }
}

impl Request {
    /// Keeps the first value on duplicate keys, because WAFs and proxies inspect the first
    /// occurrence, so taking the last would let `?role=user&role=admin` through as a value they
    /// never approved.
    #[cfg(test)]
    pub(crate) fn query_params(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        for (k, v) in form_urlencoded::parse(self.query_str().as_bytes()) {
            map.entry(k.into_owned()).or_insert_with(|| v.into_owned());
        }
        map
    }
}

/// Joins a name's several field lines with `", "` (RFC 9110 §5.3), except `cookie`, joined with
/// `"; "` because RFC 9113 §8.2.3 lets an HTTP/2 client send one `cookie` field per crumb.
#[pyclass(frozen, name = "Headers", module = "mipyre.engine", mapping)]
pub(crate) struct RequestHeaders {
    /// The request is `frozen`, so it is read without a borrow guard.
    req: Py<Request>,
}

impl RequestHeaders {
    fn map<'a>(&'a self, py: Python<'a>) -> &'a HeaderMap {
        &self.req.bind(py).get().head.parts.headers
    }
}

#[pymethods]
impl RequestHeaders {
    fn __getitem__(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        joined_value(self.map(py), name).ok_or_else(|| PyKeyError::new_err(name.to_string()))
    }

    #[pyo3(signature = (name, default=None))]
    fn get(&self, py: Python<'_>, name: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        match joined_value(self.map(py), name) {
            Some(value) => PyString::new(py, &value).into_any().unbind(),
            None => default.unwrap_or_else(|| py.None()),
        }
    }

    /// Returns every field line named `name` as sent, in the order received.
    fn get_all(&self, py: Python<'_>, name: &str) -> Vec<String> {
        self.map(py)
            .get_all(name)
            .iter()
            .map(|v| field_text(v).into_owned())
            .collect()
    }

    fn __contains__(&self, py: Python<'_>, name: &Bound<'_, PyAny>) -> bool {
        match name.cast::<PyString>().map(|s| s.to_str()) {
            Ok(Ok(name)) => self.map(py).contains_key(name),
            _ => false,
        }
    }

    fn __len__(&self, py: Python<'_>) -> usize {
        self.map(py).keys_len()
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.keys(py)?.try_iter()?.into_any())
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, self.map(py).keys().map(HeaderName::as_str))
    }

    fn values(&self, py: Python<'_>) -> Vec<String> {
        joined_fields(self.map(py))
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    fn items(&self, py: Python<'_>) -> Vec<(String, String)> {
        joined_fields(self.map(py))
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        format!("Headers({:?})", joined_fields(self.map(py)))
    }
}

/// Falls back to ISO-8859-1 for a value that is not UTF-8 (RFC 9110 §5.5 `obs-text`), which maps
/// every byte to a character, so no byte is lost or replaced.
fn field_text(value: &HeaderValue) -> Cow<'_, str> {
    match std::str::from_utf8(value.as_bytes()) {
        Ok(s) => Cow::Borrowed(s),
        Err(_) => Cow::Owned(value.as_bytes().iter().map(|&b| char::from(b)).collect()),
    }
}

fn field_separator(name: &str) -> &'static str {
    if name.eq_ignore_ascii_case(hyper::header::COOKIE.as_str()) {
        "; "
    } else {
        ", "
    }
}

fn joined_value(map: &HeaderMap, name: &str) -> Option<String> {
    let separator = field_separator(name);
    let mut lines = map.get_all(name).iter().map(field_text);
    let first = lines.next()?.into_owned();
    Some(lines.fold(first, |mut joined, line| {
        joined.push_str(separator);
        joined.push_str(&line);
        joined
    }))
}

/// Keeps the order the names first arrived in.
pub(crate) fn joined_fields(map: &HeaderMap) -> Vec<(String, String)> {
    map.keys()
        .filter_map(|name| {
            let value = joined_value(map, name.as_str())?;
            Some((name.as_str().to_string(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_buffered_request_has_no_stream() {
        assert!(matches!(
            RequestBody::Buffered(Bytes::new()).take_stream(),
            Ok(None)
        ));
    }

    #[test]
    fn a_streamed_body_is_taken_once() {
        let (_tx, rx) = mipyre_http::io::body::body_channel();
        let body = RequestBody::streamed(rx);
        assert!(matches!(body.take_stream(), Ok(Some(_))));
        assert!(matches!(body.take_stream(), Err(BodyStreamAlreadyTaken)));
    }

    use super::*;

    #[test]
    fn extract_headers_basic() {
        let mut hm = hyper::HeaderMap::new();
        hm.insert("content-type", "application/json".parse().unwrap());
        hm.insert("x-custom", "hello".parse().unwrap());
        let h: HashMap<String, String> = joined_fields(&hm).into_iter().collect();
        assert_eq!(h["content-type"], "application/json");
        assert_eq!(h["x-custom"], "hello");
    }

    #[test]
    fn extract_headers_empty() {
        let hm = hyper::HeaderMap::new();
        let h: HashMap<String, String> = joined_fields(&hm).into_iter().collect();
        assert!(h.is_empty());
    }

    #[test]
    fn extract_headers_multi_value() {
        let mut hm = hyper::HeaderMap::new();
        hm.append("accept", "text/html".parse().unwrap());
        hm.append("accept", "application/json".parse().unwrap());
        let h: HashMap<String, String> = joined_fields(&hm).into_iter().collect();
        assert!(h["accept"].contains("text/html"));
        assert!(h["accept"].contains("application/json"));
        assert!(h["accept"].contains(", "));
    }

    #[test]
    fn repeated_cookie_fields_join_with_semicolon() {
        let mut hm = HeaderMap::new();
        hm.append("cookie", "a=1".parse().unwrap());
        hm.append("cookie", "b=2".parse().unwrap());
        assert_eq!(joined_value(&hm, "Cookie").as_deref(), Some("a=1; b=2"));
    }

    #[test]
    fn non_utf8_field_reads_as_latin1() {
        let mut hm = HeaderMap::new();
        hm.append("x-name", HeaderValue::from_bytes(b"caf\xe9").unwrap());
        assert_eq!(joined_value(&hm, "x-name").as_deref(), Some("café"));
    }

    fn request_with_query(query: &str) -> Request {
        Request::py_new(
            "GET",
            "/search",
            HashMap::new(),
            query,
            Vec::new(),
            HashMap::new(),
            "127.0.0.1",
        )
        .unwrap()
    }

    #[test]
    fn query_params_parsing() {
        let req = request_with_query("q=hello+world&page=2&lang=en");
        let qp = req.query_params();
        assert_eq!(qp["q"], "hello world");
        assert_eq!(qp["page"], "2");
        assert_eq!(qp["lang"], "en");
    }

    #[test]
    fn query_params_empty() {
        let req = request_with_query("");
        assert!(req.query_params().is_empty());
    }

    #[test]
    fn query_params_percent_encoded() {
        let req = request_with_query("name=%E4%B8%AD%E6%96%87");
        assert_eq!(req.query_params()["name"], "中文");
    }
}
