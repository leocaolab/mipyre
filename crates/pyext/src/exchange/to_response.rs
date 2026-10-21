//! Turns a handler's return value into a [`HandlerResponse`], and back into a `Response` for an
//! `after_request` hook, the same way in every interpreter.

use bytes::Bytes;
use hyper::header::HeaderValue;
use hyper::StatusCode;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyString, PyTuple};

use mipyre_http::plan::response::{HandlerResponse, HeaderLines, JSON, OCTET_STREAM, TEXT};

use crate::exchange::error::{PyException, ResponseError};
use crate::exchange::response::Response;

/// Each interpreter holds its own serializer, because the PyO3 fork makes `PyOnceLock`
/// per-interpreter and one interpreter's objects must never be used from another.
struct JsonSerializer {
    dumps: Py<PyAny>,
    /// This `default=` encodes sets and frozensets as lists and raises `TypeError` for anything else.
    default: Py<PyAny>,
    kwnames: Py<PyTuple>,
}

static JSON_SERIALIZER: pyo3::sync::PyOnceLock<JsonSerializer> = pyo3::sync::PyOnceLock::new();

fn json_serializer(py: Python<'_>) -> PyResult<&JsonSerializer> {
    JSON_SERIALIZER.get_or_try_init(py, || {
        let module = pyo3::types::PyModule::from_code(
            py,
            c"import isojson as _isojson\n\ndumps = _isojson.dumps\n\ndef _default(obj):\n    if isinstance(obj, (set, frozenset)):\n        return list(obj)\n    raise TypeError(f'not serializable: {type(obj).__name__}')\n",
            c"mipyre_json",
            c"mipyre_json",
        )?;
        Ok::<_, PyErr>(JsonSerializer {
            dumps: module.getattr("dumps")?.unbind(),
            default: module.getattr("_default")?.unbind(),
            kwnames: PyTuple::new(py, ["default"])?.unbind(),
        })
    })
}

/// Loads the JSON serializer at server start, so a missing isojson stops the start instead of
/// failing every `dict` response.
pub(crate) fn require_json(py: Python<'_>) -> PyResult<()> {
    json_serializer(py).map(|_| ())
}

/// Calls `isojson.dumps(obj, default=_default)` as one vectorcall, with no wrapper frame,
/// argument tuple or keyword dict.
fn json_dumps(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<Bytes, ResponseError> {
    // A broken serializer fails every response alike; `dumps` raising blames only this value.
    let unavailable = |e: PyErr| ResponseError::SerializerUnavailable(PyException::capture(py, &e));
    let not_json = |e: PyErr| ResponseError::NotJsonSerializable(PyException::capture(py, &e));
    let helper = json_serializer(py).map_err(unavailable)?;
    let args = [obj.as_ptr(), helper.default.as_ptr()];
    // SAFETY: The thread is attached, `dumps` is callable, and `args` holds one positional and
    // one keyword argument named by the single item of `kwnames`, all live for the call; the
    // result is a new reference or NULL with an exception set.
    let result = unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            pyo3::ffi::PyObject_Vectorcall(
                helper.dumps.as_ptr(),
                args.as_ptr(),
                1,
                helper.kwnames.as_ptr(),
            ),
        )
    }
    .map_err(not_json)?;
    let bytes = result
        .cast::<PyBytes>()
        .map_err(|e| unavailable(e.into()))?;
    Ok(Bytes::copy_from_slice(bytes.as_bytes()))
}

/// Maps what a handler or hook returned to a response, taking the type from the value, never
/// from the text:
///
/// - `dict` / `list` → JSON;
/// - `str` → `text/plain`;
/// - `bytes` / `bytearray` → `application/octet-stream`;
/// - `None` → an empty 200;
/// - a `Response` → its status and headers, its body mapped by the same rules unless it
///   names its `content_type`;
/// - anything else → `str(value)` as text.
///
/// # Errors
///
/// Returns an error, never an empty or lossy body, for a `str` that is not valid Unicode, a
/// failing `__str__`, or an unserializable dict.
pub(crate) fn extract_response_data(
    py: Python<'_>,
    obj: Bound<'_, PyAny>,
) -> Result<HandlerResponse, ResponseError> {
    // The common returns skip the `Response` type lookup.
    if is_json_value(&obj) || obj.cast::<PyString>().is_ok() {
        return plain_response(py, &obj);
    }
    match obj.cast::<Response>() {
        Ok(resp) => from_response(py, resp.get()),
        Err(_) => plain_response(py, &obj),
    }
}

fn plain_response(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
) -> Result<HandlerResponse, ResponseError> {
    let (body, content_type) = body_of(py, obj)?;
    Ok(HandlerResponse::new(
        StatusCode::OK,
        body,
        content_type,
        HeaderLines::new(),
    ))
}

fn from_response(py: Python<'_>, resp: &Response) -> Result<HandlerResponse, ResponseError> {
    let (body, derived_type) = body_of(py, resp.body.bind(py))?;
    Ok(HandlerResponse::new(
        resp.status,
        body,
        resp.sent_content_type().cloned().unwrap_or(derived_type),
        resp.headers.clone(),
    ))
}

fn is_json_value(obj: &Bound<'_, PyAny>) -> bool {
    obj.cast::<PyDict>().is_ok() || obj.cast::<PyList>().is_ok()
}

fn body_of(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<(Bytes, HeaderValue), ResponseError> {
    if is_json_value(obj) {
        return Ok((json_dumps(py, obj)?, JSON));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok((text_bytes(py, s)?, TEXT));
    }
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok((Bytes::copy_from_slice(b.as_bytes()), OCTET_STREAM));
    }
    if let Ok(b) = obj.cast::<PyByteArray>() {
        return Ok((Bytes::from(b.to_vec()), OCTET_STREAM));
    }
    if obj.is_none() {
        return Ok((Bytes::new(), TEXT));
    }
    let text = obj.str().map_err(|e| ResponseError::StrRaised {
        type_name: type_name(obj),
        exception: PyException::capture(py, &e),
    })?;
    Ok((text_bytes(py, &text)?, TEXT))
}

/// Refuses a lone surrogate instead of writing a replacement character.
fn text_bytes(py: Python<'_>, s: &Bound<'_, PyString>) -> Result<Bytes, ResponseError> {
    let text = s
        .to_str()
        .map_err(|e| ResponseError::NotUnicode(PyException::capture(py, &e)))?;
    Ok(Bytes::copy_from_slice(text.as_bytes()))
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    match obj.get_type().name() {
        Ok(name) => name.to_string(),
        Err(e) => format!("<type name unavailable: {e}>"),
    }
}

/// Builds the `Response` an `after_request` hook sees; a UTF-8 body becomes `str`, any other `bytes`.
pub(crate) fn response_to_py<'py>(
    data: &HandlerResponse,
    py: Python<'py>,
) -> Result<Bound<'py, Response>, ResponseError> {
    let body = match std::str::from_utf8(&data.body) {
        Ok(text) => PyString::new(py, text).into_any(),
        Err(_) => PyBytes::new(py, &data.body).into_any(),
    };
    Bound::new(
        py,
        Response {
            body: body.unbind(),
            status: data.status,
            content_type: Some(data.content_type().clone()),
            header_type: None,
            headers: data.header_lines(),
        },
    )
    .map_err(|e| ResponseError::ResponseNotBuilt(PyException::capture(py, &e)))
}
