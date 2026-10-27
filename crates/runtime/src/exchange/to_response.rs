//! Turns a handler's return value into a [`HandlerResponse`], and back into a `Response` for an
//! `after_request` hook, the same way in every interpreter.

use bytes::Bytes;
use hyper::header::HeaderValue;
use hyper::StatusCode;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyString};

use mipyre_http::plan::response::{HandlerResponse, HeaderLines, JSON, OCTET_STREAM, TEXT};

use crate::exchange::error::{py_text, PyException, ResponseError};
use crate::exchange::json;
use crate::exchange::response::Response;

fn json_dumps(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<Bytes, ResponseError> {
    // A broken serializer fails every response alike; `dumps` raising blames only this value.
    let unavailable = |e: PyErr| ResponseError::SerializerUnavailable(PyException::capture(py, &e));
    let not_json = |e: PyErr| ResponseError::NotJsonSerializable(PyException::capture(py, &e));
    let result = json::serializer(py)
        .map_err(unavailable)?
        .dumps(py, obj)
        .map_err(not_json)?;
    let bytes = result.cast::<PyBytes>().map_err(|e| unavailable(e.into()))?;
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
/// - anything else is no response, since a tuple or an object sent as its `str()` would answer
///   200 with text the handler never meant.
///
/// # Errors
///
/// Returns an error, never an empty or lossy body, for a value of any other type, a `str` that is
/// not valid Unicode, or an unserializable dict.
pub(crate) fn extract_response_data(py: Python<'_>, obj: Bound<'_, PyAny>) -> Result<HandlerResponse, ResponseError> {
    // The common returns skip the `Response` type lookup.
    if is_json_value(&obj) || obj.cast::<PyString>().is_ok() {
        return plain_response(py, &obj);
    }
    match obj.cast::<Response>() {
        Ok(resp) => from_response(py, resp.get()),
        Err(_) => plain_response(py, &obj),
    }
}

fn plain_response(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<HandlerResponse, ResponseError> {
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
    let answer = HandlerResponse::new(
        resp.status,
        body,
        resp.sent_content_type().cloned().unwrap_or(derived_type),
        resp.headers.clone(),
    );
    Ok(match resp.validation_errors {
        Some(errors) => answer.failed_validation(errors),
        None => answer,
    })
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
    Err(ResponseError::NotAResponse {
        type_name: type_name(obj),
        repr: repr_of(py, obj),
    })
}

const REPR_LIMIT: usize = 200;

fn repr_of(py: Python<'_>, obj: &Bound<'_, PyAny>) -> String {
    match obj.repr() {
        Ok(text) => truncated(&py_text(&text), REPR_LIMIT),
        Err(e) => format!("<repr() raised {}>", PyException::capture(py, &e)),
    }
}

/// Keeps the first `limit` characters of `text`, marking a cut with an ellipsis.
fn truncated(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text.to_owned(),
    }
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
            validation_errors: data.validation_errors(),
        },
    )
    .map_err(|e| ResponseError::ResponseNotBuilt(PyException::capture(py, &e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::json::tests::{import_isojson, python_int, INTEGERS_AROUND_64_BITS};

    #[test]
    fn integers_on_both_sides_of_64_bits_are_written_in_full() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            import_isojson(py);
            for expr in INTEGERS_AROUND_64_BITS {
                let n = python_int(py, expr);
                let doc = PyDict::new(py);
                doc.set_item("n", &n).expect("a str key");
                let response = extract_response_data(py, doc.into_any()).unwrap_or_else(|e| panic!("{expr}: {e}"));
                assert_eq!(response.body, format!("{{\"n\":{n}}}").as_bytes(), "{expr}");
            }
        });
    }

    fn evaluated<'py>(py: Python<'py>, expr: &std::ffi::CStr) -> Bound<'py, PyAny> {
        let globals = PyDict::new(py);
        globals
            .set_item("Response", py.get_type::<Response>())
            .expect("a str key");
        py.run(
            c"class BadRepr:\n    def __repr__(self):\n        raise ValueError('no repr')\n",
            Some(&globals),
            None,
        )
        .expect("the class definition runs");
        py.eval(expr, Some(&globals), None).expect("the expression evaluates")
    }

    #[test]
    fn the_types_a_handler_returns_as_a_body_become_responses() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            import_isojson(py);
            for expr in [
                c"{'a': 1}",
                c"[1]",
                c"'t'",
                c"b'b'",
                c"bytearray(b'b')",
                c"None",
                c"Response(b'b')",
            ] {
                let value = evaluated(py, expr);
                assert!(extract_response_data(py, value).is_ok(), "{expr:?}");
            }
        });
    }

    #[test]
    fn any_other_returned_type_is_refused_with_its_type_name_and_repr() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            for (expr, type_name, repr) in [
                (c"('x', 201)", "tuple", "('x', 201)"),
                (c"{1, 2}", "set", "{1, 2}"),
                (c"42", "int", "42"),
                (c"Response(('x', 201))", "tuple", "('x', 201)"),
                (c"BadRepr()", "BadRepr", "<repr() raised ValueError: no repr>"),
            ] {
                match extract_response_data(py, evaluated(py, expr)) {
                    Err(ResponseError::NotAResponse { type_name: t, repr: r }) => {
                        assert_eq!((t.as_str(), r.as_str()), (type_name, repr), "{expr:?}")
                    }
                    other => panic!("{expr:?}: {:?}", other.err()),
                }
            }
        });
    }

    #[test]
    fn a_repr_at_the_limit_is_kept_and_one_past_it_is_cut_between_characters() {
        assert_eq!(truncated("abc", 3), "abc");
        assert_eq!(truncated("abcd", 3), "abc…");
        assert_eq!(truncated("ééé", 2), "éé…");
    }
}
