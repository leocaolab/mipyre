//! Encodes Python values as JSON and decodes JSON text into them, with isojson, the one codec
//! responses, `req.json()` and the framework's own JSON go through.
//!
//! When decoding, isojson holds an integer only within `i64::MIN..=u64::MAX` and silently rounds
//! a wider one to a float, so a document that may hold one goes to the standard library instead,
//! which keeps every integer exact and rounds floats the same way. Both reject `NaN`, `Infinity`
//! and numbers too large for a float.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyTuple};

/// `-9223372036854775809`, the first integer below `i64::MIN`, has 19 digits, so no shorter run
/// can spell an integer outside `i64::MIN..=u64::MAX`.
const WIDE_DIGITS: usize = 19;

/// Each interpreter gets its own decoders, because the PyO3 fork makes `PyOnceLock` per-interpreter.
static DECODERS: PyOnceLock<Decoders> = PyOnceLock::new();

struct Decoders {
    fast: Py<PyAny>,
    exact: Py<PyAny>,
}

const EXACT_SOURCE: &std::ffi::CStr = c"
import json as _json
import math as _math


def _constant(name):
    raise ValueError(f'{name} is not a JSON value')


def _float(text):
    value = float(text)
    if not _math.isfinite(value):
        raise ValueError(f'number {text} is out of range')
    return value


def loads(text):
    return _json.loads(text, parse_constant=_constant, parse_float=_float)
";

fn decoders(py: Python<'_>) -> PyResult<&Decoders> {
    DECODERS.get_or_try_init(py, || {
        let exact = pyo3::types::PyModule::from_code(py, EXACT_SOURCE, c"mipyre_json_exact", c"mipyre_json_exact")?;
        Ok(Decoders {
            fast: py.import("isojson")?.getattr("loads")?.unbind(),
            exact: exact.getattr("loads")?.unbind(),
        })
    })
}

/// Decodes `text` as JSON; text that is not JSON raises a `ValueError` subclass.
pub(crate) fn loads<'py>(py: Python<'py>, text: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    let decoders = decoders(py)?;
    let decoder = if may_hold_wide_integer(text) {
        &decoders.exact
    } else {
        &decoders.fast
    };
    decoder.bind(py).call1((PyBytes::new(py, text),))
}

/// Counts a long digit run inside a string or a float too, which only costs the slower exact decoder.
fn may_hold_wide_integer(text: &[u8]) -> bool {
    text.split(|b| !b.is_ascii_digit()).any(|run| run.len() >= WIDE_DIGITS)
}

/// Each interpreter holds its own serializer, because the PyO3 fork makes `PyOnceLock`
/// per-interpreter and one interpreter's objects must never be used from another.
pub(crate) struct JsonSerializer {
    dumps: Py<PyAny>,
    /// This `default=` encodes what isojson does not: sets and frozensets as lists, `UUID` and
    /// `Decimal` as strings (a float would lose a numeric column's digits), and raises
    /// `TypeError` for anything else.
    default: Py<PyAny>,
    kwnames: Py<PyTuple>,
}

/// isojson calls `_default` only for a value it cannot encode itself, so the common response
/// never reaches it. A `UUID` or `Decimal` exists only once its module has run, so `_default`
/// finds the module in `sys.modules` instead of importing it: no worker loads either at start.
const SERIALIZER_SOURCE: &std::ffi::CStr = c"
import sys as _sys

import isojson as _isojson

dumps = _isojson.dumps


def _default(obj):
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    uuid = _sys.modules.get('uuid')
    if uuid is not None and isinstance(obj, uuid.UUID):
        return str(obj)
    decimal = _sys.modules.get('decimal')
    if decimal is not None and isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(f'not serializable: {type(obj).__name__}')
";

static SERIALIZER: PyOnceLock<JsonSerializer> = PyOnceLock::new();

pub(crate) fn serializer(py: Python<'_>) -> PyResult<&JsonSerializer> {
    SERIALIZER.get_or_try_init(py, || {
        let module = pyo3::types::PyModule::from_code(py, SERIALIZER_SOURCE, c"mipyre_json", c"mipyre_json")?;
        Ok::<_, PyErr>(JsonSerializer {
            dumps: module.getattr("dumps")?.unbind(),
            default: module.getattr("_default")?.unbind(),
            kwnames: PyTuple::new(py, ["default"])?.unbind(),
        })
    })
}

impl JsonSerializer {
    /// Calls `isojson.dumps(obj, default=_default)` as one vectorcall, with no wrapper frame,
    /// argument tuple or keyword dict.
    pub(crate) fn dumps<'py>(&self, py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let args = [obj.as_ptr(), self.default.as_ptr()];
        // SAFETY: The thread is attached, `dumps` is callable, and `args` holds one positional
        // and one keyword argument named by the single item of `kwnames`, all live for the call;
        // the result is a new reference or NULL with an exception set.
        unsafe {
            Bound::from_owned_ptr_or_err(
                py,
                pyo3::ffi::PyObject_Vectorcall(self.dumps.as_ptr(), args.as_ptr(), 1, self.kwnames.as_ptr()),
            )
        }
    }
}

/// Loads the JSON serializer at server start, so a missing isojson stops the start instead of
/// failing every `dict` response.
pub(crate) fn require_json(py: Python<'_>) -> PyResult<()> {
    serializer(py).map(|_| ())
}

/// Encodes `value` as a response body's JSON is encoded, raising what the encoder raises.
pub(crate) fn encode<'py>(py: Python<'py>, value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let result = serializer(py)?.dumps(py, value)?;
    Ok(result.cast_into::<PyBytes>()?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Makes `isojson` importable here: a test binary's interpreter is the base install, so this adds
    /// the site-packages of the venv `PYO3_PYTHON` names, else this checkout's `.venv-gate` or `.venv`.
    pub(crate) fn import_isojson(py: Python<'_>) {
        let checkout = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let venvs: Vec<std::path::PathBuf> = std::env::var_os("PYO3_PYTHON")
            .map(std::path::PathBuf::from)
            .and_then(|python| Some(python.parent()?.parent()?.to_path_buf()))
            .into_iter()
            .chain([checkout.join(".venv-gate"), checkout.join(".venv")])
            .collect();
        let found = py.import("sys").and_then(|sys| {
            let version = sys.getattr("version_info")?;
            let lib = format!("python{}.{}", version.get_item(0)?, version.get_item(1)?);
            let path = sys.getattr("path")?;
            for venv in &venvs {
                let site = venv.join("lib").join(&lib).join("site-packages");
                if site.is_dir() {
                    path.call_method1("append", (site.to_string_lossy(),))?;
                }
            }
            py.import("isojson").map(drop)
        });
        if let Err(e) = found {
            panic!("isojson is not importable from the venvs {venvs:?}: {e}");
        }
    }

    /// Integers on both sides of the 64-bit bounds, as Python expressions.
    pub(crate) const INTEGERS_AROUND_64_BITS: &[&str] = &[
        "2**63 - 1",
        "2**63",
        "2**64 - 1",
        "2**64",
        "2**64 + 1",
        "-(2**63)",
        "-(2**63) - 1",
        "-(2**64) + 1",
        "-(2**64)",
        "-(2**64) - 1",
        "2**200",
        "-(2**200)",
    ];

    pub(crate) fn python_int<'py>(py: Python<'py>, expr: &str) -> Bound<'py, PyAny> {
        let code = std::ffi::CString::new(expr).expect("an expression without NUL");
        py.eval(&code, None, None).unwrap_or_else(|e| panic!("{expr}: {e}"))
    }

    #[test]
    fn integers_on_both_sides_of_64_bits_decode_to_the_same_int() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            import_isojson(py);
            for expr in INTEGERS_AROUND_64_BITS {
                let n = python_int(py, expr);
                let text = format!("{{\"n\": {n}}}");
                let decoded = loads(py, text.as_bytes())
                    .and_then(|doc| doc.get_item("n"))
                    .unwrap_or_else(|e| panic!("{text}: {e}"));
                assert!(
                    decoded.is_exact_instance_of::<pyo3::types::PyInt>(),
                    "{text}: {decoded}"
                );
                assert!(decoded.eq(&n).unwrap(), "{text}: {decoded}");
            }
        });
    }

    #[test]
    fn integers_isojson_holds_take_the_fast_decoder() {
        assert!(!may_hold_wide_integer(b"{\"a\": 9223372036854775}"));
        assert!(!may_hold_wide_integer(b"[1, 2.5, -3e10, \"x\"]"));
        assert!(!may_hold_wide_integer(b""));
    }

    #[test]
    fn wider_integers_take_the_exact_decoder() {
        assert!(may_hold_wide_integer(b"{\"a\": 18446744073709551616}"));
        assert!(may_hold_wide_integer(b"-9223372036854775809"));
        assert!(may_hold_wide_integer(b"[1, 12345678901234567890123]"));
    }
}
