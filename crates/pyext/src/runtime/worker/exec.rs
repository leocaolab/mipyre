//! Runs Python source in a module of a worker's interpreter, and explains the failures the
//! engine can: a relative import in the script, or an extension that can't load in a
//! sub-interpreter.

use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::path::Path;

use pyo3::exceptions::PyImportError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule, PyString, PyTraceback};

/// Python source as UTF-8, NUL-terminated with no NUL inside, because `Py_CompileStringObject`
/// takes a C string, with the file it is compiled as.
pub(super) struct PythonSource<'a> {
    text: Cow<'a, CStr>,
    file: &'a Path,
}

impl<'a> PythonSource<'a> {
    /// Fails when the text holds a NUL byte, which cannot be compiled.
    pub(super) fn new(text: &'a str, file: &'a Path) -> Result<Self, std::ffi::NulError> {
        Ok(PythonSource {
            text: Cow::Owned(CString::new(text)?),
            file,
        })
    }

    pub(super) fn file(&self) -> &Path {
        self.file
    }
}

impl PythonSource<'static> {
    /// Takes source [`bundled!`] into the engine; `file` is its path under `python/`.
    pub(super) fn bundled(text: &'static CStr, file: &'static str) -> Self {
        PythonSource {
            text: Cow::Borrowed(text),
            file: Path::new(file),
        }
    }
}

/// Builds the [`PythonSource`] of `python/<file>` bundled into the engine, compiled as `<file>`;
/// one holding a NUL byte fails the build.
macro_rules! bundled {
    ($file:literal) => {{
        const TEXT: &::std::ffi::CStr = match ::std::ffi::CStr::from_bytes_with_nul(
            concat!(include_str!(concat!("../../../../../python/", $file)), "\0").as_bytes(),
        ) {
            Ok(c) => c,
            Err(_) => panic!(concat!($file, " holds a NUL byte")),
        };
        $crate::runtime::worker::exec::PythonSource::bundled(TEXT, $file)
    }};
}
pub(super) use bundled;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScriptFailure {
    RelativeImport,
    /// It holds the explanation `mipyre._subinterp_import.explain` gave.
    NotForSubinterpreters(Box<str>),
    /// It holds each check that raised, with its error.
    NotDiagnosed(Box<str>),
}

impl std::fmt::Display for ScriptFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScriptFailure::RelativeImport => f.write_str(
                "\n(A worker executes the script as a module of its own, \
                 `__mipyre_worker__`, outside any package, so a relative import in it \
                 (`from . import x`, `from .models import X`) cannot resolve there. Import by \
                 absolute name (`from mypkg.models import X`), with the package importable \
                 from sys.path.)",
            ),
            ScriptFailure::NotForSubinterpreters(text) => f.write_str(text),
            ScriptFailure::NotDiagnosed(why) => write!(
                f,
                "\n(The engine could not tell whether this failed on a relative import or on an \
                 extension that can't load in a sub-interpreter: {why}.)"
            ),
        }
    }
}

/// What a source raised, still on its interpreter, and what the engine could tell of why.
#[derive(Debug)]
pub(super) struct ExecFailure {
    pub(super) raised: PyErr,
    pub(super) failure: Option<ScriptFailure>,
}

/// Creates a module registered in `sys.modules`, with builtins and, for the script, `__file__`.
pub(super) fn new_module<'py>(
    py: Python<'py>,
    name: &CStr,
    file: Option<&Path>,
) -> PyResult<Bound<'py, PyModule>> {
    let module = PyModule::new(py, &name.to_string_lossy())?;
    // SAFETY: The thread is attached (`py`), and `PyEval_GetBuiltins` returns a borrowed
    // reference to the current builtins dict that is never NULL.
    let builtins = unsafe { Bound::from_borrowed_ptr(py, ffi::PyEval_GetBuiltins()) };
    module.setattr("__builtins__", builtins)?;
    if let Some(path) = file {
        module.setattr("__file__", path.as_os_str())?;
    }
    py.import("sys")?
        .getattr("modules")?
        .set_item(module.name()?, &module)?;
    Ok(module)
}

/// Compiles `src` as its file and runs it in `module`'s namespace; an exception is returned, not
/// printed.
pub(super) fn exec_in(
    py: Python<'_>,
    src: &PythonSource<'_>,
    module: &Bound<'_, PyModule>,
) -> Result<(), ExecFailure> {
    // The path is spelled as `os.fsdecode` would, so a path that is not UTF-8 keeps its bytes.
    let file = src
        .file
        .as_os_str()
        .into_pyobject(py)
        .map_err(|e| ExecFailure {
            raised: PyErr::from(e),
            failure: None,
        })?;
    let dict = module.dict();
    // SAFETY: The thread is attached (`py`), `src.text` is NUL-terminated, and `file` and `dict`
    // are live; both calls return a new reference or NULL with an exception set.
    let ran = unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            ffi::Py_CompileStringObject(
                src.text.as_ptr(),
                file.as_ptr(),
                ffi::Py_file_input,
                std::ptr::null_mut(),
                -1,
            ),
        )
        .and_then(|code| {
            Bound::from_owned_ptr_or_err(
                py,
                ffi::PyEval_EvalCode(code.as_ptr(), dict.as_ptr(), dict.as_ptr()),
            )
        })
    };
    ran.map(drop).map_err(|e| ExecFailure {
        failure: script_failure(py, &e, &file, src),
        raised: e,
    })
}

/// Explains `err` when the engine can: an extension that cannot load in a sub-interpreter, or a
/// relative import on the innermost traceback line in `filename`. A check that itself raises comes
/// back as [`ScriptFailure::NotDiagnosed`], so the caller sees it.
fn script_failure(
    py: Python<'_>,
    err: &PyErr,
    filename: &Bound<'_, PyString>,
    src: &PythonSource<'_>,
) -> Option<ScriptFailure> {
    let mut undiagnosed: Vec<String> = Vec::new();
    match py
        .import("mipyre._subinterp_import")
        .and_then(|m| m.call_method1("explain", (err.value(py),)))
        .and_then(|text| text.extract::<Option<String>>())
    {
        Ok(Some(text)) => return Some(ScriptFailure::NotForSubinterpreters(text.into_boxed_str())),
        Ok(None) => {}
        Err(e) => undiagnosed.push(format!(
            "the sub-interpreter check of {filename} raised {e}"
        )),
    }
    if !err.is_instance_of::<PyImportError>(py) {
        return not_diagnosed(undiagnosed);
    }
    match failing_line(py, err, filename).and_then(|line| relative_import_at(py, src, line)) {
        Ok(true) => Some(ScriptFailure::RelativeImport),
        Ok(false) => not_diagnosed(undiagnosed),
        Err(e) => {
            undiagnosed.push(format!(
                "the relative-import check of {filename} raised {e}"
            ));
            not_diagnosed(undiagnosed)
        }
    }
}

fn not_diagnosed(undiagnosed: Vec<String>) -> Option<ScriptFailure> {
    (!undiagnosed.is_empty()).then(|| ScriptFailure::NotDiagnosed(undiagnosed.join("; ").into()))
}

/// Returns the line of the innermost traceback frame in `filename`, if any.
fn failing_line(
    py: Python<'_>,
    err: &PyErr,
    filename: &Bound<'_, PyString>,
) -> PyResult<Option<usize>> {
    let mut line = None;
    let mut tb = err.traceback(py);
    while let Some(frame) = tb {
        let code_file = frame
            .getattr("tb_frame")?
            .getattr("f_code")?
            .getattr("co_filename")?;
        if code_file.eq(filename)? {
            line = Some(frame.getattr("tb_lineno")?.extract()?);
        }
        tb = frame.getattr("tb_next")?.cast_into::<PyTraceback>().ok();
    }
    Ok(line)
}

/// Tells whether `line` is inside a relative `from` import. `src` is parsed from its bytes, as it
/// was compiled, so a coding cookie applies to both.
fn relative_import_at(
    py: Python<'_>,
    src: &PythonSource<'_>,
    line: Option<usize>,
) -> PyResult<bool> {
    let Some(line) = line else {
        return Ok(false);
    };
    let ast = py.import("ast")?;
    let import_from = ast.getattr("ImportFrom")?;
    let tree = ast.call_method1("parse", (PyBytes::new(py, src.text.to_bytes()),))?;
    for node in ast.call_method1("walk", (tree,))?.try_iter()? {
        let node = node?;
        if !node.is_instance(&import_from)? || node.getattr("level")?.extract::<u32>()? == 0 {
            continue;
        }
        let first: usize = node.getattr("lineno")?.extract()?;
        let last: usize = node.getattr("end_lineno")?.extract()?;
        if (first..=last).contains(&line) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_relative_import_hint_is_only_for_a_relative_import() {
        assert!(ScriptFailure::RelativeImport
            .to_string()
            .contains("outside any package, so a relative import"));
    }
}
