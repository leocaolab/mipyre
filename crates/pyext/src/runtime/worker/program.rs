//! Reads, on main, the program every worker of one server runs: the script and main's `sys.path`.

use std::path::PathBuf;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// The program every worker of one server runs, read on main when the workers are built.
#[derive(Clone)]
pub(crate) struct WorkerProgram {
    pub(crate) script_path: PathBuf,
    pub(crate) script: String,
    /// It is main's `sys.path`, because a new interpreter's own holds only what the process
    /// started with, and what main added since (the CLI's working directory, a test runner's
    /// root) would be missing.
    pub(crate) import_path: Vec<PathBuf>,
    /// The routers (`module:attr`) with sub-interpreter routes are the only ones a worker
    /// imports.
    pub(crate) routers: Vec<String>,
    /// It says whether the worker loads the isolation adapter for `runtime="isolated"` routes.
    pub(crate) isolation: bool,
    /// Main's `sys.path` entries that are not paths are left out of `import_path`; whether
    /// that is worth a word is the caller's to decide.
    pub(crate) skipped_import_path: Vec<SkippedPathEntry>,
}

pyo3::create_exception!(
    mipyre.engine,
    NoWorkerScript,
    pyo3::exceptions::PyRuntimeError,
    "A server with sub-interpreter workers could not start because there is no script for \
     the workers to run: they rebuild the app by executing it, and `__main__` has no \
     `__file__` (`python -c`, the interactive interpreter, a host embedding Python) while \
     no script was named. A RuntimeError."
);

/// Returns `__main__.__file__`, the script workers run when none was named.
pub(crate) fn main_script(py: Python<'_>) -> PyResult<PathBuf> {
    let main = py.import("__main__")?;
    match main.getattr_opt("__file__")? {
        Some(file) => file.extract(),
        None => {
            let argv0 = match py.import("sys")?.getattr_opt("argv")? {
                None => "absent: sys has no argv".to_owned(),
                Some(argv) => match argv.get_item(0) {
                    Ok(first) => first.repr()?.to_string(),
                    Err(e) if e.is_instance_of::<pyo3::exceptions::PyIndexError>(py) => {
                        "absent: sys.argv is empty".to_owned()
                    }
                    Err(e) => return Err(e),
                },
            };
            let main = main.repr()?;
            Err(NoWorkerScript::new_err(format!(
                "sub-interpreter workers rebuild the app by executing its script, but there is no \
                 script to run: __main__ is {main}, which has no __file__ (sys.argv[0] is {argv0}). \
                 Python sets __main__.__file__ only when it runs a file (`python app.py`, `python \
                 -m pkg`); `python -c`, the interactive interpreter and hosts that embed Python \
                 (such as an execnet / pytest-xdist worker) leave it unset. Start the app from a \
                 file (or with `mipyre run module:app`), or create it with \
                 Mipyre(runtime='main') to serve every handler on the main interpreter."
            )))
        }
    }
}

/// A `sys.path` entry of main that a worker cannot take as a path.
#[derive(Clone)]
pub(crate) struct SkippedPathEntry {
    /// It is the entry's `repr()`, or the text of the error its `repr()` raised.
    pub(crate) entry: String,
    /// It is the `os.fsdecode` or conversion error, as raised.
    pub(crate) reason: String,
}

impl WorkerProgram {
    /// Reads `script_path` and main's `sys.path` now; an unreadable script is an error.
    pub(crate) fn read(
        py: Python<'_>,
        script_path: PathBuf,
        routers: Vec<String>,
        isolation: bool,
    ) -> PyResult<Self> {
        let script = std::fs::read_to_string(&script_path).map_err(|e| {
            PyRuntimeError::new_err(format!("read script '{}': {e}", script_path.display()))
        })?;
        let (import_path, skipped_import_path) = import_path(py)?;
        Ok(WorkerProgram {
            script_path,
            script,
            import_path,
            routers,
            isolation,
            skipped_import_path,
        })
    }
}

/// Splits main's `sys.path` into what `os.fsdecode` makes a path of, undecodable bytes included,
/// and the entries it cannot, which a worker cannot take because the objects belong to main.
fn import_path(py: Python<'_>) -> PyResult<(Vec<PathBuf>, Vec<SkippedPathEntry>)> {
    let fsdecode = py.import("os")?.getattr("fsdecode")?;
    let entries = py.import("sys")?.getattr("path")?;
    let mut path = Vec::new();
    let mut skipped = Vec::new();
    for entry in entries.try_iter()? {
        let entry = entry?;
        match fsdecode
            .call1((&entry,))
            .and_then(|p| p.extract::<PathBuf>())
        {
            Ok(p) => path.push(p),
            Err(reason) => skipped.push(SkippedPathEntry {
                entry: match entry.repr() {
                    Ok(repr) => repr.to_string(),
                    Err(repr_error) => repr_error.to_string(),
                },
                reason: reason.to_string(),
            }),
        }
    }
    Ok((path, skipped))
}
