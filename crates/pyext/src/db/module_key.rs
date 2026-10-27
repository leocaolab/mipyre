//! Reads which module made a `PgPool` handle or an app, as the [`ModuleKey`] the pools compare.

use mipyre_pg::ModuleKey;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Returns the key of the module whose globals are `globals`: its `__file__` made absolute, else
/// its `__name__`.
///
/// # Errors
///
/// Returns the Python error when `os.path.abspath` fails on its `__file__`.
pub(crate) fn module_key_of_globals(globals: &Bound<'_, PyDict>) -> PyResult<ModuleKey> {
    let py = globals.py();
    if let Some(file) = globals.get_item("__file__")?.filter(|file| !file.is_none()) {
        let absolute = py.import("os.path")?.call_method1("abspath", (file,))?;
        return Ok(ModuleKey::from(absolute.extract::<String>()?));
    }
    Ok(ModuleKey::from(match globals.get_item("__name__")? {
        Some(name) => name.extract()?,
        None => String::new(),
    }))
}

/// Returns the key of the module loaded from `path`.
///
/// # Errors
///
/// Returns the Python error when `os.path.abspath` fails.
pub(crate) fn module_key_of_path(py: Python<'_>, path: &std::path::Path) -> PyResult<ModuleKey> {
    Ok(ModuleKey::from(
        py.import("os.path")?
            .call_method1("abspath", (path,))?
            .extract::<String>()?,
    ))
}

/// Returns the key of the module `name` of `sys.modules`, if it is loaded.
///
/// # Errors
///
/// Returns the Python error when `sys.modules` cannot be read.
pub(crate) fn module_key_of_module(py: Python<'_>, name: &str) -> PyResult<Option<ModuleKey>> {
    let modules = py.import("sys")?.getattr("modules")?;
    let Some(module) = modules.get_item(name).ok() else {
        return Ok(None);
    };
    let globals = module.getattr("__dict__")?;
    let globals = globals.cast::<PyDict>()?;
    module_key_of_globals(globals).map(Some)
}
