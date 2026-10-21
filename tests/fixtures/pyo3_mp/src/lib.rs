//! A PyO3 multi-phase extension with one function, `ping() -> 7`.
//!
//! The module def is hand-written, not PyO3's `#[pymodule]`: the fork's macro always declares
//! `Py_mod_multiple_interpreters = Py_MOD_PER_INTERPRETER_GIL_SUPPORTED` (`ModuleDef::new` -> `with_per_interpreter_gil`),
//! and there is no way to leave it out. This def starts at the same refcount PyO3's does (both use pyo3-ffi's
//! `PyModuleDef_HEAD_INIT`, not immortal, unlike a C extension's), which is the ownership case
//! `isolation.py`'s in-worker PyInit must not take over. The functions are still registered through PyO3.
use core::cell::UnsafeCell;
use core::ffi::{c_int, c_void};
use pyo3::ffi;
use pyo3::prelude::*;

#[pyfunction]
fn ping() -> i32 {
    7
}

fn add_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ping, m)?)?;
    Ok(())
}

/// `Py_mod_exec`: CPython hands the module being initialized.
unsafe extern "C" fn exec_module(module: *mut ffi::PyObject) -> c_int {
    Python::attach(|py| {
        // SAFETY: `module` is the live module object CPython passes to its exec slot, borrowed for this call only.
        let bound = unsafe { Bound::from_borrowed_ptr(py, module) };
        let result = bound
            .cast_into::<PyModule>()
            .map_err(PyErr::from)
            .and_then(|m| add_functions(&m));
        match result {
            Ok(()) => 0,
            Err(err) => {
                err.restore(py);
                -1
            }
        }
    })
}

/// The slot array: written once as a constant, read by CPython after `PyInit_*` hands the def out.
struct Static<T>(T);
// SAFETY: see the type's doc; nothing mutates these after initialization.
unsafe impl<T> Sync for Static<T> {}

#[cfg(feature = "declare_per_interpreter")]
static SLOTS: Static<[ffi::PyModuleDef_Slot; 3]> = Static([
    ffi::PyModuleDef_Slot {
        slot: ffi::Py_mod_exec,
        value: exec_module as *mut c_void,
    },
    ffi::PyModuleDef_Slot {
        slot: ffi::Py_mod_multiple_interpreters,
        value: ffi::Py_MOD_PER_INTERPRETER_GIL_SUPPORTED,
    },
    ffi::PyModuleDef_Slot {
        slot: 0,
        value: core::ptr::null_mut(),
    },
]);

#[cfg(not(feature = "declare_per_interpreter"))]
static SLOTS: Static<[ffi::PyModuleDef_Slot; 3]> = Static([
    ffi::PyModuleDef_Slot {
        slot: ffi::Py_mod_exec,
        value: exec_module as *mut c_void,
    },
    ffi::PyModuleDef_Slot {
        slot: 0,
        value: core::ptr::null_mut(),
    },
    ffi::PyModuleDef_Slot {
        slot: 0,
        value: core::ptr::null_mut(),
    },
]);

/// `PyModuleDef_Init` writes into the def, so it lives in writable memory (an `UnsafeCell`, as PyO3 keeps its own);
/// a plain `static` can land in read-only data, and the write faults.
static DEF: Static<UnsafeCell<ffi::PyModuleDef>> = Static(UnsafeCell::new(ffi::PyModuleDef {
    m_base: ffi::PyModuleDef_HEAD_INIT,
    m_name: c"pyo3_mp".as_ptr(),
    m_doc: core::ptr::null(),
    m_size: 0,
    m_methods: core::ptr::null_mut(),
    m_slots: SLOTS.0.as_ptr().cast_mut(),
    m_traverse: None,
    m_clear: None,
    m_free: None,
}));

/// The module's init: returns its def, which CPython then turns into the module.
///
/// # Safety
/// Called by CPython's import machinery only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn PyInit_pyo3_mp() -> *mut ffi::PyObject {
    // SAFETY: `DEF` lives for the whole process, which PyModuleDef_Init requires.
    unsafe { ffi::PyModuleDef_Init(DEF.0.get()) }
}
