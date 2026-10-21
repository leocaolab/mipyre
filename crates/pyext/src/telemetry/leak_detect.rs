//! Counts, per type, the refcounts a request's objects have when a worker lets go of them.
//!
//! The diagnostic is the opt-in `leak_detect` Cargo feature, compiled out of default builds, for
//! chasing a sub-interpreter leak (docs/memory-leak-investigation-2026-04-19.md). In the
//! histogram rc=1 is healthy, rc=2 is one co-owner such as an instance attribute, and a
//! persistent rc>=3 or any rc<0 is a refcount bug.
//!
//! It is used like this:
//!
//!   maturin develop --release --features leak_detect
//!   python examples/hello.py &
//!   wrk -t4 -c100 -d10s http://127.0.0.1:8000/
//!   python -c 'from mipyre.engine import leak_detect_dump; leak_detect_dump()'
//!
//! or inline in a test:
//!
//!   @app.get("/leak_dump")
//!   def leak_dump(req):
//!       from mipyre.engine import leak_detect_dump
//!       leak_detect_dump()
//!       return "dumped"
//!
//! The dump goes to stderr:
//!
//!   [leak_detect] mipyre_drop_rc{type="dict",rc="2"} = 8_500_000
//!   [leak_detect] mipyre_drop_rc{type="str",rc="1"} = 15_200_000
//!   [leak_detect] mipyre_drop_rc{type="Request",rc="1"} = 2_000_000

use std::sync::OnceLock;

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
use pyo3::ffi;

/// Holds, from first use, the install's reason as `Err` when another recorder was installed
/// first, so the dump says why no sample arrived instead of printing an empty histogram.
static SNAPSHOTTER: OnceLock<Result<Snapshotter, String>> = OnceLock::new();

fn ensure_recorder_installed() -> Option<&'static Snapshotter> {
    SNAPSHOTTER
        .get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snap = recorder.snapshotter();
            recorder.install().map(|()| snap).map_err(|e| {
                tracing::error!(target: "mipyre::server", error = %e,
                    "leak diagnostic disabled: the metrics recorder could not be installed");
                let reason = format!(
                    "DebuggingRecorder::install() failed: {e} (another metrics recorder \
                     is already installed in this process)"
                );
                crate::telemetry::set_degraded(crate::telemetry::Component::LeakDetect, reason.clone());
                reason
            })
        })
        .as_ref()
        .ok()
}

/// Samples the refcount of a request's object just before the worker lets go of it, where
/// refcount 1 is healthy. It takes the type-name table's lock on every call, which only a
/// diagnostic build pays.
#[inline(never)] // It stays out of line, off the request path's instruction cache.
pub(crate) fn record_drop(obj: &pyo3::Bound<'_, pyo3::PyAny>) {
    ensure_recorder_installed();
    let ptr = obj.as_ptr();

    // SAFETY: `obj` is a live object of an attached interpreter, as its `Bound` proves.
    let rc = unsafe { ffi::Py_REFCNT(ptr) };
    // SAFETY: `obj` is live as above, and `tp_name` belongs to the type object, which outlives
    // this instance of it.
    let type_name: &'static str = unsafe {
        let t = ffi::Py_TYPE(ptr);
        if t.is_null() {
            "<null_type>"
        } else {
            let name_ptr = (*t).tp_name;
            if name_ptr.is_null() {
                "<unnamed>"
            } else {
                // SAFETY: The pointer is non-null, and `type_name_bounded` reads at most its cap.
                type_name_bounded(name_ptr)
            }
        }
    };

    let rc_label = rc_label(rc);
    metrics::counter!("mipyre_drop_rc", "type" => type_name, "rc" => rc_label).increment(1);
}

/// Prints the top `mipyre_drop_rc` counters to stderr for `mipyre.engine.leak_detect_dump()`.
pub(crate) fn dump_to_stderr() {
    let snap = match SNAPSHOTTER.get() {
        None => {
            eprintln!("[leak_detect] no recorder installed yet (no drops sampled)");
            return;
        }
        Some(Err(reason)) => {
            eprintln!("[leak_detect] {reason}; no samples were captured");
            return;
        }
        Some(Ok(snap)) => snap,
    };
    let mut rows: Vec<(String, u64)> = snap
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _unit, _desc, value)| {
            let metrics_util::debugging::DebugValue::Counter(total) = value else {
                return None;
            };
            let name = key.key().name();
            if name != "mipyre_drop_rc" {
                return None;
            }
            let labels: Vec<String> = key
                .key()
                .labels()
                .map(|l| format!("{}={:?}", l.key(), l.value()))
                .collect();
            Some((format!("{}{{{}}}", name, labels.join(",")), total))
        })
        .collect();
    rows.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    eprintln!("[leak_detect] --- mipyre_drop_rc snapshot (top {DUMP_ROWS}) ---");
    for (label, n) in rows.iter().take(DUMP_ROWS) {
        eprintln!("[leak_detect]   {label} = {n}");
    }
    if rows.is_empty() {
        eprintln!("[leak_detect]   (no samples — is the feature enabled and drops flowing?)");
    }
}

const DUMP_ROWS: usize = 30;

/// Reads a `tp_name` C string, stopping at its NUL or after `MAX` bytes, and interns it.
/// `record_drop` samples corrupted objects by design (the `"<0"` bucket), whose `tp_name` may
/// have no NUL, and `CStr::from_ptr`'s unbounded scan could run into unmapped memory.
///
/// # Safety
/// `ptr` must be non-null and point at readable memory for at least its
/// NUL-terminated length (or `MAX` bytes if not terminated within `MAX`).
unsafe fn type_name_bounded(ptr: *const std::os::raw::c_char) -> &'static str {
    const MAX: usize = 256;
    let mut len = 0usize;
    while len < MAX {
        if *ptr.add(len) == 0 {
            break;
        }
        len += 1;
    }
    // SAFETY: Every byte in 0..len was just read above, so the slice is valid.
    let bytes = std::slice::from_raw_parts(ptr as *const u8, len);
    // A corrupted object's name is what the sample is about, so the bytes are kept as they are
    // with invalid UTF-8 replaced; there are at most MAX of them, so garbage stays bounded.
    let name = String::from_utf8_lossy(bytes);
    if len == MAX {
        intern(&format!("{name}… (no NUL within {MAX} bytes)"))
    } else {
        intern(&name)
    }
}

/// Interns a type name so a `metrics` label can be `&'static str`, leaking one string per
/// distinct type name, a few dozen over a process's life.
fn intern(s: &str) -> &'static str {
    use parking_lot::Mutex;

    static TABLE: OnceLock<Mutex<std::collections::HashMap<String, &'static str>>> =
        OnceLock::new();
    let t = TABLE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let mut g = t.lock();
    if let Some(&cached) = g.get(s) {
        return cached;
    }
    let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
    g.insert(s.to_string(), leaked);
    leaked
}

/// Returns `"0"`..`"8"`, `"9+"`, or `"<0"`. Only a corrupted `ob_refcnt` (double free, use
/// after free, an FFI over-decref) can be negative, so `"<0"` is never folded into `"9+"`.
fn rc_label(rc: ffi::Py_ssize_t) -> &'static str {
    const PRECOMPUTED: &[&str] = &["0", "1", "2", "3", "4", "5", "6", "7", "8"];
    if rc < 0 {
        "<0"
    } else if (0..PRECOMPUTED.len() as ffi::Py_ssize_t).contains(&rc) {
        PRECOMPUTED[rc as usize]
    } else {
        "9+"
    }
}
