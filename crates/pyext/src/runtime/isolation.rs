//! Announces each library `runtime="isolated"` copies into every worker, so the compatibility
//! option is never silent.
//!
//! Each worker loads its own copy of such a library with CPython's multi-interpreter check off,
//! which costs memory per worker; the announcement says so and how much.

use std::collections::BTreeMap;

use mipyre_subinterp::isolation;
use pyo3::prelude::*;

/// Lists the libraries for which the line advises `runtime='process'`; isolation itself treats
/// every library alike.
const BELONGS_IN_PROCESS: &[&str] = &[
    "numpy",
    "pandas",
    "pyarrow",
    "scipy",
    "sklearn",
    "polars",
    "torch",
    "_polars_runtime_32",
];

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn advice(libs: &[&str]) -> String {
    let process: Vec<&str> = libs
        .iter()
        .copied()
        .filter(|lib| BELONGS_IN_PROCESS.contains(lib))
        .collect();
    if process.is_empty() {
        String::new()
    } else {
        format!(
            "; for {}, runtime='process' is the better fit (worker processes, no copies)",
            process.join(", ")
        )
    }
}

/// Records a library a worker isolated, once per process, and announces it at once if the startup
/// line is already out.
#[pyfunction]
pub(crate) fn _note_isolated(lib: String, bytes: u64) {
    if isolation::note_isolated(lib.clone(), bytes) {
        announce(&late_line(&lib, bytes));
    }
}

/// Announces the startup line once per process; it is called after the workers are built.
pub(crate) fn report_startup(workers: usize) {
    if let Some(libs) = isolation::take_startup_report() {
        announce(&startup_line(&libs, workers));
    }
}

fn late_line(lib: &str, bytes: u64) -> String {
    format!(
        "runtime='isolated' (a compatibility option, not recommended): each worker now \
         loads its own copy of {lib} ({}), with CPython's multi-interpreter check off{}",
        mib(bytes),
        advice(&[lib]),
    )
}

fn startup_line(libs: &BTreeMap<String, u64>, workers: usize) -> String {
    let names: Vec<&str> = libs.keys().map(String::as_str).collect();
    let listed = if libs.is_empty() {
        "none yet (a library is isolated when a worker first fails to import it)".to_string()
    } else {
        libs.iter()
            .map(|(name, bytes)| format!("{name} ({})", mib(*bytes)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "runtime='isolated' (a compatibility option, not recommended): per-worker copies of \
         {listed}, in each of {workers} sub-interpreters, loaded with CPython's \
         multi-interpreter check off{}",
        advice(&names),
    )
}

/// Prints the line with the startup banner whatever the log level, and logs it too.
fn announce(line: &str) {
    println!("  {line}");
    tracing::warn!(target: "mipyre::server", "{line}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_startup_line_lists_each_copy_and_points_a_process_library_at_runtime_process() {
        let libs = BTreeMap::from([("numpy".to_string(), 3 << 20), ("orjson".to_string(), 1 << 19)]);
        let line = startup_line(&libs, 4);
        assert!(
            line.contains("numpy (3.0 MiB), orjson (0.5 MiB), in each of 4"),
            "{line}"
        );
        assert!(
            line.ends_with(
                "; for numpy, runtime='process' is the better fit (worker processes, no copies)"
            ),
            "{line}"
        );
    }

    #[test]
    fn a_startup_line_with_nothing_isolated_says_none_yet_and_gives_no_advice() {
        let line = startup_line(&BTreeMap::new(), 2);
        assert!(line.contains("copies of none yet"), "{line}");
        assert!(line.ends_with("multi-interpreter check off"), "{line}");
    }

    #[test]
    fn a_late_line_names_its_library() {
        let line = late_line("orjson", 1 << 20);
        assert!(line.contains("its own copy of orjson (1.0 MiB)"), "{line}");
        assert!(line.ends_with("check off"), "{line}");
    }
}
