//! Records, once per process, the C extensions each worker loads a private on-disk copy of.
//!
//! The copying runs in the worker (`python/mipyre/_subinterp/isolation.py`, [`SOURCE`]), which
//! `dlopen`s each copy with CPython's multi-interpreter check switched off. `dlopen` state is
//! process-wide, so the record is too.

use std::collections::BTreeMap;

use parking_lot::Mutex;

/// Names the adapter's file under `python/` once, for both its source and its traceback name.
macro_rules! adapter_file {
    () => {
        "mipyre/_subinterp/isolation.py"
    };
}

/// The adapter's source, which the caller runs in a worker's bootstrap namespace.
pub const SOURCE: &str = include_str!(concat!("../../../python/", adapter_file!()));
/// The file name a traceback shows for [`SOURCE`].
pub const SOURCE_FILE: &str = adapter_file!();

#[derive(Default)]
struct LibCopies {
    /// Maps each library to the size in bytes of one worker's copy on disk.
    libs: BTreeMap<String, u64>,
    reported: bool,
}

impl LibCopies {
    const fn new() -> Self {
        LibCopies {
            libs: BTreeMap::new(),
            reported: false,
        }
    }

    /// Returns whether `name` is new and came after the startup report, so nothing lists it yet.
    fn note(&mut self, name: String, bytes: u64) -> bool {
        if self.libs.contains_key(&name) {
            return false;
        }
        self.libs.insert(name, bytes);
        self.reported
    }

    fn take_startup_report(&mut self) -> Option<BTreeMap<String, u64>> {
        if self.reported {
            return None;
        }
        self.reported = true;
        Some(self.libs.clone())
    }
}

static ISOLATED: Mutex<LibCopies> = Mutex::new(LibCopies::new());

/// Records that a worker isolated `name` with a `bytes`-sized copy, once per process.
///
/// Returns `true` only when the library is new since the startup report was taken, so no report
/// lists it.
pub fn note_isolated(name: String, bytes: u64) -> bool {
    ISOLATED.lock().note(name, bytes)
}

/// Returns the libraries isolated so far with each copy's size in bytes, the first time it is
/// called in this process; every later call returns `None`.
pub fn take_startup_report() -> Option<BTreeMap<String, u64>> {
    ISOLATED.lock().take_startup_report()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_library_is_noted_once_and_listed_by_the_report_taken_after_it() {
        let mut copies = LibCopies::new();
        assert!(!copies.note("numpy".into(), 30), "the report to come lists it");
        assert!(!copies.note("numpy".into(), 31), "already listed");
        assert!(!copies.note("orjson".into(), 2), "the report to come lists it");
        assert_eq!(
            copies.take_startup_report(),
            Some(BTreeMap::from([("numpy".to_string(), 30), ("orjson".to_string(), 2)]))
        );
    }

    #[test]
    fn the_startup_report_is_taken_once_even_when_empty() {
        let mut copies = LibCopies::new();
        assert_eq!(copies.take_startup_report(), Some(BTreeMap::new()));
        assert_eq!(copies.take_startup_report(), None);
    }

    #[test]
    fn a_library_noted_after_the_report_is_not_in_it_and_says_so() {
        let mut copies = LibCopies::new();
        assert_eq!(copies.take_startup_report(), Some(BTreeMap::new()));
        assert!(copies.note("polars".into(), 40), "after the report: nothing lists it");
        assert!(!copies.note("polars".into(), 40), "already listed");
        assert_eq!(copies.take_startup_report(), None);
    }
}
