//! Records, once per process, the C extensions each worker loads a private on-disk copy of.
//!
//! The copying runs in the worker (`python/mipyre/_isolated/isolation.py`, [`SOURCE`]), which
//! `dlopen`s each copy with CPython's multi-interpreter check switched off. `dlopen` state is
//! process-wide, so the record is too.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use parking_lot::Mutex;

/// Names the adapter's file under `python/` once, for both its source and its traceback name.
macro_rules! adapter_file {
    () => {
        "mipyre/_isolated/isolation.py"
    };
}

/// The adapter's source, which the caller runs in a worker's bootstrap namespace.
pub const SOURCE: &str = include_str!(concat!("../../../python/", adapter_file!()));
/// The file name a traceback shows for [`SOURCE`].
pub const SOURCE_FILE: &str = adapter_file!();

/// One worker's copy of a library: its size on disk and why the worker needed it, in the
/// adapter's words from the moment it decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibCopy {
    pub bytes: u64,
    pub why: String,
}

#[derive(Default)]
struct LibCopies {
    /// Maps each library to one worker's copy of it.
    libs: BTreeMap<String, LibCopy>,
    reported: bool,
}

impl LibCopies {
    const fn new() -> Self {
        LibCopies {
            libs: BTreeMap::new(),
            reported: false,
        }
    }

    /// Returns `name` and its copy when it is new and came after the startup report, so nothing
    /// lists it yet.
    fn note(&mut self, name: String, copy: LibCopy) -> Option<(String, LibCopy)> {
        let Entry::Vacant(vacant) = self.libs.entry(name) else {
            return None;
        };
        let noted = vacant.insert_entry(copy);
        self.reported.then(|| (noted.key().clone(), noted.get().clone()))
    }

    fn take_startup_report(&mut self) -> Option<BTreeMap<String, LibCopy>> {
        if self.reported {
            return None;
        }
        self.reported = true;
        Some(self.libs.clone())
    }
}

static ISOLATED: Mutex<LibCopies> = Mutex::new(LibCopies::new());

/// Records that a worker isolated `name` with `copy`, once per process.
///
/// Returns the library and its copy only when it is new since the startup report was taken, so
/// no report lists it.
pub fn note_isolated(name: String, copy: LibCopy) -> Option<(String, LibCopy)> {
    ISOLATED.lock().note(name, copy)
}

/// Returns the libraries isolated so far with each one's copy, the first time it is called in
/// this process; every later call returns `None`.
pub fn take_startup_report() -> Option<BTreeMap<String, LibCopy>> {
    ISOLATED.lock().take_startup_report()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copy(bytes: u64) -> LibCopy {
        LibCopy {
            bytes,
            why: format!("{bytes}.so is single-phase"),
        }
    }

    #[test]
    fn a_library_is_noted_once_and_listed_by_the_report_taken_after_it() {
        let mut copies = LibCopies::new();
        assert_eq!(
            copies.note("numpy".into(), copy(30)),
            None,
            "the report to come lists it"
        );
        assert_eq!(copies.note("numpy".into(), copy(31)), None, "already listed");
        assert_eq!(
            copies.note("orjson".into(), copy(2)),
            None,
            "the report to come lists it"
        );
        assert_eq!(
            copies.take_startup_report(),
            Some(BTreeMap::from([
                ("numpy".to_string(), copy(30)),
                ("orjson".to_string(), copy(2))
            ]))
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
        assert_eq!(
            copies.note("polars".into(), copy(40)),
            Some(("polars".to_string(), copy(40))),
            "after the report: nothing lists it"
        );
        assert_eq!(copies.note("polars".into(), copy(40)), None, "already listed");
        assert_eq!(copies.take_startup_report(), None);
    }
}
