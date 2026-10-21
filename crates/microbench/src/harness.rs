//! Runs a per-call bench's cases by name.

use std::env::VarError;
use std::hint::black_box;
use std::sync::atomic::Ordering;

use crate::counting::ALLOCATIONS;

/// The calls a case makes when `BENCH_N` is not set.
pub const DEFAULT_CALLS: u64 = 1_000_000;

/// A case's name and its loop, which makes the number of calls it is given.
pub type Case = (&'static str, fn(u64));

/// `BENCH_N`, or `default` when it is not set. A `BENCH_N` that is not a count stops the bench
/// (exit 2): a run of some other length would be read as one of that length.
pub fn calls(default: u64) -> u64 {
    match std::env::var("BENCH_N") {
        Ok(text) => match text.parse() {
            Ok(n) => n,
            Err(e) => {
                eprintln!("BENCH_N={text:?} is not a call count: {e}");
                std::process::exit(2)
            }
        },
        Err(VarError::NotPresent) => default,
        Err(e @ VarError::NotUnicode(_)) => {
            eprintln!("BENCH_N is not a call count: {e}");
            std::process::exit(2)
        }
    }
}

/// Runs the case named by the first argument other than the `--bench` cargo adds, or every case,
/// with `calls(DEFAULT_CALLS)` calls. Exits with status 2 when no case has that name, and when the
/// global allocator is not an `AllocationCounter` one.
///
/// Inlined into the bench's `main`, as it was while each bench compiled its own copy: the cases'
/// code is laid out the same way, so their counts do not move with where the runner lives.
#[inline]
pub fn run(cases: &[Case]) {
    if !allocations_counted() {
        eprintln!("the bench's #[global_allocator] is not mipyre_microbench::AllocationCounter: no allocation is counted");
        std::process::exit(2);
    }
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    if args.iter().any(|a| a == "--list") {
        for (name, _) in cases {
            println!("{name}");
        }
        return;
    }
    let name = args.first().map(String::as_str);
    let chosen = selected(cases, name);
    if chosen.is_empty() {
        eprintln!("no bench named {:?}", name.unwrap_or_default());
        std::process::exit(2);
    }
    let n = calls(DEFAULT_CALLS);
    for (_, bench) in chosen {
        bench(n);
    }
}

/// The case named exactly `name`, or every case when no name is given. A name never selects a
/// case it is only part of: the gate counts one case per process, and a second case in the same
/// process would be counted into the first one's calls.
fn selected<'a>(cases: &'a [Case], name: Option<&str>) -> Vec<&'a Case> {
    cases.iter().filter(|(case, _)| name.is_none_or(|name| name == *case)).collect()
}

/// Whether an allocation reaches `ALLOCATIONS`, that is whether `AllocationCounter` is the global allocator.
pub fn allocations_counted() -> bool {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    drop(black_box(Box::new(0u8)));
    ALLOCATIONS.load(Ordering::Relaxed) != before
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nothing(_: u64) {}

    const CASES: [Case; 2] = [("poll", nothing), ("poll_ready", nothing)];

    fn names(chosen: Vec<&Case>) -> Vec<&'static str> {
        chosen.into_iter().map(|(name, _)| *name).collect()
    }

    #[test]
    fn a_name_selects_only_the_case_it_names_whole() {
        assert_eq!(names(selected(&CASES, Some("poll"))), ["poll"]);
        assert_eq!(names(selected(&CASES, Some("poll_ready"))), ["poll_ready"]);
    }

    #[test]
    fn a_name_that_is_part_of_a_case_selects_none() {
        assert!(selected(&CASES, Some("ready")).is_empty());
        assert!(selected(&CASES, Some("pol")).is_empty());
    }

    #[test]
    fn under_another_global_allocator_no_allocation_is_counted() {
        // A test binary runs on the system allocator: `tests/counted.rs` is the other side.
        assert!(!allocations_counted());
    }

    #[test]
    fn no_name_selects_every_case() {
        assert_eq!(names(selected(&CASES, None)), ["poll", "poll_ready"]);
    }
}
