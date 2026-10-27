//! The Loom models of a mount's cache (`cache.rs`), over every interleaving, running the cache's own
//! code (its counters, locks and maps are Loom's here, `crate::sync`). Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_http --lib --profile model --target-dir target/loom loom`.
//!
//! The [`CacheBudget`] models prove: what the budget holds never passes its cap, at any step either
//! thread can see; of two reservations that each fit alone but not together, exactly one is had (a
//! refused one held nothing the other was refused for); two that fit together are both had; and
//! what is held once both threads are done is exactly what was kept.
//!
//! The [`MountCache`] models race a miss against the watcher's thread. They prove: an event on a
//! file stops a miss that began before it from keeping the file, or removes what it kept; a watch
//! that fails leaves the cache empty whatever a miss was doing; of two misses for one path one keeps
//! it; and what the budget holds after is what the cache keeps.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use loom::thread;

use super::super::{CanonicalDir, FileContents, RequestPath, StaticFile};
use super::{alias_cost, CacheBudget, Generation, MissGeneration, MountCache, MountWatch};

const CAP: u64 = 10;

/// Reserves `len` and keeps it (commits), or reports the refusal; checks the cap on the way.
fn keep(budget: &CacheBudget, len: u64) -> bool {
    let Some(reservation) = budget.reserve(len) else {
        assert!(
            budget.held() <= CAP,
            "a refused reservation saw the budget past its cap"
        );
        return false;
    };
    assert!(budget.held() <= CAP, "a reservation took the budget past its cap");
    reservation.commit();
    true
}

/// Both threads keep `(one, two)` bytes; returns which were had, and what is held after both.
fn race(one: u64, two: u64) -> (bool, bool, u64) {
    let budget = Arc::new(CacheBudget::new(CAP));
    let other = Arc::clone(&budget);
    let second = thread::spawn(move || keep(&other, two));
    let first = keep(&budget, one);
    let second = second.join().unwrap();
    (first, second, budget.held())
}

#[test]
fn two_reservations_that_fit_together_are_both_had() {
    loom::model(|| {
        let (first, second, held) = race(4, 6);
        assert!(first && second, "4 + 6 fits a cap of 10, and a reservation was refused");
        assert_eq!(held, 10);
    });
}

#[test]
fn of_two_reservations_that_fit_only_alone_exactly_one_is_had() {
    loom::model(|| {
        let (first, second, held) = race(6, 6);
        assert!(first != second, "6 and 6 against 10: had {first} and {second}");
        assert_eq!(held, 6, "what is held is the one kept");
    });
}

/// A release of a kept entry racing a reservation: the reservation is had or refused by what is
/// held when it is taken, and what is held after is exactly what was kept.
#[test]
fn a_release_racing_a_reservation_leaves_what_was_kept() {
    loom::model(|| {
        let budget = Arc::new(CacheBudget::new(CAP));
        budget.reserve(8).expect("8 of 10").commit();
        let other = Arc::clone(&budget);
        let releasing = thread::spawn(move || {
            other.release(8);
            assert!(other.held() <= CAP);
        });
        let had = keep(&budget, 4);
        releasing.join().unwrap();
        assert_eq!(budget.held(), if had { 4 } else { 0 });
    });
}

/// Reservations dropped uncommitted, from both threads: every byte comes back.
#[test]
fn reservations_dropped_uncommitted_hand_every_byte_back() {
    loom::model(|| {
        let budget = Arc::new(CacheBudget::new(CAP));
        let other = Arc::clone(&budget);
        let second = thread::spawn(move || drop(other.reserve(7)));
        drop(budget.reserve(7));
        second.join().unwrap();
        assert_eq!(budget.held(), 0);
    });
}

const ROOT: &str = "/srv";

/// Holds room for every file and alias a model keeps.
const MOUNT_CAP: u64 = 1 << 20;

/// A read of `resolved` under the root, asked for as `requested`, one byte long.
struct Read {
    requested: RequestPath,
    file: Arc<StaticFile>,
}

fn read(requested: &str, resolved: &str) -> Read {
    let root = Path::new(ROOT);
    let file = StaticFile::new(
        FileContents {
            bytes: Bytes::from_static(b"x"),
            modified: None,
        },
        None,
        root.join(resolved),
    );
    Read {
        requested: RequestPath {
            path: root.join(requested),
            link_dirs: Box::default(),
        },
        file: Arc::new(file),
    }
}

/// What an alias under `key` for the root's `key` costs.
fn cost(key: &str) -> u64 {
    alias_cost(key.as_bytes(), &read(key, key).requested)
}

fn kept(cache: &MountCache, key: &str) -> bool {
    cache.with(key.as_bytes(), |_| ()).is_some()
}

/// A mount whose watcher follows the disk, under its own budget; no watcher thread is started.
fn followed() -> (Arc<MountCache>, Arc<CacheBudget>) {
    let cache = MountCache::new(PathBuf::from(ROOT), CanonicalDir(PathBuf::from(ROOT)));
    let budget = Arc::new(CacheBudget::new(MOUNT_CAP));
    cache.cache_within(&budget);
    *cache.state.watch.lock() = MountWatch::Active;
    (Arc::new(cache), budget)
}

/// Keeps `read` under `key` as a miss does: an alias of the file the cache holds, else the file
/// read under a reservation of its size; and tells whether it was kept.
fn keep_file(cache: &MountCache, key: &str, ticket: MissGeneration, read: Read) -> bool {
    let key = Box::from(key.as_bytes());
    let kept = match cache.held(&read.file.resolved) {
        Some(held) => cache.keep_held(key, ticket, read.requested, held),
        None => cache
            .reserve(read.file.cached_len())
            .and_then(|reserved| cache.keep_read(key, ticket, read.requested, reserved, read.file)),
    };
    kept.is_some()
}

/// Runs a miss of `key` from its ticket to its insert, and returns whether it began after one event.
fn miss(cache: &MountCache, key: &str, resolved: &str) -> bool {
    let ticket = cache.begin_miss().expect("the watch is active");
    keep_file(cache, key, ticket, read(key, resolved));
    ticket.0 == Generation(1)
}

/// An event on the file racing a miss of it: the file stays kept only if the miss began after the
/// event bumped the generation (the event can still drop it then), and the budget holds what stays.
#[test]
fn an_event_stops_a_miss_that_began_before_it_or_removes_what_it_kept() {
    loom::model(|| {
        let (cache, budget) = followed();
        let watcher = Arc::clone(&cache);
        let event = thread::spawn(move || watcher.state.changed(Path::new("/srv/a.txt")));
        let after_event = miss(&cache, "a.txt", "a.txt");
        event.join().unwrap();
        let stayed = kept(&cache, "a.txt");
        assert!(
            after_event || !stayed,
            "a miss that began before the event kept the file"
        );
        assert_eq!(budget.held(), if stayed { cost("a.txt") + 1 } else { 0 });
    });
}

/// An event on a held file racing a miss that keeps a second path to it: the event drops the first
/// path, and the second stays kept only if its miss began after the event.
#[test]
fn an_event_on_a_held_file_stops_a_new_path_to_it_that_began_before_it() {
    loom::model(|| {
        let (cache, budget) = followed();
        let ticket = cache.begin_miss().expect("the watch is active");
        assert!(keep_file(&cache, "a.txt", ticket, read("a.txt", "a.txt")));
        let watcher = Arc::clone(&cache);
        let event = thread::spawn(move || watcher.state.changed(Path::new("/srv/a.txt")));
        let after_event = miss(&cache, "b.txt", "a.txt");
        event.join().unwrap();
        assert!(!kept(&cache, "a.txt"));
        let stayed = kept(&cache, "b.txt");
        assert!(
            after_event || !stayed,
            "a miss that began before the event kept a path to the file"
        );
        assert_eq!(budget.held(), if stayed { cost("b.txt") + 1 } else { 0 });
    });
}

/// A watch failing while a miss keeps a file: the cache is empty after, and holds nothing.
#[test]
fn a_failed_watch_leaves_the_cache_empty_whatever_a_miss_did() {
    loom::model(|| {
        let (cache, budget) = followed();
        let watcher = Arc::clone(&cache);
        let failing = thread::spawn(move || watcher.state.stop_following());
        if let Some(ticket) = cache.begin_miss() {
            keep_file(&cache, "a.txt", ticket, read("a.txt", "a.txt"));
        }
        failing.join().unwrap();
        assert_eq!(cache.watch(), MountWatch::Failed);
        assert!(!kept(&cache, "a.txt"));
        assert_eq!(budget.held(), 0);
    });
}

/// Two misses of one path: one keeps it, and the budget holds it once.
#[test]
fn of_two_misses_of_one_path_one_keeps_it() {
    loom::model(|| {
        let (cache, budget) = followed();
        let other = Arc::clone(&cache);
        let second = thread::spawn(move || {
            let ticket = other.begin_miss().expect("the watch is active");
            keep_file(&other, "a.txt", ticket, read("a.txt", "a.txt"))
        });
        let ticket = cache.begin_miss().expect("the watch is active");
        let first = keep_file(&cache, "a.txt", ticket, read("a.txt", "a.txt"));
        let second = second.join().unwrap();
        assert!(first != second, "kept by the first {first}, by the second {second}");
        assert_eq!(budget.held(), cost("a.txt") + 1);
    });
}

/// The mount formatted for a log line while a miss keeps a file: neither waits on the other for good.
#[test]
fn formatting_the_mount_while_a_miss_keeps_a_file_finishes() {
    loom::model(|| {
        let (cache, _budget) = followed();
        let other = Arc::clone(&cache);
        let formatting = thread::spawn(move || drop(format!("{other:?}")));
        let ticket = cache.begin_miss().expect("the watch is active");
        assert!(keep_file(&cache, "a.txt", ticket, read("a.txt", "a.txt")));
        formatting.join().unwrap();
    });
}
