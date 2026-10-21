//! Caches a mount's files in memory and drops entries when a filesystem watcher reports a change.
//!
//! The first miss starts the watcher on its own thread, because watching walks the tree and a
//! large tree would hold every miss behind it. Until the watcher is up, and after it fails,
//! misses are served and nothing is kept. Each worker interpreter registers its own copy of the
//! app's mounts, so only mounts a request reaches are watched.
//!
//! A change is seen after the watcher's delivery latency, not at once: microseconds with
//! inotify, milliseconds with FSEvents. A filesystem that does not report changes, such as a
//! network mount, is not followed.

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use notify::event::{AccessKind, AccessMode};
use notify::{EventKind, RecursiveMode, Watcher};

use super::degraded::{self, StaticDegradation};
use crate::sync::{AtomicU64, Mutex, Ordering};
use super::{CanonicalDir, StaticFile};

/// Caps every mount's cache in the process at 128 MiB, enough for any reasonable static site
/// without letting a misconfigured root blow the RSS budget. Past the cap new files are served
/// uncached; entries leave only when the disk changes under them. It is built at the first mount
/// because Loom's atomics have no `const` constructor.
pub(super) static PROCESS_BUDGET: LazyLock<Budget> = LazyLock::new(|| Budget::new(128 * 1024 * 1024));

/// It is written only when an entry is kept or dropped, never on a hit.
pub(super) struct Budget {
    cap: u64,
    held: AtomicU64,
}

/// Holds bytes for an entry about to be kept; dropping it uncommitted hands them back.
#[must_use = "a reservation dropped uncommitted hands its bytes back"]
struct Reservation<'a> {
    budget: &'a Budget,
    len: u64,
}

impl Budget {
    pub(super) fn new(cap: u64) -> Self {
        Self {
            cap,
            held: AtomicU64::new(0),
        }
    }

    /// Takes `len` bytes unless that would pass the cap. It checks and takes in one step, so two
    /// concurrent reservations cannot both slip under the cap, and a sum past `u64::MAX` counts as
    /// past the cap instead of wrapping under it.
    fn reserve(&self, len: u64) -> Option<Reservation<'_>> {
        self.held
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                held.checked_add(len).filter(|&after| after <= self.cap)
            })
            .ok()?;
        Some(Reservation { budget: self, len })
    }

    fn release(&self, len: u64) {
        self.held.fetch_sub(len, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn held(&self) -> u64 {
        self.held.load(Ordering::Relaxed)
    }
}

impl Reservation<'_> {
    /// Keeps the bytes held until [`Budget::release`] and hands back `replaced`, the bytes of
    /// the entry this one took the place of.
    fn commit(self, replaced: Option<u64>) {
        let this = std::mem::ManuallyDrop::new(self);
        if let Some(len) = replaced {
            this.budget.release(len);
        }
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.budget.release(self.len);
    }
}

pub(super) struct MountCache {
    root: CanonicalDir,
    state: Arc<CacheState>,
    /// It is dropped with the mount, which stops the watcher's thread. The thread that starts the
    /// watcher holds it weakly, so a mount dropped meanwhile drops the new watcher too.
    watcher: Arc<Mutex<Option<notify::RecommendedWatcher>>>,
}

/// Only a mount whose watcher follows the disk keeps what it reads, since a change nothing sees
/// would leave the kept bytes stale. A failed mount is also listed by
/// [`degraded::uncached_mounts`] with the reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountWatch {
    Unstarted,
    /// The watcher's thread is walking the tree.
    Started,
    Active,
    /// The mount is served uncached for good.
    Failed,
}

impl MountWatch {
    pub fn keeps_reads(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Moves `from` to `to` and returns false if the watch was not at `from`.
    fn advance(&mut self, from: MountWatch, to: MountWatch) -> bool {
        if *self != from {
            return false;
        }
        *self = to;
        true
    }

    /// Marks the watch failed and returns true only the first time.
    fn fail(&mut self) -> bool {
        std::mem::replace(self, MountWatch::Failed) != MountWatch::Failed
    }
}

/// The watcher holds this rather than the [`MountCache`], so dropping the mount drops the
/// watcher and stops its thread.
struct CacheState {
    budget: &'static Budget,
    /// It is keyed on the normalized request path under the mount, so a hit resolves no path.
    files: DashMap<Box<[u8]>, StaticFile>,
    /// Every event bumps it before dropping the entries it touches.
    generation: AtomicU64,
    /// Only misses and the watcher's thread take the lock; a hit never reads it.
    watch: Mutex<MountWatch>,
}

impl std::fmt::Debug for MountCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MountCache")
            .field("root", &self.root.0)
            .field("watch", &*self.state.watch.lock())
            .field("entries", &self.state.files.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Generation(u64);

#[derive(Clone, Copy)]
pub(super) struct MissGeneration(Generation);

#[derive(Debug, thiserror::Error)]
#[error("static directory {} cannot be watched, so it is served uncached: {source}", root.display())]
pub(super) struct WatchError {
    root: PathBuf,
    #[source]
    source: WatcherStartError,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum WatcherStartError {
    #[error(transparent)]
    Watch(#[from] notify::Error),
    #[error("the thread that starts the watcher could not be spawned: {0}")]
    Thread(#[source] std::io::Error),
}

impl MountCache {
    pub(super) fn new(root: CanonicalDir, budget: &'static Budget) -> Self {
        Self {
            root,
            state: Arc::new(CacheState {
                budget,
                files: DashMap::new(),
                generation: AtomicU64::new(0),
                watch: Mutex::new(MountWatch::Unstarted),
            }),
            watcher: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn root(&self) -> &Path {
        &self.root.0
    }

    pub(super) fn watch(&self) -> MountWatch {
        *self.state.watch.lock()
    }

    /// Runs `f` under the entry's shard read lock.
    pub(super) fn with<R>(&self, key: &[u8], f: impl FnOnce(&StaticFile) -> R) -> Option<R> {
        self.state.files.get(key).map(|file| f(&file))
    }

    /// Returns where events stand before a miss reads the file, or `None` if the watcher does not
    /// follow the disk yet or any more; the first miss starts it.
    pub(super) fn begin_miss(&self) -> Option<MissGeneration> {
        let watch = *self.state.watch.lock();
        if watch.keeps_reads() {
            // The watch is active only after `watch()` registered, so every change from here on
            // reaches the watcher.
            return Some(MissGeneration(Generation(self.state.generation.load(Ordering::SeqCst))));
        }
        if watch == MountWatch::Unstarted && self.state.watch.lock().advance(MountWatch::Unstarted, MountWatch::Started) {
            self.start_watching();
        }
        None
    }

    /// Starts the watcher on its own thread; if it fails, the mount is served uncached and the
    /// reason is logged once.
    fn start_watching(&self) {
        let root = self.root.0.clone();
        let state = Arc::clone(&self.state);
        let slot = Arc::downgrade(&self.watcher);
        let spawned = std::thread::Builder::new()
            .name("mipyre-static-watch".to_string())
            .spawn(move || match watch_tree(&root, &state) {
                Ok(watcher) => keep_watcher(&slot, watcher, &state.watch),
                Err(error) => tracing::error!(error = %error, "static mount is served uncached"),
            });
        // The thread ends once the watcher is up or has failed, so nothing joins it.
        if let Err(e) = spawned {
            let error = not_watched(&self.root.0, &self.state, WatcherStartError::Thread(e));
            tracing::error!(error = %error, "static mount is served uncached");
        }
    }

    /// Keeps `file` unless an event arrived since `ticket`, the watcher failed, or the budget is
    /// spent. The checks run under the key's shard lock, which an event takes after bumping the
    /// generation and a failure after marking the watch, so either one stops this insert or
    /// removes what it inserted.
    pub(super) fn keep(&self, key: Box<[u8]>, ticket: MissGeneration, file: StaticFile) {
        let Some(reservation) = self.state.budget.reserve(file.cached_len()) else {
            return;
        };
        // The entry holds the key's shard write lock until it is dropped.
        let slot = self.state.files.entry(key);
        if Generation(self.state.generation.load(Ordering::SeqCst)) != ticket.0
            || !self.state.watch.lock().keeps_reads()
        {
            return;
        }
        match slot {
            Entry::Occupied(mut occupied) => {
                let prev = occupied.insert(file);
                reservation.commit(Some(prev.cached_len()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(file);
                reservation.commit(None);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, key: &[u8]) -> bool {
        self.state.files.contains_key(key)
    }
}

impl CacheState {
    fn changed(&self, path: &Path) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.files.retain(|_, file| {
            let stale = file.depends_on(path);
            if stale {
                self.budget.release(file.cached_len());
            }
            !stale
        });
    }

    fn clear(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.files.retain(|_, file| {
            self.budget.release(file.cached_len());
            false
        });
    }

    /// Stops caching for good, because what a failed watcher missed is unknown.
    fn stop_following(&self) {
        if self.watch.lock().fail() {
            degraded::count(StaticDegradation::UnwatchedMount);
        }
        self.clear();
    }

    fn on_event(&self, root: &Path, result: notify::Result<notify::Event>) {
        let event = match result {
            Ok(event) => event,
            Err(error) => {
                tracing::warn!(
                    root = %root.display(),
                    error = %error,
                    "static file watcher failed; this mount is served uncached from now on"
                );
                degraded::mark_uncached(
                    root,
                    format!("the watcher failed, served uncached: {error}"),
                );
                self.stop_following();
                return;
            }
        };
        if event.need_rescan() {
            self.clear();
            return;
        }
        match event.kind {
            // Opens and reads, ours among them, change nothing; a close after writing does.
            EventKind::Access(AccessKind::Close(AccessMode::Write)) => {}
            EventKind::Access(_) => return,
            _ => {}
        }
        if event.paths.is_empty() {
            self.clear();
            return;
        }
        for path in &event.paths {
            self.changed(path);
        }
    }
}

/// Keeps the watcher in the mount's slot and marks the watch active unless it already failed,
/// since its first event can be an error; if the mount is gone, the watcher is dropped.
fn keep_watcher(
    slot: &Weak<Mutex<Option<notify::RecommendedWatcher>>>,
    watcher: notify::RecommendedWatcher,
    watch: &Mutex<MountWatch>,
) {
    if let Some(slot) = slot.upgrade() {
        *slot.lock() = Some(watcher);
        watch.lock().advance(MountWatch::Started, MountWatch::Active);
    }
}

/// Watches `root` recursively and blocks while it walks the tree. Symlinks are not followed,
/// because every directory a served file can live in is inside the root, and a symlink
/// changing is an event on its own directory.
fn watch_tree(root: &Path, state: &Arc<CacheState>) -> Result<notify::RecommendedWatcher, WatchError> {
    let handler_state = Arc::clone(state);
    let handler_root = root.to_path_buf();
    notify::RecommendedWatcher::new(
        move |result| handler_state.on_event(&handler_root, result),
        notify::Config::default().with_follow_symlinks(false),
    )
    .and_then(|mut watcher| {
        watcher.watch(root, RecursiveMode::Recursive)?;
        Ok(watcher)
    })
    .map_err(|source| not_watched(root, state, WatcherStartError::Watch(source)))
}

fn not_watched(root: &Path, state: &CacheState, source: WatcherStartError) -> WatchError {
    state.stop_following();
    let error = WatchError {
        root: root.to_path_buf(),
        source,
    };
    degraded::mark_uncached(root, format!("served uncached: {error}"));
    error
}

#[cfg(test)]
#[cfg(sync_model)]
mod loom_model;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use super::super::FileContents;

    fn file(root: &Path) -> StaticFile {
        sized(root, b"x")
    }

    fn sized(root: &Path, original: &'static [u8]) -> StaticFile {
        StaticFile::new(
            FileContents {
                bytes: Bytes::from_static(original),
                modified: None,
            },
            None,
            root.join("a.txt"),
            root.join("a.txt"),
        )
    }

    /// Waits until the watcher follows the disk and returns where events stand.
    fn followed(cache: &MountCache) -> MissGeneration {
        if let Some(ticket) = cache.begin_miss() {
            return ticket;
        }
        while cache.watch() == MountWatch::Started {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        cache.begin_miss().expect("the watcher is up")
    }

    #[test]
    fn only_an_active_watch_keeps_reads() {
        assert!(!MountWatch::Unstarted.keeps_reads());
        assert!(!MountWatch::Started.keeps_reads());
        assert!(MountWatch::Active.keeps_reads());
        assert!(!MountWatch::Failed.keeps_reads());
    }

    /// The watcher can come up after its first event was an error.
    #[test]
    fn a_failed_watch_does_not_become_active() {
        let mut watch = MountWatch::Started;
        assert!(watch.fail(), "the first failure is new");
        assert!(!watch.fail(), "the second is not");
        assert!(!watch.advance(MountWatch::Started, MountWatch::Active));
        assert_eq!(watch, MountWatch::Failed);

        let mut watch = MountWatch::Started;
        assert!(watch.advance(MountWatch::Started, MountWatch::Active));
        assert_eq!(watch, MountWatch::Active);
    }

    /// Gives one test's cache its own budget, since the process budget is shared by every test.
    fn test_budget(cap: u64) -> &'static Budget {
        Box::leak(Box::new(Budget::new(cap)))
    }

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("mipyre-cache-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::canonicalize(&root).unwrap()
    }

    #[test]
    fn a_budget_holds_up_to_its_cap_and_hands_back_what_is_not_kept() {
        let budget = Budget::new(10);
        let first = budget.reserve(4).expect("4 of 10");
        assert_eq!(budget.held(), 4);
        assert!(budget.reserve(7).is_none(), "4 + 7 is past 10");
        assert_eq!(budget.held(), 4);
        let second = budget.reserve(6).expect("4 + 6 is exactly the cap");
        assert_eq!(budget.held(), 10);
        drop(second);
        assert_eq!(budget.held(), 4);
        first.commit(None);
        assert_eq!(budget.held(), 4);
        budget.reserve(5).expect("4 + 5").commit(Some(4));
        assert_eq!(budget.held(), 5);
        budget.release(5);
        assert_eq!(budget.held(), 0);
    }

    /// A length that would carry the sum past `u64::MAX` is refused like any past the cap, and
    /// takes nothing: it does not wrap round to a small number under it.
    #[test]
    fn a_reservation_whose_sum_passes_u64_max_is_refused() {
        let budget = Budget::new(u64::MAX - 1);
        let held = budget.reserve(10).expect("10 of nearly u64::MAX");
        assert!(budget.reserve(u64::MAX - 5).is_none(), "10 + (u64::MAX - 5) passes u64::MAX");
        assert_eq!(budget.held(), 10);
        drop(held);
        assert_eq!(budget.held(), 0);
    }

    #[test]
    fn a_cache_counts_each_entry_once_and_refuses_what_passes_its_budget() {
        let root = temp_root("budget");
        let budget = test_budget(8);
        let cache = MountCache::new(CanonicalDir(root.clone()), budget);

        for _ in 0..2 {
            let ticket = followed(&cache);
            cache.keep(Box::from(&b"a.txt"[..]), ticket, sized(&root, b"abc"));
        }
        assert_eq!(budget.held(), 3);
        let ticket = followed(&cache);
        cache.keep(Box::from(&b"a.txt"[..]), ticket, sized(&root, b"abcde"));
        assert_eq!(budget.held(), 5);

        let ticket = followed(&cache);
        cache.keep(Box::from(&b"b.txt"[..]), ticket, sized(&root, b"wxyz"));
        assert!(!cache.contains(b"b.txt"));
        assert_eq!(budget.held(), 5);

        let ticket = followed(&cache);
        cache.state.changed(&root.join("other.txt"));
        cache.keep(Box::from(&b"c.txt"[..]), ticket, sized(&root, b"x"));
        assert!(!cache.contains(b"c.txt"));
        assert_eq!(budget.held(), 5);

        cache.state.changed(&root);
        assert!(!cache.contains(b"a.txt"));
        assert_eq!(budget.held(), 0);

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_read_that_raced_an_event_is_not_kept() {
        let root = std::env::temp_dir().join(format!("mipyre-cache-race-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let cache = MountCache::new(CanonicalDir(root.clone()), test_budget(u64::MAX));

        let ticket = followed(&cache);
        cache.state.changed(&root.join("a.txt"));
        cache.keep(Box::from(&b"a.txt"[..]), ticket, file(&root));
        assert!(!cache.contains(b"a.txt"));

        let ticket = followed(&cache);
        cache.keep(Box::from(&b"a.txt"[..]), ticket, file(&root));
        assert!(cache.contains(b"a.txt"));
        cache.state.changed(&root.join("b.txt"));
        assert!(cache.contains(b"a.txt"));
        cache.state.changed(&root);
        assert!(!cache.contains(b"a.txt"));

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn the_first_miss_starts_the_watcher_without_waiting_for_it_and_keeps_nothing() {
        let root = temp_root("background");
        let cache = MountCache::new(CanonicalDir(root.clone()), test_budget(u64::MAX));
        assert!(cache.begin_miss().is_none(), "the first miss gets no ticket");
        let ticket = followed(&cache);
        cache.keep(Box::from(&b"a.txt"[..]), ticket, file(&root));
        assert!(cache.contains(b"a.txt"), "kept once the watcher follows");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_root_that_cannot_be_watched_keeps_nothing() {
        let root = temp_root("unwatchable");
        let cache = MountCache::new(CanonicalDir(root.clone()), test_budget(u64::MAX));
        std::fs::remove_dir_all(&root).unwrap();
        assert!(cache.begin_miss().is_none());
        while cache.watch() == MountWatch::Started {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(cache.begin_miss().is_none(), "never followed");
        assert_eq!(cache.watch(), MountWatch::Failed);
        assert!(degraded::uncached_mounts().iter().any(|(r, _)| r == &root));
    }

    #[test]
    fn a_failed_watcher_stops_caching() {
        let root = std::env::temp_dir().join(format!("mipyre-cache-fail-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let cache = MountCache::new(CanonicalDir(root.clone()), test_budget(u64::MAX));
        let ticket = followed(&cache);
        cache.keep(Box::from(&b"a.txt"[..]), ticket, file(&root));
        assert!(cache.contains(b"a.txt"));

        let ticket = followed(&cache);
        cache
            .state
            .on_event(&root, Err(notify::Error::generic("watch limit reached")));
        assert!(!cache.contains(b"a.txt"));
        assert_eq!(cache.watch(), MountWatch::Failed);
        cache.keep(Box::from(&b"a.txt"[..]), ticket, file(&root));
        assert!(!cache.contains(b"a.txt"));
        assert!(cache.begin_miss().is_none());

        std::fs::remove_dir_all(&root).unwrap();
    }
}
