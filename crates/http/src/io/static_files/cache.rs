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
use std::sync::{Arc, OnceLock, Weak};

use notify::event::{AccessKind, AccessMode, ModifyKind};
use notify::{EventKind, RecursiveMode, Watcher};

use super::degraded::{self, StaticDegradation};
use super::{CanonicalDir, RequestPath, StaticFile};
use crate::sync::{AtomicU64, DashMap, Entry, Mutex, Ordering};

/// The cap a server's static mounts are cached under, 128 MiB: enough for any reasonable static
/// site without letting a misconfigured root blow the RSS budget.
pub const DEFAULT_CACHE_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

/// The bytes the static mounts of a server may hold in memory together, one per entry point
/// (`app.run()`, one `TestClient`), handed to each mount's cache when the run is sealed. Past the
/// cap new files are served uncached; entries leave only when the disk changes under them. It is
/// written only when an entry is kept or dropped, never on a hit.
pub struct CacheBudget {
    cap: u64,
    held: AtomicU64,
}

/// Holds bytes for an entry about to be kept; dropping it uncommitted hands them back.
#[must_use = "a reservation dropped uncommitted hands its bytes back"]
pub(super) struct Reservation<'a> {
    budget: &'a CacheBudget,
    len: u64,
}

impl CacheBudget {
    pub fn new(cap: u64) -> Self {
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
    pub(super) fn held(&self) -> u64 {
        self.held.load(Ordering::Relaxed)
    }
}

impl Reservation<'_> {
    /// Keeps the bytes held until [`CacheBudget::release`].
    fn commit(self) {
        let _held = std::mem::ManuallyDrop::new(self);
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.budget.release(self.len);
    }
}

pub(super) struct MountCache {
    state: Arc<CacheState>,
    /// It is dropped with the mount, which stops the watcher's thread. The thread that starts the
    /// watcher holds it weakly, so a mount dropped meanwhile drops the new watcher too.
    watcher: Arc<Mutex<Option<notify::RecommendedWatcher>>>,
}

/// Only a mount whose watcher follows the disk keeps what it reads, since a change nothing sees
/// would leave the kept bytes stale. A failed mount says why ([`MountCache::uncached_because`]).
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

    /// Sends the watch back for the next miss to start again, unless it failed for good.
    fn restart(&mut self) {
        if *self != MountWatch::Failed {
            *self = MountWatch::Unstarted;
        }
    }

    /// Marks the watch failed and returns true only the first time.
    fn fail(&mut self) -> bool {
        std::mem::replace(self, MountWatch::Failed) != MountWatch::Failed
    }
}

/// The watcher holds this rather than the [`MountCache`], so dropping the mount drops the
/// watcher and stops its thread.
///
/// A file is held once, in `contents`, however many request paths name it; each path is an
/// [`Alias`] in `files`, charged to the budget for what it holds itself. Symlinks and a file's
/// own name are all hits, and the spellings of one name on a case-insensitive filesystem cost
/// their own bytes, so a client asking for them fills the budget instead of growing the map
/// unbounded. A file is in `contents` exactly while an alias points to it. Locks are taken
/// `files` before `contents`.
struct CacheState {
    /// It is the mount's directory as configured, absolute but with its symlinks unresolved.
    configured: PathBuf,
    /// It is what `configured` resolves to. It changes only when a symlink on `configured` is
    /// retargeted, and only misses and the watcher's thread read it.
    root: Mutex<Arc<CanonicalDir>>,
    /// It is set by the first run that serves the mount and kept by later runs of the same app,
    /// whose route tables share this cache; until then nothing is kept.
    budget: OnceLock<Arc<CacheBudget>>,
    /// It is keyed on the normalized request path under the mount, so a hit resolves no path.
    files: DashMap<Box<[u8]>, Alias>,
    /// It is keyed on the file's resolved path.
    contents: DashMap<PathBuf, CacheFile>,
    /// Every event bumps it before dropping the entries it touches.
    generation: AtomicU64,
    /// Only misses and the watcher's thread take the lock; a hit never reads it.
    watch: Mutex<MountWatch>,
    /// Holds why the mount is served uncached once its watch failed; it is written before the
    /// watch reads as failed, so whoever sees `Failed` finds it.
    uncached_because: Mutex<Option<String>>,
}

/// One request path kept in the cache, pointing at the file it names.
pub(super) struct Alias {
    requested: RequestPath,
    file: Arc<StaticFile>,
}

/// A file held in the cache, with how many aliases point to it.
struct CacheFile {
    file: Arc<StaticFile>,
    aliases: usize,
}

/// Counts what an alias costs on top of its key's and its paths' bytes: the map's slot for the
/// key and the alias. The table's spare capacity is not counted.
const ALIAS_SLOT_BYTES: usize = std::mem::size_of::<(Box<[u8]>, Alias)>();

/// Returns the bytes an alias under `key` for `requested` holds.
pub(super) fn alias_cost(key: &[u8], requested: &RequestPath) -> u64 {
    (key.len() + requested.heap_len() + ALIAS_SLOT_BYTES) as u64
}

impl std::fmt::Debug for MountCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The watch is let go before the entries are counted: an insert takes the watch while it
        // holds its key's shard, so holding the watch across the count deadlocks with a miss.
        let watch = self.watch();
        f.debug_struct("MountCache")
            .field("root", &self.root().0)
            .field("watch", &watch)
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
    #[error("the symlinks on the configured path could not be read: {0}")]
    Links(#[source] std::io::Error),
}

impl MountCache {
    pub(super) fn new(configured: PathBuf, root: CanonicalDir) -> Self {
        Self {
            state: Arc::new(CacheState {
                configured,
                root: Mutex::new(Arc::new(root)),
                budget: OnceLock::new(),
                files: DashMap::new(),
                contents: DashMap::new(),
                generation: AtomicU64::new(0),
                watch: Mutex::new(MountWatch::Unstarted),
                uncached_because: Mutex::new(None),
            }),
            watcher: Arc::new(Mutex::new(None)),
        }
    }

    /// Caches within `budget` from now on, unless an earlier run of the same app handed it one:
    /// its entries are held against that one, so it stays.
    pub(super) fn cache_within(&self, budget: &Arc<CacheBudget>) {
        self.state.budget.get_or_init(|| Arc::clone(budget));
    }

    pub(super) fn root(&self) -> Arc<CanonicalDir> {
        Arc::clone(&self.state.root.lock())
    }

    pub(super) fn watch(&self) -> MountWatch {
        *self.state.watch.lock()
    }

    /// Returns why the mount is served uncached, or `None` while its watch has not failed.
    pub(super) fn uncached_because(&self) -> Option<String> {
        self.state.uncached_because.lock().clone()
    }

    /// Runs `f` under the entry's shard read lock.
    pub(super) fn with<R>(&self, key: &[u8], f: impl FnOnce(&StaticFile) -> R) -> Option<R> {
        self.state.files.get(key).map(|alias| f(&alias.file))
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
        self.start_watch();
        None
    }

    /// Starts the watcher in the background unless it was started already: a server does this
    /// as it starts, without waiting for it, so a watch that cannot be set up is
    /// reported before the first request rather than at the first miss.
    pub(super) fn start_watch(&self) {
        if self
            .state
            .watch
            .lock()
            .advance(MountWatch::Unstarted, MountWatch::Started)
        {
            self.start_watching();
        }
    }

    /// Starts the watcher on its own thread; if it fails, the mount is served uncached and the
    /// reason is logged once.
    fn start_watching(&self) {
        let root = self.root();
        let state = Arc::clone(&self.state);
        let slot = Arc::downgrade(&self.watcher);
        let spawned = std::thread::Builder::new()
            .name("mipyre-static-watch".to_string())
            .spawn(move || match watch_tree(&root.0, &state) {
                Ok(watcher) => keep_watcher(&slot, watcher, &state.watch),
                Err(error) => tracing::error!(error = %error, "static mount is served uncached"),
            });
        // The thread ends once the watcher is up or has failed, so nothing joins it.
        if let Err(e) = spawned {
            let error = not_watched(&self.root().0, &self.state, WatcherStartError::Thread(e));
            tracing::error!(error = %error, "static mount is served uncached");
        }
    }

    /// Takes `len` bytes of the budget for a file about to be read whole, or returns `None` when
    /// the mount has no budget or it cannot hold them, so the file is not read into memory at all.
    pub(super) fn reserve(&self, len: u64) -> Option<Reservation<'_>> {
        self.state.budget.get()?.reserve(len)
    }

    /// Returns the file the cache holds at `resolved` for another request path, so a miss of a
    /// new path to it keeps an alias without reading the file again.
    pub(super) fn held(&self, resolved: &Path) -> Option<Arc<StaticFile>> {
        self.state.contents.get(resolved).map(|kept| Arc::clone(&kept.file))
    }

    /// Keeps `requested` as an alias of `file`, which this miss read whole under `reserved`, and
    /// returns the file kept under it; it refuses a file that is not the size `reserved` holds,
    /// since it changed between its `fstat` and its read, and what [`Self::insert_alias`] refuses.
    pub(super) fn keep_read(
        &self,
        key: Box<[u8]>,
        ticket: MissGeneration,
        requested: RequestPath,
        reserved: Reservation<'_>,
        file: Arc<StaticFile>,
    ) -> Option<Arc<StaticFile>> {
        if file.cached_len() != reserved.len {
            return None;
        }
        self.insert_alias(key, ticket, requested, file, Some(reserved))
    }

    /// Keeps `requested` as an alias of `file`, which [`Self::held`] returned, and returns the
    /// file kept under it, unless [`Self::insert_alias`] refuses it.
    pub(super) fn keep_held(
        &self,
        key: Box<[u8]>,
        ticket: MissGeneration,
        requested: RequestPath,
        file: Arc<StaticFile>,
    ) -> Option<Arc<StaticFile>> {
        self.insert_alias(key, ticket, requested, file, None)
    }

    /// Keeps `requested` as an alias of `file` unless an event arrived since `ticket`, the
    /// watcher failed, the key is kept already, or the budget cannot take the alias. The alias
    /// points at the file another alias holds when there is one, and `reserved` goes back;
    /// otherwise `file` is kept under `reserved`, and a file read by no one (`reserved` is
    /// `None`) that no alias holds any more is not kept. The checks run under the key's shard
    /// lock, which an event takes after bumping the generation and a failure after marking the
    /// watch, so either one stops this insert or removes what it inserted.
    fn insert_alias(
        &self,
        key: Box<[u8]>,
        ticket: MissGeneration,
        requested: RequestPath,
        file: Arc<StaticFile>,
        reserved: Option<Reservation<'_>>,
    ) -> Option<Arc<StaticFile>> {
        let budget = self.state.budget.get()?;
        let cost = alias_cost(&key, &requested);
        // The entry holds the key's shard write lock until it is dropped.
        let Entry::Vacant(slot) = self.state.files.entry(key) else {
            return None;
        };
        if Generation(self.state.generation.load(Ordering::SeqCst)) != ticket.0
            || !self.state.watch.lock().keeps_reads()
        {
            return None;
        }
        // The file another alias holds is what is on disk: an event since it was kept would have
        // dropped it, and none has come since this read began.
        let (file, alias) = match self.state.contents.entry(file.resolved.clone()) {
            Entry::Occupied(mut kept) => {
                // Handed back before the alias is taken, so the budget never holds the file twice.
                drop(reserved);
                let alias = budget.reserve(cost)?;
                kept.get_mut().aliases += 1;
                (Arc::clone(&kept.get().file), alias)
            }
            Entry::Vacant(vacant) => {
                let reserved = reserved?;
                let alias = budget.reserve(cost)?;
                vacant.insert(CacheFile {
                    file: Arc::clone(&file),
                    aliases: 1,
                });
                reserved.commit();
                (file, alias)
            }
        };
        slot.insert(Alias {
            requested,
            file: Arc::clone(&file),
        });
        alias.commit();
        Some(file)
    }

    #[cfg(test)]
    #[cfg(not(sync_model))]
    pub(super) fn contains(&self, key: &[u8]) -> bool {
        self.state.files.contains_key(key)
    }
}

/// Hands the kept bytes back to the budget when the last clone of the mount goes. The watcher's
/// handler can hold the state a while longer, but nothing keeps into it once the mount is gone.
impl Drop for MountCache {
    fn drop(&mut self) {
        self.state.clear();
    }
}

impl CacheState {
    /// Drops every alias whose path or file the change can make stale; a stale file takes all
    /// its aliases with it.
    fn changed(&self, path: &Path) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.files.retain(|key, alias| {
            let stale = alias.requested.depends_on(path) || alias.file.depends_on(path);
            if stale {
                self.forget(key, alias);
            }
            !stale
        });
    }

    fn clear(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.files.retain(|key, alias| {
            self.forget(key, alias);
            false
        });
    }

    /// Hands back the bytes of `alias`, kept under `key`, and its file's when it was the last
    /// alias pointing there; the caller holds `key`'s shard lock in `files` and removes the alias.
    /// An alias is kept only under a budget, so one is set whenever there is one to forget.
    fn forget(&self, key: &[u8], alias: &Alias) {
        let budget = self.budget.get();
        if let Some(budget) = budget {
            budget.release(alias_cost(key, &alias.requested));
        }
        let last = self.contents.remove_if_mut(&alias.file.resolved, |_, kept| {
            kept.aliases -= 1;
            kept.aliases == 0
        });
        if let (Some((_, kept)), Some(budget)) = (last, budget) {
            budget.release(kept.file.cached_len());
        }
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
                *self.uncached_because.lock() = Some(format!("the watcher failed, served uncached: {error}"));
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
        if event.paths.iter().any(|path| !path.starts_with(root)) {
            self.follow_configured();
        }
        if event.paths.iter().any(|path| leaves_root(&event.kind, path, root)) {
            tracing::info!(
                root = %root.display(),
                "static root was moved or removed; it is watched again from the next miss"
            );
            self.watch.lock().restart();
            self.clear();
            return;
        }
        for path in &event.paths {
            self.changed(path);
        }
    }
}

impl CacheState {
    /// Resolves the configured path again after an event beside one of its symlinks; if it names
    /// another directory now, the cache is cleared and the next miss watches the new one.
    fn follow_configured(&self) {
        let now = match CanonicalDir::resolve(&self.configured) {
            Ok(now) => now,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "the static root no longer resolves; serving the directory it named before"
                );
                return;
            }
        };
        let mut root = self.root.lock();
        if root.0 == now.0 {
            return;
        }
        tracing::info!(
            configured = %self.configured.display(),
            from = %root.0.display(),
            to = %now.0.display(),
            "static root now names another directory; it is watched again from the next miss"
        );
        *root = Arc::new(now);
        drop(root);
        self.watch.lock().restart();
        self.clear();
    }
}

/// Tells whether `kind` at `path` takes the watched directory away from `root`. The watch follows
/// the directory itself, not its path, so whatever is renamed into its place is not watched.
fn leaves_root(kind: &EventKind, path: &Path, root: &Path) -> bool {
    path == root && matches!(kind, EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_)))
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
    // The directory holding each symlink on the configured path is watched too, so retargeting
    // one is an event; it is watched alone, since nothing in it but the link is served.
    let link_dirs = super::read::symlink_dirs(&state.configured)
        .map_err(|source| not_watched(root, state, WatcherStartError::Links(source)))?;
    let handler_state = Arc::clone(state);
    let handler_root = root.to_path_buf();
    notify::RecommendedWatcher::new(
        move |result| handler_state.on_event(&handler_root, result),
        notify::Config::default().with_follow_symlinks(false),
    )
    .and_then(|mut watcher| {
        watcher.watch(root, RecursiveMode::Recursive)?;
        for dir in &link_dirs {
            watcher.watch(dir, RecursiveMode::NonRecursive)?;
        }
        Ok(watcher)
    })
    .map_err(|source| not_watched(root, state, WatcherStartError::Watch(source)))
}

/// Records why the mount is served uncached before the watch reads as failed, as `on_event` does:
/// whoever sees `Failed` finds the reason already there.
fn not_watched(root: &Path, state: &CacheState, source: WatcherStartError) -> WatchError {
    let error = WatchError {
        root: root.to_path_buf(),
        source,
    };
    *state.uncached_because.lock() = Some(format!("served uncached: {error}"));
    state.stop_following();
    error
}

#[cfg(test)]
#[cfg(sync_model)]
mod loom_model;

#[cfg(test)]
#[cfg(unix)]
#[cfg(not(sync_model))]
mod cache_model;

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::super::FileContents;
    use super::*;
    use bytes::Bytes;

    /// A read of `resolved` under the root, asked for as `requested`, holding `original`.
    struct Read {
        requested: RequestPath,
        file: Arc<StaticFile>,
    }

    fn file(root: &Path) -> Read {
        named(root, "a.txt", "a.txt", b"x")
    }

    fn named(root: &Path, requested: &str, resolved: &str, original: &'static [u8]) -> Read {
        let file = StaticFile::new(
            FileContents {
                bytes: Bytes::from_static(original),
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

    /// Keeps `read` under `key` as a miss does: an alias of the file the cache holds, else the
    /// file read under a reservation of its size; and tells whether it was kept.
    fn keep(cache: &MountCache, key: &str, ticket: MissGeneration, read: Read) -> bool {
        let key = Box::from(key.as_bytes());
        let kept = match cache.held(&read.file.resolved) {
            Some(held) => cache.keep_held(key, ticket, read.requested, held),
            None => cache
                .reserve(read.file.cached_len())
                .and_then(|reserved| cache.keep_read(key, ticket, read.requested, reserved, read.file)),
        };
        kept.is_some()
    }

    /// Returns what an alias under `key` for `root/key` costs.
    fn cost(root: &Path, key: &str) -> u64 {
        let requested = RequestPath {
            path: root.join(key),
            link_dirs: Box::default(),
        };
        alias_cost(key.as_bytes(), &requested)
    }

    /// A file read at another size than its `fstat` said changed while it was read: it is not
    /// kept, and the bytes taken for it go back to the budget.
    #[test]
    fn a_file_read_at_another_size_than_reserved_is_not_kept() {
        let root = temp_root("resized");
        let alias = cost(&root, "a.txt");
        let (cache, budget) = budgeted(&root, 8 + alias);
        let ticket = followed(&cache);
        let reserved = cache.reserve(4).expect("4 of 8");
        let read = named(&root, "a.txt", "a.txt", b"abc");
        assert!(cache
            .keep_read(Box::from(&b"a.txt"[..]), ticket, read.requested, reserved, read.file)
            .is_none());
        assert!(!cache.contains(b"a.txt"));
        assert_eq!(budget.held(), 0);

        let ticket = followed(&cache);
        let reserved = cache.reserve(3).expect("3 of 8");
        let read = named(&root, "a.txt", "a.txt", b"abc");
        assert!(cache
            .keep_read(Box::from(&b"a.txt"[..]), ticket, read.requested, reserved, read.file)
            .is_some());
        assert_eq!(budget.held(), 3 + alias);
        std::fs::remove_dir_all(&root).unwrap();
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
    fn only_the_root_itself_renamed_or_removed_leaves_the_root() {
        use notify::event::{CreateKind, DataChange, RemoveKind, RenameMode};
        let root = Path::new("/srv/public");
        let renamed = EventKind::Modify(ModifyKind::Name(RenameMode::From));
        assert!(leaves_root(&renamed, root, root));
        assert!(leaves_root(&EventKind::Remove(RemoveKind::Folder), root, root));
        assert!(!leaves_root(&renamed, &root.join("index.html"), root));
        assert!(!leaves_root(
            &EventKind::Remove(RemoveKind::File),
            Path::new("/srv"),
            root
        ));
        assert!(!leaves_root(
            &EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            root,
            root
        ));
        assert!(!leaves_root(&EventKind::Create(CreateKind::Folder), root, root));
    }

    #[test]
    fn a_restarted_watch_starts_again_unless_it_failed() {
        for from in [MountWatch::Unstarted, MountWatch::Started, MountWatch::Active] {
            let mut watch = from;
            watch.restart();
            assert_eq!(watch, MountWatch::Unstarted, "from {from:?}");
        }
        let mut watch = MountWatch::Failed;
        watch.restart();
        assert_eq!(watch, MountWatch::Failed);
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

    /// Makes a cache of `root` within a budget of its own, capped at `cap`.
    fn budgeted(root: &Path, cap: u64) -> (MountCache, Arc<CacheBudget>) {
        let budget = Arc::new(CacheBudget::new(cap));
        let cache = MountCache::new(root.to_path_buf(), CanonicalDir(root.to_path_buf()));
        cache.cache_within(&budget);
        (cache, budget)
    }

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("mipyre-cache-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::canonicalize(&root).unwrap()
    }

    #[test]
    fn a_budget_holds_up_to_its_cap_and_hands_back_what_is_not_kept() {
        let budget = CacheBudget::new(10);
        let first = budget.reserve(4).expect("4 of 10");
        assert_eq!(budget.held(), 4);
        assert!(budget.reserve(7).is_none(), "4 + 7 is past 10");
        assert_eq!(budget.held(), 4);
        let second = budget.reserve(6).expect("4 + 6 is exactly the cap");
        assert_eq!(budget.held(), 10);
        drop(second);
        assert_eq!(budget.held(), 4);
        first.commit();
        assert_eq!(budget.held(), 4);
        budget.reserve(5).expect("4 + 5").commit();
        budget.release(4);
        assert_eq!(budget.held(), 5);
        budget.release(5);
        assert_eq!(budget.held(), 0);
    }

    /// A length that would carry the sum past `u64::MAX` is refused like any past the cap, and
    /// takes nothing: it does not wrap round to a small number under it.
    #[test]
    fn a_reservation_whose_sum_passes_u64_max_is_refused() {
        let budget = CacheBudget::new(u64::MAX - 1);
        let held = budget.reserve(10).expect("10 of nearly u64::MAX");
        assert!(
            budget.reserve(u64::MAX - 5).is_none(),
            "10 + (u64::MAX - 5) passes u64::MAX"
        );
        assert_eq!(budget.held(), 10);
        drop(held);
        assert_eq!(budget.held(), 0);
    }

    /// A file is charged once and every alias of it for its own bytes; a stale alias goes alone,
    /// a stale file takes its aliases with it, and what passes the cap is not kept.
    #[test]
    fn a_cache_counts_each_file_and_alias_once_and_refuses_what_passes_its_budget() {
        let root = temp_root("budget");
        let alias = cost(&root, "a.txt");
        let (cache, budget) = budgeted(&root, 3 + 2 * alias + 3);

        for _ in 0..2 {
            let ticket = followed(&cache);
            keep(&cache, "a.txt", ticket, named(&root, "a.txt", "a.txt", b"abc"));
        }
        assert_eq!(budget.held(), 3 + alias, "a key kept again is counted once");
        let ticket = followed(&cache);
        keep(&cache, "l.txt", ticket, named(&root, "l.txt", "a.txt", b"abc"));
        assert!(cache.contains(b"l.txt"));
        assert_eq!(
            budget.held(),
            3 + 2 * alias,
            "a second alias of the file costs only itself"
        );

        let ticket = followed(&cache);
        keep(&cache, "b.txt", ticket, named(&root, "b.txt", "b.txt", b"wxyz"));
        assert!(!cache.contains(b"b.txt"), "4 bytes and an alias pass the 3 left");
        assert_eq!(budget.held(), 3 + 2 * alias);

        let ticket = followed(&cache);
        cache.state.changed(&root.join("other.txt"));
        keep(&cache, "c.txt", ticket, named(&root, "c.txt", "c.txt", b"x"));
        assert!(!cache.contains(b"c.txt"), "an event came after its read began");
        assert_eq!(budget.held(), 3 + 2 * alias);

        cache.state.changed(&root.join("l.txt"));
        assert!(!cache.contains(b"l.txt"));
        assert!(
            cache.contains(b"a.txt"),
            "a stale alias leaves the file to its other aliases"
        );
        assert_eq!(budget.held(), 3 + alias);

        let ticket = followed(&cache);
        keep(&cache, "l.txt", ticket, named(&root, "l.txt", "a.txt", b"abc"));
        cache.state.changed(&root.join("a.txt"));
        assert!(
            !cache.contains(b"a.txt") && !cache.contains(b"l.txt"),
            "a stale file takes every alias"
        );
        assert_eq!(budget.held(), 0);
        assert!(cache.state.contents.is_empty());

        std::fs::remove_dir_all(&root).unwrap();
    }

    /// Two servers' mounts hold their bytes against their own budgets: one spent leaves the
    /// other's room as it was.
    #[test]
    fn each_server_s_budget_is_its_own() {
        let root = temp_root("two-budgets");
        let held = 3 + cost(&root, "a.txt");
        let (first, first_budget) = budgeted(&root, held);
        let (second, second_budget) = budgeted(&root, held);
        let ticket = followed(&first);
        keep(&first, "a.txt", ticket, named(&root, "a.txt", "a.txt", b"abc"));
        assert_eq!((first_budget.held(), second_budget.held()), (held, 0));
        let ticket = followed(&second);
        keep(&second, "a.txt", ticket, named(&root, "a.txt", "a.txt", b"abc"));
        assert!(
            second.contains(b"a.txt"),
            "the first budget being spent does not refuse the second"
        );
        assert_eq!((first_budget.held(), second_budget.held()), (held, held));
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A mount shared by two runs of one app keeps the budget the first handed it, which holds
    /// its entries; a mount handed none keeps nothing.
    #[test]
    fn a_mount_keeps_the_first_budget_it_is_handed_and_none_keeps_nothing() {
        let root = temp_root("first-budget");
        let unbudgeted = MountCache::new(root.clone(), CanonicalDir(root.clone()));
        let ticket = followed(&unbudgeted);
        keep(&unbudgeted, "a.txt", ticket, named(&root, "a.txt", "a.txt", b"abc"));
        assert!(!unbudgeted.contains(b"a.txt"));

        let held = 3 + cost(&root, "a.txt");
        let (cache, first) = budgeted(&root, held);
        let later = Arc::new(CacheBudget::new(held));
        cache.cache_within(&later);
        let ticket = followed(&cache);
        keep(&cache, "a.txt", ticket, named(&root, "a.txt", "a.txt", b"abc"));
        assert_eq!((first.held(), later.held()), (held, 0));
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_read_that_raced_an_event_is_not_kept() {
        let root = std::env::temp_dir().join(format!("mipyre-cache-race-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let (cache, _budget) = budgeted(&root, u64::MAX);

        let ticket = followed(&cache);
        cache.state.changed(&root.join("a.txt"));
        keep(&cache, "a.txt", ticket, file(&root));
        assert!(!cache.contains(b"a.txt"));

        let ticket = followed(&cache);
        keep(&cache, "a.txt", ticket, file(&root));
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
        let (cache, _budget) = budgeted(&root, u64::MAX);
        assert!(cache.begin_miss().is_none(), "the first miss gets no ticket");
        let ticket = followed(&cache);
        keep(&cache, "a.txt", ticket, file(&root));
        assert!(cache.contains(b"a.txt"), "kept once the watcher follows");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_root_that_cannot_be_watched_keeps_nothing() {
        let root = temp_root("unwatchable");
        let (cache, _budget) = budgeted(&root, u64::MAX);
        std::fs::remove_dir_all(&root).unwrap();
        assert!(cache.begin_miss().is_none());
        while cache.watch() == MountWatch::Started {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(cache.begin_miss().is_none(), "never followed");
        assert_eq!(cache.watch(), MountWatch::Failed);
        assert!(cache
            .uncached_because()
            .is_some_and(|why| why.contains(&*root.to_string_lossy())));
    }

    #[test]
    fn a_watch_started_without_a_miss_fails_on_its_own_and_says_why() {
        let root = temp_root("unwatchable-at-start");
        let (cache, _budget) = budgeted(&root, u64::MAX);
        std::fs::remove_dir_all(&root).unwrap();
        cache.start_watch();
        while cache.watch() == MountWatch::Started {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(cache.watch(), MountWatch::Failed);
        assert!(cache
            .uncached_because()
            .is_some_and(|why| why.contains(&*root.to_string_lossy())));
        // A second start, as a second server of the app makes, starts nothing.
        cache.start_watch();
        assert_eq!(cache.watch(), MountWatch::Failed);
    }

    #[test]
    fn a_failed_watcher_stops_caching() {
        let root = std::env::temp_dir().join(format!("mipyre-cache-fail-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let (cache, _budget) = budgeted(&root, u64::MAX);
        let ticket = followed(&cache);
        keep(&cache, "a.txt", ticket, file(&root));
        assert!(cache.contains(b"a.txt"));

        let ticket = followed(&cache);
        cache
            .state
            .on_event(&root, Err(notify::Error::generic("watch limit reached")));
        assert!(!cache.contains(b"a.txt"));
        assert_eq!(cache.watch(), MountWatch::Failed);
        keep(&cache, "a.txt", ticket, file(&root));
        assert!(!cache.contains(b"a.txt"));
        assert!(cache.begin_miss().is_none());

        std::fs::remove_dir_all(&root).unwrap();
    }
}
