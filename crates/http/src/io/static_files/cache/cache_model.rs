//! A mount's cache against reference models: one sequence of writes, deletes, watcher events and requests
//! goes to a directory on disk served by a cached mount, and every request must be answered as the models answer it.
//!
//! The first model is a map of what the test wrote to disk: a file that is there is served with exactly its bytes, one
//! that is not is not served. The second is the same mount served uncached, which reads the disk on every request; the
//! cached mount must answer it byte for byte, headers and `304`s included. The cache itself is held to its budget: what
//! the budget holds is what the files and their aliases (one per request path) hold, never past the cap, and a file
//! read while it and its alias fit is kept.
//!
//! Each change on disk is followed by the event a watcher reports for it, delivered by hand, so the sequence decides
//! when the cache learns of a change. A mount's real watcher is never started: the cached mount is marked as followed
//! and the uncached one as failed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as Counter};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::header::{HeaderValue, ACCEPT_ENCODING, ETAG, IF_NONE_MATCH};
use hyper::{HeaderMap, Response, StatusCode};
use notify::event::{AccessKind, AccessMode, CreateKind, DataChange, Flag, ModifyKind, RemoveKind, RenameMode};
use notify::{Event, EventKind};
use proptest::prelude::*;

use super::super::send::HeldBytes;
use super::super::RequestPath;
use super::super::{try_static_file, CanonicalDir, StaticMount};
use super::{alias_cost, CacheBudget, MountCache, MountWatch};
use crate::io::body::ResponseBody;
use crate::plan::static_plan::{lookups, Copies, StaticMountRule};

/// The files a sequence writes; `d/x.txt` lives in the directory a deploy replaces whole.
const FILES: [&str; 5] = ["a.txt", "b.css", "d/x.txt", "a.txt.gz", "a.txt.br"];
/// It is a symlink to `a.txt`, made before the sequence starts.
const LINK: &str = "l.txt";

/// A request path under the mount and the file it names, if it names one the sequence can write.
const REQUESTS: [(&str, Option<&str>); 9] = [
    ("a.txt", Some("a.txt")),
    ("./a.txt", Some("a.txt")),
    ("b.css", Some("b.css")),
    ("d/x.txt", Some("d/x.txt")),
    ("d//x.txt", Some("d/x.txt")),
    ("a.txt.gz", Some("a.txt.gz")),
    (LINK, Some("a.txt")),
    ("missing.txt", None),
    ("d", None),
];
const ACCEPTS: [Option<&str>; 4] = [None, Some("gzip"), Some("br"), Some("br, gzip")];

#[derive(Clone, Debug)]
enum Op {
    Write {
        file: &'static str,
        contents: Vec<u8>,
        by_rename: bool,
    },
    Remove(&'static str),
    /// A deploy removes `d` and makes it again, with `x.txt` in it or not.
    ReplaceDir(Option<Vec<u8>>),
    /// The watcher reports something that changes no file, or asks for everything to be read again.
    Notice(Notice),
    WatcherFails,
    Get {
        request: usize,
        accept: usize,
        revalidate: bool,
    },
}

#[derive(Clone, Copy, Debug)]
enum Notice {
    Opened,
    Rescan,
    NoPaths,
}

/// A response's status, headers and body, compared whole.
#[derive(Debug, PartialEq)]
struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

/// Removes the directory at the end of a case.
struct TempRoot(PathBuf);

impl Drop for TempRoot {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            tracing::warn!(root = %self.0.display(), error = %error, "the test directory is left behind");
        }
    }
}

static ROOTS: AtomicU64 = AtomicU64::new(0);

fn temp_root() -> TempRoot {
    let n = ROOTS.fetch_add(1, Counter::Relaxed);
    let root = std::env::temp_dir().join(format!("mipyre-cache-model-{}-{n}", std::process::id()));
    match std::fs::remove_dir_all(&root) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        result => result.expect("a stale directory from an earlier run is removed"),
    }
    std::fs::create_dir_all(root.join("d")).expect("the test directory is made");
    std::os::unix::fs::symlink("a.txt", root.join(LINK)).expect("the link is made");
    TempRoot(std::fs::canonicalize(&root).expect("the test directory resolves"))
}

fn mount(root: &Path, copies: Copies, budget: &Arc<CacheBudget>, watch: MountWatch) -> StaticMount {
    let cache = MountCache::new(root.to_path_buf(), CanonicalDir(root.to_path_buf()));
    cache.cache_within(budget);
    *cache.state.watch.lock() = watch;
    StaticMount {
        rule: StaticMountRule::new("/static", copies).expect("the prefix starts with '/'"),
        cache: Arc::new(cache),
        held: Arc::new(HeldBytes::new()),
    }
}

/// What the test wrote to disk.
struct Disk(BTreeMap<&'static str, Vec<u8>>);

impl Disk {
    fn file(&self, name: &str) -> Option<&[u8]> {
        self.0.get(name).map(Vec::as_slice)
    }

    /// Returns how many bytes an entry for `file` holds: the file, and its copies on a mount that serves them.
    fn cached_len(&self, file: &str, copies: Copies) -> Option<u64> {
        let original = self.file(file)?.len();
        let twins = match copies {
            Copies::Original => 0,
            Copies::Precompressed => [".gz", ".br"]
                .iter()
                .filter_map(|suffix| self.file(&format!("{file}{suffix}")))
                .map(<[u8]>::len)
                .sum(),
        };
        Some((original + twins) as u64)
    }
}

struct Harness {
    root: TempRoot,
    copies: Copies,
    cap: u64,
    budget: Arc<CacheBudget>,
    cached: StaticMount,
    uncached: StaticMount,
    disk: Disk,
    /// The `ETag` last served for each request, sent back when a request revalidates.
    etags: BTreeMap<usize, HeaderValue>,
    failed: bool,
    rt: tokio::runtime::Runtime,
}

impl Harness {
    fn new(copies: Copies, cap: u64) -> Self {
        let root = temp_root();
        let budget = Arc::new(CacheBudget::new(cap));
        let uncached_budget = Arc::new(CacheBudget::new(u64::MAX));
        Harness {
            cached: mount(&root.0, copies, &budget, MountWatch::Active),
            uncached: mount(&root.0, copies, &uncached_budget, MountWatch::Failed),
            root,
            copies,
            cap,
            budget,
            disk: Disk(BTreeMap::new()),
            etags: BTreeMap::new(),
            failed: false,
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime builds"),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.0.join(name)
    }

    fn deliver(&self, kind: EventKind, paths: &[PathBuf]) {
        let event = paths.iter().cloned().fold(Event::new(kind), Event::add_path);
        self.cached.cache.state.on_event(&self.root.0, Ok(event));
    }

    fn step(&mut self, op: &Op) -> Result<(), TestCaseError> {
        match op {
            Op::Write {
                file,
                contents,
                by_rename,
            } => self.write(file, contents, *by_rename),
            Op::Remove(file) => {
                if self.disk.0.remove(file).is_some() {
                    let path = self.path(file);
                    std::fs::remove_file(&path).expect("the file the test wrote is removed");
                    self.deliver(EventKind::Remove(RemoveKind::File), &[path]);
                }
            }
            Op::ReplaceDir(contents) => {
                let dir = self.path("d");
                std::fs::remove_dir_all(&dir).expect("the directory is removed");
                self.disk.0.remove("d/x.txt");
                self.deliver(EventKind::Remove(RemoveKind::Folder), std::slice::from_ref(&dir));
                std::fs::create_dir(&dir).expect("the directory is made again");
                if let Some(contents) = contents {
                    std::fs::write(dir.join("x.txt"), contents).expect("the file is written");
                    self.disk.0.insert("d/x.txt", contents.clone());
                }
                self.deliver(EventKind::Create(CreateKind::Folder), &[dir]);
            }
            Op::Notice(Notice::Opened) => self.deliver(
                EventKind::Access(AccessKind::Open(AccessMode::Read)),
                &[self.path("a.txt")],
            ),
            Op::Notice(Notice::Rescan) => {
                let event = Event::new(EventKind::Other).set_flag(Flag::Rescan);
                self.cached.cache.state.on_event(&self.root.0, Ok(event));
            }
            Op::Notice(Notice::NoPaths) => self.deliver(EventKind::Any, &[]),
            Op::WatcherFails => {
                self.cached
                    .cache
                    .state
                    .on_event(&self.root.0, Err(notify::Error::generic("the model's watcher stops")));
                self.failed = true;
            }
            Op::Get {
                request,
                accept,
                revalidate,
            } => self.get(*request, *accept, *revalidate)?,
        }
        self.check_budget()
    }

    /// Writes in place, or the way a deploy does, by writing beside the file and renaming over it.
    fn write(&mut self, file: &'static str, contents: &[u8], by_rename: bool) {
        let path = self.path(file);
        if by_rename {
            let beside = self.path(&format!("{file}.tmp"));
            std::fs::write(&beside, contents).expect("the file is written");
            std::fs::rename(&beside, &path).expect("the file is renamed over");
            self.deliver(EventKind::Modify(ModifyKind::Name(RenameMode::Both)), &[beside, path]);
        } else {
            std::fs::write(&path, contents).expect("the file is written");
            self.deliver(EventKind::Modify(ModifyKind::Data(DataChange::Content)), &[path]);
        }
        self.disk.0.insert(file, contents.to_vec());
    }

    fn get(&mut self, request: usize, accept: usize, revalidate: bool) -> Result<(), TestCaseError> {
        let (spelling, file) = REQUESTS[request];
        let mut headers = HeaderMap::new();
        if let Some(accept) = ACCEPTS[accept] {
            headers.insert(ACCEPT_ENCODING, HeaderValue::from_static(accept));
        }
        if let (true, Some(etag)) = (revalidate, self.etags.get(&request)) {
            headers.insert(IF_NONE_MATCH, etag.clone());
        }
        let key = file.map(|file| if spelling == LINK { LINK } else { file });
        let was_kept = key.is_some_and(|key| self.cached.cache.contains(key.as_bytes()));
        let was_file_kept = file.is_some_and(|file| self.cached.cache.state.contents.contains_key(&self.path(file)));
        let held = self.budget.held();

        let got = self.answer(&self.cached, spelling, &headers);
        let uncached = self.answer(&self.uncached, spelling, &headers);
        prop_assert_eq!(
            &got,
            &uncached,
            "GET {} {:?}: cached and uncached differ",
            spelling,
            headers
        );

        let on_disk = file.and_then(|file| self.disk.file(file));
        if headers.is_empty() {
            prop_assert_eq!(
                got.as_ref().map(|answer| (answer.status, &answer.body[..])),
                on_disk.map(|bytes| (StatusCode::OK, bytes)),
                "GET {}: not what is on disk",
                spelling
            );
        }
        if let Some(etag) = got.as_ref().and_then(|answer| answer.headers.get(ETAG)) {
            self.etags.insert(request, etag.clone());
        }

        let (Some(key), Some(file)) = (key, file) else {
            return Ok(());
        };
        let kept = self.cached.cache.contains(key.as_bytes());
        let fits = self.disk.cached_len(file, self.copies).is_some_and(|len| {
            let file_cost = if was_file_kept { 0 } else { len };
            held.saturating_add(file_cost).saturating_add(self.alias_cost(key)) <= self.cap
        });
        let want = !self.failed && on_disk.is_some() && (was_kept || fits);
        prop_assert_eq!(
            kept,
            want,
            "GET {}: kept {} with {} of {} held before",
            spelling,
            kept,
            held,
            self.cap
        );
        Ok(())
    }

    fn answer(&self, mount: &StaticMount, spelling: &str, headers: &HeaderMap) -> Option<Answer> {
        let path = format!("/static/{spelling}");
        let response: Response<ResponseBody> =
            self.rt
                .block_on(try_static_file(lookups(&path, headers, std::slice::from_ref(mount))))?;
        let (parts, body) = response.into_parts();
        let body = self
            .rt
            .block_on(body.collect())
            .expect("nothing changes the disk while the model reads an answer")
            .to_bytes();
        Some(Answer {
            status: parts.status,
            headers: parts.headers,
            body,
        })
    }

    /// Returns what the alias of request path `key` costs: the link `l.txt` resolves through the root, so the root is
    /// its one link directory.
    fn alias_cost(&self, key: &str) -> u64 {
        let link_dirs: Box<[PathBuf]> = if key == LINK {
            Box::new([self.root.0.clone()])
        } else {
            Box::default()
        };
        alias_cost(
            key.as_bytes(),
            &RequestPath {
                path: self.path(key),
                link_dirs,
            },
        )
    }

    fn check_budget(&self) -> Result<(), TestCaseError> {
        let state = &self.cached.cache.state;
        let files: u64 = state.contents.iter().map(|entry| entry.value().file.cached_len()).sum();
        let aliases: u64 = state
            .files
            .iter()
            .map(|entry| self.alias_cost(std::str::from_utf8(entry.key()).expect("a model key is text")))
            .sum();
        let entries = files + aliases;
        prop_assert_eq!(self.budget.held(), entries, "the budget holds what the entries hold");
        prop_assert!(
            entries <= self.cap,
            "{} bytes kept past the cap of {}",
            entries,
            self.cap
        );
        Ok(())
    }
}

fn op() -> impl Strategy<Value = Op> {
    let contents = prop::collection::vec(prop::sample::select(&b"ab\n"[..]), 0..=8);
    prop_oneof![
        4 => (prop::sample::select(&FILES[..]), contents.clone(), any::<bool>())
            .prop_map(|(file, contents, by_rename)| Op::Write { file, contents, by_rename }),
        1 => prop::sample::select(&FILES[..]).prop_map(Op::Remove),
        1 => prop::option::of(contents).prop_map(Op::ReplaceDir),
        1 => prop::sample::select(&[Notice::Opened, Notice::Rescan, Notice::NoPaths][..]).prop_map(Op::Notice),
        1 => Just(Op::WatcherFails),
        8 => (0..REQUESTS.len(), 0..ACCEPTS.len(), any::<bool>())
            .prop_map(|(request, accept, revalidate)| Op::Get { request, accept, revalidate }),
    ]
}

/// Leaves room for about one alias: a request path's key and path under the temp directory, and its slot.
const ALIAS_ROOM: u64 = 256;

/// Caps from none at all to one no sequence fills, through ones a few files and their aliases fill.
const CAPS: [u64; 4] = [0, 10 + ALIAS_ROOM, 24 + 2 * ALIAS_ROOM, u64::MAX];

proptest! {
    /// Every request of any sequence of changes is answered as the disk says and as an uncached mount answers it,
    /// and the cache keeps what fits its budget and nothing past it.
    #[test]
    fn a_cached_mount_answers_every_request_as_its_reference_models(
        copies in prop::sample::select(&[Copies::Original, Copies::Precompressed][..]),
        cap in prop::sample::select(&CAPS[..]),
        ops in prop::collection::vec(op(), 0..48),
    ) {
        let mut harness = Harness::new(copies, cap);
        for op in &ops {
            harness.step(op)?;
        }
    }
}
