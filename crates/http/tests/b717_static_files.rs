//! b/717: attacks on the static file mount through its public API — the request paths a client
//! sends, and the disk changing under a mount that is serving.
//!
//! A test that waits for the watcher waits for an event it can see (the watch reading `Active`,
//! a control file answering its new bytes), bounded by [`EVENT_DEADLINE`].
// The package's clippy.toml is plan/'s list; this test writes the files it serves and waits on threads.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "an integration test writes the directory a mount serves and bounds its waits"
)]
// `allow-expect-in-tests` does not reach the helpers, which are not `#[test]`s.
#![expect(clippy::unwrap_used, reason = "the test's helpers fail the test by panicking")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use mipyre_http::io::static_files::{try_static_file, MountWatch, StaticMount};
use mipyre_http::plan::request::{decide, RequestDecision};
use mipyre_http::plan::route::RouteTable;
use mipyre_http::plan::static_plan::Copies;

/// Bounds a wait for a watcher event. FSEvents is created with zero latency by notify 8 and inotify
/// delivers in microseconds, so a delivered event shows within milliseconds; five seconds leaves
/// room for a loaded machine, as the crate's own `WATCH_DEADLINE` does.
const EVENT_DEADLINE: Duration = Duration::from_secs(5);

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A directory under the temp dir, canonical, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("mipyre-b717-{name}-{}-{n}", std::process::id()));
        match std::fs::remove_dir_all(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            result => result.unwrap(),
        }
        std::fs::create_dir_all(&path).unwrap();
        Self(std::fs::canonicalize(&path).unwrap())
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // A failed cleanup leaves a temp dir; it is not worth a panic while a failed test unwinds.
        std::fs::remove_dir_all(&self.0).unwrap_or(());
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn table(mount: &StaticMount) -> RouteTable<(), StaticMount> {
    let mut table = RouteTable::new();
    table.add_static_mount(mount.clone());
    table
}

/// What a client gets for `GET path`: `None` when no mount answers and routing would.
#[derive(Debug, PartialEq, Eq)]
struct Answer {
    status: StatusCode,
    body: Bytes,
}

async fn get_async(table: &RouteTable<(), StaticMount>, path: &str, headers: &HeaderMap) -> Option<Answer> {
    let resp = match decide(&Method::GET, path, headers, table) {
        RequestDecision::NoRoute(no_route) => try_static_file(no_route.static_lookups()).await?,
        _ => panic!("a table without routes routes nothing"),
    };
    let status = resp.status();
    let body = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    Some(Answer { status, body })
}

fn get(rt: &tokio::runtime::Runtime, mount: &StaticMount, path: &str) -> Option<Answer> {
    rt.block_on(get_async(&table(mount), path, &HeaderMap::new()))
}

fn body(rt: &tokio::runtime::Runtime, mount: &StaticMount, path: &str) -> Option<Bytes> {
    get(rt, mount, path)
        .filter(|a| a.status == StatusCode::OK)
        .map(|a| a.body)
}

/// Starts the mount's watcher with a miss and waits until it follows the disk.
fn followed(rt: &tokio::runtime::Runtime, mount: &StaticMount, any_path: &str) {
    get(rt, mount, any_path);
    let deadline = Instant::now() + EVENT_DEADLINE;
    while mount.watch() == MountWatch::Started {
        assert!(
            Instant::now() < deadline,
            "the watcher did not come up within {EVENT_DEADLINE:?}"
        );
        std::thread::yield_now();
    }
    assert_eq!(mount.watch(), MountWatch::Active);
}

/// Waits until `path` answers `want`, which proves the watcher delivered the events before it.
fn until_served(rt: &tokio::runtime::Runtime, mount: &StaticMount, path: &str, want: &[u8]) {
    let deadline = Instant::now() + EVENT_DEADLINE;
    loop {
        let got = body(rt, mount, path);
        if got.as_deref() == Some(want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{path} still answers {got:?}, not {want:?}, {EVENT_DEADLINE:?} after the change"
        );
        std::thread::yield_now();
    }
}

/// Writes `bytes` beside `path` and renames it over, as a deploy does.
fn replace(path: &Path, bytes: &[u8]) {
    let tmp = path.with_extension("b717-tmp");
    std::fs::write(&tmp, bytes).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

fn mount(root: &Path, copies: Copies) -> StaticMount {
    StaticMount::new("/static", root.to_str().unwrap(), copies).unwrap()
}

/// A secret beside the root is never answered for any of the spellings a client can send.
#[test]
fn no_spelling_of_a_path_serves_a_file_beside_the_root() {
    let scratch = Scratch::new("traversal");
    let root = scratch.path().join("public");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(scratch.path().join("secret.txt"), b"SECRET").unwrap();
    std::fs::write(root.join("ok.txt"), b"ok").unwrap();
    let rt = runtime();
    let mount = mount(&root, Copies::Precompressed);

    let long = "a/".repeat(40_000);
    let attacks: Vec<String> = [
        "../secret.txt",
        "%2e%2e/secret.txt",
        "%2E%2E%2Fsecret.txt",
        ".%2e/secret.txt",
        "%2e./secret.txt",
        "..%2fsecret.txt",
        "ok.txt/../../secret.txt",
        "ok.txt%2f..%2f..%2fsecret.txt",
        "%252e%252e/secret.txt",
        "..%5csecret.txt",
        "..\\secret.txt",
        "%c0%ae%c0%ae/secret.txt",
        "%ef%bc%8e%ef%bc%8e/secret.txt",
        "%e2%80%a5/secret.txt",
        "secret.txt%00.txt",
        "ok.txt%00",
        "//secret.txt",
        "/../secret.txt",
        &format!("{long}../../secret.txt"),
    ]
    .iter()
    .map(|s| s.to_string())
    .chain([format!("{long}x"), "a".repeat(70_000)])
    .collect();

    let root_parent = scratch.path().display().to_string();
    for rel in &attacks {
        let answer = get(&rt, &mount, &format!("/static/{rel}"));
        if let Some(answer) = &answer {
            assert_ne!(&answer.body[..], b"SECRET", "{rel:.80} served the secret");
            assert!(
                !String::from_utf8_lossy(&answer.body).contains(&root_parent),
                "{rel:.80}: the answer names a server path: {answer:?}"
            );
            assert_ne!(answer.status, StatusCode::OK, "{rel:.80} answered 200: {answer:?}");
        }
    }
    assert_eq!(body(&rt, &mount, "/static/ok.txt").as_deref(), Some(&b"ok"[..]));
}

/// A planted directory symlink out of the root is refused, whether its file is asked plainly or
/// through a `.` and `//` spelling.
#[cfg(unix)]
#[test]
fn a_directory_symlink_out_of_the_root_is_refused() {
    let scratch = Scratch::new("dirlink");
    let root = scratch.path().join("public");
    let outside = scratch.path().join("outside");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("secret.txt"), b"SECRET").unwrap();
    std::os::unix::fs::symlink(&outside, root.join("sub")).unwrap();
    let rt = runtime();
    let mount = mount(&root, Copies::Original);

    for path in [
        "/static/sub/secret.txt",
        "/static/./sub//secret.txt",
        "/static/sub%2fsecret.txt",
    ] {
        let answer = get(&rt, &mount, path).expect("a refusal answers the request");
        assert_eq!(answer.status, StatusCode::FORBIDDEN, "{path}: {answer:?}");
        assert_eq!(&answer.body[..], b"forbidden");
    }
}

/// Every answer to a burst of concurrent misses on one file is the whole file, and later hits
/// agree with them.
#[test]
fn concurrent_misses_on_one_file_all_get_the_whole_file() {
    let scratch = Scratch::new("concurrent");
    let content: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(scratch.path().join("big.bin"), &content).unwrap();
    let rt = runtime();
    let mount = mount(scratch.path(), Copies::Original);
    followed(&rt, &mount, "/static/none");

    let table = Arc::new(table(&mount));
    let answers = rt.block_on(async {
        let tasks: Vec<_> = (0..128)
            .map(|_| {
                let table = Arc::clone(&table);
                tokio::spawn(async move { get_async(&table, "/static/big.bin", &HeaderMap::new()).await })
            })
            .collect();
        let mut answers = Vec::new();
        for task in tasks {
            answers.push(task.await.unwrap());
        }
        answers
    });
    for answer in answers {
        let answer = answer.expect("the file is there");
        assert_eq!(answer.status, StatusCode::OK);
        assert!(
            answer.body[..] == content[..],
            "a burst miss got {} bytes",
            answer.body.len()
        );
    }
    assert!(body(&rt, &mount, "/static/big.bin").unwrap()[..] == content[..]);
}

/// A directory replaced by a file of the same name is followed: the old file under it is gone
/// and the new file answers.
#[test]
fn a_directory_replaced_by_a_file_is_followed() {
    let scratch = Scratch::new("dir-to-file");
    let dir = scratch.path().join("css");
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("site.css"), b"body{}").unwrap();
    std::fs::write(scratch.path().join("control.txt"), b"v1").unwrap();
    let rt = runtime();
    let mount = mount(scratch.path(), Copies::Original);
    followed(&rt, &mount, "/static/none");
    assert_eq!(
        body(&rt, &mount, "/static/css/site.css").as_deref(),
        Some(&b"body{}"[..])
    );
    assert_eq!(body(&rt, &mount, "/static/control.txt").as_deref(), Some(&b"v1"[..]));

    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::write(&dir, b"now a file").unwrap();
    replace(&scratch.path().join("control.txt"), b"v2");
    until_served(&rt, &mount, "/static/control.txt", b"v2");

    assert_eq!(get(&rt, &mount, "/static/css/site.css"), None);
    assert_eq!(body(&rt, &mount, "/static/css").as_deref(), Some(&b"now a file"[..]));
}

/// The root swapped for a new directory of the same name, as an atomic deploy does, serves the
/// new tree; nothing cached from the old one is answered.
#[test]
fn a_root_swapped_for_a_new_directory_serves_the_new_tree() {
    let scratch = Scratch::new("root-swap");
    let root = scratch.path().join("public");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("index.html"), b"release 1").unwrap();
    let rt = runtime();
    let mount = mount(&root, Copies::Original);
    followed(&rt, &mount, "/static/none");
    assert_eq!(
        body(&rt, &mount, "/static/index.html").as_deref(),
        Some(&b"release 1"[..])
    );

    let staged = scratch.path().join("public.new");
    std::fs::create_dir(&staged).unwrap();
    std::fs::write(staged.join("index.html"), b"release 2").unwrap();
    std::fs::rename(&root, scratch.path().join("public.old")).unwrap();
    std::fs::rename(&staged, &root).unwrap();

    until_served(&rt, &mount, "/static/index.html", b"release 2");
}

/// On a case-insensitive filesystem (macOS's default APFS) `PAGE.TXT` opens `page.txt`; an edit
/// of `page.txt` must not leave `PAGE.TXT` answering the old bytes.
#[test]
fn a_case_variant_of_an_edited_file_is_served_fresh() {
    let scratch = Scratch::new("case");
    let root = scratch.path();
    std::fs::write(root.join("page.txt"), b"old").unwrap();
    if !root.join("PAGE.TXT").exists() {
        return; // The temp dir's filesystem is case-sensitive; there is no variant to ask for.
    }
    std::fs::write(root.join("control.txt"), b"c1").unwrap();
    let rt = runtime();
    let mount = mount(root, Copies::Original);
    followed(&rt, &mount, "/static/none");
    assert_eq!(body(&rt, &mount, "/static/PAGE.TXT").as_deref(), Some(&b"old"[..]));
    assert_eq!(body(&rt, &mount, "/static/control.txt").as_deref(), Some(&b"c1"[..]));

    replace(&root.join("page.txt"), b"new");
    replace(&root.join("control.txt"), b"c2");
    until_served(&rt, &mount, "/static/control.txt", b"c2");

    assert_eq!(body(&rt, &mount, "/static/page.txt").as_deref(), Some(&b"new"[..]));
    assert_eq!(
        body(&rt, &mount, "/static/PAGE.TXT").as_deref(),
        Some(&b"new"[..]),
        "the case variant still answers the bytes from before the edit"
    );
}
