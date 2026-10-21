//! Measures answering a request no route matched from the static file cache.
// The package's clippy.toml is plan/'s list; a bench measures the world through io/.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_macros,
    reason = "a bench writes the files it serves and prints"
)]

use std::alloc::System;
use std::future::Future;
use std::hint::black_box;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use hyper::header::{HeaderMap, HeaderValue, ACCEPT_ENCODING, ETAG, IF_NONE_MATCH};
use hyper::Method;
use mipyre_http::io::static_files::{try_static_file, MountWatch, StaticMount};
use mipyre_http::plan::request::{decide, RequestDecision};
use mipyre_http::plan::route::RouteTable;
use mipyre_http::plan::static_plan::Copies;
use mipyre_microbench::{measure, run, AllocationCounter, Case};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// A directory with a 1 KiB `hello.txt`, as `bench_paths`' static paths serve, and its `.br` and
/// `.gz` twins. It is removed when dropped and also by [`stop`], since `process::exit` runs no
/// destructor.
struct StaticDir(std::path::PathBuf);

/// Returns one path per process, so that [`stop`] finds the directory without holding it.
fn static_dir_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("mipyre-bench-static-{}", std::process::id()))
}

/// Exits with status 1 after removing the process's static directory.
#[cold]
fn stop(why: &str) -> ! {
    remove_dir(&static_dir_path());
    eprintln!("{why}");
    std::process::exit(1)
}

fn remove_dir(dir: &std::path::Path) {
    if dir.exists() {
        if let Err(error) = std::fs::remove_dir_all(dir) {
            eprintln!("could not remove {}: {error}", dir.display());
        }
    }
}

impl StaticDir {
    fn new() -> Self {
        let dir = Self(static_dir_path());
        dir.must(std::fs::create_dir_all(&dir.0), "create the static directory");
        for (name, bytes) in [
            ("hello.txt", &[b'x'; 1024][..]),
            ("hello.txt.br", b"br"),
            ("hello.txt.gz", b"gz"),
        ] {
            dir.must(std::fs::write(dir.0.join(name), bytes), "write a static file");
        }
        dir
    }

    /// Returns the value, or stops the bench after removing the directory.
    fn must<T, E: std::fmt::Debug>(&self, result: Result<T, E>, what: &str) -> T {
        match result {
            Ok(value) => value,
            Err(error) => stop(&format!("{what}: {error:?}")),
        }
    }
}

impl Drop for StaticDir {
    fn drop(&mut self) {
        remove_dir(&self.0);
    }
}

/// Answers `GET path` from a table holding one mount at `/static`, as the pipeline answers a
/// request no route matched.
///
/// A miss is cached only once the mount's watch is active, and the first miss starts the watcher
/// on a thread of its own, so the set-up misses once, waits for the watch and misses again. Each
/// measured call is then a cache hit or a refusal, so no runtime turn is counted. An answer ready
/// on its first poll does not prove a hit, because a miss whose read finishes before that poll is
/// ready too. With `revalidate`, the set-up is answered `200` and every measured call sends the
/// `ETag` it was answered with as `If-None-Match`.
fn static_answer(
    name: &str,
    n: u64,
    copies: Copies,
    path: &str,
    (accept, revalidate): (Option<&str>, bool),
    status: u16,
) {
    let dir = StaticDir::new();
    let mount = dir.must(
        StaticMount::new("/static", &dir.0.to_string_lossy(), copies),
        "mount the static directory",
    );
    let watched = mount.clone();
    let mut table: RouteTable<(), StaticMount> = RouteTable::new();
    table.add_static_mount(mount);
    let mut headers = HeaderMap::new();
    if let Some(accept) = accept {
        headers.insert(
            ACCEPT_ENCODING,
            dir.must(HeaderValue::from_str(accept), "an Accept-Encoding value"),
        );
    }
    let rt = dir.must(
        tokio::runtime::Builder::new_current_thread().enable_all().build(),
        "build the runtime",
    );
    let answer = |headers: &HeaderMap, stage: &str, status: u16| match rt.block_on(unrouted(&table, path, headers)) {
        Some(resp) if resp.status().as_u16() == status => resp,
        other => stop(&format!("{name}: the {stage} answered {:?}, not {status}", other.map(|r| r.status()))),
    };
    let set_up_status = if revalidate { 200 } else { status };
    answer(&headers, "first request", set_up_status);
    // Looks up to a thousand times a millisecond apart; a one-file directory's watcher is up sooner.
    let mut watch = watched.watch();
    for _ in 0..1000 {
        if watch != MountWatch::Started {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        watch = watched.watch();
    }
    match watch {
        MountWatch::Active | MountWatch::Unstarted => {}
        MountWatch::Started | MountWatch::Failed => {
            stop(&format!("{name}: the mount's watch is {watch:?}, so nothing would be cached"))
        }
    }
    let cached = answer(&headers, "request after the watch was up", set_up_status);
    if revalidate {
        let etag = dir.must(cached.headers().get(ETAG).ok_or("no ETag"), "a static answer carries its ETag");
        headers.insert(IF_NONE_MATCH, etag.clone());
        answer(&headers, "revalidating request", status);
    }
    let _entered = rt.enter();
    let mut cx = Context::from_waker(Waker::noop());
    measure(name, n, |_| {
        let mut fut = std::pin::pin!(unrouted(black_box(&table), black_box(path), black_box(&headers)));
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(resp) => {
                black_box(resp);
            }
            Poll::Pending => stop(&format!("{name}: a cached answer was not ready on its first poll")),
        }
    });
}

/// Asks the static files for `GET path` as the pipeline's `unrouted` does.
async fn unrouted(
    table: &RouteTable<(), StaticMount>,
    path: &str,
    headers: &HeaderMap,
) -> Option<hyper::Response<http_body_util::Full<Bytes>>> {
    match decide(&Method::GET, path, headers, table) {
        RequestDecision::NoRoute(no_route) => try_static_file(no_route.static_lookups()).await,
        _ => stop("a table without routes routes nothing"),
    }
}

/// Serves the static part of the `static-hit` path.
fn static_hit(n: u64) {
    static_answer("static_hit", n, Copies::Original, "/static/hello.txt", (None, false), 200);
}

fn static_hit_twin(n: u64) {
    static_answer(
        "static_hit_twin",
        n,
        Copies::Precompressed,
        "/static/hello.txt",
        (Some("gzip, br"), false),
        200,
    );
}

/// Answers `304` to a client revalidating the `static_hit` file with its `ETag`.
fn static_not_modified(n: u64) {
    static_answer("static_not_modified", n, Copies::Original, "/static/hello.txt", (None, true), 304);
}

/// Refuses a path that climbs out of the mount before any lookup.
fn static_refused(n: u64) {
    static_answer("static_refused", n, Copies::Original, "/static/%2e%2e/etc/passwd", (None, false), 403);
}

fn main() {
    let cases: [Case; 4] = [
        ("static_hit", static_hit),
        ("static_hit_twin", static_hit_twin),
        ("static_not_modified", static_not_modified),
        ("static_refused", static_refused),
    ];
    run(&cases);
}
