use super::read::file_name;
use crate::plan::static_plan::{relative_key, RelativeKey, StaticPathError};
use super::*;

/// Covers the watcher's delivery latency with room for a loaded machine.
const WATCH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn body_of(rt: &tokio::runtime::Runtime, resp: Response<Full<Bytes>>) -> Bytes {
    rt.block_on(http_body_util::BodyExt::collect(resp.into_body()))
        .unwrap()
        .to_bytes()
}

fn accepting(accept: Option<&str>) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    if let Some(accept) = accept {
        headers.insert(
            hyper::header::ACCEPT_ENCODING,
            HeaderValue::from_str(accept).unwrap(),
        );
    }
    headers
}

fn mounts_only(mounts: &[StaticMount]) -> crate::plan::route::RouteTable<(), StaticMount> {
    let mut table = crate::plan::route::RouteTable::new();
    for mount in mounts {
        table.add_static_mount(mount.clone());
    }
    table
}

/// Asks `table`'s mounts for `GET path` the way the pipeline does, through `decide` and then
/// `NoRoute::static_lookups`.
fn asks_for<'r, 'q>(
    table: &'r crate::plan::route::RouteTable<(), StaticMount>,
    path: &'q str,
    headers: &'q hyper::HeaderMap,
) -> StaticLookups<'r, 'q, StaticMount> {
    match crate::plan::request::decide(&hyper::Method::GET, path, headers, table) {
        crate::plan::request::RequestDecision::NoRoute(no_route) => no_route.static_lookups(),
        _ => panic!("a table without routes routes nothing"),
    }
}

fn serve_rel(
    rt: &tokio::runtime::Runtime,
    mount: &StaticMount,
    rel: &str,
    accept: Option<&str>,
) -> Result<Option<Response<Full<Bytes>>>, StaticError> {
    serve_asking(rt, mount, rel, &accepting(accept))
}

fn serve_asking(
    rt: &tokio::runtime::Runtime,
    mount: &StaticMount,
    rel: &str,
    headers: &hyper::HeaderMap,
) -> Result<Option<Response<Full<Bytes>>>, StaticError> {
    let path = format!("/static/{rel}");
    let table = mounts_only(std::slice::from_ref(mount));
    let ask = asks_for(&table, &path, headers)
        .next()
        .expect("the path is under the mount");
    let Lookup::File(ask) = ask else {
        panic!("{path:?} is refused: {ask:?}");
    };
    rt.block_on(serve(ask))
}

async fn static_answer(
    path: &str,
    accept: Option<&str>,
    mounts: &[StaticMount],
) -> Option<Response<Full<Bytes>>> {
    let headers = accepting(accept);
    let table = mounts_only(mounts);
    try_static_file(asks_for(&table, path, &headers)).await
}

fn served(
    rt: &tokio::runtime::Runtime,
    mount: &StaticMount,
    rel: &str,
    accept: Option<&str>,
) -> Option<Bytes> {
    serve_rel(rt, mount, rel, accept)
        .unwrap()
        .map(|resp| body_of(rt, resp))
}

/// Polls until `served` returns `want` or [`WATCH_DEADLINE`] passes, and returns the last answer.
fn eventually(mut served: impl FnMut() -> Option<Bytes>, want: Option<&[u8]>) -> Option<Bytes> {
    let deadline = std::time::Instant::now() + WATCH_DEADLINE;
    loop {
        let got = served();
        if got.as_deref() == want || std::time::Instant::now() > deadline {
            return got;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// Waits until the mount's watcher is up, starting it with a miss if no request has yet.
fn watched(mount: &StaticMount) {
    if mount.cache.begin_miss().is_none() {
        while mount.cache.watch() == MountWatch::Started {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

/// Replaces `path` the way a deploy does, by writing a new file beside it and renaming it over.
fn replace(path: &Path, bytes: &[u8]) {
    let tmp = path.with_extension("tmp-replace");
    std::fs::write(&tmp, bytes).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

/// Keeps a client's over-long name from being an unthrottled error line on every request.
#[test]
fn a_name_too_long_for_the_filesystem_is_not_found() {
    let tmp = tempdir();
    for requested in ["a".repeat(300), format!("{}/b", "a".repeat(300)), "a/".repeat(3000)] {
        match read::load(tmp.path(), tmp.path().join(&requested), Copies::Original) {
            Ok(None) => {}
            Ok(Some(_)) => panic!("{} bytes: a file was found", requested.len()),
            Err(error) => panic!("{} bytes: {error}", requested.len()),
        }
    }
}

#[test]
fn an_edited_file_is_served_fresh() {
    let tmp = tempdir();
    let file = tmp.path().join("page.txt");
    std::fs::write(&file, "old").unwrap();
    let rt = runtime();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);

    assert_eq!(served(&rt, &mount, "page.txt", None).unwrap(), "old");
    assert!(mount.cache.contains(b"page.txt"));
    std::fs::write(&file, "newer").unwrap();
    assert_eq!(
        eventually(|| served(&rt, &mount, "page.txt", None), Some(b"newer")).unwrap(),
        "newer"
    );
    std::fs::remove_file(&file).unwrap();
    assert!(eventually(|| served(&rt, &mount, "page.txt", None), None).is_none());
    assert!(!mount.cache.contains(b"page.txt"));
}

#[test]
fn a_file_replaced_by_one_of_the_same_length_is_served_fresh() {
    let tmp = tempdir();
    let file = tmp.path().join("app.js");
    std::fs::write(&file, "aaaa").unwrap();
    let rt = runtime();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);

    assert_eq!(served(&rt, &mount, "app.js", None).unwrap(), "aaaa");
    replace(&file, b"bbbb");
    assert_eq!(
        eventually(|| served(&rt, &mount, "app.js", None), Some(b"bbbb")).unwrap(),
        "bbbb"
    );
}

#[test]
fn a_file_in_a_renamed_directory_is_not_served_from_the_cache() {
    let tmp = tempdir();
    std::fs::create_dir(tmp.path().join("css")).unwrap();
    std::fs::write(tmp.path().join("css/site.css"), "body{}").unwrap();
    let rt = runtime();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);

    assert_eq!(served(&rt, &mount, "css/site.css", None).unwrap(), "body{}");
    std::fs::rename(tmp.path().join("css"), tmp.path().join("old-css")).unwrap();
    assert!(eventually(|| served(&rt, &mount, "css/site.css", None), None).is_none());
}

const ARENA_ACCEPT: &str = "br;q=1, gzip;q=0.8";

/// Builds a precompressed mount where `app.js` has both copies, `font.woff2` has none, and
/// `only-gz.css` has only a `.gz`.
fn precompressed_root() -> (TempDir, StaticMount) {
    let tmp = tempdir();
    let root = tmp.path();
    std::fs::write(root.join("app.js"), "ORIGINAL-JS").unwrap();
    std::fs::write(root.join("app.js.br"), "BR-JS").unwrap();
    std::fs::write(root.join("app.js.gz"), "GZ-JS").unwrap();
    std::fs::write(root.join("font.woff2"), "WOFF2").unwrap();
    std::fs::write(root.join("only-gz.css"), "ORIGINAL-CSS").unwrap();
    std::fs::write(root.join("only-gz.css.gz"), "GZ-CSS").unwrap();
    let mount =
        StaticMount::new("/static", &root.to_string_lossy(), Copies::Precompressed).unwrap();
    watched(&mount);
    (tmp, mount)
}

fn response(
    rt: &tokio::runtime::Runtime,
    mount: &StaticMount,
    rel: &str,
    accept: Option<&str>,
) -> Response<Full<Bytes>> {
    serve_rel(rt, mount, rel, accept).unwrap().unwrap()
}

fn header<'a>(resp: &'a Response<Full<Bytes>>, name: &str) -> Option<&'a str> {
    resp.headers().get(name).map(|v| v.to_str().unwrap())
}

#[test]
fn brotli_is_preferred_when_both_are_accepted() {
    let (_tmp, mount) = precompressed_root();
    let rt = runtime();
    // The miss answers first and the hit second, and both must answer the same way.
    for _ in 0..2 {
        let resp = response(&rt, &mount, "app.js", Some(ARENA_ACCEPT));
        assert_eq!(header(&resp, "content-encoding"), Some("br"));
        assert_eq!(
            header(&resp, "content-type"),
            Some("application/javascript; charset=utf-8")
        );
        assert_eq!(header(&resp, "vary"), Some("Accept-Encoding"));
        assert_eq!(header(&resp, "x-content-type-options"), Some("nosniff"));
        assert_eq!(body_of(&rt, resp), "BR-JS");
    }
}

#[test]
fn gzip_only_client_gets_the_gz_twin() {
    let (_tmp, mount) = precompressed_root();
    let rt = runtime();
    for accept in ["gzip", "gzip, deflate", "br;q=0, gzip", "x-gzip"] {
        let resp = response(&rt, &mount, "app.js", Some(accept));
        assert_eq!(header(&resp, "content-encoding"), Some("gzip"), "{accept}");
        assert_eq!(body_of(&rt, resp), "GZ-JS", "{accept}");
    }
}

#[test]
fn a_client_accepting_neither_gets_the_original() {
    let (_tmp, mount) = precompressed_root();
    let rt = runtime();
    for accept in [
        None,
        Some("identity"),
        Some("deflate"),
        Some("br;q=0, gzip;q=0"),
    ] {
        let resp = response(&rt, &mount, "app.js", accept);
        assert_eq!(header(&resp, "content-encoding"), None, "{accept:?}");
        // The answer still depends on Accept-Encoding for any cache in between.
        assert_eq!(header(&resp, "vary"), Some("Accept-Encoding"), "{accept:?}");
        assert_eq!(body_of(&rt, resp), "ORIGINAL-JS", "{accept:?}");
    }
}

#[test]
fn a_missing_twin_falls_back() {
    let (_tmp, mount) = precompressed_root();
    let rt = runtime();
    let resp = response(&rt, &mount, "font.woff2", Some(ARENA_ACCEPT));
    assert_eq!(header(&resp, "content-encoding"), None);
    assert_eq!(header(&resp, "vary"), None);
    assert_eq!(header(&resp, "content-type"), Some("font/woff2"));
    assert_eq!(body_of(&rt, resp), "WOFF2");
    let resp = response(&rt, &mount, "only-gz.css", Some(ARENA_ACCEPT));
    assert_eq!(header(&resp, "content-encoding"), Some("gzip"));
    assert_eq!(body_of(&rt, resp), "GZ-CSS");
    let resp = response(&rt, &mount, "only-gz.css", Some("br"));
    assert_eq!(header(&resp, "content-encoding"), None);
    assert_eq!(body_of(&rt, resp), "ORIGINAL-CSS");
}

#[test]
fn a_mount_without_precompressed_ignores_the_twins() {
    let (tmp, _) = precompressed_root();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    let rt = runtime();
    let resp = response(&rt, &mount, "app.js", Some(ARENA_ACCEPT));
    assert_eq!(header(&resp, "content-encoding"), None);
    assert_eq!(header(&resp, "vary"), None);
    assert_eq!(body_of(&rt, resp), "ORIGINAL-JS");
}

#[test]
fn a_replaced_twin_is_served_fresh() {
    let (tmp, mount) = precompressed_root();
    let rt = runtime();
    assert_eq!(served(&rt, &mount, "app.js", Some("br")).unwrap(), "BR-JS");
    replace(&tmp.path().join("app.js.br"), b"BR-J2");
    assert_eq!(
        eventually(|| served(&rt, &mount, "app.js", Some("br")), Some(b"BR-J2")).unwrap(),
        "BR-J2"
    );
}

#[test]
fn a_twin_appearing_later_is_served() {
    let (tmp, mount) = precompressed_root();
    let rt = runtime();
    assert_eq!(
        served(&rt, &mount, "font.woff2", Some("br")).unwrap(),
        "WOFF2"
    );
    std::fs::write(tmp.path().join("font.woff2.br"), "BR-FONT").unwrap();
    assert_eq!(
        eventually(
            || served(&rt, &mount, "font.woff2", Some("br")),
            Some(b"BR-FONT")
        )
        .unwrap(),
        "BR-FONT"
    );
}

#[cfg(unix)]
#[test]
fn a_twin_symlinked_out_of_the_root_is_not_served() {
    let (tmp, mount) = precompressed_root();
    let secret = tempdir();
    std::fs::write(secret.path().join("secret"), "SECRET").unwrap();
    std::fs::write(tmp.path().join("page.html"), "PAGE").unwrap();
    std::os::unix::fs::symlink(
        secret.path().join("secret"),
        tmp.path().join("page.html.br"),
    )
    .unwrap();
    let rt = runtime();
    let resp = response(&rt, &mount, "page.html", Some("br"));
    assert_eq!(header(&resp, "content-encoding"), None);
    assert_eq!(body_of(&rt, resp), "PAGE");
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlink_escape_is_refused_on_a_precompressed_mount() {
    let tmp = tempdir();
    let secret = tempdir();
    write_file(&secret.path().join("secret.txt"), b"SHHH").await;
    write_file(&secret.path().join("secret.txt.br"), b"SHHH-BR").await;
    std::os::unix::fs::symlink(
        secret.path().join("secret.txt"),
        tmp.path().join("escape.txt"),
    )
    .unwrap();
    let dirs = vec![StaticMount::new(
        "/static",
        &tmp.path().to_string_lossy(),
        Copies::Precompressed,
    )
    .unwrap()];
    let resp = static_answer("/static/escape.txt", Some("br"), &dirs)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

async fn write_file(p: &std::path::Path, bytes: &[u8]) {
    tokio::fs::write(p, bytes).await.expect("test setup: write");
}

#[tokio::test]
async fn serves_a_file_inside_the_static_root() {
    let tmp = tempdir();
    let root = tmp.path();
    write_file(&root.join("ok.txt"), b"hello").await;

    let dirs =
        vec![StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/ok.txt", None, &dirs).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
}

#[tokio::test]
async fn rejects_symlink_escape() {
    let tmp = tempdir();
    let root = tmp.path();
    let secret_dir = tempdir();
    write_file(&secret_dir.path().join("secret.txt"), b"SHHH").await;

    #[cfg(unix)]
    std::os::unix::fs::symlink(
        secret_dir.path().join("secret.txt"),
        root.join("escape.txt"),
    )
    .unwrap();

    let dirs =
        vec![StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/escape.txt", None, &dirs)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rejects_parent_path_literal() {
    let tmp = tempdir();
    let dirs =
        vec![StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/../etc/passwd", None, &dirs)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn refuses_oversized_files() {
    let tmp = tempdir();
    let big = tmp.path().join("big.bin");
    let big_len = MAX_STATIC_FILE_BYTES + 1;
    {
        let file = std::fs::File::create(&big).unwrap();
        file.set_len(big_len).unwrap();
    }

    let dirs =
        vec![StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/big.bin", None, &dirs).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

/// A file exactly at the limit is served whole.
#[test]
fn a_file_that_grew_past_the_cap_after_its_stat_is_refused_with_413() {
    let path = Path::new("grown.txt");
    let grown = [7u8; 64];
    let refused = super::read::read_contents(path, &grown[..], 4, 16).unwrap_err();
    assert!(
        matches!(
            refused,
            StaticError::GrewPastLimit { stat_len: 4, limit: 16, .. }
        ),
        "{refused:?}"
    );
    assert_eq!(refused.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let whole = super::read::read_contents(path, &grown[..16], 4, 16).unwrap();
    assert_eq!(whole.as_deref(), Some(&grown[..16]));
}

#[test]
fn a_plain_file_carries_nosniff_on_the_miss_and_the_hit() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("upload.txt"), "<html>").unwrap();
    let rt = runtime();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    for _ in 0..2 {
        let resp = response(&rt, &mount, "upload.txt", None);
        assert_eq!(header(&resp, "x-content-type-options"), Some("nosniff"));
        assert_eq!(body_of(&rt, resp), "<html>");
    }
    assert!(mount.cache.contains(b"upload.txt"));
}

#[tokio::test]
async fn non_existent_file_returns_none() {
    let tmp = tempdir();
    let dirs =
        vec![StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap()];
    assert!(static_answer("/static/missing.txt", None, &dirs)
        .await
        .is_none());
}

// A counter rather than a clock gives two calls in quick succession distinct paths.
use std::sync::atomic::{AtomicU64, Ordering};
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tempdir() -> TempDir {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut base = std::env::temp_dir();
    base.push(format!(
        "mipyre-static-fs-test-{}-{}",
        std::process::id(),
        n
    ));
    // A crashed earlier run may have left it behind.
    match std::fs::remove_dir_all(&base) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        result => result.unwrap(),
    }
    std::fs::create_dir_all(&base).unwrap();
    TempDir { path: base }
}
struct TempDir {
    path: std::path::PathBuf,
}
impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}
#[expect(clippy::disallowed_macros, reason = "a cleanup that fails inside a drop is said on stderr, not panicked")]
impl Drop for TempDir {
    fn drop(&mut self) {
        // A panic here could run while a failed test unwinds, so a failed cleanup is only reported.
        if let Err(e) = std::fs::remove_dir_all(&self.path) {
            eprintln!("could not remove {}: {e}", self.path.display());
        }
    }
}

/// Joins the request's key under `root` the way `serve` does.
fn joined(root: &Path, rel: &str) -> Result<PathBuf, StaticPathError> {
    match relative_key(rel) {
        RelativeKey::Named(key) => Ok(root.join(file_name(&key).expect("a UTF-8 key"))),
        RelativeKey::Refused(refusal) => Err(refusal),
    }
}

#[test]
fn candidate_path_decodes_and_refuses_encoded_traversal() {
    let root = Path::new("/srv/public");
    assert_eq!(
        joined(root, "my%20file.txt").unwrap(),
        root.join("my file.txt")
    );
    for rel in [
        "%2e%2e/x",
        "%2E%2E/x",
        "..%2fx",
        "a/%2e%2e%2f%2e%2e/x",
        "a/../x",
    ] {
        assert!(
            matches!(joined(root, rel), Err(StaticPathError::Traversal)),
            "{rel}"
        );
    }
}

#[test]
fn candidate_path_cannot_be_replaced_by_an_absolute_segment() {
    let root = Path::new("/srv/public");
    assert_eq!(
        joined(root, "%2fetc%2fpasswd").unwrap(),
        root.join("etc/passwd")
    );
    assert_eq!(joined(root, "./a//b").unwrap(), root.join("a/b"));
}

#[test]
fn the_content_type_is_the_extensions_in_any_case() {
    assert_eq!(content_type(Path::new("/r/a.css")), "text/css; charset=utf-8");
    assert_eq!(content_type(Path::new("/r/A.CSS")), "text/css; charset=utf-8");
    assert_eq!(content_type(Path::new("/r/a.Woff2")), "font/woff2");
    assert_eq!(content_type(Path::new("/r/a")), "application/octet-stream");
    // Longer than any known extension: unknown, not cut to one.
    assert_eq!(content_type(Path::new("/r/a.htmlhtml")), "application/octet-stream");
}

#[test]
fn a_file_is_served_as_its_resolved_names_type_decided_when_read() {
    let file = StaticFile::new(
        FileContents {
            bytes: Bytes::from_static(b"x"),
            modified: None,
        },
        None,
        PathBuf::from("/r/link"),
        PathBuf::from("/r/a.css"),
    );
    let resp = file.respond(None, &hyper::HeaderMap::new());
    assert_eq!(resp.headers()[CONTENT_TYPE], "text/css; charset=utf-8");
}

#[test]
fn candidate_path_refuses_nul() {
    assert!(matches!(
        joined(Path::new("/srv"), "a%00b"),
        Err(StaticPathError::NulByte)
    ));
}

#[test]
fn mount_rejects_bad_prefix_and_non_directory_root() {
    let tmp = tempdir();
    let file = tmp.path().join("f.txt");
    std::fs::write(&file, b"x").unwrap();
    let root = tmp.path().to_string_lossy().to_string();

    assert!(matches!(
        StaticMount::new("static", &root, Copies::Original),
        Err(StaticMountError::InvalidPrefix(_))
    ));
    assert!(matches!(
        StaticMount::new("/s", &file.to_string_lossy(), Copies::Original),
        Err(StaticMountError::NotADirectory(_))
    ));
    assert!(matches!(
        StaticMount::new(
            "/s",
            &tmp.path().join("missing").to_string_lossy(),
            Copies::Original
        ),
        Err(StaticMountError::Unresolvable { .. })
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn io_errors_are_not_reported_as_missing() {
    let tmp = tempdir();
    let root = tmp.path();
    std::os::unix::fs::symlink("loop", root.join("loop")).unwrap();
    let dirs =
        vec![StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/loop", None, &dirs).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

use proptest::prelude::*;

/// Yields a dot, literal or percent-encoded in either case.
fn dot() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("."), Just("%2e"), Just("%2E")]
}

/// Yields a separator, literal or percent-encoded.
fn slash() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("/"), Just("%2f"), Just("%2F")]
}

/// Yields segments an attacker would try: names, `.`, `..`, backslashes and NUL.
fn segment() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z]{1,6}(\\.txt)?",
        Just("public.txt".to_owned()),
        Just("secret.txt".to_owned()),
        Just("link".to_owned()),
        Just("out".to_owned()),
        Just("%00".to_owned()),
        Just("..\\\\".to_owned()),
        Just("%5c..".to_owned()),
        dot().prop_map(str::to_owned),
        (dot(), dot()).prop_map(|(a, b)| format!("{a}{b}")),
    ]
}

fn rel_path() -> impl Strategy<Value = String> {
    proptest::collection::vec((segment(), slash()), 1..8).prop_map(|parts| {
        parts
            .into_iter()
            .enumerate()
            .map(|(i, (seg, sep))| if i == 0 { seg } else { format!("{sep}{seg}") })
            .collect()
    })
}

proptest! {
    #[test]
    fn a_candidate_path_stays_under_the_root(rel in rel_path()) {
        let root = Path::new("/srv/public");
        if let Ok(path) = joined(root, &rel) {
            let under = path.strip_prefix(root).expect("under the root");
            prop_assert!(
                under.components().all(|c| matches!(c, std::path::Component::Normal(_))),
                "{:?} -> {:?}", rel, path
            );
        }
    }

    #[test]
    fn a_dot_dot_segment_in_any_spelling_is_refused(
        before in proptest::collection::vec("[a-z]{1,4}", 0..3),
        dots in (dot(), dot()),
        sep in slash(),
        after in proptest::collection::vec("[a-z]{1,4}", 0..3),
    ) {
        let parent = format!("{}{}", dots.0, dots.1);
        let rel = before
            .iter()
            .map(String::as_str)
            .chain([parent.as_str()])
            .chain(after.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(sep);
        prop_assert!(matches!(joined(Path::new("/srv"), &rel), Err(StaticPathError::Traversal)), "{:?}", rel);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// It covers `..`, encodings and a symlink planted in the root.
    #[test]
    fn no_request_path_serves_a_file_outside_the_root(rel in rel_path()) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let base = tempdir();
        let root = base.path().join("root");
        let out = base.path().join("out");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(root.join("public.txt"), b"PUBLIC").unwrap();
        std::fs::write(out.join("secret.txt"), b"SECRET").unwrap();
        std::fs::write(base.path().join("secret.txt"), b"SECRET").unwrap();
        // Where no symlink can be planted the property has nothing to try.
        let Some(planted) = crate::io::os::plant_symlink(&out, &root.join("link")) else {
            return Ok(());
        };
        planted.unwrap();
        let mounts = vec![StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap()];

        let response = runtime.block_on(static_answer(&format!("/static/{rel}"), None, &mounts));
        if let Some(response) = response {
            let body = runtime
                .block_on(http_body_util::BodyExt::collect(response.into_body()))
                .unwrap()
                .to_bytes();
            prop_assert_ne!(&body[..], b"SECRET", "{:?}", rel);
        }
    }
}

fn asking(accept: Option<&str>, name: hyper::header::HeaderName, value: &str) -> hyper::HeaderMap {
    let mut headers = accepting(accept);
    headers.insert(name, HeaderValue::from_str(value).unwrap());
    headers
}

fn conditional(
    rt: &tokio::runtime::Runtime,
    mount: &StaticMount,
    rel: &str,
    headers: &hyper::HeaderMap,
) -> Response<Full<Bytes>> {
    serve_asking(rt, mount, rel, headers).unwrap().unwrap()
}

#[test]
fn a_file_answers_its_own_etag_with_304_on_the_miss_and_the_hit() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("a.txt"), "hello").unwrap();
    let rt = runtime();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    let first = response(&rt, &mount, "a.txt", None);
    let etag = header(&first, "etag").unwrap().to_string();
    let modified = header(&first, "last-modified").unwrap().to_string();
    assert!(etag.starts_with('"') && etag.ends_with('"') && etag.len() == 18, "{etag}");
    assert!(httpdate::parse_http_date(&modified).is_ok(), "{modified}");
    assert_eq!(body_of(&rt, first), "hello");
    for _ in 0..2 {
        for revalidate in [
            asking(None, hyper::header::IF_NONE_MATCH, &etag),
            asking(None, hyper::header::IF_NONE_MATCH, &format!("W/{etag}")),
            asking(None, hyper::header::IF_NONE_MATCH, "*"),
            asking(None, hyper::header::IF_MODIFIED_SINCE, &modified),
        ] {
            let resp = conditional(&rt, &mount, "a.txt", &revalidate);
            assert_eq!(resp.status(), StatusCode::NOT_MODIFIED, "{revalidate:?}");
            assert_eq!(header(&resp, "etag"), Some(etag.as_str()));
            assert_eq!(header(&resp, "last-modified"), Some(modified.as_str()));
            assert_eq!(header(&resp, "content-type"), None);
            assert_eq!(body_of(&rt, resp), "");
        }
        let other = asking(None, hyper::header::IF_NONE_MATCH, "\"0000000000000000\"");
        let resp = conditional(&rt, &mount, "a.txt", &other);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_of(&rt, resp), "hello");
    }
    assert!(mount.cache.contains(b"a.txt"));
}

#[test]
fn an_edited_file_has_a_new_etag_and_the_old_one_gets_the_new_bytes() {
    let tmp = tempdir();
    let file = tmp.path().join("a.txt");
    std::fs::write(&file, "one").unwrap();
    let rt = runtime();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    let old = header(&response(&rt, &mount, "a.txt", None), "etag").unwrap().to_string();
    replace(&file, b"two");
    let revalidate = asking(None, hyper::header::IF_NONE_MATCH, &old);
    let fresh = eventually(
        || serve_asking(&rt, &mount, "a.txt", &revalidate).unwrap().map(|r| body_of(&rt, r)),
        Some(b"two"),
    );
    assert_eq!(fresh.as_deref(), Some(&b"two"[..]));
    let new = header(&response(&rt, &mount, "a.txt", None), "etag").unwrap().to_string();
    assert_ne!(old, new);
}

#[test]
fn each_twin_is_revalidated_by_its_own_etag() {
    let (_tmp, mount) = precompressed_root();
    let rt = runtime();
    let br = response(&rt, &mount, "app.js", Some(ARENA_ACCEPT));
    let br_tag = header(&br, "etag").unwrap().to_string();
    let gz_tag = header(&response(&rt, &mount, "app.js", Some("gzip")), "etag").unwrap().to_string();
    let original_tag = header(&response(&rt, &mount, "app.js", None), "etag").unwrap().to_string();
    assert_ne!(br_tag, gz_tag);
    assert_ne!(br_tag, original_tag);
    assert_ne!(gz_tag, original_tag);

    let revalidate = asking(Some(ARENA_ACCEPT), hyper::header::IF_NONE_MATCH, &br_tag);
    let resp = conditional(&rt, &mount, "app.js", &revalidate);
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(header(&resp, "vary"), Some("Accept-Encoding"));
    assert_eq!(header(&resp, "content-encoding"), None);
    // The client's copy is the original; it now accepts brotli: it gets the twin.
    let revalidate = asking(Some(ARENA_ACCEPT), hyper::header::IF_NONE_MATCH, &original_tag);
    let resp = conditional(&rt, &mount, "app.js", &revalidate);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(header(&resp, "content-encoding"), Some("br"));
    assert_eq!(body_of(&rt, resp), "BR-JS");
}

#[test]
fn a_file_changed_after_if_modified_since_is_sent() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("a.txt"), "hello").unwrap();
    let rt = runtime();
    let mount =
        StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    let before = asking(None, hyper::header::IF_MODIFIED_SINCE, "Sun, 06 Nov 1994 08:49:37 GMT");
    let resp = conditional(&rt, &mount, "a.txt", &before);
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_of(&rt, resp), "hello");
}

#[test]
fn a_representation_has_no_last_modified_an_http_date_cannot_say() {
    let before_1970 = std::time::SystemTime::UNIX_EPOCH - std::time::Duration::from_secs(1);
    let after_9999 = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(HTTP_DATE_END);
    let content_type = HeaderValue::from_static("text/plain");
    let at = |modified| {
        let read = FileContents {
            bytes: Bytes::from_static(b"x"),
            modified: Some(modified),
        };
        Representation::new(read, None, &content_type, false)
    };
    for unsayable in [before_1970, after_9999] {
        let sent = at(unsayable);
        assert!(sent.modified.is_none());
        assert!(!sent.sent.contains_key(LAST_MODIFIED));
    }
    let last = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(HTTP_DATE_END - 1);
    assert_eq!(at(last).sent[LAST_MODIFIED], "Fri, 31 Dec 9999 23:59:59 GMT");
}
