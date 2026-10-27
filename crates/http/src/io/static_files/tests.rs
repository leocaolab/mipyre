use super::read::file_name;
use super::*;
use crate::plan::static_plan::{relative_key, RelativeKey, StaticPathError};

/// Covers the watcher's delivery latency with room for a loaded machine.
const WATCH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn body_of(rt: &tokio::runtime::Runtime, resp: Response<ResponseBody>) -> Bytes {
    rt.block_on(http_body_util::BodyExt::collect(resp.into_body()))
        .unwrap()
        .to_bytes()
}

fn accepting(accept: Option<&str>) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    if let Some(accept) = accept {
        headers.insert(hyper::header::ACCEPT_ENCODING, HeaderValue::from_str(accept).unwrap());
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
) -> Result<Option<Response<ResponseBody>>, StaticError> {
    serve_asking(rt, mount, rel, &accepting(accept))
}

fn serve_asking(
    rt: &tokio::runtime::Runtime,
    mount: &StaticMount,
    rel: &str,
    headers: &hyper::HeaderMap,
) -> Result<Option<Response<ResponseBody>>, StaticError> {
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

async fn static_answer(path: &str, accept: Option<&str>, mounts: &[StaticMount]) -> Option<Response<ResponseBody>> {
    let headers = accepting(accept);
    let table = mounts_only(mounts);
    try_static_file(asks_for(&table, path, &headers)).await
}

fn served(rt: &tokio::runtime::Runtime, mount: &StaticMount, rel: &str, accept: Option<&str>) -> Option<Bytes> {
    serve_rel(rt, mount, rel, accept).unwrap().map(|resp| body_of(rt, resp))
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

/// Hands the mount a budget of its own, as a server does when it seals its run, and waits until
/// its watcher is up, starting it with a miss if no request has yet.
fn watched(mount: &StaticMount) {
    mount.cache_within(&Arc::new(CacheBudget::new(DEFAULT_CACHE_BUDGET_BYTES)));
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
        match read::open(tmp.path(), tmp.path().join(&requested), Copies::Original) {
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
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
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
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
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
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);

    assert_eq!(served(&rt, &mount, "css/site.css", None).unwrap(), "body{}");
    std::fs::rename(tmp.path().join("css"), tmp.path().join("old-css")).unwrap();
    assert!(eventually(|| served(&rt, &mount, "css/site.css", None), None).is_none());
}

/// The root renamed away and a new directory renamed into its place, as an atomic deploy does:
/// the new tree is served, and once it is kept an edit in it is followed too.
#[test]
fn an_edit_in_a_swapped_in_root_is_followed() {
    let tmp = tempdir();
    let root = tmp.path().join("public");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("index.html"), b"release 1").unwrap();
    let rt = runtime();
    let mount = StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    assert_eq!(served(&rt, &mount, "index.html", None).unwrap(), "release 1");

    let staged = tmp.path().join("public.new");
    std::fs::create_dir(&staged).unwrap();
    std::fs::write(staged.join("index.html"), b"release 2").unwrap();
    std::fs::rename(&root, tmp.path().join("public.old")).unwrap();
    std::fs::rename(&staged, &root).unwrap();
    let swapped = eventually(|| served(&rt, &mount, "index.html", None), Some(b"release 2"));
    assert_eq!(swapped.as_deref(), Some(&b"release 2"[..]));
    watched(&mount);
    assert_eq!(served(&rt, &mount, "index.html", None).unwrap(), "release 2");
    assert!(
        mount.cache.contains(b"index.html"),
        "the new tree is cached once it is watched"
    );

    replace(&root.join("index.html"), b"release 3");
    let edited = eventually(|| served(&rt, &mount, "index.html", None), Some(b"release 3"));
    assert_eq!(
        edited.as_deref(),
        Some(&b"release 3"[..]),
        "an edit in the new tree is not seen"
    );
}

/// The mount's directory is a symlink, `public -> v1`, switched to `v2` the way a deploy does it
/// (a new link renamed over the old one): the new release is served, and once it is kept an edit
/// in it is followed too.
#[test]
fn a_root_that_is_a_symlink_follows_its_switch() {
    let tmp = tempdir();
    for (dir, body) in [("v1", "one"), ("v2", "two")] {
        std::fs::create_dir(tmp.path().join(dir)).unwrap();
        std::fs::write(tmp.path().join(dir).join("index.html"), body).unwrap();
    }
    if !symlink("v1", &tmp.path().join("public")) {
        return;
    }
    let rt = runtime();
    let mount = StaticMount::new(
        "/static",
        &tmp.path().join("public").to_string_lossy(),
        Copies::Original,
    )
    .unwrap();
    watched(&mount);
    assert_eq!(served(&rt, &mount, "index.html", None).unwrap(), "one");
    assert!(mount.cache.contains(b"index.html"));

    symlink("v2", &tmp.path().join("public.tmp"));
    std::fs::rename(tmp.path().join("public.tmp"), tmp.path().join("public")).unwrap();
    let switched = eventually(|| served(&rt, &mount, "index.html", None), Some(b"two"));
    assert_eq!(
        switched.as_deref(),
        Some(&b"two"[..]),
        "the mount still serves what the link named at start"
    );
    watched(&mount);
    assert_eq!(served(&rt, &mount, "index.html", None).unwrap(), "two");
    assert!(
        mount.cache.contains(b"index.html"),
        "the new release is cached once it is watched"
    );

    replace(&tmp.path().join("v2/index.html"), b"two, edited");
    let edited = eventually(|| served(&rt, &mount, "index.html", None), Some(b"two, edited"));
    assert_eq!(
        edited.as_deref(),
        Some(&b"two, edited"[..]),
        "an edit in the new release is not seen"
    );
}

/// Mounts `root` at `/static`, serving `copies`, caching within `budget`.
fn mount_on(root: &Path, copies: Copies, budget: &Arc<CacheBudget>) -> StaticMount {
    let cache = MountCache::new(root.to_path_buf(), CanonicalDir::resolve(root).unwrap());
    cache.cache_within(budget);
    StaticMount {
        rule: StaticMountRule::new("/static", copies).unwrap(),
        cache: Arc::new(cache),
        held: Arc::new(HeldBytes::new()),
    }
}

fn own_budget(cap: u64) -> Arc<CacheBudget> {
    Arc::new(CacheBudget::new(cap))
}

/// Plants `link -> target` and returns false where making a symlink needs privileges.
fn symlink(target: &str, link: &Path) -> bool {
    let Some(planted) = crate::io::os::plant_symlink(Path::new(target), link) else {
        return false;
    };
    planted.unwrap();
    true
}

/// A mount dropped with files kept, as an app's routes are when the app goes, hands their bytes
/// back to the budget, so a later mount can keep files again.
#[test]
fn a_dropped_mount_hands_its_bytes_back_to_the_budget() {
    let first = tempdir();
    let second = tempdir();
    let alias = |root: &Path, key: &str| {
        let root = std::fs::canonicalize(root).unwrap();
        let requested = RequestPath {
            path: root.join(key),
            link_dirs: Box::default(),
        };
        cache::alias_cost(key.as_bytes(), &requested)
    };
    // One 8-byte file and its alias fit, two do not.
    let budget = own_budget(8 + alias(first.path(), "a.txt").max(alias(second.path(), "b.txt")));
    let rt = runtime();
    std::fs::write(first.path().join("a.txt"), b"12345678").unwrap();
    let mount = mount_on(first.path(), Copies::Original, &budget);
    watched(&mount);
    assert_eq!(served(&rt, &mount, "a.txt", None).unwrap(), "12345678");
    assert!(mount.cache.contains(b"a.txt"));
    drop(mount);
    assert_eq!(budget.held(), 0, "the dropped mount's file and alias are still held");

    std::fs::write(second.path().join("b.txt"), b"87654321").unwrap();
    let mount = mount_on(second.path(), Copies::Original, &budget);
    watched(&mount);
    assert_eq!(served(&rt, &mount, "b.txt", None).unwrap(), "87654321");
    assert!(
        mount.cache.contains(b"b.txt"),
        "a later mount keeps nothing: the dropped mount's bytes are still held"
    );
}

/// Opens and reads the file at `requested` whole, as a miss the cache keeps does.
fn load(root: &Path, requested: PathBuf, copies: Copies) -> Result<Option<(RequestPath, StaticFile)>, StaticError> {
    let Some((name, opened)) = read::open(root, requested, copies)? else {
        return Ok(None);
    };
    Ok(opened.read()?.map(|file| (name, file)))
}

/// `page.txt -> hop.txt -> v1.txt`: retargeting `hop.txt` changes what `page.txt` names, so an
/// event on `hop.txt` makes the cached `page.txt` stale.
#[test]
fn a_file_depends_on_every_symlink_in_its_chain() {
    let tmp = tempdir();
    let root = std::fs::canonicalize(tmp.path()).unwrap();
    std::fs::write(root.join("v1.txt"), b"version 1").unwrap();
    if !symlink("v1.txt", &root.join("hop.txt")) {
        return;
    }
    symlink("hop.txt", &root.join("page.txt"));

    let (name, file) = load(&root, root.join("page.txt"), Copies::Original)
        .unwrap()
        .expect("page.txt resolves inside the root");
    assert_eq!(&file.original.bytes[..], b"version 1");
    let stale = |changed: &Path| name.depends_on(changed) || file.depends_on(changed);
    assert!(stale(&root.join("page.txt")));
    assert!(stale(&root.join("v1.txt")));
    assert!(
        stale(&root.join("hop.txt")),
        "an event on the middle link leaves page.txt cached"
    );
}

/// A directory symlink with an absolute target, `latest -> <root>/releases/v1`, is met on the way
/// to `latest/app.js`, so its directory is a dependency; a file reached through no symlink walks
/// nothing and has none.
#[test]
fn a_directory_symlink_with_an_absolute_target_is_a_dependency() {
    let tmp = tempdir();
    let root = std::fs::canonicalize(tmp.path()).unwrap();
    let release = root.join("releases/v1");
    std::fs::create_dir_all(&release).unwrap();
    std::fs::write(release.join("app.js"), b"v1").unwrap();
    if !symlink(&release.to_string_lossy(), &root.join("latest")) {
        return;
    }

    let (name, file) = load(&root, root.join("latest/app.js"), Copies::Original)
        .unwrap()
        .unwrap();
    assert_eq!(&name.link_dirs[..], std::slice::from_ref(&root));
    assert!(name.depends_on(&root.join("latest")));
    assert!(!name.depends_on(&release.join("other.js")) && !file.depends_on(&release.join("other.js")));

    let (direct, file) = load(&root, release.join("app.js"), Copies::Original).unwrap().unwrap();
    assert!(direct.link_dirs.is_empty());
    assert!(!direct.depends_on(&root.join("latest")) && !file.depends_on(&root.join("latest")));
}

/// The same chain through a directory, `page.txt -> links/hop.txt -> ../v1.txt`, retargeted on
/// disk to `v2.txt`, is followed by the real watcher. The hop sits in a directory of its own, so an
/// event on the root does not drop `page.txt` by accident.
#[test]
fn retargeting_the_middle_of_a_symlink_chain_is_followed() {
    let tmp = tempdir();
    let root = tmp.path();
    let links = root.join("links");
    std::fs::create_dir(&links).unwrap();
    std::fs::write(root.join("v1.txt"), b"version 1").unwrap();
    std::fs::write(root.join("v2.txt"), b"version 2").unwrap();
    if !symlink("../v1.txt", &links.join("hop.txt")) {
        return;
    }
    symlink("links/hop.txt", &root.join("page.txt"));
    std::fs::write(links.join("control.txt"), b"c1").unwrap();
    let rt = runtime();
    let mount = StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    assert_eq!(served(&rt, &mount, "page.txt", None).unwrap(), "version 1");
    assert_eq!(served(&rt, &mount, "links/control.txt", None).unwrap(), "c1");

    symlink("../v2.txt", &links.join("hop.tmp"));
    std::fs::rename(links.join("hop.tmp"), links.join("hop.txt")).unwrap();
    replace(&links.join("control.txt"), b"c2");
    let control = eventually(|| served(&rt, &mount, "links/control.txt", None), Some(b"c2"));
    assert_eq!(
        control.as_deref(),
        Some(&b"c2"[..]),
        "the watcher delivered the events before it"
    );

    assert_eq!(std::fs::read(root.join("page.txt")).unwrap(), b"version 2");
    assert_eq!(
        served(&rt, &mount, "page.txt", None).unwrap(),
        "version 2",
        "the cache still answers what the old chain named"
    );
}

/// On a case-insensitive filesystem (macOS's default APFS) every spelling of `page.txt` opens it.
/// Its bytes are held once, and each spelling a client asks for is an alias that costs its own
/// bytes, so the spellings fill the budget and stop there instead of growing the cache without
/// bound. On a case-sensitive filesystem there are no spellings to ask for and the test has
/// nothing to try.
#[test]
fn the_spellings_of_one_file_are_held_by_the_budget() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("page.txt"), b"1234").unwrap();
    if !tmp.path().join("PAGE.TXT").exists() {
        return;
    }
    let rt = runtime();
    let root = std::fs::canonicalize(tmp.path()).unwrap();
    let alias = |key: &str| {
        let requested = RequestPath {
            path: root.join(key),
            link_dirs: Box::default(),
        };
        cache::alias_cost(key.as_bytes(), &requested)
    };
    let cap = 4 + 2 * alias("page.txt");
    let budget = own_budget(cap);
    let mount = mount_on(tmp.path(), Copies::Original, &budget);
    watched(&mount);

    let spellings = [
        "page.txt", "PAGE.TXT", "Page.txt", "pAge.txt", "paGe.txt", "pagE.txt", "PAGE.txt", "page.TXT",
    ];
    for spelling in spellings {
        assert_eq!(served(&rt, &mount, spelling, None).unwrap(), "1234");
        assert!(budget.held() <= cap, "{} held past the cap of {cap}", budget.held());
    }
    let kept = spellings
        .iter()
        .filter(|spelling| mount.cache.contains(spelling.as_bytes()))
        .count();
    assert_eq!(kept, 2, "the file once and two aliases fill the budget");
    assert_eq!(budget.held(), cap);

    replace(&tmp.path().join("page.txt"), b"abcd");
    for spelling in spellings {
        let fresh = eventually(|| served(&rt, &mount, spelling, None), Some(b"abcd"));
        assert_eq!(fresh.as_deref(), Some(&b"abcd"[..]), "{spelling} answers the old bytes");
    }
}

const ARENA_ACCEPT: &str = "br;q=1, gzip;q=0.8";

/// An answer for a file the budget cannot keep holds one chunk of it at a time, not a copy of the
/// whole file, so N slow clients do not hold N copies of it.
#[test]
fn an_answer_of_an_unkept_file_holds_a_chunk_not_the_file() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("big.bin"), vec![7u8; 1 << 20]).unwrap();
    let rt = runtime();
    let mount = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    watched(&mount);
    let answer = serve_rel(&rt, &mount, "big.bin", None).unwrap().unwrap();
    assert!(!mount.cache.contains(b"big.bin"));
    let mut body = answer.into_body();
    let first = rt
        .block_on(http_body_util::BodyExt::frame(&mut body))
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(
        first.len() <= send::CHUNK,
        "one frame holds {} bytes of a 1 MiB file",
        first.len()
    );
}

/// Returns `len` bytes that differ from chunk to chunk, so a chunk sent twice or out of place shows.
fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at / 7 % 251) as u8).collect()
}

/// Polls `body` for its next frame's data, or the error that ended it.
fn next_frame(rt: &tokio::runtime::Runtime, body: &mut ResponseBody) -> Option<Result<Bytes, FileSendError>> {
    let frame = rt.block_on(http_body_util::BodyExt::frame(body))?;
    Some(frame.map(|frame| frame.into_data().unwrap()))
}

/// A file sent from disk answers as the cached one would: the same bytes, `ETag`, length and
/// headers, and `304` to its own `ETag`.
#[test]
fn a_file_sent_from_disk_answers_as_the_kept_file_does() {
    let tmp = tempdir();
    let bytes = patterned(3 * send::CHUNK + 5);
    std::fs::write(tmp.path().join("big.bin"), &bytes).unwrap();
    let rt = runtime();
    let kept = mount_on(tmp.path(), Copies::Original, &own_budget(u64::MAX));
    watched(&kept);
    let kept_answer = serve_rel(&rt, &kept, "big.bin", None).unwrap().unwrap();
    assert!(kept.cache.contains(b"big.bin"));
    let sent = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let sent_answer = serve_rel(&rt, &sent, "big.bin", None).unwrap().unwrap();
    assert!(matches!(sent_answer.body(), ResponseBody::File(_)));
    assert_eq!(sent_answer.headers(), kept_answer.headers());
    assert_eq!(
        hyper::body::Body::size_hint(sent_answer.body()).exact(),
        Some(bytes.len() as u64),
        "HEAD's Content-Length"
    );
    assert_eq!(&body_of(&rt, sent_answer)[..], &bytes[..]);
    assert_eq!(sent.held.get(), 0, "every chunk was handed back once the body was read");

    let etag = kept_answer.headers()[ETAG].to_str().unwrap().to_owned();
    let revalidated = serve_asking(
        &rt,
        &sent,
        "big.bin",
        &asking(None, hyper::header::IF_NONE_MATCH, &etag),
    )
    .unwrap()
    .unwrap();
    assert_eq!(revalidated.status(), StatusCode::NOT_MODIFIED);
}

/// A server's copy of a mount counts the bytes its answers hold apart from another server's copy
/// of the same mount, while plain clones share the count.
#[test]
fn a_copy_counting_its_own_sends_holds_apart_from_the_other_copies() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("big.bin"), patterned(3 * send::CHUNK)).unwrap();
    let rt = runtime();
    let first = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let shared = first.clone();
    let mut second = first.clone();
    second.count_own_sends();

    let mut answer = serve_rel(&rt, &second, "big.bin", None).unwrap().unwrap().into_body();
    let chunk = next_frame(&rt, &mut answer).unwrap().unwrap();
    assert_eq!(second.bytes_in_flight(), chunk.len() as u64);
    assert_eq!(
        first.bytes_in_flight(),
        0,
        "the other server's copy counts this server's chunk"
    );

    let mut answer = serve_rel(&rt, &first, "big.bin", None).unwrap().unwrap().into_body();
    let other = next_frame(&rt, &mut answer).unwrap().unwrap();
    assert_eq!(
        shared.bytes_in_flight(),
        other.len() as u64,
        "a clone does not see its mount's chunk"
    );
    assert_eq!(second.bytes_in_flight(), chunk.len() as u64);
}

/// The copy a client accepts is sent from disk with its own `Content-Encoding` and `ETag`.
#[test]
fn a_twin_is_sent_from_disk_with_its_coding() {
    let (_tmp, kept) = precompressed_root();
    let rt = runtime();
    let sent = mount_on(&kept.cache.root().0, Copies::Precompressed, &own_budget(0));
    for accept in [Some("br"), Some("gzip"), None] {
        let answer = serve_rel(&rt, &sent, "app.js", accept).unwrap().unwrap();
        assert!(matches!(answer.body(), ResponseBody::File(_)), "{accept:?}");
        let cached = serve_rel(&rt, &kept, "app.js", accept).unwrap().unwrap();
        assert_eq!(answer.headers(), cached.headers(), "{accept:?}");
        assert_eq!(body_of(&rt, answer), body_of(&rt, cached), "{accept:?}");
    }
}

/// An empty file has nothing to stream.
#[test]
fn an_empty_file_sent_from_disk_has_a_whole_empty_body() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("empty.txt"), b"").unwrap();
    let rt = runtime();
    let mount = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let answer = serve_rel(&rt, &mount, "empty.txt", None).unwrap().unwrap();
    assert!(matches!(answer.body(), ResponseBody::Full(_)));
    assert_eq!(body_of(&rt, answer).len(), 0);
}

/// A file rewritten in place while it is sent ends the answer before its last chunk, so the client
/// never gets a body of the announced length that is not the announced file.
#[test]
fn a_file_rewritten_while_sent_is_cut_off_before_its_last_chunk() {
    use std::io::{Seek, SeekFrom, Write};
    let tmp = tempdir();
    let path = tmp.path().join("big.bin");
    std::fs::write(&path, patterned(2 * send::CHUNK)).unwrap();
    let rt = runtime();
    let mount = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let mut body = serve_rel(&rt, &mount, "big.bin", None).unwrap().unwrap().into_body();
    assert_eq!(next_frame(&rt, &mut body).unwrap().unwrap().len(), send::CHUNK);

    let mut writer = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    writer.seek(SeekFrom::Start(send::CHUNK as u64)).unwrap();
    writer.write_all(b"edited").unwrap();
    drop(writer);
    let cut = next_frame(&rt, &mut body)
        .unwrap()
        .expect_err("the last chunk is not the announced one");
    assert!(cut.to_string().contains("changed while it was sent"), "{cut}");
    assert!(next_frame(&rt, &mut body).is_none());
}

/// A file truncated while it is sent ends the answer short.
#[test]
fn a_file_truncated_while_sent_is_cut_off() {
    let tmp = tempdir();
    let path = tmp.path().join("big.bin");
    std::fs::write(&path, patterned(2 * send::CHUNK)).unwrap();
    let rt = runtime();
    let mount = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let mut body = serve_rel(&rt, &mount, "big.bin", None).unwrap().unwrap().into_body();
    assert!(next_frame(&rt, &mut body).unwrap().is_ok());

    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(10)
        .unwrap();
    let cut = next_frame(&rt, &mut body)
        .unwrap()
        .expect_err("the file ended before its length");
    assert!(cut.to_string().contains("ended before"), "{cut}");
}

/// A file replaced by a rename while it is sent, as a deploy replaces it, is sent whole as it was:
/// the open file is the old one.
#[cfg(unix)]
#[test]
fn a_file_replaced_while_sent_is_sent_whole_as_it_was() {
    let tmp = tempdir();
    let path = tmp.path().join("big.bin");
    let before = patterned(2 * send::CHUNK);
    std::fs::write(&path, &before).unwrap();
    let rt = runtime();
    let mount = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let mut body = serve_rel(&rt, &mount, "big.bin", None).unwrap().unwrap().into_body();
    let first = next_frame(&rt, &mut body).unwrap().unwrap();
    replace(&path, b"release 2");
    let last = next_frame(&rt, &mut body).unwrap().unwrap();
    assert_eq!([first, last].concat(), before);
    assert!(next_frame(&rt, &mut body).is_none());
}

/// Lets hyper queue the body's own chunks instead of copying them, as it does over a socket that
/// takes vectored writes, so the count of held bytes sees everything hyper holds of the body.
struct WritevPipe(tokio::io::DuplexStream);

impl tokio::io::AsyncRead for WritevPipe {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for WritevPipe {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// hyper 1's HTTP/1 write buffer (`DEFAULT_MAX_BUFFER_SIZE`): it asks the body for more only
/// while it holds less than this.
const HYPER_WRITE_BUFFER: u64 = 8192 + 4096 * 100;

/// Clients that ask for a large file the cache does not keep and then read nothing make the
/// server hold at most hyper's write buffer and two chunks each (the one taken while the buffer
/// was just under its size, and the one partly written to the pipe), however large the file: the
/// disk is read only as fast as the clients read.
///
/// The clock is paused: it moves only when every task waits for a timer and no read is on the
/// blocking pool, so the long sleep returns once every connection is stuck on its full pipe.
#[test]
fn slow_clients_of_an_unkept_file_hold_a_bounded_number_of_bytes() {
    use tokio::io::AsyncWriteExt;
    const CLIENTS: u64 = 8;
    const PIPE: usize = 64 * 1024;
    const FILE: usize = 4 << 20;
    let tmp = tempdir();
    std::fs::write(tmp.path().join("big.bin"), patterned(FILE)).unwrap();
    let mount = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let held = rt.block_on(async {
        let mut clients = Vec::new();
        for _ in 0..CLIENTS {
            let (mut client, server) = tokio::io::duplex(PIPE);
            tokio::spawn(serve_connection(mount.clone(), server));
            client
                .write_all(b"GET /static/big.bin HTTP/1.1\r\nhost: test\r\n\r\n")
                .await
                .unwrap();
            clients.push(client);
        }
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        let held = mount.held.get();
        drop(clients);
        held
    });
    assert!(held > 0, "the answers streamed nothing");
    let bound = CLIENTS * (HYPER_WRITE_BUFFER + 2 * send::CHUNK as u64);
    assert!(
        held <= bound,
        "{CLIENTS} clients reading nothing hold {held} bytes, past {bound}; the file is {FILE} bytes"
    );
}

/// Serves one HTTP/1 connection over `server` from `mount` alone, and returns how it ended.
async fn serve_connection(mount: StaticMount, server: tokio::io::DuplexStream) -> Result<(), hyper::Error> {
    let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        let mount = mount.clone();
        async move {
            let table = mounts_only(std::slice::from_ref(&mount));
            let answer = try_static_file(asks_for(&table, req.uri().path(), req.headers())).await;
            Ok::<_, std::convert::Infallible>(answer.expect("the file is under the mount"))
        }
    });
    hyper::server::conn::http1::Builder::new()
        .serve_connection(hyper_util::rt::TokioIo::new(WritevPipe(server)), service)
        .await
}

/// A file truncated while it is sent ends the connection: the client gets fewer bytes than the
/// `Content-Length`, and the connection's error carries the cut, for the server to report it.
#[test]
fn a_cut_answer_ends_its_connection_with_the_cut_in_the_error() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const FILE: usize = 16 * send::CHUNK;
    let tmp = tempdir();
    let path = tmp.path().join("big.bin");
    std::fs::write(&path, patterned(FILE)).unwrap();
    let mount = mount_on(tmp.path(), Copies::Original, &own_budget(0));
    let rt = runtime();
    let (got, ended) = rt.block_on(async {
        let (mut client, server) = tokio::io::duplex(1024);
        let connection = tokio::spawn(serve_connection(mount, server));
        client
            .write_all(b"GET /static/big.bin HTTP/1.1\r\nhost: test\r\n\r\n")
            .await
            .unwrap();
        let mut first = [0u8; 1];
        client.read_exact(&mut first).await.unwrap();
        // hyper reads ahead no more than its write buffer, far less than the file.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(10)
            .unwrap();
        let mut rest = Vec::new();
        client.read_to_end(&mut rest).await.unwrap();
        (1 + rest.len(), connection.await.unwrap())
    });
    assert!(
        got < FILE,
        "the client got {got} bytes of head and body, the whole file is {FILE}"
    );
    let ended = ended.expect_err("a cut answer fails its connection");
    let cut = std::iter::successors(Some(&ended as &(dyn std::error::Error + 'static)), |e| e.source())
        .find_map(|e| e.downcast_ref::<FileSendError>());
    assert!(cut.is_some(), "{ended:?}");
}

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
    let mount = StaticMount::new("/static", &root.to_string_lossy(), Copies::Precompressed).unwrap();
    watched(&mount);
    (tmp, mount)
}

fn response(
    rt: &tokio::runtime::Runtime,
    mount: &StaticMount,
    rel: &str,
    accept: Option<&str>,
) -> Response<ResponseBody> {
    serve_rel(rt, mount, rel, accept).unwrap().unwrap()
}

fn header<'a>(resp: &'a Response<ResponseBody>, name: &str) -> Option<&'a str> {
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
    for accept in [None, Some("identity"), Some("deflate"), Some("br;q=0, gzip;q=0")] {
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
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
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
    assert_eq!(served(&rt, &mount, "font.woff2", Some("br")).unwrap(), "WOFF2");
    std::fs::write(tmp.path().join("font.woff2.br"), "BR-FONT").unwrap();
    assert_eq!(
        eventually(|| served(&rt, &mount, "font.woff2", Some("br")), Some(b"BR-FONT")).unwrap(),
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
    std::os::unix::fs::symlink(secret.path().join("secret"), tmp.path().join("page.html.br")).unwrap();
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
    std::os::unix::fs::symlink(secret.path().join("secret.txt"), tmp.path().join("escape.txt")).unwrap();
    let dirs = vec![StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Precompressed).unwrap()];
    let resp = static_answer("/static/escape.txt", Some("br"), &dirs).await.unwrap();
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

    let dirs = vec![StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/ok.txt", None, &dirs).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("x-content-type-options").unwrap(), "nosniff");
}

#[tokio::test]
async fn rejects_symlink_escape() {
    let tmp = tempdir();
    let root = tmp.path();
    let secret_dir = tempdir();
    write_file(&secret_dir.path().join("secret.txt"), b"SHHH").await;

    #[cfg(unix)]
    std::os::unix::fs::symlink(secret_dir.path().join("secret.txt"), root.join("escape.txt")).unwrap();

    let dirs = vec![StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/escape.txt", None, &dirs).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rejects_parent_path_literal() {
    let tmp = tempdir();
    let dirs = vec![StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap()];
    let resp = static_answer("/static/../etc/passwd", None, &dirs).await.unwrap();
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

    let dirs = vec![StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap()];
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
            StaticError::GrewPastLimit {
                stat_len: 4,
                limit: 16,
                ..
            }
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
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
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
    let dirs = vec![StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap()];
    assert!(static_answer("/static/missing.txt", None, &dirs).await.is_none());
}

// A counter rather than a clock gives two calls in quick succession distinct paths.
use std::sync::atomic::{AtomicU64, Ordering};
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tempdir() -> TempDir {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut base = std::env::temp_dir();
    base.push(format!("mipyre-static-fs-test-{}-{}", std::process::id(), n));
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
#[expect(
    clippy::disallowed_macros,
    reason = "a cleanup that fails inside a drop is said on stderr, not panicked"
)]
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
    assert_eq!(joined(root, "my%20file.txt").unwrap(), root.join("my file.txt"));
    for rel in ["%2e%2e/x", "%2E%2E/x", "..%2fx", "a/%2e%2e%2f%2e%2e/x", "a/../x"] {
        assert!(matches!(joined(root, rel), Err(StaticPathError::Traversal)), "{rel}");
    }
}

#[test]
fn candidate_path_cannot_be_replaced_by_an_absolute_segment() {
    let root = Path::new("/srv/public");
    assert_eq!(joined(root, "%2fetc%2fpasswd").unwrap(), root.join("etc/passwd"));
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
        StaticMount::new("/s", &tmp.path().join("missing").to_string_lossy(), Copies::Original),
        Err(StaticMountError::Unresolvable { .. })
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn io_errors_are_not_reported_as_missing() {
    let tmp = tempdir();
    let root = tmp.path();
    std::os::unix::fs::symlink("loop", root.join("loop")).unwrap();
    let dirs = vec![StaticMount::new("/static", &root.to_string_lossy(), Copies::Original).unwrap()];
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
) -> Response<ResponseBody> {
    serve_asking(rt, mount, rel, headers).unwrap().unwrap()
}

#[test]
fn a_file_answers_its_own_etag_with_304_on_the_miss_and_the_hit() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("a.txt"), "hello").unwrap();
    let rt = runtime();
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    let first = response(&rt, &mount, "a.txt", None);
    let etag = header(&first, "etag").unwrap().to_string();
    let modified = header(&first, "last-modified").unwrap().to_string();
    assert!(
        etag.starts_with('"') && etag.ends_with('"') && etag.len() == 18,
        "{etag}"
    );
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
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    let old = header(&response(&rt, &mount, "a.txt", None), "etag")
        .unwrap()
        .to_string();
    replace(&file, b"two");
    let revalidate = asking(None, hyper::header::IF_NONE_MATCH, &old);
    let fresh = eventually(
        || {
            serve_asking(&rt, &mount, "a.txt", &revalidate)
                .unwrap()
                .map(|r| body_of(&rt, r))
        },
        Some(b"two"),
    );
    assert_eq!(fresh.as_deref(), Some(&b"two"[..]));
    let new = header(&response(&rt, &mount, "a.txt", None), "etag")
        .unwrap()
        .to_string();
    assert_ne!(old, new);
}

#[test]
fn each_twin_is_revalidated_by_its_own_etag() {
    let (_tmp, mount) = precompressed_root();
    let rt = runtime();
    let br = response(&rt, &mount, "app.js", Some(ARENA_ACCEPT));
    let br_tag = header(&br, "etag").unwrap().to_string();
    let gz_tag = header(&response(&rt, &mount, "app.js", Some("gzip")), "etag")
        .unwrap()
        .to_string();
    let original_tag = header(&response(&rt, &mount, "app.js", None), "etag")
        .unwrap()
        .to_string();
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
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
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
        assert!(sent.head.modified.is_none());
        assert!(!sent.head.sent.contains_key(LAST_MODIFIED));
    }
    let last = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(HTTP_DATE_END - 1);
    assert_eq!(at(last).head.sent[LAST_MODIFIED], "Fri, 31 Dec 9999 23:59:59 GMT");
}

/// Bounds a request judged for a hang. A miss on a small file takes well under a millisecond and
/// the static path has no clock of its own, so a request unanswered after ten seconds is waiting
/// on something that will not come.
const HANG_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// Asks `mount` for `rel` on a thread of its own and returns `None` if no answer came within
/// [`HANG_DEADLINE`]; a stuck thread and its runtime are left behind, since dropping the runtime
/// would wait for the stuck blocking-pool thread.
fn answered_in_time(mount: &StaticMount, rel: &str, accept: Option<&str>) -> Option<Option<Bytes>> {
    let (tx, rx) = std::sync::mpsc::channel();
    let mount = mount.clone();
    let rel = rel.to_string();
    let accept = accept.map(str::to_string);
    std::thread::spawn(move || {
        let rt = runtime();
        let answer = served(&rt, &mount, &rel, accept.as_deref());
        // The receiver gives up after the deadline; then nobody wants the answer.
        tx.send(answer).unwrap_or(());
        std::mem::forget(rt);
    });
    rx.recv_timeout(HANG_DEADLINE).ok()
}

/// A FIFO is not a regular file, so it is "not there"; asking for it answers at once instead of
/// holding a blocking-pool thread in `open` until a writer comes.
#[test]
fn a_fifo_in_the_root_is_answered_not_waited_on() {
    let tmp = tempdir();
    let Some(planted) = crate::io::os::plant_fifo(&tmp.path().join("pipe.txt")) else {
        return;
    };
    planted.unwrap();
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();

    assert_eq!(
        answered_in_time(&mount, "pipe.txt", None),
        Some(None),
        "None: no answer within {HANG_DEADLINE:?}"
    );
}

/// A regular file whose `.br` copy is a FIFO is answered with the original to a client that
/// accepts `br`.
#[test]
fn a_fifo_twin_does_not_hold_the_original() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("app.js"), b"js").unwrap();
    let Some(planted) = crate::io::os::plant_fifo(&tmp.path().join("app.js.br")) else {
        return;
    };
    planted.unwrap();
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Precompressed).unwrap();

    let answer = answered_in_time(&mount, "app.js", Some("br")).expect("no answer within the hang deadline");
    assert_eq!(answer.as_deref(), Some(&b"js"[..]));
}

/// A FIFO in the root when the watcher starts does not hold the watch from coming up. The
/// watcher's macOS backend opens every file it watches, and a FIFO's blocking open waits for a
/// writer; Linux's watches only directories.
#[test]
fn a_fifo_in_the_root_does_not_hold_the_watcher() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("a.txt"), b"a").unwrap();
    let Some(planted) = crate::io::os::plant_fifo(&tmp.path().join("pipe")) else {
        return;
    };
    planted.unwrap();
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    assert!(mount.cache.begin_miss().is_none(), "the first miss starts the watcher");
    let deadline = std::time::Instant::now() + WATCH_DEADLINE;
    while mount.watch() == MountWatch::Started && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(
        mount.watch(),
        MountWatch::Active,
        "still walking the tree after {WATCH_DEADLINE:?}"
    );
}

/// A FIFO made in the root after the watcher is up does not stop later edits from being seen.
#[test]
fn a_fifo_made_later_does_not_stop_the_events() {
    let tmp = tempdir();
    std::fs::write(tmp.path().join("a.txt"), b"v1").unwrap();
    let rt = runtime();
    let mount = StaticMount::new("/static", &tmp.path().to_string_lossy(), Copies::Original).unwrap();
    watched(&mount);
    assert_eq!(served(&rt, &mount, "a.txt", None).unwrap(), "v1");
    let Some(planted) = crate::io::os::plant_fifo(&tmp.path().join("pipe")) else {
        return;
    };
    planted.unwrap();
    replace(&tmp.path().join("a.txt"), b"v2");
    let got = eventually(|| served(&rt, &mount, "a.txt", None), Some(b"v2"));
    assert_eq!(got.as_deref(), Some(&b"v2"[..]), "the edit after the FIFO is not seen");
}
