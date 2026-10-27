//! Serves static files from a mount's directory, cached in memory and kept in step with the disk.
//!
//! A hit makes no filesystem call; a miss opens the file and its pre-compressed copies in one
//! trip to the blocking pool, and reads them whole only when the cache keeps them. A file the
//! cache does not keep is sent from disk a chunk at a time (`send`), so an answer's memory does
//! not grow with the file.

mod cache;
pub mod degraded;
pub mod keyed_log_throttle;
mod read;
mod send;

use bytes::Bytes;
use http_body_util::Full;
use httpdate::HttpDate;
use hyper::header::{
    HeaderMap, HeaderValue, CONTENT_ENCODING, CONTENT_TYPE, ETAG, LAST_MODIFIED, SERVER, VARY, X_CONTENT_TYPE_OPTIONS,
};
use hyper::{Response, StatusCode};
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};

use crate::io::body::ResponseBody;
use crate::plan::compression::{Coding, CodingSet};
use crate::plan::conditional::{not_modified, EntityTag};
use crate::plan::static_plan::{
    choose_precompressed, mime_from_ext, Copies, Lookup, StaticLookup, StaticLookups, StaticMountRule,
    StaticPrefixError,
};
pub use cache::{CacheBudget, MountWatch, DEFAULT_CACHE_BUDGET_BYTES};
use cache::{MissGeneration, MountCache};
use keyed_log_throttle::{HeldLines, KeyedLogThrottle};
use mipyre_util::clock;
use read::{file_name, FileSet};
use send::{Digest, Fingerprint, HeldBytes};
pub use send::{FileBody, FileSendError};

/// Larger files are refused with 413, because a file the cache keeps is read whole into memory,
/// and a file sent from disk is read once for its `ETag` before it is sent.
const MAX_STATIC_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Holds an absolute, symlink-resolved path that was a directory when it was resolved.
#[derive(Debug, Clone)]
struct CanonicalDir(PathBuf);

impl CanonicalDir {
    fn resolve(dir: &Path) -> Result<Self, StaticMountError> {
        let path = std::fs::canonicalize(dir).map_err(|source| StaticMountError::Unresolvable {
            dir: dir.display().to_string(),
            source,
        })?;
        if !path.is_dir() {
            return Err(StaticMountError::NotADirectory(path));
        }
        Ok(Self(path))
    }
}

/// Clones share one cache, so every run's route table serves from it, and one count of the bytes
/// their answers send from disk until [`StaticMount::count_own_sends`] gives a clone its own.
#[derive(Debug, Clone)]
pub struct StaticMount {
    rule: StaticMountRule,
    cache: Arc<MountCache>,
    held: Arc<HeldBytes>,
}

impl AsRef<StaticMountRule> for StaticMount {
    fn as_ref(&self) -> &StaticMountRule {
        &self.rule
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StaticMountError {
    #[error(transparent)]
    InvalidPrefix(#[from] StaticPrefixError),
    #[error("static directory {dir:?} cannot be resolved: {source}")]
    Unresolvable { dir: String, source: io::Error },
    #[error("static directory {0:?} is not a directory")]
    NotADirectory(PathBuf),
}

impl StaticMount {
    pub fn new(prefix: &str, dir: &str, copies: Copies) -> Result<Self, StaticMountError> {
        let rule = StaticMountRule::new(prefix, copies)?;
        let configured = std::path::absolute(dir).map_err(|source| StaticMountError::Unresolvable {
            dir: dir.to_string(),
            source,
        })?;
        let root = CanonicalDir::resolve(&configured)?;
        Ok(Self {
            rule,
            cache: Arc::new(MountCache::new(configured, root)),
            held: Arc::new(HeldBytes::new()),
        })
    }

    /// Caches the mount's files within `budget` from now on; a server hands its budget to each
    /// mount as its run is sealed. Clones share one cache, so a later run of the same app keeps
    /// the budget the first one handed.
    pub fn cache_within(&self, budget: &Arc<CacheBudget>) {
        self.cache.cache_within(budget);
    }

    /// Reports whether a miss is kept now or every request reads the file from disk.
    pub fn watch(&self) -> MountWatch {
        self.cache.watch()
    }

    /// Returns the mount's root and why it is served uncached, or `None` while its file watch
    /// has not failed.
    pub fn uncached_because(&self) -> Option<(PathBuf, String)> {
        self.cache
            .uncached_because()
            .map(|why| (self.cache.root().0.clone(), why))
    }

    /// Starts watching the mount's files in the background, unless that started already; a
    /// server calls it as it starts and does not wait for it.
    pub fn start_watching(&self) {
        self.cache.start_watch();
    }

    /// Counts the bytes this clone's answers send from disk apart from every other clone's; a
    /// server calls it on its copy of each mount, so what one server holds is its own.
    pub fn count_own_sends(&mut self) {
        self.held = Arc::new(HeldBytes::new());
    }

    /// Returns how many bytes this mount's answers sent from disk hold, read and not yet written
    /// out.
    pub fn bytes_in_flight(&self) -> u64 {
        self.held.get()
    }
}

/// Lets a bench count what a miss of a file the cache does not keep runs on the blocking pool,
/// apart from the hand-off, whose wake-ups follow timing.
impl StaticMount {
    /// Opens `key` under the mount's root, picks the representation `accept_encoding` asks for
    /// and fingerprints it on the calling thread, without the cache, and answers `request`.
    #[doc(hidden)]
    pub fn answer_from_disk_here(
        &self,
        key: &[u8],
        accept_encoding: Option<&str>,
        request: &HeaderMap,
    ) -> Option<Response<ResponseBody>> {
        let opened = || {
            let root = self.cache.root();
            let requested = root.0.join(file_name(key)?);
            let Some((_, opened)) = read::open(&root.0, requested, self.rule.copies())? else {
                return Ok(None);
            };
            opened.into_disk(accept_encoding)
        };
        match opened() {
            Ok(file) => file.map(|file| file.respond(request, &self.held)),
            Err(error) => Some(error_response(error.status())),
        }
    }
}

/// A missing file is not an error, so that the next mount and then routing can answer.
#[derive(Debug, thiserror::Error)]
enum StaticError {
    /// It happens only on platforms whose file names must be UTF-8.
    #[error("request path is not valid UTF-8 after percent-decoding")]
    NotUtf8,
    #[error("{path:?} resolves outside the static root {root:?}")]
    Escape { path: PathBuf, root: PathBuf },
    #[error("{path:?} is {len} bytes, over the {MAX_STATIC_FILE_BYTES}-byte static file limit")]
    TooLarge { path: PathBuf, len: u64 },
    /// The file was within the limit when stat'd and grew past it while read; it is refused, not served cut short.
    #[error("{path:?} grew past the {limit}-byte static file limit while it was read ({stat_len} bytes when stat'd)")]
    GrewPastLimit { path: PathBuf, stat_len: u64, limit: u64 },
    #[error("{path:?}: {source}")]
    PermissionDenied { path: PathBuf, source: io::Error },
    #[error("{path:?} became a symlink after the containment check: {source}")]
    SymlinkSwapped { path: PathBuf, source: io::Error },
    #[error("{path:?}: {source}")]
    Unreadable { path: PathBuf, source: io::Error },
    #[error("reading {path:?} did not finish: {source}")]
    Unfinished {
        path: PathBuf,
        source: tokio::task::JoinError,
    },
}

/// Lets one warning a minute through per file whose answer was cut off, since every client of a
/// file being rewritten meets it.
static CUT_THROTTLE: KeyedLogThrottle<PathBuf> = KeyedLogThrottle::new(
    Duration::from_secs(60),
    std::num::NonZeroUsize::new(256).expect("256 is not zero"),
);

/// Lets one permission warning a minute through per path, since a scanner asks for the same path over and over.
static PERMISSION_THROTTLE: KeyedLogThrottle<PathBuf> = KeyedLogThrottle::new(
    Duration::from_secs(60),
    std::num::NonZeroUsize::new(256).expect("256 is not zero"),
);

impl StaticError {
    /// Answers EACCES with 403, as nginx does, because it is an access decision about the file
    /// rather than a fault in serving it.
    fn status(&self) -> StatusCode {
        match self {
            Self::NotUtf8 => StatusCode::BAD_REQUEST,
            Self::Escape { .. } | Self::PermissionDenied { .. } | Self::SymlinkSwapped { .. } => StatusCode::FORBIDDEN,
            Self::TooLarge { .. } | Self::GrewPastLimit { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Unreadable { .. } | Self::Unfinished { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Logs refusals a client can trigger at debug, so a scanner cannot flood the log.
    fn log(&self) {
        match self {
            Self::Unreadable { .. } | Self::Unfinished { .. } => {
                tracing::error!(error = %self, "static file error")
            }
            // EACCES on a contained file means the deployment is wrong, so it stays a warning.
            Self::PermissionDenied { path, .. } => match PERMISSION_THROTTLE.admit(path.as_path(), clock::now()) {
                Some(HeldLines::OfKey(held)) => tracing::warn!(error = %self, held, "static file refused"),
                Some(HeldLines::OfUnrememberedKeys(held)) => {
                    tracing::warn!(error = %self, held_among_other_paths = held, "static file refused")
                }
                None => tracing::debug!(error = %self, "static file refused"),
            },
            Self::SymlinkSwapped { .. } => {
                tracing::warn!(error = %self, "static file refused")
            }
            _ => {
                tracing::debug!(error = %self, "static request refused")
            }
        }
    }
}

#[derive(Debug)]
struct FileContents {
    bytes: Bytes,
    /// It is `None` when the platform or filesystem keeps no modification time.
    modified: Option<SystemTime>,
}

/// The validators a client revalidates one representation with, and the headers of both
/// answers.
///
/// The validators are the one source: `sent` and `not_modified` are their wire form, built once
/// in [`Head::new`] and never changed, so a hit clones a header map instead of inserting headers.
#[derive(Clone, Debug)]
struct Head {
    /// It is a hash of the bytes alone, so the same file gets the same tag in every process and
    /// after a restart, kept or sent from disk.
    etag: EntityTag,
    /// It is `None` when there is no modification time or no HTTP-date can say it (before 1970,
    /// from the year 10000 on).
    modified: Option<HttpDate>,
    /// It holds no `Content-Length`, because hyper writes that from the body's length.
    sent: HeaderMap,
    /// It holds the validators and `Vary`, but nothing describing a body it does not send.
    not_modified: HeaderMap,
}

/// Seconds since 1970 of 10000-01-01, the first instant an HTTP-date cannot say.
const HTTP_DATE_END: u64 = 253_402_300_800;

impl Head {
    /// Builds the headers of bytes with `digest` sent in `coding`, or as the original when
    /// `coding` is `None`; `vary` says the file has copies, so every answer depends on
    /// `Accept-Encoding`.
    #[expect(
        clippy::expect_used,
        reason = "an HTTP-date is visible ASCII: the header-value parse cannot fail"
    )]
    fn new(
        digest: u64,
        modified: Option<SystemTime>,
        coding: Option<Coding>,
        content_type: &HeaderValue,
        vary: bool,
    ) -> Self {
        let etag = EntityTag::of_digest(digest);
        let modified = modified
            .and_then(|at| at.duration_since(SystemTime::UNIX_EPOCH).ok())
            .filter(|since| since.as_secs() < HTTP_DATE_END)
            .map(|since| HttpDate::from(SystemTime::UNIX_EPOCH + since));
        let mut not_modified = HeaderMap::with_capacity(5);
        not_modified.insert(SERVER, SERVER_VALUE);
        // A user-uploaded file in the static directory must not be sniffed as HTML.
        not_modified.insert(X_CONTENT_TYPE_OPTIONS, NOSNIFF);
        not_modified.insert(ETAG, etag.header_value());
        if let Some(date) = modified {
            let value = HeaderValue::try_from(date.to_string()).expect("an HTTP-date is visible ASCII");
            not_modified.insert(LAST_MODIFIED, value);
        }
        if vary {
            not_modified.insert(VARY, HeaderValue::from_static("Accept-Encoding"));
        }
        let mut sent = HeaderMap::with_capacity(not_modified.len() + 2);
        sent.insert(CONTENT_TYPE, content_type.clone());
        if let Some(coding) = coding {
            sent.insert(CONTENT_ENCODING, coding.content_encoding());
        }
        sent.extend(not_modified.iter().map(|(name, value)| (name.clone(), value.clone())));
        Self {
            etag,
            modified,
            sent,
            not_modified,
        }
    }

    /// Answers `304` with no body when the `request`'s preconditions say the client has this,
    /// else `200` with the body `body` makes.
    #[inline]
    fn respond(&self, request: &HeaderMap, body: impl FnOnce() -> ResponseBody) -> Response<ResponseBody> {
        if not_modified(request, &self.etag, self.modified) {
            let mut resp = Response::new(ResponseBody::empty());
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
            *resp.headers_mut() = self.not_modified.clone();
            return resp;
        }
        let mut resp = Response::new(body());
        *resp.headers_mut() = self.sent.clone();
        resp
    }
}

/// One file as the cache keeps it, the original or a copy.
#[derive(Clone, Debug)]
struct Representation {
    bytes: Bytes,
    head: Head,
}

impl Representation {
    fn new(read: FileContents, coding: Option<Coding>, content_type: &HeaderValue, vary: bool) -> Self {
        Self {
            head: Head::new(Digest::of(&read.bytes), read.modified, coding, content_type, vary),
            bytes: read.bytes,
        }
    }

    fn respond(&self, request: &HeaderMap) -> Response<ResponseBody> {
        self.head
            .respond(request, || ResponseBody::Full(Full::new(self.bytes.clone())))
    }
}

/// One file the cache does not keep, the original or the copy the client is answered with, open
/// and read once for its `ETag`; its bytes are read again as they are sent.
struct DiskRepresentation {
    head: Head,
    /// It stands at the file's start.
    file: File,
    path: std::path::PathBuf,
    fingerprint: Fingerprint,
}

impl DiskRepresentation {
    fn respond(self, request: &HeaderMap, held: &Arc<HeldBytes>) -> Response<ResponseBody> {
        let Self {
            head,
            file,
            path,
            fingerprint,
        } = self;
        head.respond(request, || match fingerprint.len {
            0 => ResponseBody::empty(),
            len => ResponseBody::File(FileBody::new(file, path, len, fingerprint.digest, Arc::clone(held))),
        })
    }
}

/// One file on disk as it is answered, shared by every request path that resolves to it.
struct StaticFile {
    original: Representation,
    /// It is `None` on a mount that serves no copies, because none were looked for; when it is
    /// `Some`, a copy appearing, changing or going makes the file stale.
    precompressed: Option<PrecompressedCopies<Representation>>,
    copies: CodingSet,
    resolved: PathBuf,
}

/// A request's path under the root, before symlinks resolve, and what resolving it went through.
struct RequestPath {
    path: PathBuf,
    /// It holds the resolved directory of every symlink met on the way from `path` to the file,
    /// since retargeting any of them changes what `path` names.
    link_dirs: Box<[PathBuf]>,
}

impl RequestPath {
    /// Tells whether a change at `changed` can make `path` name another file.
    ///
    /// A change of any entry in a directory holding one of the symlinks counts, because the event
    /// names the link as the disk spells it, which on a case-insensitive filesystem need not be
    /// the request's spelling; the directory is resolved, so it is spelled as the disk does.
    fn depends_on(&self, changed: &Path) -> bool {
        self.path.starts_with(changed)
            || self
                .link_dirs
                .iter()
                .any(|dir| dir.starts_with(changed) || changed.parent() == Some(dir.as_path()))
    }

    /// Returns the bytes the path holds on the heap: its own and its link directories'.
    fn heap_len(&self) -> usize {
        let dirs: usize = self.link_dirs.iter().map(|dir| dir.as_os_str().len()).sum();
        self.path.as_os_str().len() + dirs + std::mem::size_of_val(&*self.link_dirs)
    }
}

#[derive(Clone)]
struct PrecompressedCopies<T> {
    br: Option<T>,
    gz: Option<T>,
}

impl<T> Default for PrecompressedCopies<T> {
    fn default() -> Self {
        Self { br: None, gz: None }
    }
}

impl<T> PrecompressedCopies<T> {
    fn get(&self, coding: Coding) -> Option<&T> {
        match coding {
            Coding::Brotli => self.br.as_ref(),
            Coding::Gzip => self.gz.as_ref(),
        }
    }

    fn any(&self) -> bool {
        self.br.is_some() || self.gz.is_some()
    }
}

impl PrecompressedCopies<Representation> {
    fn len(&self) -> usize {
        let len = |copy: &Option<Representation>| copy.as_ref().map_or(0, |copy| copy.bytes.len());
        len(&self.br) + len(&self.gz)
    }
}

fn copy_path(coding: Coding, path: &Path) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(coding.file_suffix());
    PathBuf::from(name)
}

impl StaticFile {
    fn copy(&self, coding: Coding) -> Option<&Representation> {
        self.precompressed.as_ref()?.get(coding)
    }

    /// Labels the original and its copies with the content type of the resolved file's
    /// extension and builds every answer's headers.
    fn new(
        original: FileContents,
        precompressed: Option<PrecompressedCopies<FileContents>>,
        resolved: PathBuf,
    ) -> Self {
        let copies = CodingSet::of(|coding| precompressed.as_ref().is_some_and(|found| found.get(coding).is_some()));
        let content_type = HeaderValue::from_static(content_type(&resolved));
        let vary = precompressed.as_ref().is_some_and(PrecompressedCopies::any);
        let precompressed = precompressed.map(|found| PrecompressedCopies {
            br: found
                .br
                .map(|read| Representation::new(read, Some(Coding::Brotli), &content_type, vary)),
            gz: found
                .gz
                .map(|read| Representation::new(read, Some(Coding::Gzip), &content_type, vary)),
        });
        Self {
            original: Representation::new(original, None, &content_type, vary),
            precompressed,
            copies,
            resolved,
        }
    }

    fn cached_len(&self) -> u64 {
        let copies = self.precompressed.as_ref().map_or(0, PrecompressedCopies::len);
        (self.original.bytes.len() + copies) as u64
    }

    /// Tells whether a change at `changed`, which may be a directory above the file, can make this stale.
    fn depends_on(&self, changed: &Path) -> bool {
        self.resolved.starts_with(changed)
            || (self.precompressed.is_some()
                && Coding::PREFERENCE
                    .iter()
                    .any(|&coding| copy_path(coding, &self.resolved).as_path() == changed))
    }

    /// Answers with the first copy, in server preference, that the client accepts, else the
    /// original.
    fn respond(&self, accept_encoding: Option<&str>, request: &HeaderMap) -> Response<ResponseBody> {
        choose_precompressed(accept_encoding, self.copies)
            .and_then(|coding| self.copy(coding))
            .unwrap_or(&self.original)
            .respond(request)
    }
}

/// Lowercases the extension on the stack, since every known extension fits in `EXT_BUF` bytes
/// and a longer one is unknown anyway.
fn content_type(path: &Path) -> &'static str {
    const EXT_BUF: usize = 8;
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let mut buf = [0u8; EXT_BUF];
    let Some(lower) = buf.get_mut(..ext.len()) else {
        return mime_from_ext("");
    };
    lower.copy_from_slice(ext.as_bytes());
    lower.make_ascii_lowercase();
    // ASCII lowering keeps UTF-8 valid.
    mime_from_ext(std::str::from_utf8(lower).unwrap_or(""))
}

/// Serves the file from the first mount that has it, or returns `None` so that routing answers.
pub async fn try_static_file(lookups: StaticLookups<'_, '_, StaticMount>) -> Option<Response<ResponseBody>> {
    for lookup in lookups {
        let lookup = match lookup {
            Lookup::File(lookup) => lookup,
            Lookup::Refused(refusal) => {
                // The path is the client's, so it logs at debug and a scanner cannot flood the log.
                tracing::debug!(error = %refusal, "static request refused");
                return Some(error_response(refusal.status()));
            }
        };
        match serve(lookup).await {
            Ok(Some(response)) => return Some(response),
            Ok(None) => continue,
            Err(e) => {
                e.log();
                return Some(error_response(e.status()));
            }
        }
    }
    None
}

/// Where a miss is answered from: the file the cache now keeps, or the disk.
#[expect(
    clippy::large_enum_variant,
    reason = "moved once per miss, out of the blocking pool: boxing would add an allocation to every miss"
)]
enum AnswerSource {
    Memory(Arc<StaticFile>),
    Disk(DiskRepresentation),
}

async fn serve(lookup: StaticLookup<'_, '_, StaticMount>) -> Result<Option<Response<ResponseBody>>, StaticError> {
    let mount = lookup.mount();
    let accept_encoding = lookup.accept_encoding();
    let request = lookup.headers();
    let cache = &mount.cache;
    if let Some(resp) = cache.with(lookup.key(), |file| file.respond(accept_encoding, request)) {
        return Ok(Some(resp));
    }

    let key: Box<[u8]> = lookup.into_key().into_owned().into_boxed_slice();
    let root = cache.root();
    let requested = root.0.join(file_name(&key)?);
    let loader = Arc::clone(cache);
    let loading = requested.clone();
    let copies = mount.rule.copies();
    // The blocking pool needs it owned; it is `Some` only on a mount that serves copies.
    let accepted = accept_encoding.map(str::to_owned);
    let source = tokio::task::spawn_blocking(move || {
        // Taken before the open, so an event on the file from here on stops it being kept.
        let ticket = loader.begin_miss();
        let Some((name, opened)) = read::open(&root.0, loading, copies)? else {
            return Ok(None);
        };
        if let Some(kept) = read_and_keep(&loader, key, ticket, name, &opened)? {
            return Ok(Some(AnswerSource::Memory(kept)));
        }
        Ok::<_, StaticError>(opened.into_disk(accepted.as_deref())?.map(AnswerSource::Disk))
    })
    .await
    .map_err(|source| StaticError::Unfinished {
        path: requested,
        source,
    })??;
    Ok(source.map(|source| match source {
        AnswerSource::Memory(file) => file.respond(accept_encoding, request),
        AnswerSource::Disk(file) => file.respond(request, &mount.held),
    }))
}

/// Keeps `requested` as an alias of the file the cache holds for another path, else reads
/// `opened` whole and keeps it, and returns the file the cache keeps there. Only bytes the cache
/// holds are read whole, so the bytes a miss holds in memory are bounded by the cache's budget,
/// not by how many clients are being answered.
///
/// It keeps nothing while the watcher does not follow the disk (before it is up, and for a mount
/// it could not follow), when the mount has no budget or the budget cannot hold the file, when an
/// event arrived during the read, or when the file changed between its `fstat` and its read.
fn read_and_keep(
    cache: &MountCache,
    key: Box<[u8]>,
    ticket: Option<MissGeneration>,
    requested: RequestPath,
    opened: &FileSet,
) -> Result<Option<Arc<StaticFile>>, StaticError> {
    let Some(ticket) = ticket else {
        return Ok(None);
    };
    if let Some(held) = cache.held(opened.resolved()) {
        return Ok(cache.keep_held(key, ticket, requested, held));
    }
    let Some(reserved) = cache.reserve(opened.len()) else {
        return Ok(None);
    };
    let Some(file) = opened.read()? else {
        return Ok(None);
    };
    Ok(cache.keep_read(key, ticket, requested, reserved, Arc::new(file)))
}

const SERVER_VALUE: HeaderValue = HeaderValue::from_static(crate::plan::response::SERVER_HEADER);
const NOSNIFF: HeaderValue = HeaderValue::from_static("nosniff");

/// Builds a body that says no more than the status, so file paths and OS errors stay in the server log.
fn error_response(status: StatusCode) -> Response<ResponseBody> {
    let text = match status {
        StatusCode::BAD_REQUEST => "bad request",
        StatusCode::FORBIDDEN => "forbidden",
        StatusCode::PAYLOAD_TOO_LARGE => "payload too large",
        _ => "internal server error",
    };
    let mut resp = Response::new(ResponseBody::Full(Full::new(Bytes::from_static(text.as_bytes()))));
    *resp.status_mut() = status;
    *resp.headers_mut() = ERROR_HEADERS.clone();
    resp
}

/// The headers of every refusal and failure answer, built once: an answer clones them, as a hit
/// clones its representation's.
static ERROR_HEADERS: LazyLock<HeaderMap> = LazyLock::new(|| {
    let mut headers = HeaderMap::with_capacity(2);
    headers.insert(SERVER, SERVER_VALUE);
    headers.insert(X_CONTENT_TYPE_OPTIONS, NOSNIFF);
    headers
});

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests;
