//! Serves static files from a mount's directory, cached in memory and kept in step with the disk.
//!
//! A hit makes no filesystem call; a miss reads the file and its pre-compressed copies in one
//! trip to the blocking pool.

mod cache;
pub mod keyed_log_throttle;
pub mod degraded;
mod read;

use bytes::Bytes;
use http_body_util::Full;
use httpdate::HttpDate;
use hyper::header::{
    HeaderMap, HeaderValue, CONTENT_ENCODING, CONTENT_TYPE, ETAG, LAST_MODIFIED, SERVER, VARY,
    X_CONTENT_TYPE_OPTIONS,
};
use hyper::{Response, StatusCode};
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime};

use cache::MountCache;
pub use cache::MountWatch;
use crate::plan::compression::{Coding, CodingSet};
use crate::plan::conditional::{not_modified, EntityTag};
use crate::plan::static_plan::{
    choose_precompressed, mime_from_ext, Copies, Lookup, StaticLookup, StaticLookups,
    StaticMountRule, StaticPrefixError,
};
use mipyre_util::clock;
use keyed_log_throttle::{HeldLines, KeyedLogThrottle};
use read::{file_name, load};

/// Larger files are refused with 413, because the whole file is read into memory.
const MAX_STATIC_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Holds an absolute, symlink-resolved path that was a directory when it was resolved.
#[derive(Debug, Clone)]
struct CanonicalDir(PathBuf);

impl CanonicalDir {
    fn resolve(dir: &str) -> Result<Self, StaticMountError> {
        let path = std::fs::canonicalize(dir).map_err(|source| StaticMountError::Unresolvable {
            dir: dir.to_string(),
            source,
        })?;
        if !path.is_dir() {
            return Err(StaticMountError::NotADirectory(path));
        }
        Ok(Self(path))
    }
}

/// Clones share one cache, so every run's route table serves from it.
#[derive(Debug, Clone)]
pub struct StaticMount {
    rule: StaticMountRule,
    cache: Arc<MountCache>,
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
        let root = CanonicalDir::resolve(dir)?;
        Ok(Self {
            rule,
            cache: Arc::new(MountCache::new(root, &cache::PROCESS_BUDGET)),
        })
    }

    /// Reports whether a miss is kept now or every request reads the file from disk.
    pub fn watch(&self) -> MountWatch {
        self.cache.watch()
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
    GrewPastLimit {
        path: PathBuf,
        stat_len: u64,
        limit: u64,
    },
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

/// Lets one permission warning a minute through per path, since a scanner asks for the same path over and over.
static PERMISSION_THROTTLE: KeyedLogThrottle<PathBuf> =
    KeyedLogThrottle::new(Duration::from_secs(60), std::num::NonZeroUsize::new(256).expect("256 is not zero"));

impl StaticError {
    /// Answers EACCES with 403, as nginx does, because it is an access decision about the file
    /// rather than a fault in serving it.
    fn status(&self) -> StatusCode {
        match self {
            Self::NotUtf8 => StatusCode::BAD_REQUEST,
            Self::Escape { .. } | Self::PermissionDenied { .. } | Self::SymlinkSwapped { .. } => {
                StatusCode::FORBIDDEN
            }
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
            Self::PermissionDenied { path, .. } => {
                match PERMISSION_THROTTLE.admit(path.as_path(), clock::now()) {
                    Some(HeldLines::OfKey(held)) => tracing::warn!(error = %self, held, "static file refused"),
                    Some(HeldLines::OfUnrememberedKeys(held)) => {
                        tracing::warn!(error = %self, held_among_other_paths = held, "static file refused")
                    }
                    None => tracing::debug!(error = %self, "static file refused"),
                }
            }
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

/// One file as it is sent, the original or a copy, with the validators a client revalidates it
/// with and the headers of both answers.
///
/// The validators are the one source: `sent` and `not_modified` are their wire form, built once
/// in [`Representation::new`] and never changed, so a hit clones a header map instead of
/// inserting headers.
#[derive(Clone, Debug)]
struct Representation {
    bytes: Bytes,
    /// It is a hash of the bytes alone, so the same file gets the same tag in every process and
    /// after a restart.
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

impl Representation {
    /// Builds the answers for `read` sent in `coding`, or as the original when `coding` is
    /// `None`; `vary` says the file has copies, so every answer depends on `Accept-Encoding`.
    #[expect(
        clippy::expect_used,
        reason = "an HTTP-date is visible ASCII: the header-value parse cannot fail"
    )]
    fn new(read: FileContents, coding: Option<Coding>, content_type: &HeaderValue, vary: bool) -> Self {
        use std::hash::Hasher;
        let mut hasher = std::hash::DefaultHasher::new();
        hasher.write(&read.bytes);
        let etag = EntityTag::of_digest(hasher.finish());
        let modified = read
            .modified
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
            bytes: read.bytes,
            etag,
            modified,
            sent,
            not_modified,
        }
    }

    /// Answers `304` with no body when the `request`'s preconditions say the client has this.
    fn respond(&self, request: &HeaderMap) -> Response<Full<Bytes>> {
        if not_modified(request, &self.etag, self.modified) {
            let mut resp = Response::new(Full::new(Bytes::new()));
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
            *resp.headers_mut() = self.not_modified.clone();
            return resp;
        }
        let mut resp = Response::new(Full::new(self.bytes.clone()));
        *resp.headers_mut() = self.sent.clone();
        resp
    }
}

/// Cloning shares the bytes instead of copying them.
#[derive(Clone)]
struct StaticFile {
    original: Representation,
    /// It is `None` on a mount that serves no copies, because none were looked for; when it is
    /// `Some`, a copy appearing, changing or going makes the file stale.
    precompressed: Option<PrecompressedCopies<Representation>>,
    copies: CodingSet,
    /// It is the path under the root before symlinks resolve.
    requested: PathBuf,
    resolved: PathBuf,
}

#[derive(Clone, Default)]
struct PrecompressedCopies<T> {
    br: Option<T>,
    gz: Option<T>,
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
        requested: PathBuf,
        resolved: PathBuf,
    ) -> Self {
        let copies = CodingSet::of(|coding| {
            precompressed
                .as_ref()
                .is_some_and(|found| found.get(coding).is_some())
        });
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
            requested,
            resolved,
        }
    }

    fn cached_len(&self) -> u64 {
        let copies = self
            .precompressed
            .as_ref()
            .map_or(0, PrecompressedCopies::len);
        (self.original.bytes.len() + copies) as u64
    }

    /// Tells whether a change at `changed`, which may be a directory above the file, can make this stale.
    fn depends_on(&self, changed: &Path) -> bool {
        self.requested.starts_with(changed)
            || self.resolved.starts_with(changed)
            || (self.precompressed.is_some()
                && Coding::PREFERENCE
                    .iter()
                    .any(|&coding| copy_path(coding, &self.resolved).as_path() == changed))
    }

    /// Answers with the first copy, in server preference, that the client accepts, else the
    /// original.
    fn respond(&self, accept_encoding: Option<&str>, request: &HeaderMap) -> Response<Full<Bytes>> {
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
pub async fn try_static_file(
    lookups: StaticLookups<'_, '_, StaticMount>,
) -> Option<Response<Full<Bytes>>> {
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

async fn serve(
    lookup: StaticLookup<'_, '_, StaticMount>,
) -> Result<Option<Response<Full<Bytes>>>, StaticError> {
    let mount = lookup.mount();
    let accept_encoding = lookup.accept_encoding();
    let request = lookup.headers();
    let cache = &mount.cache;
    if let Some(resp) = cache.with(lookup.key(), |file| file.respond(accept_encoding, request)) {
        return Ok(Some(resp));
    }

    let key: Box<[u8]> = lookup.into_key().into_owned().into_boxed_slice();
    let requested = cache.root().join(file_name(&key)?);
    let loader = Arc::clone(cache);
    let loading = requested.clone();
    let copies = mount.rule.copies();
    let loaded = tokio::task::spawn_blocking(move || {
        // Kept only while the watcher follows the disk: before it is up, and for a mount it
        // could not follow, the file is served and not kept.
        let ticket = loader.begin_miss();
        let file = load(loader.root(), loading, copies)?;
        if let (Some(ticket), Some(file)) = (ticket, &file) {
            loader.keep(key, ticket, file.clone());
        }
        Ok::<_, StaticError>(file)
    })
    .await
    .map_err(|source| StaticError::Unfinished {
        path: requested,
        source,
    })??;
    Ok(loaded.map(|file| file.respond(accept_encoding, request)))
}

const SERVER_VALUE: HeaderValue = HeaderValue::from_static(crate::plan::response::SERVER_HEADER);
const NOSNIFF: HeaderValue = HeaderValue::from_static("nosniff");

/// Builds a body that says no more than the status, so file paths and OS errors stay in the server log.
fn error_response(status: StatusCode) -> Response<Full<Bytes>> {
    let text = match status {
        StatusCode::BAD_REQUEST => "bad request",
        StatusCode::FORBIDDEN => "forbidden",
        StatusCode::PAYLOAD_TOO_LARGE => "payload too large",
        _ => "internal server error",
    };
    let mut resp = Response::new(Full::new(Bytes::from_static(text.as_bytes())));
    *resp.status_mut() = status;
    *resp.headers_mut() = ERROR_HEADERS.clone();
    resp
}

/// The headers of every refusal and failure answer, built once: an answer clones them, as a hit
/// clones its representation's (b/612).
static ERROR_HEADERS: LazyLock<HeaderMap> = LazyLock::new(|| {
    let mut headers = HeaderMap::with_capacity(2);
    headers.insert(SERVER, SERVER_VALUE);
    headers.insert(X_CONTENT_TYPE_OPTIONS, NOSNIFF);
    headers
});

#[cfg(test)]
mod tests;
