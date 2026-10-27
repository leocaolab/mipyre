//! Reads a mount's file and its pre-compressed copies from disk without leaving the mount's root.

use bytes::Bytes;
use hyper::header::HeaderValue;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime};

use super::keyed_log_throttle::{HeldLines, KeyedLogThrottle};
use crate::io::os;
use crate::plan::compression::{Coding, CodingSet};
use crate::plan::static_plan::{choose_precompressed, Copies};
use mipyre_util::clock;

use super::degraded::{self, StaticDegradation};
use super::send::{fingerprint, Fingerprint};
use super::{
    content_type, copy_path, DiskRepresentation, FileContents, Head, PrecompressedCopies, RequestPath, StaticError,
    StaticFile, MAX_STATIC_FILE_BYTES,
};

/// Treats a missing path, including a file used as a directory or a name too long for the
/// filesystem to hold, as `Ok(None)`.
fn found<T>(path: &Path, result: io::Result<T>) -> Result<Option<T>, StaticError> {
    let source = match result {
        Ok(value) => return Ok(Some(value)),
        Err(source) => source,
    };
    let path = path.to_path_buf();
    match source.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory | io::ErrorKind::InvalidFilename => Ok(None),
        io::ErrorKind::PermissionDenied => Err(StaticError::PermissionDenied { path, source }),
        _ => Err(StaticError::Unreadable { path, source }),
    }
}

pub(super) fn file_name(bytes: &[u8]) -> Result<&OsStr, StaticError> {
    os::file_name(bytes).map_err(|_| StaticError::NotUtf8)
}

/// A regular file opened without following a symlink, with what its `fstat` said.
pub(super) struct DiskFile {
    path: PathBuf,
    file: File,
    len: u64,
    /// It is `None` when the platform or filesystem keeps no modification time.
    modified: Option<SystemTime>,
}

/// The file a request names and the copies beside it, opened and not yet read.
pub(super) struct FileSet {
    original: DiskFile,
    /// It is `None` on a mount that serves no copies.
    precompressed: Option<PrecompressedCopies<DiskFile>>,
    resolved: PathBuf,
}

/// Opens the file at `requested` and its copies, and returns the path the request named with
/// them, or `None` if it is not there or not a regular file. It blocks, so it runs on the
/// blocking pool.
pub(super) fn open(
    root: &Path,
    requested: PathBuf,
    copies: Copies,
) -> Result<Option<(RequestPath, FileSet)>, StaticError> {
    let Some(resolved) = canonical_within(root, &requested)? else {
        return Ok(None);
    };
    // A path that resolves to itself went through no symlink, so the common miss walks nothing.
    let link_dirs = if requested == resolved {
        Box::default()
    } else {
        let Some(dirs) = found(&requested, symlink_dirs(&requested))? else {
            return Ok(None);
        };
        dirs.into_boxed_slice()
    };
    let Some(original) = open_regular_file(&resolved)? else {
        return Ok(None);
    };
    let precompressed = match copies {
        Copies::Original => None,
        Copies::Precompressed => Some(PrecompressedCopies {
            br: open_copy(&resolved, Coding::Brotli),
            gz: open_copy(&resolved, Coding::Gzip),
        }),
    };
    let name = RequestPath {
        path: requested,
        link_dirs,
    };
    Ok(Some((
        name,
        FileSet {
            original,
            precompressed,
            resolved,
        },
    )))
}

/// Bounds the symlinks one resolution follows, as the kernel's `MAXSYMLINKS` does.
const MAX_SYMLINKS: usize = 40;

/// Resolves `path` one component at a time, as `realpath` does, and returns the resolved
/// directory of every symlink met.
///
/// # Errors
///
/// It fails as `canonicalize` would on the same disk; a disk that changed since the caller's
/// `canonicalize` can fail it here, and the watcher's event for that change keeps the read from
/// being cached.
pub(super) fn symlink_dirs(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut at = PathBuf::new();
    let mut remaining = path.to_path_buf();
    let mut followed = 0;
    loop {
        let mut components = remaining.components();
        let Some(first) = components.next() else {
            return Ok(dirs);
        };
        let rest = components.as_path().to_path_buf();
        match first {
            Component::Prefix(_) | Component::RootDir => at.push(first),
            Component::CurDir => {}
            Component::ParentDir => {
                at.pop();
            }
            Component::Normal(name) => {
                let next = at.join(name);
                if std::fs::symlink_metadata(&next)?.file_type().is_symlink() {
                    followed += 1;
                    if followed > MAX_SYMLINKS {
                        return Err(io::Error::other(format!(
                            "{} follows more than {MAX_SYMLINKS} symlinks",
                            path.display()
                        )));
                    }
                    let dir = std::fs::canonicalize(&at)?;
                    if !dirs.contains(&dir) {
                        dirs.push(dir);
                    }
                    let target = std::fs::read_link(&next)?;
                    if target.is_absolute() {
                        at = PathBuf::new();
                    }
                    remaining = target.join(rest);
                    continue;
                }
                at = next;
            }
        }
        remaining = rest;
    }
}

impl FileSet {
    pub(super) fn resolved(&self) -> &Path {
        &self.resolved
    }

    /// Returns the bytes the file and its copies take by their `fstat`, which is what keeping
    /// them costs if they do not change before they are read.
    pub(super) fn len(&self) -> u64 {
        let copy = |copy: &Option<DiskFile>| copy.as_ref().map_or(0, |copy| copy.len);
        self.original.len
            + self
                .precompressed
                .as_ref()
                .map_or(0, |found| copy(&found.br) + copy(&found.gz))
    }

    /// Reads the file and its copies whole from their start; a copy that cannot be read is
    /// skipped, because the original is always a correct answer.
    pub(super) fn read(&self) -> Result<Option<StaticFile>, StaticError> {
        let Some(original) = read_whole(&self.original)? else {
            return Ok(None);
        };
        let read_copy = |copy: &Option<DiskFile>| {
            let copy = copy.as_ref()?;
            read_whole(copy).unwrap_or_else(|error| {
                skip_copy(&copy.path, &error);
                None
            })
        };
        let precompressed = self.precompressed.as_ref().map(|found| PrecompressedCopies {
            br: read_copy(&found.br),
            gz: read_copy(&found.gz),
        });
        Ok(Some(StaticFile::new(original, precompressed, self.resolved.clone())))
    }

    /// Picks the representation the client is answered with, by `accept_encoding`, and reads it
    /// once for its `ETag`, leaving it to be sent from disk. A copy that cannot be read is
    /// skipped for the original.
    pub(super) fn into_disk(self, accept_encoding: Option<&str>) -> Result<Option<DiskRepresentation>, StaticError> {
        let content_type = HeaderValue::from_static(content_type(&self.resolved));
        let mut precompressed = self.precompressed.unwrap_or_default();
        let vary = precompressed.any();
        let has = CodingSet::of(|coding| precompressed.get(coding).is_some());
        if let Some(coding) = choose_precompressed(accept_encoding, has) {
            let copy = match coding {
                Coding::Brotli => precompressed.br.take(),
                Coding::Gzip => precompressed.gz.take(),
            };
            if let Some(copy) = copy {
                match fingerprinted(&copy) {
                    Ok(Some(fingerprint)) => {
                        return Ok(Some(DiskRepresentation::new(
                            copy,
                            fingerprint,
                            Some(coding),
                            &content_type,
                            vary,
                        )))
                    }
                    Ok(None) => {}
                    Err(error) => {
                        skip_copy(&copy.path, &error);
                    }
                }
            }
        }
        let Some(fingerprint) = fingerprinted(&self.original)? else {
            return Ok(None);
        };
        Ok(Some(DiskRepresentation::new(
            self.original,
            fingerprint,
            None,
            &content_type,
            vary,
        )))
    }
}

/// Reads `opened` from its start for its length and digest, refusing a file grown past the limit.
fn fingerprinted(opened: &DiskFile) -> Result<Option<Fingerprint>, StaticError> {
    let Some(fingerprint) = found(&opened.path, fingerprint(&opened.file, MAX_STATIC_FILE_BYTES))? else {
        return Ok(None);
    };
    fingerprint.map(Some).ok_or_else(|| StaticError::GrewPastLimit {
        path: opened.path.clone(),
        stat_len: opened.len,
        limit: MAX_STATIC_FILE_BYTES,
    })
}

impl DiskRepresentation {
    fn new(
        opened: DiskFile,
        fingerprint: Fingerprint,
        coding: Option<Coding>,
        content_type: &HeaderValue,
        vary: bool,
    ) -> Self {
        Self {
            head: Head::new(fingerprint.digest, opened.modified, coding, content_type, vary),
            file: opened.file,
            path: opened.path,
            fingerprint,
        }
    }
}

/// Refuses a path whose symlinks resolve outside `root`, such as a planted link to `/etc/passwd`.
fn canonical_within(root: &Path, candidate: &Path) -> Result<Option<PathBuf>, StaticError> {
    let Some(path) = found(candidate, std::fs::canonicalize(candidate))? else {
        return Ok(None);
    };
    if !path.starts_with(root) {
        return Err(StaticError::Escape {
            path,
            root: root.to_path_buf(),
        });
    }
    Ok(Some(path))
}

/// Opens the `coding` copy beside `resolved`, without following a symlink so that it cannot
/// lead out of the root. A copy that cannot be opened is skipped.
fn open_copy(resolved: &Path, coding: Coding) -> Option<DiskFile> {
    let path = copy_path(coding, resolved);
    open_regular_file(&path).unwrap_or_else(|error| {
        skip_copy(&path, &error);
        None
    })
}

/// Logs and counts a copy that cannot be served, so the original answers; a copy that is a
/// symlink is not served and not reported.
fn skip_copy(path: &Path, error: &StaticError) {
    if matches!(error, StaticError::SymlinkSwapped { .. }) {
        return;
    }
    degraded::count(StaticDegradation::PrecompressedFallback);
    match TWIN_THROTTLE.admit(path, clock::now()) {
        Some(HeldLines::OfKey(held)) => tracing::warn!(
            error = %error,
            held,
            path = %path.display(),
            "pre-compressed twin not served; answering with the original"
        ),
        Some(HeldLines::OfUnrememberedKeys(held)) => tracing::warn!(
            error = %error,
            held_among_other_paths = held,
            path = %path.display(),
            "pre-compressed twin not served; answering with the original"
        ),
        None => tracing::debug!(
            error = %error,
            path = %path.display(),
            "pre-compressed twin not served; answering with the original"
        ),
    }
}

/// Lets one warning a minute through per unreadable copy, since requests keep asking for it.
static TWIN_THROTTLE: KeyedLogThrottle<PathBuf> = KeyedLogThrottle::new(
    Duration::from_secs(60),
    std::num::NonZeroUsize::new(256).expect("256 is not zero"),
);

/// Opens a regular file of at most `MAX_STATIC_FILE_BYTES` and takes its length and
/// modification time from the same `fstat`, or returns `None` if it is gone or not a regular file.
///
/// The size check uses the opened file's metadata, so it and every later read see the same inode.
/// The open does not follow a symlink, because one swapped in after `canonical_within` checked
/// containment could point anywhere on disk.
fn open_regular_file(path: &Path) -> Result<Option<DiskFile>, StaticError> {
    let opened = match os::open_no_follow(path) {
        Err(source) if os::is_symlink_refusal(&source) => {
            return Err(StaticError::SymlinkSwapped {
                path: path.to_path_buf(),
                source,
            })
        }
        opened => found(path, opened)?,
    };
    let Some(file) = opened else {
        return Ok(None);
    };
    let Some(metadata) = found(path, file.metadata())? else {
        return Ok(None);
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    if metadata.len() > MAX_STATIC_FILE_BYTES {
        return Err(StaticError::TooLarge {
            path: path.to_path_buf(),
            len: metadata.len(),
        });
    }
    Ok(Some(DiskFile {
        path: path.to_path_buf(),
        file,
        len: metadata.len(),
        // A filesystem without modification times gets no `Last-Modified`; the `ETag` still revalidates.
        modified: metadata.modified().ok(),
    }))
}

/// Reads an opened file whole from its start.
fn read_whole(opened: &DiskFile) -> Result<Option<FileContents>, StaticError> {
    let mut file = &opened.file;
    if found(&opened.path, file.seek(SeekFrom::Start(0)))?.is_none() {
        return Ok(None);
    }
    let bytes = read_contents(&opened.path, file, opened.len, MAX_STATIC_FILE_BYTES)?;
    Ok(bytes.map(|bytes| FileContents {
        bytes,
        modified: opened.modified,
    }))
}

/// Reads at most one byte past `cap`, because the file may grow after its stat; a file that
/// grew past `cap` is refused rather than served cut short.
#[inline(always)]
pub(super) fn read_contents(
    path: &Path,
    file: impl Read,
    stat_len: u64,
    cap: u64,
) -> Result<Option<Bytes>, StaticError> {
    let mut contents = Vec::with_capacity(stat_len as usize);
    let read = file.take(cap.saturating_add(1)).read_to_end(&mut contents);
    if found(path, read)?.is_none() {
        return Ok(None);
    }
    if contents.len() as u64 > cap {
        return Err(StaticError::GrewPastLimit {
            path: path.to_path_buf(),
            stat_len,
            limit: cap,
        });
    }
    // The cache hands every hit a clone. An owned `Bytes` clones by one count; one made from a
    // `Vec` would first promote itself and then check on every clone whether it has.
    Ok(Some(Bytes::from_owner(contents)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::StatusCode;

    fn answer_body(refused: &StaticError) -> String {
        use hyper::body::Body;
        let mut body = super::super::error_response(refused.status()).into_body();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match std::pin::Pin::new(&mut body).poll_frame(&mut cx) {
            std::task::Poll::Ready(Some(Ok(frame))) => {
                let data = frame.into_data().expect("a Full body's frame is data");
                String::from_utf8(data.to_vec()).expect("the answer is text")
            }
            other => panic!("a Full body is ready with its one frame: {other:?}"),
        }
    }

    /// A test run as root reads every file, so this decides from the OS's error kind alone.
    #[test]
    fn a_permission_denied_read_is_refused_403() {
        let path = Path::new("/srv/static/secret.txt");
        let refused = found::<()>(path, Err(io::Error::from(io::ErrorKind::PermissionDenied)))
            .expect_err("EACCES is not \"not there\"");
        assert!(
            matches!(&refused, StaticError::PermissionDenied { path: p, .. } if p == path),
            "{refused:?}"
        );
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let body = answer_body(&refused);
        assert_eq!(body, "forbidden");
        assert!(!body.contains("secret"));

        assert!(matches!(
            found::<()>(path, Err(io::Error::from(io::ErrorKind::NotFound))),
            Ok(None)
        ));
        let other = found::<()>(path, Err(io::Error::other("disk on fire"))).unwrap_err();
        assert_eq!(other.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Opening a path that already is a symlink is the same open a swap would meet, so no race
    /// has to be staged.
    #[cfg(unix)]
    #[test]
    fn a_file_that_is_a_symlink_at_open_is_refused_403_without_following_it() {
        let dir = std::env::temp_dir().join(format!("mipyre-static-read-symlink-{}", std::process::id()));
        match std::fs::remove_dir_all(&dir) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            result => result.unwrap(),
        }
        std::fs::create_dir_all(&dir).unwrap();
        let secret = dir.join("secret.txt");
        std::fs::write(&secret, b"SHHH").unwrap();
        let link = dir.join("index.html");
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        let refused = open_regular_file(&link).err().expect("a symlink is not followed");
        assert!(
            matches!(&refused, StaticError::SymlinkSwapped { path, .. } if path == &link),
            "{refused:?}"
        );
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        assert!(!answer_body(&refused).contains("SHHH"));
        assert_eq!(
            open_regular_file(&secret)
                .unwrap()
                .and_then(|opened| read_whole(&opened).unwrap())
                .map(|read| read.bytes),
            Some(Bytes::from_static(b"SHHH"))
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
