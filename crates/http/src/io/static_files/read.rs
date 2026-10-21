//! Reads a mount's file and its pre-compressed copies from disk without leaving the mount's root.

use bytes::Bytes;
use std::ffi::OsStr;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::plan::compression::Coding;
use crate::plan::static_plan::Copies;
use mipyre_util::clock;
use super::keyed_log_throttle::{HeldLines, KeyedLogThrottle};
use crate::io::os;

use super::degraded::{self, StaticDegradation};
use super::{
    copy_path, FileContents, PrecompressedCopies, StaticError, StaticFile, MAX_STATIC_FILE_BYTES,
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

/// Reads the file at `requested` and returns `None` if it is not there or not a regular file.
/// It blocks, so it runs on the blocking pool.
pub(super) fn load(
    root: &Path,
    requested: PathBuf,
    copies: Copies,
) -> Result<Option<StaticFile>, StaticError> {
    let Some(resolved) = canonical_within(root, &requested)? else {
        return Ok(None);
    };
    let Some(original) = read_regular_file(&resolved)? else {
        return Ok(None);
    };
    let precompressed = match copies {
        Copies::Original => None,
        Copies::Precompressed => Some(PrecompressedCopies {
            br: read_copy(&resolved, Coding::Brotli),
            gz: read_copy(&resolved, Coding::Gzip),
        }),
    };
    Ok(Some(StaticFile::new(
        original,
        precompressed,
        requested,
        resolved,
    )))
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

/// Reads the `coding` copy beside `resolved`, opened without following a symlink so that it
/// cannot lead out of the root. A copy that cannot be read is logged and skipped, because the
/// original is always a correct answer.
fn read_copy(resolved: &Path, coding: Coding) -> Option<FileContents> {
    let path = copy_path(coding, resolved);
    match read_regular_file(&path) {
        Ok(bytes) => bytes,
        Err(StaticError::SymlinkSwapped { .. }) => None,
        Err(error) => {
            degraded::count(StaticDegradation::PrecompressedFallback);
            match TWIN_THROTTLE.admit(path.as_path(), clock::now()) {
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
            None
        }
    }
}

/// Lets one warning a minute through per unreadable copy, since requests keep asking for it.
static TWIN_THROTTLE: KeyedLogThrottle<PathBuf> =
    KeyedLogThrottle::new(Duration::from_secs(60), std::num::NonZeroUsize::new(256).expect("256 is not zero"));

/// Reads a regular file of at most `MAX_STATIC_FILE_BYTES` with its modification time from the
/// same `fstat`, or returns `None` if it is gone or not a regular file.
///
/// The size check uses the opened file's metadata, so it and the read see the same inode. The
/// open does not follow a symlink, because one swapped in after `canonical_within` checked
/// containment could point anywhere on disk.
fn read_regular_file(path: &Path) -> Result<Option<FileContents>, StaticError> {
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

    // A filesystem without modification times gets no `Last-Modified`; the `ETag` still revalidates.
    let modified = metadata.modified().ok();
    let bytes = read_contents(path, file, metadata.len(), MAX_STATIC_FILE_BYTES)?;
    Ok(bytes.map(|bytes| FileContents { bytes, modified }))
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
    Ok(Some(Bytes::from(contents)))
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
        let dir = std::env::temp_dir().join(format!(
            "mipyre-static-read-symlink-{}",
            std::process::id()
        ));
        match std::fs::remove_dir_all(&dir) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            result => result.unwrap(),
        }
        std::fs::create_dir_all(&dir).unwrap();
        let secret = dir.join("secret.txt");
        std::fs::write(&secret, b"SHHH").unwrap();
        let link = dir.join("index.html");
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        let refused = read_regular_file(&link).expect_err("a symlink is not followed");
        assert!(
            matches!(&refused, StaticError::SymlinkSwapped { path, .. } if path == &link),
            "{refused:?}"
        );
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        assert!(!answer_body(&refused).contains("SHHH"));
        assert_eq!(
            read_regular_file(&secret).unwrap().map(|read| read.bytes),
            Some(Bytes::from_static(b"SHHH"))
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
