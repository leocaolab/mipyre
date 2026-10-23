//! Wraps the file calls that differ between platforms; it is the only file of this crate with a
//! platform `#[cfg]`.

use std::io;

/// Turns a static file key into a file name.
///
/// # Errors
///
/// Off Unix a file name must be UTF-8, so a key that is not is refused.
#[cfg(unix)]
pub(crate) fn file_name(bytes: &[u8]) -> Result<&std::ffi::OsStr, std::str::Utf8Error> {
    use std::os::unix::ffi::OsStrExt;
    Ok(std::ffi::OsStr::from_bytes(bytes))
}

#[cfg(not(unix))]
pub(crate) fn file_name(bytes: &[u8]) -> Result<&std::ffi::OsStr, std::str::Utf8Error> {
    std::str::from_utf8(bytes).map(std::ffi::OsStr::new)
}

/// Opens `path` for reading, refusing a symlink at its last component where the platform has
/// `O_NOFOLLOW`.
///
/// The open never waits: a FIFO's read end opened without `O_NONBLOCK` waits for a writer, so a
/// caller learns what the path is from the opened file's `fstat` instead. A regular file's reads
/// ignore the flag.
#[cfg(unix)]
pub(crate) fn open_no_follow(path: &std::path::Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

#[cfg(unix)]
pub(crate) fn is_symlink_refusal(e: &io::Error) -> bool {
    e.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(not(unix))]
pub(crate) fn open_no_follow(path: &std::path::Path) -> io::Result<std::fs::File> {
    std::fs::File::open(path)
}

#[cfg(not(unix))]
pub(crate) fn is_symlink_refusal(_: &io::Error) -> bool {
    false
}

/// Plants a symlink for tests; `None` means making one needs privileges here, as on Windows.
#[cfg(all(test, unix))]
pub(crate) fn plant_symlink(original: &std::path::Path, link: &std::path::Path) -> Option<io::Result<()>> {
    Some(std::os::unix::fs::symlink(original, link))
}

#[cfg(all(test, not(unix)))]
pub(crate) fn plant_symlink(_original: &std::path::Path, _link: &std::path::Path) -> Option<io::Result<()>> {
    None
}

/// Makes a FIFO for tests with the `mkfifo` tool, so the crate needs no `unsafe` call; `None`
/// means the platform has no FIFOs, as on Windows.
#[cfg(all(test, unix))]
pub(crate) fn plant_fifo(path: &std::path::Path) -> Option<io::Result<()>> {
    let made = std::process::Command::new("mkfifo").arg(path).status();
    Some(made.and_then(|status| {
        if status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!("mkfifo {}: {status}", path.display())))
        }
    }))
}

#[cfg(all(test, not(unix)))]
pub(crate) fn plant_fifo(_path: &std::path::Path) -> Option<io::Result<()>> {
    None
}
