//! Makes a doorbell an asyncio loop waits on, for an async engine or a producer waiting for room: an eventfd on Linux,
//! a pipe on macOS, which has no eventfd. Either way the loop waits on one nonblocking, close-on-exec fd that one ring
//! makes readable and one quiet makes unreadable again.

use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd, RawFd};

use rustix::io::Errno;

/// The fd a loop waits on, readable once rung until quieted.
#[cfg(target_os = "linux")]
pub(crate) struct Doorbell(OwnedFd);

#[cfg(target_os = "linux")]
impl Doorbell {
    pub(crate) fn new() -> io::Result<Self> {
        use rustix::event::{eventfd, EventfdFlags};
        Ok(Doorbell(eventfd(0, EventfdFlags::NONBLOCK | EventfdFlags::CLOEXEC)?))
    }

    pub(crate) fn ring(&self) -> io::Result<()> {
        rustix::io::write(&self.0, &1u64.to_ne_bytes())?;
        Ok(())
    }

    /// Reads the rings away; a doorbell nobody rang is already quiet.
    pub(crate) fn quiet(&self) -> io::Result<()> {
        let mut count = [0u8; 8];
        quieted(rustix::io::read(&self.0, &mut count))
    }

    pub(crate) fn fd(&self) -> RawFd {
        self.0.as_fd().as_raw_fd()
    }
}

/// The fd a loop waits on, readable once rung until quieted.
#[cfg(target_os = "macos")]
pub(crate) struct Doorbell {
    read: OwnedFd,
    write: OwnedFd,
}

#[cfg(target_os = "macos")]
impl Doorbell {
    /// Makes the pipe and then sets its flags, since macOS has no `pipe2`; a process forked in between inherits the
    /// two ends, which costs it two fds and nothing else, since no reader here waits for end of file.
    pub(crate) fn new() -> io::Result<Self> {
        let (read, write) = rustix::pipe::pipe()?;
        for end in [&read, &write] {
            rustix::io::fcntl_setfd(end, rustix::io::FdFlags::CLOEXEC)?;
            let flags = rustix::fs::fcntl_getfl(end)?;
            rustix::fs::fcntl_setfl(end, flags | rustix::fs::OFlags::NONBLOCK)?;
        }
        Ok(Doorbell { read, write })
    }

    pub(crate) fn ring(&self) -> io::Result<()> {
        rustix::io::write(&self.write, &[1u8])?;
        Ok(())
    }

    /// Reads the rings away; a doorbell nobody rang is already quiet. It holds at most one unread ring, so one read
    /// empties it.
    pub(crate) fn quiet(&self) -> io::Result<()> {
        let mut rings = [0u8; 8];
        quieted(rustix::io::read(&self.read, &mut rings))
    }

    pub(crate) fn fd(&self) -> RawFd {
        self.read.as_fd().as_raw_fd()
    }
}

fn quieted(read: Result<usize, Errno>) -> io::Result<()> {
    match read {
        Ok(_) | Err(Errno::AGAIN) => Ok(()),
        Err(errno) => Err(errno.into()),
    }
}

// Miri runs no system call.
#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    fn readable(bell: &Doorbell) -> bool {
        use rustix::event::{poll, PollFd, PollFlags};
        // SAFETY: the doorbell owns the fd and outlives this borrow.
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(bell.fd()) };
        let mut fds = [PollFd::new(&fd, PollFlags::IN)];
        poll(&mut fds, Some(&rustix::time::Timespec { tv_sec: 0, tv_nsec: 0 })).expect("poll") == 1
    }

    #[test]
    fn a_rung_doorbell_is_readable_until_quieted_and_a_quiet_one_quiets_again() {
        let bell = Doorbell::new().expect("a doorbell");
        assert!(!readable(&bell));
        bell.quiet().expect("quieting an unrung doorbell");
        bell.ring().expect("a ring");
        assert!(readable(&bell));
        bell.quiet().expect("a quiet");
        assert!(!readable(&bell));
    }

    #[test]
    fn a_read_with_nothing_to_read_is_quiet_and_any_other_failure_is_not() {
        assert!(quieted(Ok(8)).is_ok());
        assert!(quieted(Err(Errno::AGAIN)).is_ok());
        assert_eq!(
            quieted(Err(Errno::BADF)).map_err(|e| e.raw_os_error()).err(),
            Some(Some(Errno::BADF.raw_os_error()))
        );
    }
}
