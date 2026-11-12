//! Watches the fds a TPC thread's event loop is asked to watch, and holds the loop's one kernel
//! timer, through one fd of its own that is readable while one of them is ready or the timer has
//! fired: level-triggered, an epoll with a timerfd on Linux and a kqueue with an `EVFILT_TIMER` on
//! macOS (`os.rs`).
//!
//! It holds fd numbers, not fds: it does not own, dup or keep open what it watches. It does not
//! remember what it was told either: each change names what the fd was watched for, which the
//! loop's fd table knows, and the timer is set for whatever delay it is given, which the loop's
//! timers decide. It does not know callbacks, Python or the loop.

mod os;

use std::io;
use std::os::fd::OwnedFd;

use tokio::io::unix::AsyncFd;

/// The most events one scan takes; a watch left out is level-triggered, so the next scan finds it.
const MAX_EVENTS: usize = 64;

/// Says what an fd is watched for, or found ready for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FdInterest {
    Read,
    Write,
    ReadWrite,
}

impl FdInterest {
    /// Returns what an fd watched, or found ready, for reading, writing or both is; one for neither
    /// is not watched.
    pub(crate) fn of(read: bool, write: bool) -> Option<FdInterest> {
        match (read, write) {
            (true, true) => Some(FdInterest::ReadWrite),
            (true, false) => Some(FdInterest::Read),
            (false, true) => Some(FdInterest::Write),
            (false, false) => None,
        }
    }

    pub(crate) fn reads(self) -> bool {
        matches!(self, FdInterest::Read | FdInterest::ReadWrite)
    }

    pub(crate) fn writes(self) -> bool {
        matches!(self, FdInterest::Write | FdInterest::ReadWrite)
    }
}

/// Says whether a scan handed over every fd the kernel had ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanEnd {
    Drained,
    /// The scan filled its room, or a signal interrupted it, so more fds may be ready.
    Partial,
}

/// Says what one scan found besides the fds it handed over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Scan {
    pub(crate) end: ScanEnd,
    /// It is the kernel's word that the timer fired, which a kqueue gives before the thread's clock
    /// may read the deadline as passed.
    pub(crate) timer_fired: bool,
}

/// Returns whether a scan that took `events` events saw them all: one that filled its room may
/// have left some behind.
fn scan_end(events: usize) -> ScanEnd {
    if events < MAX_EVENTS {
        ScanEnd::Drained
    } else {
        ScanEnd::Partial
    }
}

/// Watches fds for one event loop; its own fd is readable while a watched fd is ready.
///
/// A registration belongs to what the kernel keys it by. kqueue keys it by the fd number, so
/// closing the fd drops it. epoll keys it by the open file: closing the fd drops it only once every
/// fd of that file is closed (a `dup`, or a child's inherited copy, keeps it), and until then it
/// goes on reporting under the old number, which nothing can unwatch any more. A caller that closes
/// a watched fd before unwatching it leaves that behind, as with asyncio's selector loop.
pub(crate) struct Poller(os::KernelQueue);

/// Holds a duplicate of a poller's fd, registered with a tokio reactor, which wakes its waiter when
/// a watched fd becomes ready.
pub struct PollerReadiness(AsyncFd<OwnedFd>);

impl PollerReadiness {
    /// Waits until a watched fd may have become ready, and clears the reactor's report of it before
    /// it returns, so only a new event wakes a later wait; the loop's `next_wait` says how an fd
    /// that stays ready is served.
    ///
    /// # Errors
    ///
    /// Returns the reactor's error, which it gives once it has shut down.
    pub async fn wait(&self) -> io::Result<()> {
        self.0.readable().await?.clear_ready();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scan_that_fills_its_room_may_have_left_fds_ready_and_one_that_does_not_has_not() {
        assert_eq!(scan_end(0), ScanEnd::Drained);
        assert_eq!(scan_end(MAX_EVENTS - 1), ScanEnd::Drained);
        assert_eq!(scan_end(MAX_EVENTS), ScanEnd::Partial);
    }

    #[test]
    fn an_fd_watched_for_neither_reading_nor_writing_is_not_watched() {
        assert_eq!(FdInterest::of(true, false), Some(FdInterest::Read));
        assert_eq!(FdInterest::of(false, true), Some(FdInterest::Write));
        assert_eq!(FdInterest::of(true, true), Some(FdInterest::ReadWrite));
        assert_eq!(FdInterest::of(false, false), None);
    }
}
