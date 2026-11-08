//! Rings each side's doorbell: a futex wake for the peer, and for the caller a byte into a pipe whose read end the
//! caller's reactor watches, since the caller's thread sleeps in its reactor and not on a word.

use std::convert::Infallible;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

use rustix::io::Errno;

use crate::os::{self, Syscall, SyscallError};
use crate::protocol::doorbell::Doorbell;
use crate::sync::AtomicU32;

/// Wakes a peer sleeping on a futex. The wake cannot fail, and a peer that sleeps on nothing loses nothing by it.
pub struct FutexDoorbell;

impl Doorbell for FutexDoorbell {
    type Outcome = ();
    type Error = Infallible;

    fn ring(&self, word: &AtomicU32) -> Result<(), Infallible> {
        os::wake(word);
        Ok(())
    }
}

/// Wakes the caller: the peer's write end of the caller's wake pipe.
pub struct PipeDoorbell {
    fd: OwnedFd,
}

impl PipeDoorbell {
    pub fn new(fd: OwnedFd) -> Self {
        PipeDoorbell { fd }
    }
}

/// What a byte into the caller's wake pipe found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequesterBellOutcome {
    /// The byte is in the pipe, or the pipe is full and wakes the caller already.
    Rung,
    /// No one holds the read end: the caller is gone.
    RequesterGone,
}

impl Doorbell for PipeDoorbell {
    type Outcome = RequesterBellOutcome;
    type Error = SyscallError;

    fn ring(&self, _: &AtomicU32) -> Result<RequesterBellOutcome, SyscallError> {
        bell_outcome(os::write_byte(self.fd.as_fd())).map_err(|errno| SyscallError {
            call: Syscall::Write,
            errno,
        })
    }
}

/// Sorts what writing the wake byte returned: a byte written or a full pipe rang, EPIPE found the caller gone, and any
/// other errno is the pipe's.
pub fn bell_outcome(written: Result<usize, Errno>) -> Result<RequesterBellOutcome, Errno> {
    match written {
        Ok(_) | Err(Errno::AGAIN) => Ok(RequesterBellOutcome::Rung),
        Err(Errno::PIPE) => Ok(RequesterBellOutcome::RequesterGone),
        Err(errno) => Err(errno),
    }
}

/// The caller's end of its wake pipe, which the caller registers in its own reactor.
pub struct RequesterBell {
    fd: OwnedFd,
}

impl AsFd for RequesterBell {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for RequesterBell {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// Opens a caller wake pipe: the caller's read end and the write end for the peer. Both are nonblocking and
/// close-on-exec; the fd travels to the peer by `SCM_RIGHTS`, which needs no inheritable fd.
pub fn open_requester_doorbell() -> Result<(RequesterBell, OwnedFd), SyscallError> {
    let (read, write) = os::pipe()?;
    Ok((RequesterBell { fd: read }, write))
}

/// What one read of the caller's wake pipe left in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    Empty,
    MoreLeft,
}

/// One read of the caller's wake pipe: the bytes it took, each a wake some peer wrote, and what it left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WakeRead {
    pub bytes: usize,
    pub outcome: DrainOutcome,
}

const BELL_READ_BYTES: usize = 256;

/// Reads the caller's wake pipe once, so a peer that keeps writing cannot hold the caller's thread in a loop: whatever
/// it leaves is read on a later round.
///
/// # Errors
///
/// When the pipe cannot be read; reading it again would fail the same way.
pub fn drain_once(bell: &RequesterBell) -> Result<WakeRead, SyscallError> {
    let mut buf = [0u8; BELL_READ_BYTES];
    drain_outcome(os::read(bell.as_fd(), &mut buf), BELL_READ_BYTES).map_err(|errno| SyscallError {
        call: Syscall::Read,
        errno,
    })
}

/// Says what a read of a wake pipe into a buffer of `cap` bytes took and whether the pipe may hold more after it
/// returned `read`. A full read or an interrupted one may have left bytes; a short read, end of file and EAGAIN leave
/// none. Any other errno is the pipe's and goes back to whoever read the pipe, which alone can decide what a caller whose
/// wake pipe cannot be read does.
pub fn drain_outcome(read: Result<usize, Errno>, cap: usize) -> Result<WakeRead, Errno> {
    match read {
        Ok(n) if n == cap => Ok(WakeRead {
            bytes: n,
            outcome: DrainOutcome::MoreLeft,
        }),
        Err(Errno::INTR) => Ok(WakeRead {
            bytes: 0,
            outcome: DrainOutcome::MoreLeft,
        }),
        Ok(n) => Ok(WakeRead {
            bytes: n,
            outcome: DrainOutcome::Empty,
        }),
        Err(Errno::AGAIN) => Ok(WakeRead {
            bytes: 0,
            outcome: DrainOutcome::Empty,
        }),
        Err(errno) => Err(errno),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_byte_written_or_a_full_pipe_rings_a_gone_reader_is_the_caller_gone_and_the_rest_fails() {
        assert_eq!(bell_outcome(Ok(1)), Ok(RequesterBellOutcome::Rung));
        assert_eq!(bell_outcome(Err(Errno::AGAIN)), Ok(RequesterBellOutcome::Rung));
        assert_eq!(bell_outcome(Err(Errno::PIPE)), Ok(RequesterBellOutcome::RequesterGone));
        assert_eq!(bell_outcome(Err(Errno::BADF)), Err(Errno::BADF));
    }

    /// A read that filled the buffer, or was interrupted, may have left bytes; one byte short of full, EAGAIN and end
    /// of file leave none; any other error is the pipe's and comes back.
    #[test]
    fn a_wake_read_says_more_is_left_only_when_it_filled_the_buffer_or_was_interrupted() {
        let cap = BELL_READ_BYTES;
        let drain = |bytes, outcome| Ok(WakeRead { bytes, outcome });
        assert_eq!(drain_outcome(Ok(cap), cap), drain(cap, DrainOutcome::MoreLeft));
        assert_eq!(drain_outcome(Ok(cap - 1), cap), drain(cap - 1, DrainOutcome::Empty));
        assert_eq!(drain_outcome(Ok(0), cap), drain(0, DrainOutcome::Empty));
        assert_eq!(drain_outcome(Ok(1), 1), drain(1, DrainOutcome::MoreLeft));
        assert_eq!(drain_outcome(Err(Errno::INTR), cap), drain(0, DrainOutcome::MoreLeft));
        assert_eq!(drain_outcome(Err(Errno::AGAIN), cap), drain(0, DrainOutcome::Empty));
        assert_eq!(drain_outcome(Err(Errno::BADF), cap), Err(Errno::BADF));
        assert_eq!(drain_outcome(Err(Errno::IO), cap), Err(Errno::IO));
    }

    // Miri runs no system call.
    #[cfg(not(miri))]
    mod pipe {
        use super::super::*;

        #[test]
        fn both_ends_of_a_caller_doorbell_are_close_on_exec() {
            let (bell, write) = open_requester_doorbell().unwrap();
            for end in [bell.as_fd(), write.as_fd()] {
                assert!(rustix::io::fcntl_getfd(end)
                    .unwrap()
                    .contains(rustix::io::FdFlags::CLOEXEC));
            }
        }

        #[test]
        fn an_empty_doorbell_drains_empty_and_a_full_one_still_rings() {
            let (bell, write) = open_requester_doorbell().unwrap();
            assert_eq!(
                drain_once(&bell),
                Ok(WakeRead {
                    bytes: 0,
                    outcome: DrainOutcome::Empty
                })
            );
            let ringer = PipeDoorbell::new(write);
            let word = AtomicU32::new(0);
            while rustix::io::write(&ringer.fd, &[1u8; 4096]).is_ok() {}
            while os::write_byte(ringer.fd.as_fd()).is_ok() {}
            assert_eq!(os::write_byte(ringer.fd.as_fd()), Err(Errno::AGAIN));
            assert_eq!(ringer.ring(&word), Ok(RequesterBellOutcome::Rung));
            assert_eq!(
                drain_once(&bell),
                Ok(WakeRead {
                    bytes: BELL_READ_BYTES,
                    outcome: DrainOutcome::MoreLeft
                })
            );
        }

        #[test]
        fn a_doorbell_whose_caller_closed_its_end_finds_the_caller_gone() {
            let (bell, write) = open_requester_doorbell().unwrap();
            drop(bell);
            assert_eq!(
                PipeDoorbell::new(write).ring(&AtomicU32::new(0)),
                Ok(RequesterBellOutcome::RequesterGone)
            );
        }

        /// One drain of a pipe holding more than one read's worth takes one read's worth and says more is left,
        /// however much the peer wrote.
        #[test]
        fn a_drain_reads_once_however_much_was_written() {
            let (bell, write) = open_requester_doorbell().unwrap();
            assert_eq!(
                rustix::io::write(&write, &[1u8; 3 * BELL_READ_BYTES]),
                Ok(3 * BELL_READ_BYTES)
            );
            for _ in 0..3 {
                assert_eq!(
                    drain_once(&bell),
                    Ok(WakeRead {
                        bytes: BELL_READ_BYTES,
                        outcome: DrainOutcome::MoreLeft
                    })
                );
            }
            assert_eq!(
                drain_once(&bell),
                Ok(WakeRead {
                    bytes: 0,
                    outcome: DrainOutcome::Empty
                })
            );
            drop(write);
            assert_eq!(
                drain_once(&bell),
                Ok(WakeRead {
                    bytes: 0,
                    outcome: DrainOutcome::Empty
                })
            );
        }

        #[test]
        fn a_doorbell_end_that_cannot_be_read_returns_the_read_and_its_errno() {
            let (_bell, write) = open_requester_doorbell().unwrap();
            let not_a_bell = RequesterBell { fd: write };
            assert_eq!(
                drain_once(&not_a_bell),
                Err(SyscallError {
                    call: Syscall::Read,
                    errno: Errno::BADF
                })
            );
        }
    }
}
