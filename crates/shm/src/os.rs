//! Makes the channel's system calls: the shared memory object and its mapping, the address wait and wake, and the
//! host's wake pipe. It is the crate's one file with platform `#[cfg]`s and its only caller of the OS; each failure
//! goes back as the call that failed and the errno it gave, without a log line.

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::ptr::NonNull;

use rustix::io::Errno;

use crate::sync::AtomicU32;

/// A system call the channel makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Syscall {
    MemfdCreate,
    ShmOpen,
    ShmUnlink,
    Ftruncate,
    AddSeals,
    Fstat,
    Mmap,
    Madvise,
    Minherit,
    Pipe,
    Fcntl,
    Read,
    Write,
    WaitOnAddress,
}

impl Syscall {
    /// Returns the call's name as the OS's manual pages give it.
    pub const fn name(self) -> &'static str {
        match self {
            Syscall::MemfdCreate => "memfd_create",
            Syscall::ShmOpen => "shm_open",
            Syscall::ShmUnlink => "shm_unlink",
            Syscall::Ftruncate => "ftruncate",
            Syscall::AddSeals => "fcntl(F_ADD_SEALS)",
            Syscall::Fstat => "fstat",
            Syscall::Mmap => "mmap",
            Syscall::Madvise => "madvise(MADV_DONTFORK)",
            Syscall::Minherit => "minherit(VM_INHERIT_NONE)",
            Syscall::Pipe => "pipe",
            Syscall::Fcntl => "fcntl",
            Syscall::Read => "read",
            Syscall::Write => "write",
            Syscall::WaitOnAddress => WAIT_CALL,
        }
    }
}

#[cfg(target_os = "linux")]
const WAIT_CALL: &str = "futex wait";
#[cfg(target_os = "macos")]
const WAIT_CALL: &str = "os_sync_wait_on_address_with_timeout";

/// A system call the channel made failed with this errno.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{}: {errno}", call.name())]
pub struct SyscallError {
    pub call: Syscall,
    pub errno: Errno,
}

fn failed(call: Syscall) -> impl FnOnce(Errno) -> SyscallError {
    move |errno| SyscallError { call, errno }
}

/// Creates a shared memory object of `bytes` zeroed bytes whose size is fixed for good, close-on-exec. Every peer
/// holds the fd and runs user C extensions; one that shrank the object would make the host's next touch of a cut
/// page a SIGBUS that ends the host, so on Linux the size is sealed before the fd leaves.
#[cfg(target_os = "linux")]
pub(crate) fn shared_memory(bytes: u64) -> Result<OwnedFd, SyscallError> {
    use rustix::fs::{MemfdFlags, SealFlags};
    let fd = rustix::fs::memfd_create(c"mipyre-shm", MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING)
        .map_err(failed(Syscall::MemfdCreate))?;
    rustix::fs::ftruncate(&fd, bytes).map_err(failed(Syscall::Ftruncate))?;
    rustix::fs::fcntl_add_seals(&fd, SealFlags::SHRINK | SealFlags::GROW | SealFlags::SEAL)
        .map_err(failed(Syscall::AddSeals))?;
    Ok(fd)
}

/// Creates a shared memory object of `bytes` zeroed bytes whose size is fixed for good (macOS sizes a POSIX shared
/// memory object once and refuses every later `ftruncate`), close-on-exec, with no name left behind.
///
/// A name in use by another creation of this process, or left by a process that had this pid, is passed over for
/// the next attempt's name.
#[cfg(target_os = "macos")]
pub(crate) fn shared_memory(bytes: u64) -> Result<OwnedFd, SyscallError> {
    use rustix::fs::Mode;
    use rustix::shm::OFlags;
    let pid = std::process::id();
    let mut attempt = 0u32;
    let (fd, name) = loop {
        let name = shm_name(pid, attempt);
        match rustix::shm::open(
            name.as_str(),
            OFlags::CREATE | OFlags::EXCL | OFlags::RDWR,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(fd) => break (fd, name),
            Err(Errno::EXIST) if attempt < u32::MAX => attempt += 1,
            Err(errno) => {
                return Err(SyscallError {
                    call: Syscall::ShmOpen,
                    errno,
                })
            }
        }
    };
    // The object lives on through the fds and nothing else should find it by name: a name left behind would outlive
    // the process, so failing to remove it fails the creation.
    rustix::shm::unlink(name.as_str()).map_err(failed(Syscall::ShmUnlink))?;
    rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC).map_err(failed(Syscall::Fcntl))?;
    rustix::fs::ftruncate(&fd, bytes).map_err(failed(Syscall::Ftruncate))?;
    Ok(fd)
}

/// Returns the name of a shared memory object for creation `attempt` of process `pid`; macOS allows 31 bytes.
#[cfg(any(target_os = "macos", test))]
fn shm_name(pid: u32, attempt: u32) -> String {
    format!("/mshm.{pid}.{attempt}")
}

/// Returns the size the OS records for the object `fd`.
pub(crate) fn file_bytes(fd: BorrowedFd<'_>) -> Result<i64, SyscallError> {
    Ok(rustix::fs::fstat(fd).map_err(failed(Syscall::Fstat))?.st_size)
}

/// A shared mapping of a shared memory object, readable and writable, that no child of this process inherits;
/// unmapped on drop.
pub(crate) struct Mapping {
    base: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is a range of addresses; who reads and writes the bytes in it, and how, is its user's to say.
unsafe impl Send for Mapping {}
// SAFETY: as for `Send`: a shared `Mapping` gives out only its base address, never a reference into the bytes.
unsafe impl Sync for Mapping {}

impl Mapping {
    /// Maps the first `len` bytes of the object `fd`, shared with every process that maps it, at an address the
    /// kernel picks; the mapping keeps the object alive after `fd` closes. A process this one forks gets no mapping
    /// at that address: the children of either end run code neither end trusts, and one that kept the mapping could
    /// write into the channel after the end that forked it is gone.
    pub(crate) fn new(fd: BorrowedFd<'_>, len: usize) -> Result<Self, SyscallError> {
        use rustix::mm::{MapFlags, ProtFlags};
        // SAFETY: a NULL hint lets the kernel pick an address no mapping of this process uses, so no memory Rust knows
        // of changes.
        let at = unsafe {
            rustix::mm::mmap(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                fd.as_fd(),
                0,
            )
        }
        .map_err(failed(Syscall::Mmap))?;
        let Some(base) = NonNull::new(at.cast::<u8>()) else {
            return Err(SyscallError {
                call: Syscall::Mmap,
                errno: Errno::INVAL,
            });
        };
        let mapping = Mapping { base, len };
        mapping.keep_from_children()?;
        Ok(mapping)
    }

    #[cfg(target_os = "linux")]
    fn keep_from_children(&self) -> Result<(), SyscallError> {
        // SAFETY: the range is this mapping, whole; MADV_DONTFORK changes only what a fork copies, not its contents.
        unsafe { rustix::mm::madvise(self.base.as_ptr().cast(), self.len, rustix::mm::Advice::LinuxDontFork) }
            .map_err(failed(Syscall::Madvise))
    }

    #[cfg(target_os = "macos")]
    fn keep_from_children(&self) -> Result<(), SyscallError> {
        use std::ffi::{c_int, c_void};
        const VM_INHERIT_NONE: c_int = 2;
        extern "C" {
            fn minherit(addr: *mut c_void, len: usize, inherit: c_int) -> c_int;
        }
        // SAFETY: the range is this mapping, whole; VM_INHERIT_NONE changes only what a fork copies, not its contents.
        if unsafe { minherit(self.base.as_ptr().cast(), self.len, VM_INHERIT_NONE) } == 0 {
            return Ok(());
        }
        // SAFETY: `__error` returns this thread's errno slot, which lives as long as the thread.
        Err(SyscallError {
            call: Syscall::Minherit,
            errno: Errno::from_raw_os_error(unsafe { *libc::__error() }),
        })
    }

    pub(crate) fn base(&self) -> NonNull<u8> {
        self.base
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the whole of this mapping, made by `new` and not unmapped since; nothing borrows it past its owner.
        let unmapped = unsafe { rustix::mm::munmap(self.base.as_ptr().cast(), self.len) };
        // Unmapping a whole mapping of one's own cannot fail: no part is split off (no ENOMEM) and the arguments are
        // the ones `mmap` returned (no EINVAL).
        debug_assert!(unmapped.is_ok(), "munmap: {unmapped:?}");
    }
}

/// Sleeps while `a` holds `expect`, until a [`wake`] on it, `timeout_ns` passes or nothing in particular: every `Ok`
/// means "look at `a` again". It works across processes that share the mapping `a` is in.
///
/// # Errors
///
/// When the OS refuses the wait; waiting again would return at once.
#[cfg(target_os = "macos")]
pub(crate) fn wait(a: &AtomicU32, expect: u32, timeout_ns: u64) -> Result<(), SyscallError> {
    use std::ffi::{c_int, c_void};
    const OS_SYNC_WAIT_ON_ADDRESS_SHARED: u32 = 1;
    const OS_CLOCK_MACH_ABSOLUTE_TIME: u32 = 32;
    extern "C" {
        fn os_sync_wait_on_address_with_timeout(
            addr: *mut c_void,
            value: u64,
            size: usize,
            flags: u32,
            clockid: u32,
            timeout_ns: u64,
        ) -> c_int;
    }
    // SAFETY: `a` is a live, 4-byte aligned 4-byte atomic for the call, and `size` is 4.
    let rc = unsafe {
        os_sync_wait_on_address_with_timeout(
            a.as_ptr().cast::<c_void>(),
            u64::from(expect),
            4,
            OS_SYNC_WAIT_ON_ADDRESS_SHARED,
            OS_CLOCK_MACH_ABSOLUTE_TIME,
            timeout_ns,
        )
    };
    let ended = if rc < 0 {
        // SAFETY: `__error` returns this thread's errno slot, which lives as long as the thread.
        Err(Errno::from_raw_os_error(unsafe { *libc::__error() }))
    } else {
        Ok(())
    };
    wait_outcome(ended)
}

/// Wakes every [`wait`] on `a`, in any process.
#[cfg(target_os = "macos")]
pub(crate) fn wake(a: &AtomicU32) {
    use std::ffi::{c_int, c_void};
    const OS_SYNC_WAKE_BY_ADDRESS_SHARED: u32 = 1;
    extern "C" {
        fn os_sync_wake_by_address_all(addr: *mut c_void, size: usize, flags: u32) -> c_int;
    }
    // SAFETY: `a` is a live, 4-byte aligned 4-byte atomic for the call, and `size` is 4. The result is not read: its
    // one failure, ENOENT, says no one was waiting.
    unsafe { os_sync_wake_by_address_all(a.as_ptr().cast::<c_void>(), 4, OS_SYNC_WAKE_BY_ADDRESS_SHARED) };
}

/// Sleeps while `a` holds `expect` (see the macOS one); FUTEX_WAIT without the private flag, so it works across
/// processes sharing the mapping.
///
/// # Errors
///
/// When the OS refuses the wait; waiting again would return at once.
#[cfg(target_os = "linux")]
pub(crate) fn wait(a: &AtomicU32, expect: u32, timeout_ns: u64) -> Result<(), SyscallError> {
    use rustix::thread::futex;
    let timeout = rustix::time::Timespec {
        tv_sec: (timeout_ns / 1_000_000_000) as i64,
        tv_nsec: (timeout_ns % 1_000_000_000) as _,
    };
    wait_outcome(futex::wait(a, futex::Flags::empty(), expect, Some(&timeout)))
}

/// Wakes every [`wait`] on `a`, in any process.
#[cfg(target_os = "linux")]
pub(crate) fn wake(a: &AtomicU32) {
    use rustix::thread::futex;
    // How many woke is not needed, and it cannot fail: the type rules out EFAULT and EINVAL.
    let woken = futex::wake(a, futex::Flags::empty(), i32::MAX as u32);
    debug_assert!(woken.is_ok(), "futex wake: {woken:?}");
}

/// Sorts how an address wait ended: a wake, a value that had changed (EAGAIN), a timeout and an interrupt all send
/// the caller back to look at the address; anything else is the OS refusing.
fn wait_outcome(ended: Result<(), Errno>) -> Result<(), SyscallError> {
    match ended {
        Ok(()) | Err(Errno::AGAIN | Errno::TIMEDOUT | Errno::INTR) => Ok(()),
        Err(errno) => Err(SyscallError {
            call: Syscall::WaitOnAddress,
            errno,
        }),
    }
}

/// Makes a pipe whose two ends are nonblocking and close-on-exec: `(read, write)`.
#[cfg(target_os = "linux")]
pub(crate) fn pipe() -> Result<(OwnedFd, OwnedFd), SyscallError> {
    use rustix::pipe::PipeFlags;
    rustix::pipe::pipe_with(PipeFlags::NONBLOCK | PipeFlags::CLOEXEC).map_err(failed(Syscall::Pipe))
}

/// Makes a pipe whose two ends are nonblocking and close-on-exec: `(read, write)`. macOS has no `pipe2`, so the
/// flags are set after; no other thread of a process that starts others with this pipe open may fork in between.
#[cfg(target_os = "macos")]
pub(crate) fn pipe() -> Result<(OwnedFd, OwnedFd), SyscallError> {
    let (read, write) = rustix::pipe::pipe().map_err(failed(Syscall::Pipe))?;
    for end in [&read, &write] {
        rustix::io::fcntl_setfd(end, rustix::io::FdFlags::CLOEXEC).map_err(failed(Syscall::Fcntl))?;
        let flags = rustix::fs::fcntl_getfl(end).map_err(failed(Syscall::Fcntl))?;
        rustix::fs::fcntl_setfl(end, flags | rustix::fs::OFlags::NONBLOCK).map_err(failed(Syscall::Fcntl))?;
    }
    Ok((read, write))
}

/// Reads what `fd` holds, up to `buf`'s length, and returns the OS's result as it is.
pub(crate) fn read(fd: BorrowedFd<'_>, buf: &mut [u8]) -> Result<usize, Errno> {
    rustix::io::read(fd, buf)
}

/// Writes one byte to `fd` and returns the OS's result as it is. SIGPIPE is the caller's to ignore (Python ignores
/// it at start-up), so a reader that is gone shows as EPIPE.
pub(crate) fn write_byte(fd: BorrowedFd<'_>) -> Result<usize, Errno> {
    rustix::io::write(fd, &[1u8])
}

// Miri runs no system call.
#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test]
    fn a_wake_a_changed_value_a_timeout_or_an_interrupt_is_a_wait_that_ended_and_the_rest_a_refusal() {
        for ended in [Ok(()), Err(Errno::AGAIN), Err(Errno::TIMEDOUT), Err(Errno::INTR)] {
            assert_eq!(wait_outcome(ended), Ok(()), "{ended:?}");
        }
        for errno in [Errno::INVAL, Errno::NOMEM, Errno::FAULT] {
            assert_eq!(
                wait_outcome(Err(errno)),
                Err(SyscallError {
                    call: Syscall::WaitOnAddress,
                    errno
                })
            );
        }
    }

    #[test]
    fn a_wait_on_a_value_already_changed_returns_at_once() {
        let a = AtomicU32::new(1);
        assert_eq!(wait(&a, 0, 60_000_000_000), Ok(()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_wait_the_os_refuses_is_reported_with_its_errno() {
        let a = AtomicU32::new(0);
        let refused = wait(&a, 0, 0).expect_err("macOS refuses a zero timeout");
        assert_eq!(
            refused,
            SyscallError {
                call: Syscall::WaitOnAddress,
                errno: Errno::INVAL
            }
        );
        assert_eq!(
            refused.to_string(),
            format!("os_sync_wait_on_address_with_timeout: {}", Errno::INVAL)
        );
    }

    #[test]
    fn creations_of_one_process_get_different_names() {
        assert_ne!(shm_name(42, 0), shm_name(42, 1));
        assert_ne!(shm_name(42, 0), shm_name(43, 0));
        assert!(shm_name(u32::MAX, u32::MAX).len() <= 31);
    }

    #[test]
    fn both_ends_of_a_pipe_are_close_on_exec() {
        let (read, write) = pipe().unwrap();
        for end in [&read, &write] {
            assert!(rustix::io::fcntl_getfd(end)
                .unwrap()
                .contains(rustix::io::FdFlags::CLOEXEC));
        }
    }

    /// Runs the shrink a peer could attempt from a real second process holding the fd the host hands out, then
    /// touches the last byte of the mapping: a shrink that went through would end this test process with SIGBUS.
    #[test]
    fn a_peer_process_cannot_shrink_the_shared_memory_under_the_host() {
        use std::os::fd::AsRawFd;
        let bytes = 1 << 16;
        let fd = shared_memory(bytes as u64).unwrap();
        let mapping = Mapping::new(fd.as_fd(), bytes).unwrap();
        let raw = fd.as_raw_fd();
        // SAFETY: the child calls only `ftruncate` and `_exit`, which are async-signal-safe, so no lock another test
        // thread held at the fork can block it.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
        if pid == 0 {
            // SAFETY: as above; `raw` is open in the child, which inherited it.
            unsafe {
                let shrunk = libc::ftruncate(raw, 4096) == 0;
                libc::_exit(i32::from(shrunk));
            }
        }
        let mut status = 0;
        // SAFETY: `status` is a writable int and `pid` is this process's child.
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(libc::WIFEXITED(status), "the peer ended with wait status {status}");
        assert_eq!(libc::WEXITSTATUS(status), 0, "the peer shrank the shared memory");
        assert_eq!(file_bytes(fd.as_fd()).unwrap(), bytes as i64);
        // SAFETY: the last byte lies inside the mapping, and no other thread uses it.
        let last = unsafe { mapping.base().add(bytes - 1).read_volatile() };
        assert_eq!(last, 0);
    }
}
