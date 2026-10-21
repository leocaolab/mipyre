//! The process lanes' system calls that rustix has no safe call for: the shared mapping, the
//! macOS address wait, the macOS process-exit queue, `posix_spawn`, `strsignal` and `_exit`.
//! Each is a safe function whose one `unsafe` block says why the call holds. The wake pipe's
//! read is here too, so the doorbell's system calls are in one file.

use std::io;
use std::os::fd::BorrowedFd;
#[cfg(target_os = "macos")]
use std::os::fd::{AsFd, AsRawFd, OwnedFd, RawFd};
use std::sync::atomic::AtomicU32;

use pyo3::Python;

/// Says which side of a start the caller is on; only a fork returns `Child`.
pub(super) enum StartedProcess {
    Parent { pid: libc::pid_t },
    Child,
}

impl StartedProcess {
    /// Returns what `os.fork()` would, because the Python launcher reads it that way.
    pub(super) fn fork_answer(self) -> libc::pid_t {
        match self {
            StartedProcess::Parent { pid } => pid,
            StartedProcess::Child => 0,
        }
    }
}

/// Forks this Python process as `os.fork()` does, at-fork callbacks included, but without its
/// multi-thread check (the caller says why). The caller is the process's only thread: a child
/// forked beside another thread inherits whatever locks that thread held.
pub(super) fn fork_python(_py: Python<'_>) -> io::Result<StartedProcess> {
    // SAFETY: attached (`_py`), as the three hooks require. They bracket `fork` exactly as
    // `os.fork()` does: before it, then the child's or the parent's after it, so CPython's
    // locks and thread state are consistent on both sides. The callers fork only while this
    // process has one thread (the launcher and the template are single-threaded, see
    // `spawn_python`), which is the condition the skipped check warns about. errno is read
    // before `PyOS_AfterFork_Parent`, which may change it.
    unsafe {
        pyo3::ffi::PyOS_BeforeFork();
        let pid = libc::fork();
        if pid == 0 {
            pyo3::ffi::PyOS_AfterFork_Child();
            return Ok(StartedProcess::Child);
        }
        let err = io::Error::last_os_error();
        pyo3::ffi::PyOS_AfterFork_Parent();
        if pid < 0 {
            return Err(err);
        }
        Ok(StartedProcess::Parent { pid })
    }
}

/// Clears close-on-exec on `fd`, so a process this one starts inherits it.
///
/// # Errors
///
/// `EBADF` when `fd` is not open.
#[cfg(target_os = "macos")]
pub(super) fn keep_across_exec(fd: RawFd) -> io::Result<()> {
    // SAFETY: F_GETFD and F_SETFD take an int, read or set one flag in this process's
    // descriptor table and touch no memory; a number that is no open fd fails with EBADF.
    let set = unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        flags >= 0 && libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) >= 0
    };
    if !set {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Maps `len` bytes of the shared memory object `fd`, readable and writable, shared with every
/// process that maps it, at an address the kernel picks.
///
/// The mapping is never unmapped: it lives as long as the process.
pub(super) fn map_shared(fd: BorrowedFd<'_>, len: usize) -> io::Result<*mut u8> {
    // SAFETY: a NULL hint lets the kernel pick an address no mapping of this process uses, so
    // no memory Rust knows of changes; `fd` is open for the call.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            std::os::fd::AsRawFd::as_raw_fd(&fd),
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(p.cast::<u8>())
}

/// Sleeps while `a` holds `expect`, until a [`wake`] on it, `timeout_ns` passes, or nothing in
/// particular: every `Ok` means "look at `a` again". Works across processes that share the
/// mapping `a` is in.
///
/// # Errors
///
/// [`WaitError`] when the OS refuses the wait; waiting again would return at once.
#[cfg(target_os = "macos")]
pub(super) fn wait(a: &AtomicU32, expect: u32, timeout_ns: u64) -> Result<(), WaitError> {
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
        Err(rustix::io::Errno::from_raw_os_error(unsafe { *libc::__error() }))
    } else {
        Ok(())
    };
    wait_outcome(ended)
}

/// Wakes every [`wait`] on `a`, in any process.
#[cfg(target_os = "macos")]
pub(super) fn wake(a: &AtomicU32) {
    use std::ffi::{c_int, c_void};
    const OS_SYNC_WAKE_BY_ADDRESS_SHARED: u32 = 1;
    extern "C" {
        fn os_sync_wake_by_address_all(addr: *mut c_void, size: usize, flags: u32) -> c_int;
    }
    // SAFETY: `a` is a live, 4-byte aligned 4-byte atomic for the call, and `size` is 4. The
    // result is not read: its one failure, ENOENT, says no one was waiting.
    unsafe { os_sync_wake_by_address_all(a.as_ptr().cast::<c_void>(), 4, OS_SYNC_WAKE_BY_ADDRESS_SHARED) };
}

/// Sleeps while `a` holds `expect` (see the macOS one); FUTEX_WAIT without the private flag, so
/// it works across processes sharing the mapping.
///
/// # Errors
///
/// [`WaitError`] when the OS refuses the wait; waiting again would return at once.
#[cfg(target_os = "linux")]
pub(super) fn wait(a: &AtomicU32, expect: u32, timeout_ns: u64) -> Result<(), WaitError> {
    use rustix::thread::futex;
    let timeout = rustix::time::Timespec {
        tv_sec: (timeout_ns / 1_000_000_000) as i64,
        tv_nsec: (timeout_ns % 1_000_000_000) as _,
    };
    wait_outcome(futex::wait(a, futex::Flags::empty(), expect, Some(&timeout)))
}

#[cfg(target_os = "linux")]
const WAIT_CALL: &str = "futex wait";
#[cfg(target_os = "macos")]
const WAIT_CALL: &str = "os_sync_wait_on_address_with_timeout";

/// Reports that the OS refused an address wait.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {errno}", WAIT_CALL)]
pub(super) struct WaitError {
    errno: rustix::io::Errno,
}

/// Sorts how an address wait ended: a wake, a value that had changed (EAGAIN), a timeout and an
/// interrupt all send the caller back to look at the address; anything else is the OS refusing.
fn wait_outcome(ended: Result<(), rustix::io::Errno>) -> Result<(), WaitError> {
    use rustix::io::Errno;
    match ended {
        Ok(()) | Err(Errno::AGAIN | Errno::TIMEDOUT | Errno::INTR) => Ok(()),
        Err(errno) => Err(WaitError { errno }),
    }
}

/// Wakes every [`wait`] on `a`, in any process.
#[cfg(target_os = "linux")]
pub(super) fn wake(a: &AtomicU32) {
    use rustix::thread::futex;
    // How many woke is not needed, and it cannot fail: the type rules out EFAULT and EINVAL.
    let woken = futex::wake(a, futex::Flags::empty(), i32::MAX as u32);
    debug_assert!(woken.is_ok(), "futex wake: {woken:?}");
}

/// Reads what a wake pipe holds, up to `buf`'s length, and returns the OS's result as it is.
pub(super) fn read_bell(fd: BorrowedFd<'_>, buf: &mut [u8]) -> Result<usize, rustix::io::Errno> {
    rustix::io::read(fd, buf)
}

/// A kqueue that holds only process-exit registrations, so no event in it names a descriptor:
/// that is what makes its `kevent` calls sound.
#[cfg(target_os = "macos")]
pub(super) struct ExitQueue(OwnedFd);

#[cfg(target_os = "macos")]
impl ExitQueue {
    pub(super) fn new() -> io::Result<Self> {
        Ok(ExitQueue(rustix::event::kqueue::kqueue()?))
    }

    /// Reports `pid`'s exit once, tagged with `tag`.
    ///
    /// # Errors
    ///
    /// `ESRCH` when `pid` is already gone.
    pub(super) fn add(&self, pid: rustix::process::Pid, tag: usize) -> io::Result<()> {
        use rustix::event::kqueue::{Event, EventFilter, EventFlags, ProcessEvents};
        let change = Event::new(
            EventFilter::Proc { pid, flags: ProcessEvents::EXIT },
            EventFlags::ADD | EventFlags::ONESHOT,
            tag as *mut std::ffi::c_void,
        );
        // No room for events: kevent only registers the change, and reports a failed one (ESRCH
        // for a pid already gone) as its error.
        let mut none: Vec<Event> = Vec::new();
        // SAFETY: rustix asks that every descriptor an event names stay open as long as the
        // kqueue; a process-exit event names a pid, not a descriptor, and it is the only kind
        // this queue holds.
        unsafe { rustix::event::kqueue::kevent(&self.0, &[change], &mut none, None)? };
        Ok(())
    }

    /// Hands `each` up to `ROOM` exits as `(pid, tag)`, waiting at most `timeout` (`None`:
    /// until one comes), and returns how many there were. An interrupted wait is made again.
    /// The events are read into the stack: the lane poller asks every round.
    pub(super) fn exited<const ROOM: usize>(
        &self,
        timeout: Option<std::time::Duration>,
        mut each: impl FnMut(rustix::process::Pid, usize) -> io::Result<()>,
    ) -> io::Result<usize> {
        use rustix::event::kqueue::{Event, EventFilter, ProcessEvents};
        let mut room = [const { std::mem::MaybeUninit::<Event>::uninit() }; ROOM];
        loop {
            // SAFETY: as in `add`: no event in this queue names a descriptor.
            let events: &[Event] =
                match unsafe { rustix::event::kqueue::kevent(&self.0, &[], &mut room, timeout) } {
                    Ok((events, _)) => events,
                    Err(rustix::io::Errno::INTR) => continue,
                    Err(e) => return Err(e.into()),
                };
            for ev in events {
                match ev.filter() {
                    EventFilter::Proc { pid, flags } if flags.contains(ProcessEvents::EXIT) => {
                        each(pid, ev.udata() as usize)?;
                    }
                    _ => {
                        return Err(io::Error::other(format!(
                            "the exit queue reported an event other than a process exit (flags {:?})",
                            ev.flags()
                        )))
                    }
                }
            }
            return Ok(events.len());
        }
    }
}

#[cfg(target_os = "macos")]
impl AsFd for ExitQueue {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

#[cfg(target_os = "macos")]
impl AsRawFd for ExitQueue {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

/// Starts `argv[0]` with arguments `argv` and environment `envp`; it inherits every descriptor
/// without close-on-exec. Returns its pid.
#[cfg(target_os = "macos")]
pub(super) fn spawn(argv: &[std::ffi::CString], envp: &[std::ffi::CString]) -> io::Result<i32> {
    let program = argv
        .first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "posix_spawn needs a program"))?;
    let null_ended = |strings: &[std::ffi::CString]| {
        strings
            .iter()
            .map(|c| c.as_ptr().cast_mut())
            .chain(std::iter::once(std::ptr::null_mut()))
            .collect::<Vec<*mut libc::c_char>>()
    };
    let (argv_p, envp_p) = (null_ended(argv), null_ended(envp));
    let mut pid: libc::pid_t = 0;
    // SAFETY: `program` is NUL-terminated; `argv_p` and `envp_p` are NULL-ended arrays of
    // NUL-terminated strings, all borrowed from `argv` / `envp` and alive for the call; `pid` is
    // a writable out-value; NULL file actions and attributes ask for the defaults.
    let rc = unsafe {
        libc::posix_spawn(
            &mut pid,
            program.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            argv_p.as_ptr(),
            envp_p.as_ptr(),
        )
    };
    // posix_spawn returns the errno instead of setting it.
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    Ok(pid)
}

/// Returns the OS's name for `signal`: `Killed` for SIGKILL on Linux, `Killed: 9` on macOS.
pub(super) fn signal_name(signal: i32) -> String {
    // SAFETY: `strsignal` takes any int; it returns NULL or a NUL-terminated string that stays
    // valid until the next call on this thread, and it is copied before this returns.
    let text = unsafe { libc::strsignal(signal) };
    if text.is_null() {
        return format!("unknown signal {signal}");
    }
    // SAFETY: not NULL (above), NUL-terminated and unchanged until this thread's next
    // `strsignal` (its contract).
    unsafe { std::ffi::CStr::from_ptr(text) }
        .to_string_lossy()
        .into_owned()
}

/// Returns the pid of the parent of process `pid` as the kernel records it, or `None` when no
/// process has that pid. A zombie not yet reaped still has its parent.
#[cfg(target_os = "linux")]
pub(super) fn parent_of(pid: i32) -> io::Result<Option<i32>> {
    let path = format!("/proc/{pid}/stat");
    let stat = match std::fs::read_to_string(&path) {
        Ok(stat) => stat,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    parent_in_stat(&stat).map(Some).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, format!("{path} has no parent pid: {stat:?}"))
    })
}

/// Reads the parent pid from a `/proc/<pid>/stat` line: the second field after the command,
/// which is in parentheses and may itself hold spaces and parentheses.
#[cfg(any(target_os = "linux", test))]
fn parent_in_stat(stat: &str) -> Option<i32> {
    let (_, after_command) = stat.rsplit_once(')')?;
    after_command.split_ascii_whitespace().nth(1)?.parse().ok()
}

/// Returns the pid of the parent of process `pid` as the kernel records it, or `None` when no
/// process has that pid.
#[cfg(target_os = "macos")]
pub(super) fn parent_of(pid: i32) -> io::Result<Option<i32>> {
    // SAFETY: `proc_bsdinfo` is plain integers and byte arrays, for which all zeroes is a value.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: `info` is a writable `proc_bsdinfo` of `size` bytes, which is what the
    // PROC_PIDTBSDINFO flavor fills; the call writes nothing past `size`.
    let written = unsafe {
        libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size)
    };
    if written == size {
        return Ok(Some(info.pbi_ppid as i32));
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ESRCH) => Ok(None),
        _ if written > 0 => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("proc_pidinfo({pid}) wrote {written} of {size} bytes"),
        )),
        _ => Err(err),
    }
}

/// Ends this process at once with `status`, running no exit handler and no Python
/// finalization: what a worker does when it can no longer serve.
pub(super) fn exit_now(status: i32) -> ! {
    // SAFETY: `_exit` takes an int and does not return; it runs no handler that could see the
    // state of this process half-way.
    unsafe { libc::_exit(status) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_parent_pid_is_read_past_a_command_holding_spaces_and_parentheses() {
        assert_eq!(parent_in_stat("4242 (python3) S 17 4242 4242 0 -1"), Some(17));
        assert_eq!(parent_in_stat("4242 (a) b (c)) Z 1 0 0"), Some(1));
        assert_eq!(parent_in_stat("4242 (cut"), None);
        assert_eq!(parent_in_stat("4242 (python3) S"), None);
    }

    #[test]
    fn a_child_has_this_process_as_its_parent_and_a_gone_pid_has_none() {
        let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
        let pid = child.id() as i32;
        assert_eq!(parent_of(pid).unwrap(), Some(std::process::id() as i32));
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(parent_of(pid).unwrap(), None);
    }

    #[test]
    fn a_signal_is_named_as_the_os_names_it() {
        let killed = signal_name(libc::SIGKILL);
        assert!(killed.starts_with("Killed"), "{killed}");
        #[cfg(target_os = "linux")]
        assert_eq!(killed, "Killed");
        assert!(!signal_name(1000).is_empty());
    }

    #[test]
    fn a_wake_a_changed_value_a_timeout_or_an_interrupt_is_a_wait_that_ended_and_the_rest_a_refusal() {
        use rustix::io::Errno;
        for ended in [Ok(()), Err(Errno::AGAIN), Err(Errno::TIMEDOUT), Err(Errno::INTR)] {
            assert_eq!(wait_outcome(ended), Ok(()), "{ended:?}");
        }
        for errno in [Errno::INVAL, Errno::NOMEM, Errno::FAULT] {
            assert_eq!(wait_outcome(Err(errno)), Err(WaitError { errno }));
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
        assert_eq!(refused, WaitError { errno: rustix::io::Errno::INVAL });
        assert_eq!(refused.to_string(), format!("os_sync_wait_on_address_with_timeout: {}", rustix::io::Errno::INVAL));
    }
}
