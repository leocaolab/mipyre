//! The process lanes' system calls, and every difference between the platforms they run on (b/581):
//! how a new template or worker starts, how a process is signalled and its exit watched, how a
//! parent is read, how a worker ends with its template. What uses them is the same on Linux and
//! macOS, and builds and is tested on both. The calls rustix has no safe call for (the macOS
//! process-exit queue, `posix_spawn`, `strsignal`, `_exit`) are safe functions whose one `unsafe`
//! block says why the call holds. The lane's clock is here too. The channel's own calls (the
//! mapping, the address wait, the wake pipe) are `mipyre_shm`'s.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

use mipyre_shm::adapter::doorbell::HostBell;
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

/// Whether a new template or `start="import"` worker is spawned (`python -m <module>`) instead of
/// forked. On macOS numpy 2.5.x fails to import at random in any process that has forked, parent or
/// child (numpy gh-32124), so nothing that may import numpy forks there until numpy ships the fix;
/// elsewhere a fork takes about a millisecond.
pub(super) const SPAWNS_NEW_PROCESSES: bool = cfg!(target_os = "macos");

/// Whether the kernel ends a worker when its template dies: Linux does, by the parent-death signal
/// the worker asks for as it starts (`PR_SET_PDEATHSIG`, `_process_lane/worker.py`); macOS has no
/// such signal, so a worker watches its template's exit itself.
pub(super) const KERNEL_ENDS_ORPHANED_WORKERS: bool = cfg!(target_os = "linux");

/// The handle the server signals one process by, which can never reach another process once the
/// pid is reused: on Linux a pidfd; macOS has none, so the pid itself, until the process's exit
/// has been seen.
#[derive(Debug)]
pub(super) struct ProcessHandle {
    /// `None` when no process had the pid as it was opened: nothing is left to signal.
    #[cfg(target_os = "linux")]
    pidfd: Option<OwnedFd>,
    /// Set once the process's exit has been seen; from then on its pid may be another's.
    #[cfg(target_os = "macos")]
    exited: crate::sync::AtomicBool,
}

/// What registering a process with [`ExitWatch::add`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Registration {
    /// The exit is reported once it happens.
    Watching,
    /// The process was gone already: no exit of it will be reported.
    Gone,
}

/// Watches processes' exits through one descriptor a reactor can wait on: an epoll of pidfds on
/// Linux, a kqueue of `EVFILT_PROC` registrations on macOS. Each process is watched under a token
/// its watcher chose, by which its exit is reported.
pub(super) struct ExitWatch(
    #[cfg(target_os = "linux")] OwnedFd,
    #[cfg(target_os = "macos")] ExitQueue,
);

// Linux: a pidfd per process, an epoll of them.
#[cfg(target_os = "linux")]
impl ProcessHandle {
    /// Opens the handle of `pid`. A pid with no process opens a handle that is gone already.
    ///
    /// # Errors
    ///
    /// The errno of a pidfd that could not be opened.
    pub(super) fn open(pid: rustix::process::Pid) -> Result<Self, rustix::io::Errno> {
        match rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()) {
            Ok(pidfd) => Ok(ProcessHandle { pidfd: Some(pidfd) }),
            Err(rustix::io::Errno::SRCH) => Ok(ProcessHandle { pidfd: None }),
            Err(errno) => Err(errno),
        }
    }

    /// Whether the process is known to be gone: nothing goes to it any more.
    pub(super) fn known_gone(&self) -> bool {
        self.pidfd.is_none()
    }

    /// Records that the process's exit was seen: a pidfd reaches no other process after it, so
    /// there is nothing to record.
    pub(super) fn record_exit(&self) {}

    /// Sends the process SIGKILL through its pidfd; one that was gone at open is sent nothing.
    ///
    /// # Errors
    ///
    /// The OS's refusal; `ESRCH` for a process that has just ended.
    pub(super) fn kill(&self, _pid: rustix::process::Pid) -> rustix::io::Result<()> {
        match &self.pidfd {
            Some(pidfd) => rustix::process::pidfd_send_signal(pidfd, rustix::process::Signal::KILL),
            None => Ok(()),
        }
    }
}

#[cfg(target_os = "linux")]
impl ExitWatch {
    pub(super) fn new() -> io::Result<Self> {
        Ok(ExitWatch(rustix::event::epoll::create(
            rustix::event::epoll::CreateFlags::CLOEXEC,
        )?))
    }

    /// Reports the exit of the process `handle` holds once, under `token`. Its pidfd must stay open
    /// until [`ExitWatch::forget`].
    ///
    /// # Errors
    ///
    /// The registration the OS refused.
    pub(super) fn add(
        &self,
        handle: &ProcessHandle,
        _pid: rustix::process::Pid,
        token: u64,
    ) -> io::Result<Registration> {
        use rustix::event::epoll;
        let Some(pidfd) = &handle.pidfd else {
            return Ok(Registration::Gone);
        };
        epoll::add(&self.0, pidfd, epoll::EventData::new_u64(token), epoll::EventFlags::IN)?;
        Ok(Registration::Watching)
    }

    /// Stops watching the process `handle` holds, whose exit was reported: its pidfd stays
    /// readable for as long as it is open, and would be reported again.
    ///
    /// # Errors
    ///
    /// The OS's refusal.
    pub(super) fn forget(&self, handle: &ProcessHandle) -> io::Result<()> {
        if let Some(pidfd) = &handle.pidfd {
            rustix::event::epoll::delete(&self.0, pidfd)?;
        }
        Ok(())
    }

    /// Hands `each` the token of up to `ROOM` exits, waiting at most `timeout` (`None`: until one
    /// comes), and returns how many there were. An interrupted wait is made again. The events are
    /// read into the stack: the lane poller asks every round.
    ///
    /// # Errors
    ///
    /// The OS's, or the first `each` returned.
    pub(super) fn exited<const ROOM: usize>(
        &self,
        timeout: Option<std::time::Duration>,
        mut each: impl FnMut(u64) -> io::Result<()>,
    ) -> io::Result<usize> {
        use rustix::event::epoll;
        let timeout = timeout.map(|t| rustix::time::Timespec {
            tv_sec: t.as_secs() as _,
            tv_nsec: t.subsec_nanos() as _,
        });
        let mut room = [std::mem::MaybeUninit::<epoll::Event>::uninit(); ROOM];
        loop {
            let events: &[epoll::Event] = match epoll::wait(&self.0, &mut room, timeout.as_ref()) {
                Ok((events, _)) => events,
                Err(rustix::io::Errno::INTR) => continue,
                Err(e) => return Err(e.into()),
            };
            for ev in events {
                each(ev.data.u64())?;
            }
            return Ok(events.len());
        }
    }
}

// macOS: the pid until the exit is seen, a kqueue of process-exit registrations.
#[cfg(target_os = "macos")]
impl ProcessHandle {
    /// Opens the handle of `pid`: macOS has none to open.
    ///
    /// # Errors
    ///
    /// None on macOS.
    pub(super) fn open(_pid: rustix::process::Pid) -> Result<Self, rustix::io::Errno> {
        Ok(ProcessHandle {
            exited: crate::sync::AtomicBool::new(false),
        })
    }

    /// Whether the process is known to be gone: nothing goes to its pid any more.
    pub(super) fn known_gone(&self) -> bool {
        self.exited.load(crate::sync::Ordering::SeqCst)
    }

    /// Records that the process's exit was seen: its pid may be another's from now on.
    pub(super) fn record_exit(&self) {
        self.exited.store(true, crate::sync::Ordering::SeqCst);
    }

    /// Sends `pid` SIGKILL by its number, unless its exit was seen.
    ///
    /// # Errors
    ///
    /// The OS's refusal; `ESRCH` for a process that has just ended.
    pub(super) fn kill(&self, pid: rustix::process::Pid) -> rustix::io::Result<()> {
        if self.known_gone() {
            return Ok(());
        }
        rustix::process::kill_process(pid, rustix::process::Signal::KILL)
    }
}

#[cfg(target_os = "macos")]
impl ExitWatch {
    pub(super) fn new() -> io::Result<Self> {
        Ok(ExitWatch(ExitQueue::new()?))
    }

    /// Reports `pid`'s exit once, under `token`.
    ///
    /// # Errors
    ///
    /// The registration the OS refused.
    pub(super) fn add(
        &self,
        handle: &ProcessHandle,
        pid: rustix::process::Pid,
        token: u64,
    ) -> io::Result<Registration> {
        if handle.known_gone() {
            return Ok(Registration::Gone);
        }
        match self.0.add(pid, token as usize) {
            Ok(()) => Ok(Registration::Watching),
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(Registration::Gone),
            Err(e) => Err(e),
        }
    }

    /// Stops watching a process whose exit was reported: a kqueue reports an exit once and drops
    /// the registration itself.
    ///
    /// # Errors
    ///
    /// None on macOS.
    pub(super) fn forget(&self, _handle: &ProcessHandle) -> io::Result<()> {
        Ok(())
    }

    /// Hands `each` the token of up to `ROOM` exits, waiting at most `timeout` (`None`: until one
    /// comes), and returns how many there were. An interrupted wait is made again.
    ///
    /// # Errors
    ///
    /// The OS's, or the first `each` returned.
    pub(super) fn exited<const ROOM: usize>(
        &self,
        timeout: Option<std::time::Duration>,
        mut each: impl FnMut(u64) -> io::Result<()>,
    ) -> io::Result<usize> {
        self.0.exited::<ROOM>(timeout, |_, tag| each(tag as u64))
    }
}

impl AsFd for ExitWatch {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl AsRawFd for ExitWatch {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_fd().as_raw_fd()
    }
}

/// A kqueue that holds only process-exit registrations, so no event in it names a descriptor:
/// that is what makes its `kevent` calls sound.
#[cfg(target_os = "macos")]
struct ExitQueue(OwnedFd);

#[cfg(target_os = "macos")]
impl ExitQueue {
    fn new() -> io::Result<Self> {
        Ok(ExitQueue(rustix::event::kqueue::kqueue()?))
    }

    /// Reports `pid`'s exit once, tagged with `tag`.
    ///
    /// # Errors
    ///
    /// `ESRCH` when `pid` is already gone.
    fn add(&self, pid: rustix::process::Pid, tag: usize) -> io::Result<()> {
        use rustix::event::kqueue::{Event, EventFilter, EventFlags, ProcessEvents};
        let change = Event::new(
            EventFilter::Proc {
                pid,
                flags: ProcessEvents::EXIT,
            },
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
    fn exited<const ROOM: usize>(
        &self,
        timeout: Option<std::time::Duration>,
        mut each: impl FnMut(rustix::process::Pid, usize) -> io::Result<()>,
    ) -> io::Result<usize> {
        use rustix::event::kqueue::{Event, EventFilter, ProcessEvents};
        let mut room = [const { std::mem::MaybeUninit::<Event>::uninit() }; ROOM];
        loop {
            // SAFETY: as in `add`: no event in this queue names a descriptor.
            let events: &[Event] = match unsafe { rustix::event::kqueue::kevent(&self.0, &[], &mut room, timeout) } {
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

/// A host's lanes' wake pipes, registered once in one epoll (Linux) or kqueue (macOS) a reactor
/// waits on: a wait says which lane's pipe holds bytes, by the lane's place among the host's
/// lanes. It owns the pipes, so none closes while the queue names it.
pub(super) struct BellQueue {
    queue: OwnedFd,
    bells: Vec<HostBell>,
}

impl BellQueue {
    /// Registers every one of `bells`, under its place in the list.
    ///
    /// # Errors
    ///
    /// The OS's refusal to make the queue or register a pipe.
    pub(super) fn new(bells: Vec<HostBell>) -> io::Result<Self> {
        let queue = BellQueue {
            queue: Self::create()?,
            bells,
        };
        for k in 0..queue.bells.len() {
            queue.listen(k)?;
        }
        Ok(queue)
    }

    /// Returns the pipe of the lane at place `k`.
    pub(super) fn bell(&self, k: usize) -> &HostBell {
        &self.bells[k]
    }

    /// Writes into `rung` the places of up to its length of lanes whose pipes hold bytes, without
    /// waiting, and returns how many there were. An interrupted look is made again.
    ///
    /// # Errors
    ///
    /// The OS's.
    pub(super) fn rung<const ROOM: usize>(&self, rung: &mut [usize; ROOM]) -> io::Result<usize> {
        let mut n = 0;
        self.ready::<ROOM>(|k| {
            rung[n] = k;
            n += 1;
        })?;
        Ok(n)
    }
}

#[cfg(target_os = "linux")]
impl BellQueue {
    fn create() -> io::Result<OwnedFd> {
        Ok(rustix::event::epoll::create(
            rustix::event::epoll::CreateFlags::CLOEXEC,
        )?)
    }

    /// Reports the pipe of the lane at place `k` when it holds bytes, again after [`BellQueue::mute`].
    ///
    /// # Errors
    ///
    /// The OS's refusal.
    pub(super) fn listen(&self, k: usize) -> io::Result<()> {
        use rustix::event::epoll;
        Ok(epoll::add(
            &self.queue,
            &self.bells[k],
            epoll::EventData::new_u64(k as u64),
            epoll::EventFlags::IN,
        )?)
    }

    /// Stops reporting the pipe of the lane at place `k`.
    ///
    /// # Errors
    ///
    /// The OS's refusal.
    pub(super) fn mute(&self, k: usize) -> io::Result<()> {
        Ok(rustix::event::epoll::delete(&self.queue, &self.bells[k])?)
    }

    fn ready<const ROOM: usize>(&self, mut each: impl FnMut(usize)) -> io::Result<()> {
        use rustix::event::epoll;
        let mut room = [std::mem::MaybeUninit::<epoll::Event>::uninit(); ROOM];
        let now = rustix::time::Timespec { tv_sec: 0, tv_nsec: 0 };
        loop {
            let events: &[epoll::Event] = match epoll::wait(&self.queue, &mut room, Some(&now)) {
                Ok((events, _)) => events,
                Err(rustix::io::Errno::INTR) => continue,
                Err(e) => return Err(e.into()),
            };
            events.iter().for_each(|ev| each(ev.data.u64() as usize));
            return Ok(());
        }
    }
}

#[cfg(target_os = "macos")]
impl BellQueue {
    fn create() -> io::Result<OwnedFd> {
        Ok(rustix::event::kqueue::kqueue()?)
    }

    fn change(&self, k: usize, flags: rustix::event::kqueue::EventFlags) -> io::Result<()> {
        use rustix::event::kqueue::{Event, EventFilter};
        let change = Event::new(
            EventFilter::Read(self.bells[k].as_raw_fd()),
            flags,
            k as *mut std::ffi::c_void,
        );
        let mut none: Vec<Event> = Vec::new();
        // SAFETY: rustix asks that every descriptor an event names stay open as long as the
        // kqueue; the queue owns the pipes its events name, and is closed before them (field order).
        unsafe { rustix::event::kqueue::kevent(&self.queue, &[change], &mut none, None)? };
        Ok(())
    }

    /// Reports the pipe of the lane at place `k` when it holds bytes, again after [`BellQueue::mute`].
    ///
    /// # Errors
    ///
    /// The OS's refusal.
    pub(super) fn listen(&self, k: usize) -> io::Result<()> {
        self.change(k, rustix::event::kqueue::EventFlags::ADD)
    }

    /// Stops reporting the pipe of the lane at place `k`.
    ///
    /// # Errors
    ///
    /// The OS's refusal.
    pub(super) fn mute(&self, k: usize) -> io::Result<()> {
        self.change(k, rustix::event::kqueue::EventFlags::DELETE)
    }

    fn ready<const ROOM: usize>(&self, mut each: impl FnMut(usize)) -> io::Result<()> {
        use rustix::event::kqueue::Event;
        let mut room = [const { std::mem::MaybeUninit::<Event>::uninit() }; ROOM];
        loop {
            // SAFETY: as in `change`: the queue owns every pipe its events name.
            let events: &[Event] = match unsafe {
                rustix::event::kqueue::kevent(&self.queue, &[], &mut room, Some(std::time::Duration::ZERO))
            } {
                Ok((events, _)) => events,
                Err(rustix::io::Errno::INTR) => continue,
                Err(e) => return Err(e.into()),
            };
            events.iter().for_each(|ev| each(ev.udata() as usize));
            return Ok(());
        }
    }
}

impl AsFd for BellQueue {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.queue.as_fd()
    }
}

impl AsRawFd for BellQueue {
    fn as_raw_fd(&self) -> RawFd {
        self.queue.as_raw_fd()
    }
}

/// Starts `argv[0]` with arguments `argv` and environment `envp`; of this process's descriptors
/// it inherits the standard streams and `keep_fds` at their own numbers. Returns its pid.
///
/// On macOS it inherits nothing else, whatever the flags: a template holds every lane's channel
/// without close-on-exec there (macOS has no `MSG_CMSG_CLOEXEC`), and a worker it spawns must get
/// its own lane's pair only, not another lane's or the template's socket. On Linux every
/// descriptor without close-on-exec goes along too; spawn starts only a test server's template
/// there, before any lane exists.
///
/// Safe beside other threads: `keep_fds` keep their close-on-exec flag in this process. Each is
/// duplicated close-on-exec and the new process's file actions put the copy back at the
/// original's number, which clears the flag there only, so a process another thread starts at
/// the same time inherits none of them.
pub(super) fn spawn(argv: &[std::ffi::CString], envp: &[std::ffi::CString], keep_fds: &[RawFd]) -> io::Result<i32> {
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
    // A copy is a new number, so it is never one of `keep_fds` nor overwritten by their dup2s.
    let copies = keep_fds
        .iter()
        .map(|&fd| duplicate_close_on_exec(fd))
        .collect::<io::Result<Vec<_>>>()?;
    let mut actions = FileActions::new()?;
    for (copy, &fd) in copies.iter().zip(keep_fds) {
        actions.dup2(copy.as_fd(), fd)?;
    }
    let attributes = only_what_is_handed_on(&mut actions)?;
    let mut pid: libc::pid_t = 0;
    // SAFETY: `program` is NUL-terminated; `argv_p` and `envp_p` are NULL-ended arrays of
    // NUL-terminated strings, all borrowed from `argv` / `envp` and alive for the call; `pid` is
    // a writable out-value; `actions` is initialised and its fds (`copies`) are open for the
    // call; so is `attributes`.
    let rc = unsafe {
        libc::posix_spawn(
            &mut pid,
            program.as_ptr(),
            actions.as_ptr(),
            attributes.as_ptr(),
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

/// Returns the spawn's attributes. On macOS they close every descriptor that `actions` does not
/// hand on, and `actions` hands on the open standard streams.
#[cfg(target_os = "macos")]
fn only_what_is_handed_on(actions: &mut FileActions) -> io::Result<SpawnAttributes> {
    for stream in 0..3 {
        // SAFETY: the number is only looked at for this call; a closed one answers EBADF.
        let open = rustix::io::fcntl_getfd(unsafe { BorrowedFd::borrow_raw(stream) }).is_ok();
        // A server started with a stream closed spawns with it closed: handing on a closed fd
        // fails the spawn.
        if open {
            actions.inherit(stream)?;
        }
    }
    let mut attributes = SpawnAttributes::new()?;
    attributes.close_the_rest()?;
    Ok(attributes)
}

/// Returns the spawn's attributes: the defaults, which leave the descriptors to close-on-exec.
#[cfg(not(target_os = "macos"))]
fn only_what_is_handed_on(_actions: &mut FileActions) -> io::Result<SpawnAttributes> {
    SpawnAttributes::new()
}

/// A `posix_spawnattr_t`, destroyed when dropped.
struct SpawnAttributes(libc::posix_spawnattr_t);

impl SpawnAttributes {
    fn new() -> io::Result<Self> {
        let mut raw = std::mem::MaybeUninit::<libc::posix_spawnattr_t>::uninit();
        // SAFETY: `raw` is writable storage for one attributes object, which init fills.
        let rc = unsafe { libc::posix_spawnattr_init(raw.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        // SAFETY: init returned 0, so `raw` is initialised.
        Ok(Self(unsafe { raw.assume_init() }))
    }

    /// Has the new process get only the descriptors the file actions hand on (Apple's
    /// `POSIX_SPAWN_CLOEXEC_DEFAULT`).
    #[cfg(target_os = "macos")]
    fn close_the_rest(&mut self) -> io::Result<()> {
        // SAFETY: `self.0` is initialised (`new`); the call records a flag word in it.
        let rc =
            unsafe { libc::posix_spawnattr_setflags(&mut self.0, libc::POSIX_SPAWN_CLOEXEC_DEFAULT as libc::c_short) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }

    fn as_ptr(&self) -> *const libc::posix_spawnattr_t {
        &self.0
    }
}

impl Drop for SpawnAttributes {
    fn drop(&mut self) {
        // SAFETY: `self.0` is initialised (`new`) and destroyed only here, once.
        unsafe {
            libc::posix_spawnattr_destroy(&mut self.0);
        }
    }
}

/// Returns a close-on-exec copy of `fd` at a new number; `EBADF` when `fd` is not open.
fn duplicate_close_on_exec(fd: RawFd) -> io::Result<OwnedFd> {
    // SAFETY: F_DUPFD_CLOEXEC takes an int and touches no memory; a number that is no open fd
    // fails with EBADF.
    let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if copy < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `copy` was just opened by this call and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(copy) })
}

/// A `posix_spawn_file_actions_t`, destroyed when dropped.
struct FileActions(libc::posix_spawn_file_actions_t);

impl FileActions {
    fn new() -> io::Result<Self> {
        let mut raw = std::mem::MaybeUninit::<libc::posix_spawn_file_actions_t>::uninit();
        // SAFETY: `raw` is writable storage for one file-actions object, which init fills.
        let rc = unsafe { libc::posix_spawn_file_actions_init(raw.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        // SAFETY: init returned 0, so `raw` is initialised.
        Ok(Self(unsafe { raw.assume_init() }))
    }

    /// Adds "dup2 `from` onto `to`" for the new process; `from` must stay open until the spawn.
    fn dup2(&mut self, from: BorrowedFd<'_>, to: RawFd) -> io::Result<()> {
        // SAFETY: `self.0` is initialised (`new`); the call records two ints in it.
        let rc = unsafe { libc::posix_spawn_file_actions_adddup2(&mut self.0, from.as_raw_fd(), to) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }

    /// Adds "hand `fd` on as it is" for the new process (Apple's `addinherit_np`), which the spawn
    /// keeps open when it closes the rest.
    #[cfg(target_os = "macos")]
    fn inherit(&mut self, fd: RawFd) -> io::Result<()> {
        extern "C" {
            fn posix_spawn_file_actions_addinherit_np(
                actions: *mut libc::posix_spawn_file_actions_t,
                fd: libc::c_int,
            ) -> libc::c_int;
        }
        // SAFETY: `self.0` is initialised (`new`); the call records an int in it.
        let rc = unsafe { posix_spawn_file_actions_addinherit_np(&mut self.0, fd) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }

    fn as_ptr(&self) -> *const libc::posix_spawn_file_actions_t {
        &self.0
    }
}

impl Drop for FileActions {
    fn drop(&mut self) {
        // SAFETY: `self.0` is initialised (`new`) and destroyed only here, once.
        unsafe {
            libc::posix_spawn_file_actions_destroy(&mut self.0);
        }
    }
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
    unsafe { std::ffi::CStr::from_ptr(text) }.to_string_lossy().into_owned()
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
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{path} has no parent pid: {stat:?}"),
        )
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
    let written = unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size) };
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

/// Reads `CLOCK_MONOTONIC` in nanoseconds, which every process on the machine shares.
pub(super) fn monotonic_ns() -> u64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
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
    fn a_spawned_process_gets_a_kept_fd_which_stays_close_on_exec_here() {
        use std::ffi::CString;
        use std::io::Read;

        let (mut read_end, write_end) = std::io::pipe().unwrap();
        let fd = write_end.as_raw_fd();
        // Through /dev/fd: dash's `>&N` takes one digit, and the fd may be 10 or more.
        let argv = ["/bin/sh", "-c", &format!("printf kept > /dev/fd/{fd}")].map(|a| CString::new(a).unwrap());
        let pid = spawn(&argv, &[], &[fd]).unwrap();
        let flags = rustix::io::fcntl_getfd(&write_end).unwrap();
        assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
        drop(write_end);
        let mut got = String::new();
        read_end.read_to_string(&mut got).unwrap();
        assert_eq!(got, "kept");
        let mut status = 0;
        // SAFETY: `status` is a writable int; `pid` is this test's own child.
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "status {status}"
        );
    }

    /// A template holds every lane's fds without close-on-exec on macOS; a worker it spawns gets
    /// only those it is handed.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_spawned_process_gets_no_fd_it_was_not_handed_though_the_fd_is_inheritable() {
        use std::ffi::CString;

        let (_read_end, leaked) = std::io::pipe().unwrap();
        rustix::io::fcntl_setfd(&leaked, rustix::io::FdFlags::empty()).unwrap();
        let script = format!("test ! -e /dev/fd/{}", leaked.as_raw_fd());
        let argv = ["/bin/sh", "-c", &script].map(|a| CString::new(a).unwrap());
        let pid = spawn(&argv, &[], &[]).unwrap();
        let mut status = 0;
        // SAFETY: `status` is a writable int; `pid` is this test's own child.
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "status {status}"
        );
    }

    #[test]
    fn a_signal_is_named_as_the_os_names_it() {
        let killed = signal_name(libc::SIGKILL);
        assert!(killed.starts_with("Killed"), "{killed}");
        #[cfg(target_os = "linux")]
        assert_eq!(killed, "Killed");
        assert!(!signal_name(1000).is_empty());
    }

    /// Returns a child that sleeps until killed, and its pid.
    fn sleeper() -> (std::process::Child, rustix::process::Pid) {
        let child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
        let pid = rustix::process::Pid::from_raw(child.id() as i32).unwrap();
        (child, pid)
    }

    /// A watched process's exit is reported once it happens, under the token it was watched with,
    /// and no more once forgotten.
    #[test]
    fn an_exit_is_reported_under_its_token_and_not_again_once_forgotten() {
        let (mut child, pid) = sleeper();
        let handle = ProcessHandle::open(pid).unwrap();
        let watch = ExitWatch::new().unwrap();
        assert_eq!(watch.add(&handle, pid, 7).unwrap(), Registration::Watching);
        let none = |_| -> io::Result<()> { panic!("an exit was reported") };
        assert_eq!(watch.exited::<4>(Some(std::time::Duration::ZERO), none).unwrap(), 0);
        child.kill().unwrap();
        child.wait().unwrap();
        let mut seen = Vec::new();
        let reported = watch
            .exited::<4>(None, |token| {
                seen.push(token);
                Ok(())
            })
            .unwrap();
        assert_eq!((reported, seen), (1, vec![7]));
        watch.forget(&handle).unwrap();
        assert_eq!(watch.exited::<4>(Some(std::time::Duration::ZERO), none).unwrap(), 0);
    }

    /// A process already gone and reaped is not watched: there is no exit left to report.
    #[test]
    fn a_process_gone_before_it_is_watched_is_gone() {
        let (mut child, pid) = sleeper();
        child.kill().unwrap();
        child.wait().unwrap();
        let handle = ProcessHandle::open(pid).unwrap();
        assert_eq!(
            ExitWatch::new().unwrap().add(&handle, pid, 1).unwrap(),
            Registration::Gone
        );
    }

    /// Once a process's exit is recorded, a kill sends it nothing: on macOS its pid may be
    /// another's by then (a pidfd reaches no other process, so on Linux the kill still goes to it,
    /// harmlessly).
    #[test]
    fn a_process_whose_exit_was_recorded_is_signalled_no_more_by_its_pid() {
        use std::os::unix::process::ExitStatusExt;
        let (mut child, pid) = sleeper();
        let handle = ProcessHandle::open(pid).unwrap();
        handle.record_exit();
        handle.kill(pid).unwrap();
        // Ended by the test's own SIGTERM: one sent SIGKILL first dies of that.
        rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
        let status = child.wait().unwrap();
        let expected = if cfg!(target_os = "linux") {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        assert_eq!(status.signal(), Some(expected));
    }
}
