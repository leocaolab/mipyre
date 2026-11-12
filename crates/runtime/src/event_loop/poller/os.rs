//! The poller's calls into the kernel: an epoll with a timerfd on Linux, a kqueue with an `EVFILT_TIMER` on
//! macOS, and the registration of the poller's own fd with tokio's reactor. Every `unsafe` of the poller is
//! in this file.

use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::BorrowedFd;
use std::os::fd::{OwnedFd, RawFd};
use std::time::Duration;

use rustix::io::Errno;
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;

use super::{scan_end, FdInterest, Poller, PollerReadiness, Scan, MAX_EVENTS};

/// Holds the kernel objects of one poller: an epoll and the timerfd registered in it.
#[cfg(target_os = "linux")]
pub(super) struct KernelQueue {
    epoll: OwnedFd,
    timer: OwnedFd,
}

/// Holds the kernel object of one poller: a kqueue, whose timer is one of its filters.
#[cfg(target_os = "macos")]
pub(super) struct KernelQueue(OwnedFd);

impl Poller {
    /// Registers a duplicate of the poller's fd with the current tokio runtime's reactor.
    ///
    /// # Panics
    ///
    /// tokio panics when the calling thread is not in a tokio runtime with I/O enabled.
    ///
    /// # Errors
    ///
    /// Returns the OS's refusal to duplicate the fd, or the reactor's to register it.
    pub(crate) fn register_with_reactor(&self) -> io::Result<PollerReadiness> {
        let fd = self.fd().try_clone()?;
        // SAFETY: the `AsyncFd` owns `fd`, a duplicate nothing else holds, so it stays open until the `AsyncFd` is
        // dropped, and `OwnedFd::as_raw_fd` returns that one number every time.
        let registered = unsafe { AsyncFd::register_with_interest(fd, Interest::READABLE) }?;
        Ok(PollerReadiness(registered))
    }
}

#[cfg(target_os = "linux")]
impl Poller {
    pub(crate) fn new() -> io::Result<Self> {
        use rustix::event::epoll;
        use rustix::time::{timerfd_create, TimerfdClockId, TimerfdFlags};
        let queue = epoll::create(epoll::CreateFlags::CLOEXEC)?;
        let timer = timerfd_create(
            TimerfdClockId::Monotonic,
            TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC,
        )?;
        epoll::add(
            &queue,
            &timer,
            epoll::EventData::new_u64(TIMER_TOKEN),
            epoll::EventFlags::IN,
        )?;
        Ok(Poller(KernelQueue { epoll: queue, timer }))
    }

    /// Returns the fd that is readable while a watched fd is ready or the timer has fired.
    fn fd(&self) -> &OwnedFd {
        &self.0.epoll
    }

    /// Sets the timer to make the poller readable `after` from now, or disarms it for `None`.
    /// Setting it clears a firing not yet read.
    ///
    /// # Errors
    ///
    /// Returns the kernel's refusal, and `EINVAL` for a delay too long for a `timespec`.
    pub(crate) fn arm(&self, after: Option<Duration>) -> io::Result<()> {
        use rustix::time::{timerfd_settime, Itimerspec, TimerfdTimerFlags, Timespec};
        let value = match after {
            Some(after) => Timespec::try_from(after.max(SOONEST)).map_err(|_| Errno::INVAL)?,
            None => NO_WAIT,
        };
        let spec = Itimerspec {
            it_interval: NO_WAIT,
            it_value: value,
        };
        timerfd_settime(&self.0.timer, TimerfdTimerFlags::empty(), &spec)?;
        Ok(())
    }

    /// Changes what the kernel watches `fd` for from `was`, what it was last told for `fd`, to `now`;
    /// `None` is not watched.
    ///
    /// # Errors
    ///
    /// Returns the change the kernel refused, such as watching a regular file or a closed fd.
    /// Deleting a registration the kernel no longer has under `fd` is no error.
    pub(crate) fn watch(&self, fd: RawFd, was: Option<FdInterest>, now: Option<FdInterest>) -> io::Result<()> {
        use rustix::event::epoll;
        let Some(op) = epoll_op(was, now) else {
            return Ok(());
        };
        let Ok(token) = u64::try_from(fd) else {
            return Err(Errno::BADF.into());
        };
        // SAFETY: `fd` is not negative, so not -1, and the borrow serves this one `epoll_ctl` on the loop's own epoll,
        // which neither reads, writes nor closes it. Another thread may close the number meanwhile, which breaks
        // `borrow_raw`'s promise that it stays open but no memory: the kernel then refuses with EBADF, or, the number
        // reused, registers that other file in this epoll, which only changes what this loop is told is ready.
        let source = unsafe { BorrowedFd::borrow_raw(fd) };
        let data = epoll::EventData::new_u64(token);
        let changed = match op {
            EpollOp::Add(now) => epoll::add(&self.0.epoll, source, data, epoll_flags(now)),
            EpollOp::Modify(now) => epoll::modify(&self.0.epoll, source, data, epoll_flags(now)),
            EpollOp::Delete => delete_tolerating_gone(epoll::delete(&self.0.epoll, source)),
        };
        Ok(changed?)
    }

    /// Hands `each` the fds that are ready, at most [`MAX_EVENTS`] events of them and the timer's,
    /// with what each is ready for, without waiting, and says whether more may be ready and whether
    /// the timer fired. A wait a signal interrupted hands none, and says so.
    ///
    /// # Errors
    ///
    /// Returns the error the kernel's wait returned, other than an interruption. Returns, after
    /// handing over the other fds, an event whose token is neither an fd number nor the timer's,
    /// which `watch` and `new` never write.
    pub(crate) fn scan(&self, mut each: impl FnMut(RawFd, FdInterest)) -> io::Result<Scan> {
        use rustix::event::epoll;
        let mut room = [std::mem::MaybeUninit::<epoll::Event>::uninit(); MAX_EVENTS];
        let events: &[epoll::Event] = match epoll::wait(&self.0.epoll, &mut room, Some(&NO_WAIT)) {
            Ok((events, _)) => events,
            Err(Errno::INTR) => return Ok(interrupted()),
            Err(errno) => return Err(errno.into()),
        };
        let mut stray = None;
        let mut timer_fired = false;
        for &epoll::Event { flags, data } in events {
            if data.u64() == TIMER_TOKEN {
                timer_fired = true;
                continue;
            }
            let Ok(fd) = RawFd::try_from(data.u64()) else {
                stray = Some(data.u64());
                continue;
            };
            if let Some(ready) = epoll_ready(flags) {
                each(fd, ready);
            }
        }
        match stray {
            None => Ok(Scan {
                end: scan_end(events.len()),
                timer_fired,
            }),
            Some(token) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the epoll reported token {token}, which is no fd number this poller watches"),
            )),
        }
    }
}

#[cfg(target_os = "linux")]
const NO_WAIT: rustix::time::Timespec = rustix::time::Timespec { tv_sec: 0, tv_nsec: 0 };

/// Marks the timer's events in the epoll; no fd number is this large.
#[cfg(target_os = "linux")]
const TIMER_TOKEN: u64 = u64::MAX;

/// Names the poller's one timer among its kqueue's timer filters, which are told apart from its fd
/// filters by kind.
#[cfg(target_os = "macos")]
const TIMER_IDENT: isize = 0;

/// It is the shortest delay the timer is set for: a zero delay would disarm a timerfd instead of
/// firing it.
const SOONEST: Duration = Duration::from_nanos(1);

/// Returns what a scan a signal interrupted found: no event, so more may be ready.
fn interrupted() -> Scan {
    Scan {
        end: super::ScanEnd::Partial,
        timer_fired: false,
    }
}

/// Says which `epoll_ctl` operation changes an fd's registration.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EpollOp {
    Add(FdInterest),
    Modify(FdInterest),
    Delete,
}

/// Returns the `epoll_ctl` operation that changes an fd's registration from `was` to `now`, or none
/// when nothing changes.
#[cfg(target_os = "linux")]
fn epoll_op(was: Option<FdInterest>, now: Option<FdInterest>) -> Option<EpollOp> {
    match (was, now) {
        (None, None) => None,
        (None, Some(now)) => Some(EpollOp::Add(now)),
        (Some(_), None) => Some(EpollOp::Delete),
        (Some(was), Some(now)) => (was != now).then_some(EpollOp::Modify(now)),
    }
}

#[cfg(target_os = "linux")]
fn epoll_flags(interest: FdInterest) -> rustix::event::epoll::EventFlags {
    use rustix::event::epoll::EventFlags;
    match interest {
        FdInterest::Read => EventFlags::IN,
        FdInterest::Write => EventFlags::OUT,
        FdInterest::ReadWrite => EventFlags::IN | EventFlags::OUT,
    }
}

/// Returns what a ready fd's epoll flags make it ready for: a flag other than `OUT` (a hang-up, an error) wakes its
/// reader and one other than `IN` its writer, as Python's `selectors.EpollSelector` reads them.
#[cfg(target_os = "linux")]
fn epoll_ready(flags: rustix::event::epoll::EventFlags) -> Option<FdInterest> {
    use rustix::event::epoll::EventFlags;
    FdInterest::of(
        !(flags - EventFlags::OUT).is_empty(),
        !(flags - EventFlags::IN).is_empty(),
    )
}

#[cfg(target_os = "macos")]
impl Poller {
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Poller(KernelQueue(rustix::event::kqueue::kqueue()?)))
    }

    /// Returns the fd that is readable while a watched fd is ready or the timer has fired.
    fn fd(&self) -> &OwnedFd {
        &self.0 .0
    }

    /// Sets the timer to make the poller readable `after` from now, or disarms it for `None`. The
    /// timer fires once; the turn that reads its event sets it again.
    ///
    /// # Errors
    ///
    /// Returns the kernel's refusal. Disarming a timer that fired already is no error.
    pub(crate) fn arm(&self, after: Option<Duration>) -> io::Result<()> {
        use rustix::event::kqueue::{EventFilter, EventFlags};
        let timer = |timer| EventFilter::Timer {
            ident: TIMER_IDENT,
            timer,
        };
        match after {
            Some(after) => self.change(
                timer(Some(whole_millis(after.max(SOONEST)))),
                EventFlags::ADD | EventFlags::ONESHOT,
            ),
            None => self.change(timer(None), EventFlags::DELETE),
        }
    }

    /// Changes what the kernel watches `fd` for from `was`, what it was last told for `fd`, to `now`;
    /// `None` is not watched. Reading and writing are two filters, each changed in a call of its own.
    ///
    /// # Errors
    ///
    /// Returns the change the kernel refused, such as watching a closed fd. Deleting a filter the
    /// kernel no longer has for `fd` is no error.
    pub(crate) fn watch(&self, fd: RawFd, was: Option<FdInterest>, now: Option<FdInterest>) -> io::Result<()> {
        use rustix::event::kqueue::{EventFilter, EventFlags};
        let reads = |i: Option<FdInterest>| i.is_some_and(FdInterest::reads);
        let writes = |i: Option<FdInterest>| i.is_some_and(FdInterest::writes);
        let flags = |on: bool| if on { EventFlags::ADD } else { EventFlags::DELETE };
        if reads(was) != reads(now) {
            self.change(EventFilter::Read(fd), flags(reads(now)))?;
        }
        if writes(was) != writes(now) {
            self.change(EventFilter::Write(fd), flags(writes(now)))?;
        }
        Ok(())
    }

    /// Adds, changes or deletes the filter; the kernel reports a failed change as the call's error, since no room
    /// is given for events.
    fn change(
        &self,
        filter: rustix::event::kqueue::EventFilter,
        flags: rustix::event::kqueue::EventFlags,
    ) -> io::Result<()> {
        use rustix::event::kqueue::{kevent, Event, EventFlags};
        let change = Event::new(filter, flags, std::ptr::null_mut());
        let mut none: Vec<Event> = Vec::new();
        // SAFETY: rustix asks that the fds the events name stay open as long as the kqueue. These fds are the
        // callers' and may be closed first; closing an fd removes every kevent naming it (kqueue(2)), so a closed
        // one leaves nothing behind, and the queue reports fds only as numbers, from which nothing here makes an fd.
        // A timer filter names no fd.
        let changed = unsafe { kevent(self.fd(), &[change], &mut none, None) };
        let changed = changed.map(drop);
        Ok(if flags.contains(EventFlags::DELETE) {
            delete_tolerating_gone(changed)
        } else {
            changed
        }?)
    }

    /// Hands `each` the filters that are ready, at most [`MAX_EVENTS`] events of them and the
    /// timer's, as the fd and what it is ready for, without waiting, and says whether more may be
    /// ready and whether the timer fired; reading the timer's event is what takes it off the queue.
    /// A wait a signal interrupted hands none, and says so.
    ///
    /// # Errors
    ///
    /// Returns the error the kernel's wait returned, other than an interruption.
    pub(crate) fn scan(&self, mut each: impl FnMut(RawFd, FdInterest)) -> io::Result<Scan> {
        use rustix::event::kqueue::{kevent, Event, EventFilter};
        let mut room = [const { std::mem::MaybeUninit::<Event>::uninit() }; MAX_EVENTS];
        // SAFETY: as in `change`: no event read here is made into an fd.
        let events: &[Event] = match unsafe { kevent(self.fd(), &[], &mut room, Some(std::time::Duration::ZERO)) } {
            Ok((events, _)) => events,
            Err(Errno::INTR) => return Ok(interrupted()),
            Err(errno) => return Err(errno.into()),
        };
        let mut timer_fired = false;
        for event in events {
            match event.filter() {
                EventFilter::Read(fd) => each(fd, FdInterest::Read),
                EventFilter::Write(fd) => each(fd, FdInterest::Write),
                EventFilter::Timer { .. } => timer_fired = true,
                _ => {}
            }
        }
        Ok(Scan {
            end: scan_end(events.len()),
            timer_fired,
        })
    }
}

/// Returns `after` rounded up to a whole millisecond. rustix (1.1) hands kqueue a timer whose
/// sub-second part is under a millisecond as whole seconds, dropping that part, so a shorter delay
/// fired at once and a longer one early; a whole millisecond is passed as it is.
#[cfg(target_os = "macos")]
fn whole_millis(after: Duration) -> Duration {
    let up = after.saturating_add(Duration::from_nanos(999_999));
    Duration::new(up.as_secs(), up.subsec_millis() * 1_000_000)
}

/// Reads the result of deleting a registration: the kernel refusing because the number is closed
/// (EBADF) or has no registration (ENOENT) counts as deleted, since nothing more can be deleted
/// through that number. On Linux a registration of a closed number can still be alive (see
/// [`Poller`]); this cannot reach it.
fn delete_tolerating_gone(deleted: Result<(), Errno>) -> Result<(), Errno> {
    match deleted {
        Err(Errno::NOENT | Errno::BADF) => Ok(()),
        deleted => deleted,
    }
}

#[cfg(all(test, target_os = "linux"))]
impl Poller {
    /// Watches `fd` for reading under `token` instead of its number, as `watch` never does.
    pub(crate) fn watch_under_token(&self, fd: std::os::fd::BorrowedFd<'_>, token: u64) -> io::Result<()> {
        use rustix::event::epoll;
        Ok(epoll::add(
            &self.0.epoll,
            fd,
            epoll::EventData::new_u64(token),
            epoll::EventFlags::IN,
        )?)
    }
}

// Miri runs no system call.
#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    use super::super::ScanEnd;
    use super::*;

    fn readable(poller: &Poller) -> bool {
        use rustix::event::{poll, PollFd, PollFlags};
        let mut fds = [PollFd::new(poller.fd(), PollFlags::IN)];
        poll(&mut fds, Some(&rustix::time::Timespec { tv_sec: 0, tv_nsec: 0 })).expect("poll") == 1
    }

    fn scanned(poller: &Poller) -> Vec<(RawFd, FdInterest)> {
        let mut found = Vec::new();
        let scan = poller.scan(|fd, ready| found.push((fd, ready))).expect("a scan");
        assert_eq!(
            scan,
            Scan {
                end: ScanEnd::Drained,
                timer_fired: false
            }
        );
        found
    }

    #[test]
    fn a_watched_fd_is_found_and_makes_the_poller_readable_only_while_it_is_ready() {
        let poller = Poller::new().expect("a poller");
        let (mut a, b) = UnixStream::pair().expect("a socket pair");
        let fd = b.as_raw_fd();
        poller.watch(fd, None, Some(FdInterest::Read)).expect("watched");
        assert!(scanned(&poller).is_empty());
        assert!(!readable(&poller));
        a.write_all(b"x").expect("a byte sent");
        assert!(readable(&poller));
        assert_eq!(scanned(&poller), [(fd, FdInterest::Read)]);
        poller.watch(fd, Some(FdInterest::Read), None).expect("unwatched");
        assert!(scanned(&poller).is_empty());
        assert!(!readable(&poller));
    }

    #[test]
    fn unwatching_an_fd_the_kernel_dropped_succeeds_and_any_other_failure_does_not() {
        assert_eq!(delete_tolerating_gone(Ok(())), Ok(()));
        assert_eq!(delete_tolerating_gone(Err(Errno::NOENT)), Ok(()));
        assert_eq!(delete_tolerating_gone(Err(Errno::BADF)), Ok(()));
        assert_eq!(delete_tolerating_gone(Err(Errno::PERM)), Err(Errno::PERM));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_token_that_is_no_fd_number_fails_the_scan_after_the_other_fds_are_handed_over() {
        use std::os::fd::AsFd;
        let poller = Poller::new().expect("a poller");
        let (mut a, b) = UnixStream::pair().expect("a socket pair");
        let (mut c, d) = UnixStream::pair().expect("a socket pair");
        poller
            .watch(b.as_raw_fd(), None, Some(FdInterest::Read))
            .expect("watched");
        let stray = TIMER_TOKEN - 1;
        poller.watch_under_token(d.as_fd(), stray).expect("watched");
        a.write_all(b"x").expect("a byte sent");
        c.write_all(b"x").expect("a byte sent");
        let mut found = Vec::new();
        let failed = poller
            .scan(|fd, ready| found.push((fd, ready)))
            .expect_err("the scan fails");
        assert_eq!(found, [(b.as_raw_fd(), FdInterest::Read)]);
        assert!(failed.to_string().contains(&stray.to_string()), "{failed}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_registration_is_added_changed_deleted_or_left_as_was_and_now_say() {
        use FdInterest::{Read, ReadWrite, Write};
        assert_eq!(epoll_op(None, None), None);
        assert_eq!(epoll_op(Some(Read), Some(Read)), None);
        assert_eq!(epoll_op(None, Some(Read)), Some(EpollOp::Add(Read)));
        assert_eq!(epoll_op(Some(Read), Some(ReadWrite)), Some(EpollOp::Modify(ReadWrite)));
        assert_eq!(epoll_op(Some(ReadWrite), Some(Write)), Some(EpollOp::Modify(Write)));
        assert_eq!(epoll_op(Some(Write), None), Some(EpollOp::Delete));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_hang_up_wakes_both_sides_and_plain_readiness_only_its_own() {
        use rustix::event::epoll::EventFlags;
        assert_eq!(epoll_ready(EventFlags::IN), Some(FdInterest::Read));
        assert_eq!(epoll_ready(EventFlags::OUT), Some(FdInterest::Write));
        assert_eq!(
            epoll_ready(EventFlags::IN | EventFlags::OUT),
            Some(FdInterest::ReadWrite)
        );
        assert_eq!(epoll_ready(EventFlags::HUP), Some(FdInterest::ReadWrite));
        assert_eq!(epoll_ready(EventFlags::empty()), None);
    }

    /// Waits up to `within` for the poller's fd to turn readable, as a TPC thread's driver waits
    /// on it.
    async fn turns_readable(readiness: &PollerReadiness, within: Duration) -> bool {
        match tokio::time::timeout(within, readiness.wait()).await {
            Ok(waited) => {
                waited.expect("the reactor reports readiness");
                true
            }
            Err(_) => false,
        }
    }

    fn timer_fired(poller: &Poller) -> bool {
        poller
            .scan(|fd, _| panic!("the timer is handed out as fd {fd}"))
            .expect("a scan")
            .timer_fired
    }

    #[test]
    fn an_armed_poller_turns_readable_no_sooner_than_its_delay_and_a_disarmed_one_stays_quiet() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        rt.block_on(async {
            let poller = Poller::new().expect("a poller");
            let readiness = poller.register_with_reactor().expect("the poller's fd on the reactor");
            let delay = Duration::from_millis(20);

            let armed = std::time::Instant::now();
            poller.arm(Some(delay)).expect("arming");
            assert!(
                turns_readable(&readiness, Duration::from_secs(5)).await,
                "the timer fires"
            );
            assert!(armed.elapsed() >= delay, "fired after {:?}", armed.elapsed());
            assert!(timer_fired(&poller), "the scan says the timer fired");

            poller.arm(None).expect("disarming a timer that fired");
            assert!(!timer_fired(&poller), "a disarmed timer's firing is cleared");
            assert!(
                !turns_readable(&readiness, Duration::from_millis(50)).await,
                "a disarmed poller stays quiet"
            );

            poller.arm(Some(delay)).expect("arming again after a firing");
            assert!(
                turns_readable(&readiness, Duration::from_secs(5)).await,
                "the timer fires again"
            );
            assert!(timer_fired(&poller), "the scan says the timer fired");

            poller.arm(Some(Duration::from_secs(60))).expect("arming far ahead");
            poller.arm(None).expect("disarming before it fires");
            assert!(
                !turns_readable(&readiness, Duration::from_millis(50)).await,
                "a disarmed timer does not fire"
            );
        });
    }

    #[test]
    fn a_timer_set_for_less_than_a_millisecond_fires_no_sooner_than_its_delay() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        rt.block_on(async {
            let poller = Poller::new().expect("a poller");
            let readiness = poller.register_with_reactor().expect("the poller's fd on the reactor");
            for delay in [
                Duration::from_micros(500),
                Duration::from_micros(999),
                Duration::from_micros(1_500),
            ] {
                let armed = std::time::Instant::now();
                poller.arm(Some(delay)).expect("arming");
                assert!(
                    turns_readable(&readiness, Duration::from_secs(5)).await,
                    "the timer fires"
                );
                assert!(armed.elapsed() >= delay, "{delay:?} fired after {:?}", armed.elapsed());
                assert!(timer_fired(&poller), "the scan says the timer fired");
            }
        });
    }
}
