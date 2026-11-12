//! The fds an event loop watches, with the callbacks asyncio set on each, and the poller that
//! watches them for the loop: the table and what the kernel was told change together, only here.
//! The poller also holds the loop's timer, which the table sets as it is told and does not decide;
//! it records the deadline the kernel took, the only one that knows whether the kernel took it.
//!
//! It runs no callback and does not know when the loop turns: a turn asks it for the callbacks of
//! the fds found ready, and lets go of what it hands back after the loop's lock.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::RawFd;
use std::time::Instant;

use pyo3::prelude::*;

use super::handle::Handle;
use super::poller::{FdInterest, Poller, PollerReadiness, Scan, ScanEnd};
use super::Ready;

/// Names one of the two callbacks asyncio sets on an fd apart: the reader or the writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Side {
    Reader,
    Writer,
}

impl Side {
    fn in_interest(self, interest: FdInterest) -> bool {
        match self {
            Side::Reader => interest.reads(),
            Side::Writer => interest.writes(),
        }
    }
}

/// Holds the callbacks asyncio set on one fd. The table holds no fd with neither: a change that
/// leaves both empty takes the fd out.
struct FdWatch {
    reader: Option<Py<Handle>>,
    writer: Option<Py<Handle>>,
}

impl FdWatch {
    fn new(side: Side, callback: Py<Handle>) -> Self {
        let (reader, writer) = match side {
            Side::Reader => (Some(callback), None),
            Side::Writer => (None, Some(callback)),
        };
        FdWatch { reader, writer }
    }

    fn get(&self, side: Side) -> Option<&Py<Handle>> {
        match side {
            Side::Reader => self.reader.as_ref(),
            Side::Writer => self.writer.as_ref(),
        }
    }

    fn slot(&mut self, side: Side) -> &mut Option<Py<Handle>> {
        match side {
            Side::Reader => &mut self.reader,
            Side::Writer => &mut self.writer,
        }
    }

    fn interest(&self) -> Option<FdInterest> {
        FdInterest::of(self.reader.is_some(), self.writer.is_some())
    }
}

/// Says what one side of an fd holds, as far as a turn that finds the fd ready goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    Empty,
    Live,
    Cancelled,
}

impl Slot {
    fn of(callback: Option<&Py<Handle>>) -> Slot {
        match callback {
            None => Slot::Empty,
            Some(handle) if handle.get().is_cancelled() => Slot::Cancelled,
            Some(_) => Slot::Live,
        }
    }
}

/// Says what a turn does with one side of an fd the poller found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SideStep {
    Queue,
    Clear,
    Leave,
}

impl SideStep {
    /// Returns whether the step touched the side, which leaves the fd possibly still ready: a
    /// side left alone says nothing about the fd, as when the event came from a registration that
    /// a closed fd left behind under a number the table now holds for another file.
    fn acted(self) -> bool {
        matches!(self, SideStep::Queue | SideStep::Clear)
    }
}

/// Returns what a turn does with a side of an fd the poller found, `ready` saying whether it was
/// found ready for that side. A cancelled callback is cleared, not skipped, as asyncio's selector
/// loop does: left watched, a level-triggered fd is found ready on every turn, and the driver turns
/// without end.
fn side_step(ready: bool, slot: Slot) -> SideStep {
    match (ready, slot) {
        (true, Slot::Live) => SideStep::Queue,
        (true, Slot::Cancelled) => SideStep::Clear,
        (true, Slot::Empty) | (false, _) => SideStep::Leave,
    }
}

/// Holds the fds the loop watches, each with its callbacks, and the poller that watches them.
pub(super) struct FdTable {
    watches: HashMap<RawFd, FdWatch>,
    poller: Poller,
    /// It is the deadline the kernel accepted for the poller's timer, `None` while the timer is
    /// not set. A change the kernel refused leaves it `None`, as the timer's state is then unknown,
    /// so the next turn sets it again.
    timer: Option<Instant>,
}

/// Holds a change to an fd's watch the poller refused, which took the fd out of the table, as
/// Python's `selectors` drops the key of a change that failed: what the kernel holds for the fd is
/// then unknown. A later add of another file under the number starts over; on epoll, one of the
/// same file is refused as already watched, as with `selectors`. The callbacks it took are let go
/// of with it, after the loop's lock.
pub(super) struct WatchRefusal {
    pub(super) fd: RawFd,
    pub(super) error: io::Error,
    _taken: FdWatch,
    _replaced: Option<Py<Handle>>,
}

/// Says what a turn's look at the fds found, for how the driver waits next and for what is done
/// after the loop's lock.
pub(super) struct ReadyFds {
    /// It says whether the turn queued or cleared a callback of an fd the scan found, which may
    /// then still be ready.
    pub(super) acted: bool,
    /// It holds the poller's scan, which says whether the loop's timer fired, or its failure, after
    /// which the rest holds what the scan handed over before it failed.
    pub(super) scan: io::Result<Scan>,
    /// It holds the cancelled callbacks taken out, to be let go of after the loop's lock.
    pub(super) cleared: Vec<Py<Handle>>,
    /// It holds the changes the poller refused while cancelled callbacks were taken out.
    pub(super) refused: Vec<WatchRefusal>,
}

impl FdTable {
    pub(super) fn new() -> io::Result<Self> {
        Ok(FdTable {
            watches: HashMap::new(),
            poller: Poller::new()?,
            timer: None,
        })
    }

    pub(super) fn interest(&self, fd: RawFd) -> Option<FdInterest> {
        self.watches.get(&fd).and_then(FdWatch::interest)
    }

    /// Registers a duplicate of the poller's fd with this thread's tokio reactor; see
    /// [`Poller::register_with_reactor`].
    pub(super) fn register_poller(&self) -> io::Result<PollerReadiness> {
        self.poller.register_with_reactor()
    }

    /// Returns the deadline the poller's timer is set for, if it is set.
    pub(super) fn timer(&self) -> Option<Instant> {
        self.timer
    }

    /// Sets the loop's timer, which the table's poller holds, to fire at `at`, or disarms it for
    /// `None`, and records what the kernel took; see [`Poller::arm`].
    ///
    /// # Errors
    ///
    /// Returns the kernel's refusal, after which the timer counts as not set.
    pub(super) fn arm_timer(&mut self, at: Option<Instant>) -> io::Result<()> {
        let armed = self
            .poller
            .arm(at.map(|at| at.saturating_duration_since(Instant::now())));
        self.timer = if armed.is_ok() { at } else { None };
        armed
    }

    /// Puts `callback` in `side`'s slot of `fd`, tells the poller when what the fd is watched for
    /// changes, and returns the callback it replaced.
    ///
    /// # Errors
    ///
    /// Returns the poller's refusal, which took `fd` out of the table.
    pub(super) fn set(
        &mut self,
        fd: RawFd,
        side: Side,
        callback: Option<Py<Handle>>,
    ) -> Result<Option<Py<Handle>>, WatchRefusal> {
        set_slot(&mut self.watches, &self.poller, fd, side, callback)
    }

    /// Queues on `ready` the live callbacks of every watched fd the poller finds ready for them,
    /// as the selector loop does at the start of its turn, and takes the cancelled ones out. A
    /// table with no fd asks the poller nothing unless the loop's timer is set. A change the poller
    /// refuses on the way takes only that fd out. A failed scan still hands back what it took out
    /// before it failed.
    pub(super) fn take_ready(&mut self, py: Python<'_>, ready: &mut VecDeque<Ready>) -> ReadyFds {
        let mut acted = false;
        let mut cleared = Vec::new();
        let mut refused = Vec::new();
        if self.watches.is_empty() && self.timer.is_none() {
            return ReadyFds {
                acted,
                scan: Ok(Scan {
                    end: ScanEnd::Drained,
                    timer_fired: false,
                }),
                cleared,
                refused,
            };
        }
        let FdTable { watches, poller, .. } = self;
        let scan = poller.scan(|fd, ready_for| {
            for side in [Side::Reader, Side::Writer] {
                let Some(watch) = watches.get(&fd) else { return };
                let callback = watch.get(side);
                let step = side_step(side.in_interest(ready_for), Slot::of(callback));
                acted |= step.acted();
                match step {
                    SideStep::Queue => ready.extend(callback.map(|h| Ready::Handle(h.clone_ref(py)))),
                    SideStep::Clear => match set_slot(watches, poller, fd, side, None) {
                        Ok(taken) => cleared.extend(taken),
                        Err(refusal) => refused.push(refusal),
                    },
                    SideStep::Leave => {}
                }
            }
        });
        ReadyFds {
            acted,
            scan,
            cleared,
            refused,
        }
    }

    /// Takes every callback out, for a loop that closes; the poller keeps watching what it was
    /// told, and is closed with the loop.
    pub(super) fn take_all(&mut self) -> Vec<Py<Handle>> {
        std::mem::take(&mut self.watches)
            .into_values()
            .flat_map(|watch| [watch.reader, watch.writer])
            .flatten()
            .collect()
    }
}

/// Puts `callback` in `side`'s slot of `fd` in `watches` and tells `poller` what the fd is watched
/// for when that changes, so the two change together; [`FdTable::set`] says the rest.
fn set_slot(
    watches: &mut HashMap<RawFd, FdWatch>,
    poller: &Poller,
    fd: RawFd,
    side: Side,
    callback: Option<Py<Handle>>,
) -> Result<Option<Py<Handle>>, WatchRefusal> {
    match watches.entry(fd) {
        Entry::Vacant(vacant) => {
            let Some(callback) = callback else { return Ok(None) };
            let watch = FdWatch::new(side, callback);
            match poller.watch(fd, None, watch.interest()) {
                Ok(()) => {
                    vacant.insert(watch);
                    Ok(None)
                }
                Err(error) => Err(WatchRefusal {
                    fd,
                    error,
                    _taken: watch,
                    _replaced: None,
                }),
            }
        }
        Entry::Occupied(mut occupied) => {
            let was = occupied.get().interest();
            let replaced = std::mem::replace(occupied.get_mut().slot(side), callback);
            let now = occupied.get().interest();
            match poller.watch(fd, was, now) {
                Ok(()) => {
                    if now.is_none() {
                        occupied.remove();
                    }
                    Ok(replaced)
                }
                Err(error) => Err(WatchRefusal {
                    fd,
                    error,
                    _taken: occupied.remove(),
                    _replaced: replaced,
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(target_os = "linux", not(miri)))]
    #[test]
    fn a_failed_scan_still_hands_back_the_callbacks_it_cleared_and_the_changes_refused() {
        use std::io::Write;
        use std::os::fd::{AsFd, AsRawFd};
        use std::os::unix::net::UnixStream;

        use pyo3::types::PyTuple;

        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let handle = || Py::new(py, Handle::new(py.None(), PyTuple::empty(py), py.None())).expect("a handle");
            let mut table = FdTable::new().expect("a table");
            let (cleared, mut cleared_peer) = UnixStream::pair().expect("a socket pair");
            let cancelled = handle();
            table
                .set(cleared.as_raw_fd(), Side::Reader, Some(cancelled.clone_ref(py)))
                .map_err(|refusal| refusal.error)
                .expect("watched");
            cancelled.get().cancel();
            let (refused, mut refused_peer) = UnixStream::pair().expect("a socket pair");
            let kept = refused.try_clone().expect("a dup");
            let number = refused.as_raw_fd();
            let reader = handle();
            table
                .set(number, Side::Reader, Some(reader.clone_ref(py)))
                .map_err(|refusal| refusal.error)
                .expect("watched");
            table
                .set(number, Side::Writer, Some(handle()))
                .map_err(|refusal| refusal.error)
                .expect("watched");
            reader.get().cancel();
            let (stray, mut stray_peer) = UnixStream::pair().expect("a socket pair");
            table
                .poller
                .watch_under_token(stray.as_fd(), u64::MAX - 1)
                .expect("watched");
            drop(refused);
            for peer in [&mut cleared_peer, &mut refused_peer, &mut stray_peer] {
                peer.write_all(b"x").expect("a byte sent");
            }

            let mut ready = VecDeque::new();
            let found = table.take_ready(py, &mut ready);

            assert!(found.scan.is_err());
            assert_eq!(found.cleared.len(), 1);
            assert_eq!(found.refused.iter().map(|r| r.fd).collect::<Vec<_>>(), [number]);
            assert!(ready.is_empty());
            drop(found);
            drop(kept);
        });
    }

    #[test]
    fn a_ready_side_queues_a_live_callback_and_clears_a_cancelled_one() {
        assert_eq!(side_step(true, Slot::Live), SideStep::Queue);
        assert_eq!(side_step(true, Slot::Cancelled), SideStep::Clear);
        assert_eq!(side_step(true, Slot::Empty), SideStep::Leave);
    }

    #[test]
    fn a_side_not_found_ready_is_left_as_it_is_even_when_cancelled() {
        assert_eq!(side_step(false, Slot::Live), SideStep::Leave);
        assert_eq!(side_step(false, Slot::Cancelled), SideStep::Leave);
        assert_eq!(side_step(false, Slot::Empty), SideStep::Leave);
    }

    #[test]
    fn only_a_step_that_queues_or_clears_leaves_the_fd_possibly_still_ready() {
        assert!(SideStep::Queue.acted());
        assert!(SideStep::Clear.acted());
        assert!(!SideStep::Leave.acted());
    }

    #[test]
    fn a_side_reads_its_own_direction_of_what_an_fd_was_found_ready_for() {
        assert!(Side::Reader.in_interest(FdInterest::Read));
        assert!(!Side::Reader.in_interest(FdInterest::Write));
        assert!(Side::Writer.in_interest(FdInterest::ReadWrite));
        assert!(!Side::Writer.in_interest(FdInterest::Read));
    }
}
