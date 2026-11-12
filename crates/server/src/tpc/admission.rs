//! Decides whether a TPC thread takes one more `async def` request onto its event loop, before its
//! body is read and again once the body's bytes cross the small-body line.
//!
//! It counts the requests reading their bodies; the loop counts its live Tasks. It does not know
//! Tasks: a request's seat is given back once its Task is started, and from then on the loop
//! counts it. Nothing here waits: a request over a limit is refused at once.

use std::cell::Cell;
use std::num::NonZeroUsize;

use mipyre_http::io::body::BodyGate;
use mipyre_http::plan::limits::ByteLimit;
use mipyre_runtime::exchange::{refuse, Capacity, HandlerError, UnansweredError};
use mipyre_runtime::LiveTasks;

/// How many `async def` requests one TPC thread holds at once, and how many of them with a large
/// body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoopLimits {
    tasks: NonZeroUsize,
    large_bodies: NonZeroUsize,
}

impl LoopLimits {
    /// Sets how many requests a thread holds when `async_queue_capacity` is not set.
    pub const DEFAULT_TASKS: NonZeroUsize = NonZeroUsize::new(128).expect("128 is a positive literal");

    /// Holds `tasks` requests per thread, a sixteenth of them with large bodies.
    pub fn new(tasks: NonZeroUsize) -> Self {
        LoopLimits {
            tasks,
            large_bodies: large_body_limit(tasks),
        }
    }

    pub fn tasks(&self) -> NonZeroUsize {
        self.tasks
    }

    pub fn large_bodies(&self) -> NonZeroUsize {
        self.large_bodies
    }
}

/// Returns how many of `tasks` requests may hold a large body: a sixteenth, and at least one.
fn large_body_limit(tasks: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::new(tasks.get() / 16).unwrap_or(NonZeroUsize::MIN)
}

/// Returns whether one more request fits beside the `taken` ones under `limit`.
fn admits(taken: usize, limit: NonZeroUsize) -> bool {
    taken < limit.get()
}

/// Counts one thread's `async def` requests that are reading their bodies, and how many of those
/// hold a large seat.
#[derive(Default)]
pub(crate) struct LoopSeats {
    reading: Cell<usize>,
    reading_large: Cell<usize>,
}

impl LoopSeats {
    /// Seats a request beside the loop's `live` Tasks under `limits`; `declared_large` takes a
    /// large seat now, for a body whose `Content-Length` crosses the small-body line.
    ///
    /// # Errors
    ///
    /// Refuses as overloaded when the thread holds `limits` already.
    pub(crate) fn take(
        &self,
        live: LiveTasks,
        limits: LoopLimits,
        declared_large: bool,
    ) -> Result<LoopSeat<'_>, HandlerError> {
        if !admits(live.count + self.reading.get(), limits.tasks) {
            return Err(overloaded(Capacity::LoopTasks));
        }
        if declared_large && !admits(live.large + self.reading_large.get(), limits.large_bodies) {
            return Err(overloaded(Capacity::LoopLargeBodies));
        }
        self.reading.set(self.reading.get() + 1);
        if declared_large {
            self.reading_large.set(self.reading_large.get() + 1);
        }
        Ok(LoopSeat {
            seats: self,
            large: declared_large,
        })
    }
}

/// One request's seat while it reads its body; dropping it gives the seat back.
pub(crate) struct LoopSeat<'s> {
    seats: &'s LoopSeats,
    large: bool,
}

impl LoopSeat<'_> {
    pub(crate) fn large(&self) -> bool {
        self.large
    }

    /// Takes a large seat in place of the small one, beside the loop's `live` Tasks.
    ///
    /// # Errors
    ///
    /// Refuses as overloaded when the thread's large seats are taken.
    fn grow(&mut self, live: LiveTasks, limits: LoopLimits) -> Result<(), HandlerError> {
        if self.large {
            return Ok(());
        }
        if !admits(live.large + self.seats.reading_large.get(), limits.large_bodies) {
            return Err(overloaded(Capacity::LoopLargeBodies));
        }
        self.seats.reading_large.set(self.seats.reading_large.get() + 1);
        self.large = true;
        Ok(())
    }
}

impl Drop for LoopSeat<'_> {
    fn drop(&mut self) {
        self.seats.reading.set(self.seats.reading.get() - 1);
        if self.large {
            self.seats.reading_large.set(self.seats.reading_large.get() - 1);
        }
    }
}

/// The body gate of a seated request: once the bytes buffered cross `skip_bytes` its seat must
/// grow to a large one, so a body that declared less than it sends is counted by what it sent.
pub(crate) struct LoopAdmission<'a, 's, L: Fn() -> Option<LiveTasks>> {
    pub(crate) seat: &'a mut LoopSeat<'s>,
    pub(crate) skip_bytes: ByteLimit,
    pub(crate) limits: LoopLimits,
    /// Reads the loop's live Tasks at the moment the seat grows, `None` once the loop is closed.
    pub(crate) live: L,
}

impl<L: Fn() -> Option<LiveTasks>> BodyGate for LoopAdmission<'_, '_, L> {
    type Error = HandlerError;

    fn admit(&mut self, held: usize) -> Result<(), HandlerError> {
        if self.seat.large || !self.skip_bytes.is_exceeded_by(held as u64) {
            return Ok(());
        }
        let Some(live) = (self.live)() else {
            return Err(refuse(UnansweredError::LoopClosed));
        };
        self.seat.grow(live, self.limits)
    }
}

fn overloaded(capacity: Capacity) -> HandlerError {
    refuse(UnansweredError::Overloaded(capacity))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONE_LIVE: LiveTasks = LiveTasks { count: 0, large: 0 };

    fn limits(tasks: usize) -> LoopLimits {
        LoopLimits::new(NonZeroUsize::new(tasks).expect("a positive limit"))
    }

    fn refused_for(refused: Result<impl Sized, HandlerError>) -> Capacity {
        match refused {
            Err(HandlerError::Refused(UnansweredError::Overloaded(capacity))) => capacity,
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("admitted"),
        }
    }

    #[test]
    fn a_thread_admits_below_its_limit_and_refuses_at_it() {
        let limit = NonZeroUsize::new(128).expect("128");
        assert!(admits(127, limit));
        assert!(!admits(128, limit));
    }

    #[test]
    fn the_large_body_limit_is_a_sixteenth_and_at_least_one() {
        let of = |tasks: usize| large_body_limit(NonZeroUsize::new(tasks).expect("positive")).get();
        assert_eq!(of(128), 8);
        assert_eq!(of(16), 1);
        assert_eq!(of(15), 1);
    }

    #[test]
    fn a_seat_counts_until_dropped_and_the_live_tasks_count_with_the_seats() {
        let seats = LoopSeats::default();
        let first = seats.take(NONE_LIVE, limits(2), false).expect("the first of two");
        assert_eq!(
            refused_for(seats.take(LiveTasks { count: 1, large: 0 }, limits(2), false)),
            Capacity::LoopTasks
        );
        drop(first);
        assert!(seats.take(LiveTasks { count: 1, large: 0 }, limits(2), false).is_ok());
    }

    #[test]
    fn a_declared_large_body_is_refused_before_it_is_read_once_the_large_seats_are_taken() {
        let seats = LoopSeats::default();
        let large = seats.take(NONE_LIVE, limits(16), true).expect("the one large seat");
        assert!(large.large());
        assert_eq!(
            refused_for(seats.take(NONE_LIVE, limits(16), true)),
            Capacity::LoopLargeBodies
        );
        assert!(
            seats.take(NONE_LIVE, limits(16), false).is_ok(),
            "a small one still fits"
        );
    }

    #[test]
    fn an_undeclared_body_takes_a_large_seat_when_its_bytes_cross_the_line() {
        let seats = LoopSeats::default();
        let skip = ByteLimit(64);
        let mut seat = seats.take(NONE_LIVE, limits(16), false).expect("a seat");
        let mut gate = LoopAdmission {
            seat: &mut seat,
            skip_bytes: skip,
            limits: limits(16),
            live: || Some(NONE_LIVE),
        };
        gate.admit(64).expect("at the line it stays small");
        assert!(!gate.seat.large());
        gate.admit(65).expect("past the line it grows");
        assert!(gate.seat.large());

        let mut other = seats.take(NONE_LIVE, limits(16), false).expect("a second seat");
        let mut full = LoopAdmission {
            seat: &mut other,
            skip_bytes: skip,
            limits: limits(16),
            live: || Some(NONE_LIVE),
        };
        assert_eq!(refused_for(full.admit(65)), Capacity::LoopLargeBodies);
    }
}
