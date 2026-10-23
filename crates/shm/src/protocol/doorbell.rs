//! Publishes a counter and wakes the other side through a [`Doorbell`]; waits for a counter to move, spinning first
//! and then sleeping one slice at a time through a [`Parker`].

use crate::plan::spin_window::{SpinStep, SpinWindow};
use crate::sync::{fence, spin_loop, Ordering::*};

/// The word a doorbell rings on and a parker sleeps on, so a port implemented outside this package can name it.
pub use crate::sync::AtomicU32;

/// Wakes the other side, which sleeps on `word`. Each direction has its own outcome and error: waking a peer that
/// sleeps on a futex can neither fail nor find no one, while a byte to a host can find the host gone.
pub trait Doorbell {
    type Outcome;
    type Error;
    fn ring(&self, word: &AtomicU32) -> Result<Self::Outcome, Self::Error>;
}

/// Sleeps while `word` holds `expect`, for at most `slice_ns`; any `Ok` means "look at `word` again", since a sleep
/// may also end for nothing.
pub trait Parker {
    type Error;
    fn park(&self, word: &AtomicU32, expect: u32, slice_ns: u64) -> Result<(), Self::Error>;
}

/// How one slice of waiting for a counter ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkOutcome {
    /// The counter holds this value, which is not the one waited past.
    Changed(u32),
    /// The slice passed and the counter had not moved.
    SliceOver,
}

/// How a spin ended: `ready` is `false` when the window ran out first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpinOutcome {
    pub rounds: u32,
    pub ready: bool,
}

/// Stores `value` into `counter` and rings `bell` when the other side said, in `sleeping`, that it sleeps;
/// `Ok(None)` is a publish that rang nothing.
///
/// The store and the look at `sleeping` are one half of a store-buffering pair, [`park_slice`] (or the host's
/// `sleep_all`) the other: each side writes its own word, then a `SeqCst` fence, then reads the other's. Of the two
/// fences one comes first, and the side after it reads what the side before it wrote, so either the sleeper sees
/// the new value and does not sleep or the publisher sees it sleeping and rings. Fences and not `SeqCst` accesses,
/// because Loom checks the fences exactly and treats a `SeqCst` access as `AcqRel`.
///
/// # Errors
///
/// The bell's error, after the value is published.
#[inline]
pub fn publish<B: Doorbell>(
    counter: &AtomicU32,
    sleeping: &AtomicU32,
    value: u32,
    bell: &B,
) -> Result<Option<B::Outcome>, B::Error> {
    counter.store(value, Release);
    fence(SeqCst);
    if sleeping.load(Relaxed) != 1 {
        return Ok(None);
    }
    bell.ring(counter).map(Some)
}

/// Sleeps one slice of `slice_ns` until `counter` is no longer `seen`, raising `sleeping` and looking at the counter
/// once more, after a `SeqCst` fence, before it sleeps, so a publisher cannot miss it (the pair is told at
/// [`publish`]); `sleeping` is down again when it returns.
///
/// # Errors
///
/// The parker's error: the OS refused the sleep.
pub fn park_slice<P: Parker>(
    counter: &AtomicU32,
    sleeping: &AtomicU32,
    seen: u32,
    slice_ns: u64,
    parker: &P,
) -> Result<ParkOutcome, P::Error> {
    sleeping.store(1, Relaxed);
    fence(SeqCst);
    let before = counter.load(Acquire);
    if before != seen {
        sleeping.store(0, Relaxed);
        return Ok(ParkOutcome::Changed(before));
    }
    let parked = parker.park(counter, seen, slice_ns);
    sleeping.store(0, Relaxed);
    parked?;
    match counter.load(Acquire) {
        after if after != seen => Ok(ParkOutcome::Changed(after)),
        _ => Ok(ParkOutcome::SliceOver),
    }
}

/// Polls `ready` until it returns `true` or `window` is over; the caller handles what was found after the spin, so
/// the loop stays a bare readiness check. `now` is the caller's clock, read only when the window asks.
#[inline(always)]
pub fn spin(window: SpinWindow, mut now: impl FnMut() -> u64, mut ready: impl FnMut() -> bool) -> SpinOutcome {
    if window.step(0, &mut now) == SpinStep::Park {
        return SpinOutcome {
            rounds: 0,
            ready: ready(),
        };
    }
    let mut rounds = 0u32;
    loop {
        if ready() {
            return SpinOutcome { rounds, ready: true };
        }
        rounds = rounds.wrapping_add(1);
        if window.step(rounds, &mut now) == SpinStep::Park {
            return SpinOutcome { rounds, ready: false };
        }
        spin_loop();
    }
}

#[cfg(test)]
#[cfg(not(shm_model))]
mod tests {
    use super::*;
    use crate::plan::spin_window::CLOCK_EVERY;
    use std::cell::Cell;
    use std::convert::Infallible;

    /// A bell that counts its rings and answers each with `outcome`.
    struct FakeBell<O: Copy, E: Copy> {
        rings: Cell<u32>,
        outcome: Result<O, E>,
    }

    impl<O: Copy, E: Copy> Doorbell for FakeBell<O, E> {
        type Outcome = O;
        type Error = E;
        fn ring(&self, _: &AtomicU32) -> Result<O, E> {
            self.rings.set(self.rings.get() + 1);
            self.outcome
        }
    }

    fn bell<O: Copy, E: Copy>(outcome: Result<O, E>) -> FakeBell<O, E> {
        FakeBell {
            rings: Cell::new(0),
            outcome,
        }
    }

    /// A parker that runs `slice` in place of a sleep.
    struct SliceParker<F: Fn(&AtomicU32) -> Result<(), &'static str>>(F);

    impl<F: Fn(&AtomicU32) -> Result<(), &'static str>> Parker for SliceParker<F> {
        type Error = &'static str;
        fn park(&self, word: &AtomicU32, _: u32, _: u64) -> Result<(), &'static str> {
            (self.0)(word)
        }
    }

    #[test]
    fn a_publish_to_an_awake_side_rings_nothing() {
        let (counter, sleeping, b) = (AtomicU32::new(0), AtomicU32::new(0), bell::<(), Infallible>(Ok(())));
        assert_eq!(publish(&counter, &sleeping, 7, &b), Ok(None));
        assert_eq!((counter.load(SeqCst), b.rings.get()), (7, 0));
    }

    #[test]
    fn a_publish_to_a_sleeping_side_rings_once_and_returns_the_bells_outcome() {
        let (counter, sleeping, b) = (AtomicU32::new(0), AtomicU32::new(1), bell::<u8, ()>(Ok(3)));
        assert_eq!(publish(&counter, &sleeping, 7, &b), Ok(Some(3)));
        assert_eq!((counter.load(SeqCst), b.rings.get()), (7, 1));
    }

    #[test]
    fn a_bell_that_fails_fails_the_publish_after_the_value_is_out() {
        let (counter, sleeping, b) = (AtomicU32::new(0), AtomicU32::new(1), bell::<(), &str>(Err("broken")));
        assert_eq!(publish(&counter, &sleeping, 7, &b), Err("broken"));
        assert_eq!(counter.load(SeqCst), 7);
    }

    #[test]
    fn a_change_already_there_is_returned_without_parking() {
        let (counter, sleeping) = (AtomicU32::new(3), AtomicU32::new(0));
        let parker = SliceParker(|_| panic!("no park for a change already there"));
        assert_eq!(
            park_slice(&counter, &sleeping, 2, 1, &parker),
            Ok(ParkOutcome::Changed(3))
        );
        assert_eq!(sleeping.load(SeqCst), 0);
    }

    #[test]
    fn a_slice_in_which_the_counter_moves_returns_its_value() {
        let (counter, sleeping) = (AtomicU32::new(0), AtomicU32::new(0));
        let parker = SliceParker(|word| {
            assert_eq!(sleeping.load(SeqCst), 1, "the flag is up while parked");
            word.store(1, SeqCst);
            Ok(())
        });
        assert_eq!(
            park_slice(&counter, &sleeping, 0, 1, &parker),
            Ok(ParkOutcome::Changed(1))
        );
        assert_eq!(sleeping.load(SeqCst), 0);
    }

    #[test]
    fn a_slice_that_passes_without_a_change_says_so() {
        let (counter, sleeping) = (AtomicU32::new(0), AtomicU32::new(0));
        let parker = SliceParker(|_| Ok(()));
        assert_eq!(
            park_slice(&counter, &sleeping, 0, 1, &parker),
            Ok(ParkOutcome::SliceOver)
        );
        assert_eq!(sleeping.load(SeqCst), 0);
    }

    #[test]
    fn a_refused_park_ends_the_slice_with_its_error_and_lowers_the_flag() {
        let (counter, sleeping) = (AtomicU32::new(0), AtomicU32::new(0));
        let parker = SliceParker(|_| Err("refused"));
        assert_eq!(park_slice(&counter, &sleeping, 0, 1, &parker), Err("refused"));
        assert_eq!(sleeping.load(SeqCst), 0);
    }

    #[test]
    fn a_spin_counts_the_empty_polls_before_the_ready_one() {
        let mut polls = 0;
        let spun = spin(
            SpinWindow::until(u64::MAX),
            || 0,
            || {
                polls += 1;
                polls == 5
            },
        );
        assert_eq!(spun, SpinOutcome { rounds: 4, ready: true });
    }

    #[test]
    fn a_spin_that_finds_nothing_ends_at_the_deadline() {
        let mut clock = [0, 99, 100].into_iter();
        let spun = spin(SpinWindow::until(100), || clock.next().unwrap(), || false);
        assert_eq!(
            spun,
            SpinOutcome {
                rounds: 2 * CLOCK_EVERY,
                ready: false
            }
        );
    }

    #[test]
    fn a_window_already_over_polls_once() {
        assert_eq!(
            spin(SpinWindow::until(5), || 5, || true),
            SpinOutcome { rounds: 0, ready: true }
        );
        assert_eq!(
            spin(SpinWindow::until(5), || 5, || false),
            SpinOutcome {
                rounds: 0,
                ready: false
            }
        );
    }
}
