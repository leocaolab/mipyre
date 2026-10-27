//! The time a connection reads, and the wake-ups at its deadlines, kept off the request path.
//!
//! A deadline is one store into an [`Alarm`]; no request inserts into or removes from a timer.
//! One thread ticks the [`Clock`]: it records the time and wakes each alarm whose deadline has
//! passed. So the time read is up to a tick old and a deadline fires up to a tick late. The clock
//! does not know what a deadline bounds; whoever armed it decides, when woken, whether it passed.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use atomic_waker::AtomicWaker;

use crate::sync::{AtomicBool, AtomicU64, Mutex, Ordering};

/// How often the process's clock ticks.
pub const TICK: Duration = Duration::from_millis(100);

/// The alarms are spread over this many lists, so connections opening on different threads seldom
/// take the same lock.
const SHARDS: usize = 16;

/// An alarm's deadline when it has none.
const UNARMED: u64 = u64::MAX;

static PROCESS: LazyLock<Clock> = LazyLock::new(Clock::new);

/// The time as of the last tick, and the alarms it wakes.
pub struct Clock {
    origin: Instant,
    /// Nanoseconds from `origin` to the last tick.
    now: AtomicU64,
    alarms: [Mutex<Vec<Weak<Alarm>>>; SHARDS],
    ticking: AtomicBool,
}

/// One deadline and the task to wake at it. It holds at most one deadline at a time.
pub struct Alarm {
    /// Nanoseconds from the clock's origin, or `UNARMED`.
    at: AtomicU64,
    waker: AtomicWaker,
}

impl Clock {
    /// Returns a clock that reads its creation time until it is ticked.
    pub fn new() -> Self {
        Self::starting_at(Instant::now())
    }

    /// Returns a clock that reads `origin` until it is ticked; a tick before `origin` reads as it.
    pub fn starting_at(origin: Instant) -> Self {
        Self {
            origin,
            now: AtomicU64::new(0),
            alarms: std::array::from_fn(|_| Mutex::new(Vec::new())),
            ticking: AtomicBool::new(false),
        }
    }

    /// Returns the clock the server's connections read; it does not move until [`Self::start`].
    pub fn process() -> &'static Clock {
        &PROCESS
    }

    /// Starts the thread that ticks this clock every [`TICK`], unless it was started already.
    ///
    /// # Errors
    ///
    /// Returns why the thread could not be spawned; the clock then stands still and no deadline
    /// fires, so the server must not serve.
    pub fn start(&'static self) -> std::io::Result<()> {
        if self.ticking.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let spawned = std::thread::Builder::new()
            .name("mipyre-clock".to_owned())
            .spawn(move || loop {
                std::thread::sleep(TICK);
                self.tick(Instant::now());
            });
        if let Err(e) = spawned {
            self.ticking.store(false, Ordering::Release);
            return Err(e);
        }
        Ok(())
    }

    /// Returns the time of the last tick.
    #[inline]
    pub fn now(&self) -> Instant {
        self.origin + Duration::from_nanos(self.now.load(Ordering::Acquire))
    }

    /// Records `now` as the time, then wakes every alarm whose deadline is at or before it and
    /// forgets the alarms nobody holds.
    pub fn tick(&self, now: Instant) {
        let now = self.since_origin(now);
        // Sequentially consistent with `Deadline::poll`'s store and load: either this tick sees
        // the deadline, or the poll sees this tick's time.
        self.now.store(now, Ordering::SeqCst);
        for shard in &self.alarms {
            shard.lock().retain(|alarm| match alarm.upgrade() {
                Some(alarm) => {
                    alarm.ring_if_due(now);
                    true
                }
                None => false,
            });
        }
    }

    /// Returns the earliest deadline armed on this clock, if any.
    pub fn next_deadline(&self) -> Option<Instant> {
        let at = self
            .alarms
            .iter()
            .flat_map(|shard| {
                shard
                    .lock()
                    .iter()
                    .filter_map(|alarm| Some(alarm.upgrade()?.at.load(Ordering::Acquire)))
                    .collect::<Vec<_>>()
            })
            .min()
            .filter(|&at| at != UNARMED)?;
        Some(self.origin + Duration::from_nanos(at))
    }

    /// Returns a new alarm this clock wakes; the clock forgets it once every holder drops it.
    pub fn alarm(&self) -> Arc<Alarm> {
        let alarm = Arc::new(Alarm {
            at: AtomicU64::new(UNARMED),
            waker: AtomicWaker::new(),
        });
        self.alarms[shard_of(&alarm)].lock().push(Arc::downgrade(&alarm));
        alarm
    }

    /// Returns a deadline at `at` that wakes its task through `alarm`, which must be this clock's.
    #[inline]
    pub fn deadline(&'static self, alarm: Arc<Alarm>, at: Instant) -> Deadline {
        Deadline {
            clock: self,
            at: self.since_origin(at),
            alarm,
        }
    }

    /// Returns `at` as nanoseconds from the origin; a time past what a `u64` holds, centuries
    /// away, is never reached and reads as the farthest.
    fn since_origin(&self, at: Instant) -> u64 {
        u64::try_from(at.saturating_duration_since(self.origin).as_nanos()).unwrap_or(u64::MAX - 1)
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

/// Picks an alarm's list by its address, which spreads the alarms of one thread as well.
fn shard_of(alarm: &Arc<Alarm>) -> usize {
    (Arc::as_ptr(alarm) as usize >> 6) % SHARDS
}

/// Says whether a deadline `at` has passed at `now`, both nanoseconds from one origin.
#[inline]
fn passed(at: u64, now: u64) -> bool {
    at <= now
}

impl Alarm {
    fn ring_if_due(&self, now: u64) {
        let at = self.at.load(Ordering::SeqCst);
        if passed(at, now)
            && self
                .at
                .compare_exchange(at, UNARMED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.waker.wake();
        }
    }
}

/// A future that is ready once its clock's time reaches `at`. It arms its alarm while it waits and
/// disarms it when dropped, so only one deadline may use an alarm at a time.
pub struct Deadline {
    clock: &'static Clock,
    at: u64,
    alarm: Arc<Alarm>,
}

impl Deadline {
    /// Moves the deadline to `at`.
    #[inline]
    pub fn reset(&mut self, at: Instant) {
        self.at = self.clock.since_origin(at);
    }
}

impl Future for Deadline {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if passed(self.at, self.clock.now.load(Ordering::Acquire)) {
            return Poll::Ready(());
        }
        self.alarm.waker.register(cx.waker());
        self.alarm.at.store(self.at, Ordering::SeqCst);
        // A tick between the check above and the store would not have seen this deadline.
        if passed(self.at, self.clock.now.load(Ordering::SeqCst)) {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

impl Drop for Deadline {
    #[inline]
    fn drop(&mut self) {
        self.alarm.at.store(UNARMED, Ordering::Release);
    }
}

/// Times hyper's HTTP/1 header reads on a clock: a header read is one deadline written, and
/// reading the time is one load. A connection's header reads take turns on one alarm.
#[derive(Clone)]
pub struct ConnectionTimer {
    clock: &'static Clock,
    alarm: Arc<Alarm>,
}

impl ConnectionTimer {
    /// Returns a timer for one connection on `clock`.
    pub fn new(clock: &'static Clock) -> Self {
        Self {
            clock,
            alarm: clock.alarm(),
        }
    }
}

impl hyper::rt::Timer for ConnectionTimer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
        self.sleep_until(self.clock.now() + duration)
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(self.clock.deadline(Arc::clone(&self.alarm), deadline))
    }

    fn now(&self) -> Instant {
        self.clock.now()
    }

    fn reset(&self, sleep: &mut Pin<Box<dyn hyper::rt::Sleep>>, new_deadline: Instant) {
        match sleep.as_mut().downcast_mut_pin::<Deadline>() {
            Some(deadline) => Pin::into_inner(deadline).reset(new_deadline),
            None => *sleep = self.sleep_until(new_deadline),
        }
    }
}

impl hyper::rt::Sleep for Deadline {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Wake, Waker};

    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    impl Count {
        fn get(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    fn leaked() -> &'static Clock {
        Box::leak(Box::new(Clock::new()))
    }

    #[test]
    fn a_deadline_has_passed_at_its_own_time_and_not_a_nanosecond_before() {
        assert!(!passed(10, 9));
        assert!(passed(10, 10));
        assert!(!passed(UNARMED, u64::MAX - 1));
    }

    #[test]
    fn a_waiting_deadline_is_woken_by_the_first_tick_at_or_past_it_and_only_once() {
        let clock = leaked();
        let count = Arc::new(Count::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let at = clock.origin + Duration::from_secs(10);
        let mut deadline = Box::pin(clock.deadline(clock.alarm(), at));

        assert!(deadline.as_mut().poll(&mut cx).is_pending());
        clock.tick(at - Duration::from_nanos(1));
        assert_eq!(count.get(), 0, "woken before its time");
        assert!(deadline.as_mut().poll(&mut cx).is_pending());

        clock.tick(at);
        assert_eq!(count.get(), 1, "woken at its time");
        clock.tick(at + TICK);
        assert_eq!(count.get(), 1, "woken again after it rang");
        assert!(deadline.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn a_dropped_deadline_wakes_nobody_and_its_alarm_is_forgotten() {
        let clock = leaked();
        let count = Arc::new(Count::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let at = clock.origin + Duration::from_secs(1);
        let mut deadline = Box::pin(clock.deadline(clock.alarm(), at));
        assert!(deadline.as_mut().poll(&mut cx).is_pending());
        drop(deadline);

        clock.tick(at);
        assert_eq!(count.get(), 0);
        assert!(clock.alarms.iter().all(|shard| shard.lock().is_empty()));
    }

    #[test]
    fn a_reset_deadline_waits_for_its_new_time() {
        let clock = leaked();
        let count = Arc::new(Count::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let first = clock.origin + Duration::from_secs(1);
        let mut deadline = Box::pin(clock.deadline(clock.alarm(), first));
        assert!(deadline.as_mut().poll(&mut cx).is_pending());
        deadline.as_mut().get_mut().reset(first + Duration::from_secs(1));
        assert!(deadline.as_mut().poll(&mut cx).is_pending());

        clock.tick(first);
        assert_eq!(count.get(), 0);
        assert!(deadline.as_mut().poll(&mut cx).is_pending());
        clock.tick(first + Duration::from_secs(1));
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn the_time_read_is_the_last_tick() {
        let clock = leaked();
        let at = clock.origin + Duration::from_millis(1500);
        assert_eq!(clock.now(), clock.origin);
        clock.tick(at);
        assert_eq!(clock.now(), at);
    }
}
