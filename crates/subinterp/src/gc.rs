//! Decides when a worker's `gc.collect()` is due; running it is the worker's job.

use std::num::NonZeroU64;
use std::str::FromStr;
use std::time::Duration;

/// The GC mode named by `MIPYRE_GC_MODE`.
///
/// `count` is predictable but can pause during a burst; `idle` puts the pause in a lull and
/// needs the per-thread-listener TPC topology; under `off`, `gc.disable()` still runs at
/// worker start, so only the app's own `gc.collect()` calls collect cycles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcMode {
    Count,
    Idle,
    Off,
}

impl std::fmt::Display for GcMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            GcMode::Count => "count",
            GcMode::Idle => "idle",
            GcMode::Off => "off",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not a GC mode (expected \"count\", \"idle\" or \"off\")")]
pub struct UnknownGcMode(pub String);

impl FromStr for GcMode {
    type Err = UnknownGcMode;

    fn from_str(raw: &str) -> Result<Self, UnknownGcMode> {
        match raw {
            "count" => Ok(GcMode::Count),
            "idle" => Ok(GcMode::Idle),
            "off" => Ok(GcMode::Off),
            other => Err(UnknownGcMode(other.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcPolicy {
    /// Each worker collects every `every` requests; a threshold of 0 is [`GcPolicy::Off`].
    Count { every: NonZeroU64 },
    /// The idle tick collects once a worker ran requests and then none for a whole `tick`.
    ///
    /// A worker also collects after `failsafe` requests, so sustained traffic cannot starve the
    /// collector; `None` collects only in lulls.
    Idle {
        failsafe: Option<NonZeroU64>,
        tick: Duration,
    },
    Off,
}

impl GcPolicy {
    pub fn mode(&self) -> GcMode {
        match self {
            GcPolicy::Count { .. } => GcMode::Count,
            GcPolicy::Idle { .. } => GcMode::Idle,
            GcPolicy::Off => GcMode::Off,
        }
    }

    /// Returns how often the idle tick looks for a lull, only in idle mode.
    pub fn idle_tick(&self) -> Option<Duration> {
        match *self {
            GcPolicy::Idle { tick, .. } => Some(tick),
            _ => None,
        }
    }
}

/// A worker's collection schedule, counted in units of work.
///
/// Only a collection that returned is booked with [`Self::collected`], so one that failed stays
/// due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcSchedule {
    count: GcCount,
    in_lulls: bool,
    /// Kept in every mode so that `after_unit` stores without a branch; read only in idle mode.
    lull: Lull,
}

impl GcSchedule {
    pub fn new(policy: GcPolicy) -> Self {
        let (every, in_lulls) = match policy {
            GcPolicy::Count { every } => (Some(every), false),
            GcPolicy::Idle { failsafe, .. } => (failsafe, true),
            GcPolicy::Off => (None, false),
        };
        GcSchedule {
            count: GcCount::new(every),
            in_lulls,
            lull: Lull::Collected,
        }
    }

    /// Counts a finished unit and returns whether a collection is now due.
    #[inline]
    pub fn after_unit(&mut self) -> bool {
        self.lull = Lull::Ran;
        self.count.count_due()
    }

    /// Returns whether a collection is due because units ran since the last one and none since
    /// the previous tick; it is never due in count and off modes.
    pub fn on_idle_tick(&mut self) -> bool {
        self.in_lulls && self.lull.on_tick()
    }

    /// Books a collection that returned, so both triggers start again.
    #[inline]
    pub fn collected(&mut self) {
        self.count.collected();
        self.lull = Lull::Collected;
    }
}

/// The count stops at `every`, since a due collection stays due until one returns, so `ran` is
/// at most `every` (0 without one) and cannot overflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GcCount {
    every: Option<NonZeroU64>,
    ran: u64,
}

impl GcCount {
    fn new(every: Option<NonZeroU64>) -> Self {
        GcCount { every, ran: 0 }
    }

    #[inline]
    fn count_due(&mut self) -> bool {
        // Branch-free because it runs after every unit; without a threshold `ran < 0` never holds.
        let every = self.every.map_or(0, NonZeroU64::get);
        self.ran += u64::from(self.ran < every);
        self.due()
    }

    fn due(&self) -> bool {
        self.every.is_some_and(|every| self.ran >= every.get())
    }

    fn collected(&mut self) {
        self.ran = 0;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lull {
    Collected,
    Ran,
    /// Units ran since the last collection and none since the previous tick.
    Quiet,
}

impl Lull {
    /// Returns whether to collect at this tick; a quiet worker stays quiet, so a collection
    /// that failed is due again at the next tick.
    fn on_tick(&mut self) -> bool {
        match *self {
            Lull::Collected => false,
            Lull::Ran => {
                *self = Lull::Quiet;
                false
            }
            Lull::Quiet => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every(n: u64) -> Option<NonZeroU64> {
        NonZeroU64::new(n)
    }

    fn counted(every: u64, units: u64) -> GcCount {
        let mut count = GcCount::new(NonZeroU64::new(every));
        for _ in 0..units {
            count.count_due();
        }
        count
    }

    #[test]
    fn a_count_is_due_at_its_threshold_and_not_one_unit_before() {
        assert!(!counted(3, 2).due());
        assert!(counted(3, 3).due());
        assert!(counted(3, 4).due());
        assert!(counted(1, 1).due());
        assert!(!counted(1, 0).due());
    }

    #[test]
    fn a_count_stops_at_its_threshold() {
        assert_eq!(counted(3, 1_000), counted(3, 3));
    }

    #[test]
    fn a_schedule_without_a_count_threshold_is_never_due_after_a_unit() {
        assert_eq!(ran(GcPolicy::Off, 1_000).1, [false; 1_000]);
        assert_eq!(ran(idle(None), 1_000).1, [false; 1_000]);
    }

    fn ran(policy: GcPolicy, units: u64) -> (GcSchedule, Vec<bool>) {
        let mut schedule = GcSchedule::new(policy);
        let due = (0..units).map(|_| schedule.after_unit()).collect();
        (schedule, due)
    }

    #[test]
    fn after_unit_says_due_at_the_threshold_unit_and_not_one_unit_before() {
        assert_eq!(ran(count(3), 3).1, [false, false, true]);
        assert_eq!(ran(count(1), 1).1, [true]);
        assert_eq!(ran(GcPolicy::Off, 5).1, [false; 5]);
    }

    #[test]
    fn a_collection_that_returned_starts_the_count_again() {
        let (mut schedule, _) = ran(count(3), 3);
        schedule.collected();
        assert_eq!(schedule, GcSchedule::new(count(3)));
        assert!(!schedule.after_unit());
        assert!(!schedule.after_unit());
        assert!(schedule.after_unit());
    }

    #[test]
    fn a_due_collection_that_is_not_booked_stays_due() {
        let (mut schedule, _) = ran(idle(every(3)), 3);
        assert!(schedule.after_unit());
        assert!(schedule.after_unit(), "and at the unit after that");
        assert!(!schedule.on_idle_tick(), "units ran since the previous tick");
        assert!(schedule.on_idle_tick(), "quiet, and still uncollected");
    }

    #[test]
    fn a_lull_collects_after_a_quiet_tick() {
        let mut lull = Lull::Collected;
        assert!(!lull.on_tick());
        lull = Lull::Ran;
        assert!(!lull.on_tick());
        assert!(lull.on_tick());
        assert!(lull.on_tick());
        lull = Lull::Collected;
        assert!(!lull.on_tick());
        lull = Lull::Ran;
        assert!(!lull.on_tick());
        lull = Lull::Ran;
        assert!(!lull.on_tick());
        assert!(lull.on_tick());
    }

    fn due(schedule: &mut GcSchedule, event: Event) -> bool {
        match event {
            Event::Request => schedule.after_unit(),
            Event::Tick => schedule.on_idle_tick(),
        }
    }

    fn settle(schedule: &mut GcSchedule, outcome: Outcome) {
        if outcome == Outcome::Returned {
            schedule.collected();
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Event {
        Request,
        Tick,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Outcome {
        Returned,
        Failed,
    }

    /// Parses a script step: `req` or `tick`, then `:gc` for a due collection that returned or
    /// `:gc-failed` for one that did not, then `*n` to repeat it.
    fn step(token: &str) -> (Event, Option<Outcome>, usize) {
        let (step, times) = match token.split_once('*') {
            Some((step, times)) => (step, times.parse().expect("a repeat count")),
            None => (token, 1),
        };
        let (event, due) = step.split_once(':').unwrap_or((step, ""));
        let event = match event {
            "req" => Event::Request,
            "tick" => Event::Tick,
            other => panic!("{other:?} is not an event"),
        };
        let due = match due {
            "" => None,
            "gc" => Some(Outcome::Returned),
            "gc-failed" => Some(Outcome::Failed),
            other => panic!("{other:?} is not a collection"),
        };
        (event, due, times)
    }

    fn run(policy: GcPolicy, script: &str) {
        let mut schedule = GcSchedule::new(policy);
        for (at, token) in script.split_whitespace().enumerate() {
            let (event, expected, times) = step(token);
            for _ in 0..times {
                let due = due(&mut schedule, event);
                assert_eq!(
                    due,
                    expected.is_some(),
                    "{policy:?}, step {at} ({token:?}) of {script:?}"
                );
                if let Some(outcome) = expected {
                    settle(&mut schedule, outcome);
                }
            }
        }
    }

    fn count(every: u64) -> GcPolicy {
        GcPolicy::Count {
            every: NonZeroU64::new(every).expect("a count policy collects"),
        }
    }

    fn idle(failsafe: Option<NonZeroU64>) -> GcPolicy {
        GcPolicy::Idle {
            failsafe,
            tick: Duration::from_millis(100),
        }
    }

    #[test]
    fn idle_mode_collects_at_the_first_quiet_tick_or_at_the_failsafe_whichever_comes_first() {
        run(idle(every(3)), "req req tick tick:gc");
        run(idle(every(3)), "req tick req tick req:gc tick");
        run(idle(None), "req*200000 tick tick:gc");
    }

    #[test]
    fn a_collection_by_either_trigger_starts_both_again() {
        run(idle(every(3)), "req req tick tick:gc tick req req req:gc");
        run(idle(every(3)), "req req tick tick:gc req tick tick:gc");
        // The tick that saw two requests before the failsafe must not mistake two requests
        // after it for quiet.
        run(idle(every(3)), "req req tick req:gc req req tick tick:gc");
    }

    #[test]
    fn idle_mode_keeps_a_failed_collection_due() {
        run(idle(every(3)), "req req tick tick:gc-failed tick:gc");
        run(idle(every(3)), "req req req:gc-failed req:gc-failed tick tick:gc");
    }

    #[test]
    fn count_mode_collects_every_n_requests_and_never_at_a_tick() {
        run(count(3), "req req req:gc req req req:gc");
        run(count(1), "req:gc req:gc tick");
        run(count(3), "req tick tick tick req req:gc tick tick");
        run(count(3), "req req req:gc-failed req:gc req req req:gc");
    }

    #[test]
    fn off_mode_never_collects() {
        run(GcPolicy::Off, "req*200000 tick tick req tick tick");
    }

    /// Recomputes from the whole history, rather than incrementally, whether `now` makes a
    /// collection due.
    fn reference_due(policy: GcPolicy, history: &[(Event, Option<Outcome>)], now: Event) -> bool {
        let since_collect = history
            .iter()
            .rposition(|&(_, due)| due == Some(Outcome::Returned))
            .map_or(history, |at| &history[at + 1..]);
        let requests = |steps: &[(Event, Option<Outcome>)]| {
            steps.iter().filter(|&&(event, _)| event == Event::Request).count() as u64
        };
        match now {
            Event::Request => {
                let threshold = match policy {
                    GcPolicy::Count { every } => Some(every),
                    GcPolicy::Idle { failsafe, .. } => failsafe,
                    GcPolicy::Off => None,
                };
                threshold.is_some_and(|threshold| requests(since_collect) + 1 >= threshold.get())
            }
            Event::Tick => {
                let since_tick = history
                    .iter()
                    .rposition(|&(event, _)| event == Event::Tick)
                    .map_or(history, |at| &history[at + 1..]);
                matches!(policy, GcPolicy::Idle { .. })
                    && requests(since_collect) > 0
                    && requests(since_tick) == 0
            }
        }
    }

    /// Checks every sequence of up to `depth` more steps against [`reference_due`], with each
    /// due collection both returning and failing.
    fn explore(
        policy: GcPolicy,
        schedule: GcSchedule,
        history: &mut Vec<(Event, Option<Outcome>)>,
        depth: usize,
    ) {
        if depth == 0 {
            return;
        }
        for event in [Event::Request, Event::Tick] {
            let mut next = schedule;
            let due = due(&mut next, event);
            assert_eq!(
                due,
                reference_due(policy, history, event),
                "{policy:?} after {history:?}, then {event:?}"
            );
            let outcomes: &[Option<Outcome>] = if due {
                &[Some(Outcome::Returned), Some(Outcome::Failed)]
            } else {
                &[None]
            };
            for &outcome in outcomes {
                let mut settled = next;
                if let Some(outcome) = outcome {
                    settle(&mut settled, outcome);
                }
                history.push((event, outcome));
                explore(policy, settled, history, depth - 1);
                history.pop();
            }
        }
    }

    /// Nine steps reach each threshold up to 3 more than twice.
    #[test]
    fn every_short_sequence_matches_the_policy_read_off_its_history() {
        let policies = [
            count(1),
            count(2),
            count(3),
            idle(None),
            idle(every(1)),
            idle(every(2)),
            idle(every(3)),
            GcPolicy::Off,
        ];
        for policy in policies {
            explore(policy, GcSchedule::new(policy), &mut Vec::new(), 9);
        }
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// Returns any count `GcCount::new` and its steps can reach; the three proofs below are the
    /// induction that shows these are all of them.
    fn any_reachable_count() -> GcCount {
        let every: Option<NonZeroU64> = kani::any();
        let ran: u64 = kani::any();
        kani::assume(ran <= every.map_or(0, NonZeroU64::get));
        GcCount { every, ran }
    }

    #[kani::proof]
    fn a_new_count_is_within_its_threshold_and_not_due() {
        let count = GcCount::new(kani::any());
        assert_eq!(count.ran, 0);
        assert!(!count.due());
    }

    #[kani::proof]
    fn a_unit_keeps_the_count_within_its_threshold_and_is_due_exactly_at_it() {
        let mut count = any_reachable_count();
        let before = count.ran;
        let due = count.count_due();
        match count.every.map(NonZeroU64::get) {
            Some(every) => {
                assert_eq!(count.ran, if before < every { before + 1 } else { every });
                assert_eq!(due, before >= every - 1);
            }
            None => {
                assert_eq!(count.ran, 0);
                assert!(!due);
            }
        }
    }

    #[kani::proof]
    fn a_collection_starts_the_count_again() {
        let mut count = any_reachable_count();
        count.collected();
        assert_eq!(count, GcCount::new(count.every));
    }

    fn any_policy() -> GcPolicy {
        match kani::any::<u8>() % 3 {
            0 => GcPolicy::Count { every: kani::any() },
            1 => GcPolicy::Idle {
                failsafe: kani::any(),
                tick: Duration::ZERO,
            },
            _ => GcPolicy::Off,
        }
    }

    const STEPS: u64 = 8;

    #[kani::proof]
    #[kani::unwind(9)]
    fn the_schedule_says_what_the_policy_read_off_its_history_says() {
        let policy = any_policy();
        let threshold = match policy {
            GcPolicy::Count { every } => Some(every),
            GcPolicy::Idle { failsafe, .. } => failsafe,
            GcPolicy::Off => None,
        };
        let lulls = matches!(policy, GcPolicy::Idle { .. });
        let mut schedule = GcSchedule::new(policy);
        let (mut since_collect, mut since_tick) = (0u64, 0u64);
        for _ in 0..STEPS {
            let due = if kani::any() {
                since_collect += 1;
                since_tick += 1;
                let due = schedule.after_unit();
                assert_eq!(due, threshold.is_some_and(|t| since_collect >= t.get()));
                due
            } else {
                let due = schedule.on_idle_tick();
                assert_eq!(due, lulls && since_collect > 0 && since_tick == 0);
                since_tick = 0;
                due
            };
            if due && kani::any() {
                schedule.collected();
                since_collect = 0;
            }
        }
    }
}
