//! Makes the process lanes' decisions as pure functions: what a failure does to the run, which
//! lane or host takes a request, when a handler has overrun and when a crashing lane respawns.
//!
//! Times come in as values from the caller's monotonic clock, so nothing here does IO.

use std::sync::Arc;
use std::time::Duration;

use crate::exchange::{Capacity, LaneIndex, UnansweredError, WorkerExit, WorkerGroup};

/// Says what a lost lane or a failed host does to the server run (`MIPYRE_LANE_HOST_FAILURE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailurePolicy {
    /// The failed part is taken out, and the server ends only once no lane is left.
    Isolate,
    /// The server ends at the first failure, as the kernel does with `panic_on_oops`.
    Exit,
}

/// Tracks a server run's failures and keeps its first cause; each run starts a new one.
#[derive(Debug)]
pub(crate) struct RunHealth<C> {
    lanes: usize,
    policy: FailurePolicy,
    broken: usize,
    status: RunStatus<C>,
}

#[derive(Debug)]
enum RunStatus<C> {
    Serving { first_cause: Option<C> },
    Stopped(C),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FailureResponse<C> {
    Isolate,
    /// It carries the run's first cause, which may be an earlier, isolated failure.
    Stop(C),
    AlreadyStopped,
}

impl<C: Clone> RunHealth<C> {
    pub(crate) fn new(lanes: usize, policy: FailurePolicy) -> Self {
        RunHealth {
            lanes,
            policy,
            broken: 0,
            status: RunStatus::Serving { first_cause: None },
        }
    }

    /// Records a failure that left `newly_broken` lanes without a worker for good and returns
    /// what the run does about it.
    pub(crate) fn failed(&mut self, newly_broken: usize, cause: C) -> FailureResponse<C> {
        self.broken = self.broken.saturating_add(newly_broken).min(self.lanes);
        let RunStatus::Serving { first_cause } = &mut self.status else {
            return FailureResponse::AlreadyStopped;
        };
        let first = first_cause.get_or_insert(cause);
        let none_left = self.broken >= self.lanes;
        match self.policy {
            FailurePolicy::Exit => {}
            FailurePolicy::Isolate if none_left => {}
            FailurePolicy::Isolate => return FailureResponse::Isolate,
        }
        let first = first.clone();
        self.status = RunStatus::Stopped(first.clone());
        FailureResponse::Stop(first)
    }

    pub(crate) fn broken(&self) -> usize {
        self.broken
    }

    pub(crate) fn first_cause(&self) -> Option<&C> {
        match &self.status {
            RunStatus::Serving { first_cause } => first_cause.as_ref(),
            RunStatus::Stopped(cause) => Some(cause),
        }
    }

    /// Returns why the lanes stopped the server, once they did.
    pub(crate) fn fatal(&self) -> Option<&C> {
        match &self.status {
            RunStatus::Serving { .. } => None,
            RunStatus::Stopped(cause) => Some(cause),
        }
    }
}

/// A lane below this load takes new requests before any later lane of its host, so under light
/// load a few workers stay hot and the rest sleep.
pub(crate) const HOT_LOAD: usize = 2;

/// Picks the first usable lane under [`HOT_LOAD`], else the least loaded usable one; a `None`
/// load marks a lane that takes no requests.
///
/// # Errors
///
/// Returns [`UnansweredError::Overloaded`] when the pick already holds `queue_per_worker`
/// requests, and what `unusable` returns when no lane is usable.
pub(crate) fn pick_lane(
    loads: impl Iterator<Item = Option<usize>> + Clone,
    queue_per_worker: usize,
    unusable: impl FnOnce() -> UnansweredError,
) -> Result<usize, UnansweredError> {
    match lane_pick(loads, queue_per_worker) {
        LanePick::Lane(k) => Ok(k),
        LanePick::Full => Err(UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Lanes))),
        LanePick::NoLane => Err(unusable()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LanePick {
    Lane(usize),
    /// The lane the rule picks holds `queue_per_worker` already.
    Full,
    NoLane,
}

fn lane_pick(loads: impl Iterator<Item = Option<usize>> + Clone, queue_per_worker: usize) -> LanePick {
    match picked_by_load(loads) {
        None => LanePick::NoLane,
        Some((_, load)) if load >= queue_per_worker => LanePick::Full,
        Some((k, _)) => LanePick::Lane(k),
    }
}

/// Returns the first usable lane under [`HOT_LOAD`], else the first of the least loaded usable
/// ones, with its load.
fn picked_by_load(loads: impl Iterator<Item = Option<usize>> + Clone) -> Option<(usize, usize)> {
    let usable = loads.enumerate().filter_map(|(k, load)| Some((k, load?)));
    usable
        .clone()
        .find(|&(_, load)| load < HOT_LOAD)
        .or_else(|| usable.min_by_key(|&(_, load)| load))
}

/// Returns whether a host has at least [`HOT_LOAD`] requests per lane, counting a host with no
/// lane as having one.
pub(crate) fn loaded_to_spread(own_load: usize, own_lanes: usize) -> bool {
    own_load >= HOT_LOAD.saturating_mul(own_lanes.max(1))
}

/// A busy host hands requests to another host only once its running handlers have run this
/// long, because for short handlers the cross-thread hop costs more than the parallelism buys
/// (a ping benchmark made 131k req/s kept local and 70k spread).
pub(crate) const SPREAD_AFTER_NS: u64 = 50_000;

pub(crate) fn ran_at_least(running: bool, started_ns: u64, now_ns: u64, at_least_ns: u64) -> bool {
    running && now_ns.saturating_sub(started_ns) >= at_least_ns
}

/// The host's own sighting of the oldest request a lane has not answered, which its worker runs
/// or is about to: which one, and since when (monotonic ns) the host has seen it there. The
/// lane timeout is judged on it, never on a start time the worker writes, since a worker that
/// wrote 0 there would never time out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HeadOfLine {
    pub(crate) seq: u32,
    pub(crate) since_ns: u64,
}

impl HeadOfLine {
    /// Looks at request `read`, the oldest unanswered, at `now_ns`, and returns the sighting:
    /// `seen` is kept while it is of the same request, else (or with none yet) it becomes one
    /// of `read` from now. The time is late by up to one look, so a handler is never killed
    /// early. `seen` is written only when the head moved.
    #[inline]
    pub(crate) fn look(seen: &mut Option<HeadOfLine>, read: u32, now_ns: u64) -> HeadOfLine {
        match *seen {
            Some(head) if head.seq == read => head,
            Some(_) | None => *seen.insert(HeadOfLine {
                seq: read,
                since_ns: now_ns,
            }),
        }
    }

    /// Compared in ns: a `Duration` here would split both sides into seconds and nanos on every
    /// look of the poller.
    #[inline]
    pub(crate) fn overran(self, now_ns: u64, timeout_ns: u64) -> bool {
        now_ns.saturating_sub(self.since_ns) > timeout_ns
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct HostLoad {
    pub(crate) index: usize,
    pub(crate) load: usize,
}

/// Returns the least loaded of `others` when it holds less than half of `own_load`; among
/// equals the first wins.
pub(crate) fn spread_to(own_load: usize, others: impl Iterator<Item = HostLoad>) -> Option<usize> {
    let least = others.min_by_key(|h| h.load)?;
    (least.load.saturating_mul(2) < own_load).then_some(least.index)
}

/// Slows a lane's respawns while its workers keep dying without answering, so a request that
/// kills every worker it reaches does not cost a fork per request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CrashBackoff {
    /// From this many consecutive crashes on, the next fork waits.
    pub(crate) after: u32,
    /// The wait after `after` crashes doubles for each crash past it, up to `cap`.
    pub(crate) first: Duration,
    pub(crate) cap: Duration,
    /// A crash-looping lane repeats its alert this often.
    pub(crate) alert_every: Duration,
}

impl CrashBackoff {
    pub(crate) const DEFAULT: CrashBackoff = CrashBackoff {
        after: 3,
        first: Duration::from_millis(100),
        cap: Duration::from_secs(5),
        alert_every: Duration::from_secs(60),
    };

    /// Returns how long the fork after the `crashes`-th consecutive crash waits, counted from
    /// that crash.
    pub(crate) fn wait(&self, crashes: u32) -> Duration {
        Duration::from_nanos(self.wait_ns(crashes))
    }

    fn wait_ns(&self, crashes: u32) -> u64 {
        crash_wait_ns(nanos(self.first), nanos(self.cap), self.after, crashes)
    }

    pub(crate) fn looping(&self, crashes: u32) -> bool {
        crashes >= self.after
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RespawnStep {
    Fork,
    /// The lane refuses its requests while it waits.
    Delay(Duration),
}

/// Decides when to fork a lane's next worker after `crashes` consecutive crashes, the last at
/// `died_ns`.
pub(crate) fn respawn_after(backoff: &CrashBackoff, crashes: u32, died_ns: u64, now_ns: u64) -> RespawnStep {
    match left_ns(backoff.wait_ns(crashes), died_ns, now_ns) {
        0 => RespawnStep::Fork,
        left_ns => RespawnStep::Delay(Duration::from_nanos(left_ns)),
    }
}

fn left_ns(wait_ns: u64, died_ns: u64, now_ns: u64) -> u64 {
    wait_ns.saturating_sub(now_ns.saturating_sub(died_ns))
}

/// Counts one lane's consecutive crashes, deaths with no answer read between them; a lane that
/// answers has no streak.
#[derive(Debug)]
pub(crate) struct CrashStreak {
    crashes: u32,
    died_ns: u64,
    exit: WorkerExit,
    /// It is `None` while the crash-loop alert is not raised.
    alerted_ns: Option<u64>,
}

impl CrashStreak {
    pub(crate) fn first(now_ns: u64, exit: WorkerExit) -> Self {
        CrashStreak {
            crashes: 1,
            died_ns: now_ns,
            exit,
            alerted_ns: None,
        }
    }

    pub(crate) fn crashes(&self) -> u32 {
        self.crashes
    }

    pub(crate) fn last_exit(&self) -> &WorkerExit {
        &self.exit
    }

    pub(crate) fn alerted(&self) -> bool {
        self.alerted_ns.is_some()
    }

    /// Records another death and returns the streak's crash count.
    pub(crate) fn died(&mut self, now_ns: u64, exit: WorkerExit) -> u32 {
        self.crashes = self.crashes.saturating_add(1);
        self.died_ns = now_ns;
        self.exit = exit;
        self.crashes
    }

    /// Ends the streak because the lane answered, returning the crash count when its alert was
    /// raised so the recovery can be reported.
    pub(crate) fn answered(self) -> Option<u32> {
        self.alerted_ns.map(|_| self.crashes)
    }

    pub(crate) fn respawn(&self, backoff: &CrashBackoff, now_ns: u64) -> RespawnStep {
        respawn_after(backoff, self.crashes, self.died_ns, now_ns)
    }

    pub(crate) fn wait(&self, lane: LaneIndex, backoff: &CrashBackoff) -> CrashLoopWait {
        CrashLoopWait {
            lane,
            crashes: self.crashes,
            exit: self.exit.clone(),
            fork_at_ns: self.died_ns.saturating_add(backoff.wait_ns(self.crashes)),
        }
    }

    /// Returns when the crash-loop alert is next due, in monotonic ns, or `None` while the lane is
    /// not looping.
    pub(crate) fn alert_at_ns(&self, backoff: &CrashBackoff) -> Option<u64> {
        if !backoff.looping(self.crashes) {
            return None;
        }
        Some(
            self.alerted_ns
                .map_or(0, |at| at.saturating_add(nanos(backoff.alert_every))),
        )
    }

    /// Returns whether the crash-loop alert is due at `now_ns`, and records it as said when it is.
    pub(crate) fn alert_due(&mut self, backoff: &CrashBackoff, now_ns: u64) -> bool {
        let due = self.alert_at_ns(backoff).is_some_and(|at| now_ns >= at);
        if due {
            self.alerted_ns = Some(now_ns);
        }
        due
    }
}

/// Doubles by shifting, and a wait doubled past its top bit saturates, since it is past any cap.
fn crash_wait_ns(first_ns: u64, cap_ns: u64, after: u32, crashes: u32) -> u64 {
    let Some(past) = crashes.checked_sub(after) else {
        return 0;
    };
    let doubled = if first_ns == 0 {
        0
    } else if past < first_ns.leading_zeros() {
        first_ns << past
    } else {
        u64::MAX
    };
    doubled.min(cap_ns)
}

fn nanos(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Describes a lane's wait before its next fork, for the refusal of a request it cannot take.
#[derive(Debug, Clone)]
pub(crate) struct CrashLoopWait {
    pub(crate) lane: LaneIndex,
    pub(crate) crashes: u32,
    pub(crate) exit: WorkerExit,
    /// It is in monotonic ns.
    pub(crate) fork_at_ns: u64,
}

impl CrashLoopWait {
    /// Returns the refusal at `now_ns`, which tells the client to retry once the wait is over.
    pub(crate) fn refusal(self, now_ns: u64) -> UnansweredError {
        UnansweredError::LaneCrashLooping {
            lane: self.lane,
            crashes: self.crashes,
            exit: self.exit,
            retry_in: Duration::from_nanos(self.fork_at_ns.saturating_sub(now_ns)),
        }
    }

    pub(crate) fn sooner(self, other: CrashLoopWait) -> CrashLoopWait {
        if other.fork_at_ns < self.fork_at_ns {
            other
        } else {
            self
        }
    }
}

/// Returns the refusal for a request no lane of its host can take; a lane that is only waiting
/// to respawn wins over a broken one, so the client is told when to retry.
pub(crate) fn no_usable_lane(
    run_stopping: bool,
    wait: Option<(CrashLoopWait, u64)>,
    cause: impl FnOnce() -> Arc<str>,
) -> UnansweredError {
    if run_stopping {
        return UnansweredError::WorkersGone(WorkerGroup::Lanes);
    }
    match wait {
        Some((wait, now_ns)) => wait.refusal(now_ns),
        None => UnansweredError::NoUsableLane {
            group: WorkerGroup::Lanes,
            cause: cause(),
        },
    }
}

/// Returns the refusal for a request no host can take; a lane that is only waiting to respawn
/// wins over a failure, so the client is told when to retry.
pub(crate) fn no_host_left(
    run_stopping: bool,
    waiting: Option<UnansweredError>,
    cause: Option<Arc<str>>,
) -> UnansweredError {
    if run_stopping {
        return UnansweredError::WorkersGone(WorkerGroup::Lanes);
    }
    if let Some(waiting) = waiting {
        return waiting;
    }
    let cause = match cause {
        Some(cause) => cause,
        // No host recorded a failure, yet none takes requests, so their threads ended.
        None => Arc::from("the process lanes have no host left serving"),
    };
    UnansweredError::WorkersDied {
        group: WorkerGroup::Lanes,
        cause,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health(lanes: usize, policy: FailurePolicy) -> RunHealth<&'static str> {
        RunHealth::new(lanes, policy)
    }

    #[test]
    fn isolate_keeps_the_run_until_the_last_lane_breaks() {
        let mut h = health(2, FailurePolicy::Isolate);
        assert_eq!(h.failed(1, "first"), FailureResponse::Isolate);
        assert_eq!(h.fatal(), None);
        assert_eq!(h.failed(1, "second"), FailureResponse::Stop("first"));
        assert_eq!(h.fatal(), Some(&"first"));
        assert_eq!(h.broken(), 2);
    }

    #[test]
    fn exit_stops_at_the_first_failure() {
        let mut h = health(4, FailurePolicy::Exit);
        assert_eq!(h.failed(1, "first"), FailureResponse::Stop("first"));
        assert_eq!(h.broken(), 1);
    }

    #[test]
    fn a_failed_host_breaking_every_lane_left_stops_the_run() {
        // The first host's lane is isolated, and the other host's, the last one, stops the run.
        let mut h = health(2, FailurePolicy::Isolate);
        assert_eq!(h.failed(1, "host 0"), FailureResponse::Isolate);
        assert_eq!(h.failed(1, "host 1"), FailureResponse::Stop("host 0"));
    }

    #[test]
    fn a_failure_breaking_no_new_lane_still_records_the_first_cause() {
        let mut h = health(1, FailurePolicy::Isolate);
        assert_eq!(h.failed(0, "host without new breaks"), FailureResponse::Isolate);
        assert_eq!(h.first_cause(), Some(&"host without new breaks"));
        assert_eq!(h.fatal(), None);
    }

    #[test]
    fn once_stopped_later_failures_change_nothing() {
        let mut h = health(1, FailurePolicy::Isolate);
        assert_eq!(h.failed(1, "first"), FailureResponse::Stop("first"));
        assert_eq!(h.failed(1, "again"), FailureResponse::AlreadyStopped);
        assert_eq!(h.fatal(), Some(&"first"));
        assert_eq!(h.broken(), 1, "broken lanes never exceed the run's lanes");
    }

    fn lanes(loads: &[(bool, usize)]) -> impl Iterator<Item = Option<usize>> + Clone + '_ {
        loads.iter().map(|&(usable, load)| usable.then_some(load))
    }

    /// Returns `None` when no lane is usable.
    fn picked(loads: &[(bool, usize)], queue_per_worker: usize) -> Result<usize, Option<UnansweredError>> {
        let none_usable = || UnansweredError::NoWorkers(WorkerGroup::Lanes);
        pick_lane(lanes(loads), queue_per_worker, none_usable).map_err(|refusal| match refusal {
            UnansweredError::NoWorkers(WorkerGroup::Lanes) => None,
            other => Some(other),
        })
    }

    #[test]
    fn the_first_lane_under_the_hot_load_is_picked() {
        assert_eq!(picked(&[(true, 5), (true, HOT_LOAD - 1), (true, 0)], 8).ok(), Some(1));
        assert_eq!(
            picked(&[(true, HOT_LOAD), (true, HOT_LOAD + 1)], 8).ok(),
            Some(0),
            "no lane under the hot load: the least loaded"
        );
    }

    #[test]
    fn a_broken_lane_is_never_picked() {
        assert_eq!(picked(&[(false, 0), (true, 3)], 8).ok(), Some(1));
        assert!(matches!(picked(&[(false, 0), (false, 0)], 8), Err(None)));
        assert!(matches!(picked(&[], 8), Err(None)));
    }

    #[test]
    fn the_picked_lane_at_its_queue_limit_is_full() {
        assert_eq!(picked(&[(true, 3)], 4).ok(), Some(0));
        assert!(matches!(
            picked(&[(true, 4)], 4),
            Err(Some(UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Lanes))))
        ));
    }

    #[test]
    fn a_host_spreads_only_at_the_hot_load_per_lane() {
        assert!(!loaded_to_spread(HOT_LOAD * 2 - 1, 2));
        assert!(loaded_to_spread(HOT_LOAD * 2, 2));
        assert!(loaded_to_spread(HOT_LOAD, 0), "no lane counts as one");
    }

    #[test]
    fn spread_goes_to_a_host_with_under_half_the_load() {
        let others = |loads: &[usize]| {
            loads
                .iter()
                .enumerate()
                .map(|(i, &load)| HostLoad { index: i + 1, load })
                .collect::<Vec<_>>()
        };
        assert_eq!(spread_to(8, others(&[3, 5]).into_iter()), Some(1));
        assert_eq!(spread_to(8, others(&[4]).into_iter()), None, "half is not under half");
        assert_eq!(spread_to(8, others(&[]).into_iter()), None);
        assert_eq!(
            spread_to(8, others(&[2, 2]).into_iter()),
            Some(1),
            "the first of equals, in the order given"
        );
    }

    #[test]
    fn a_handler_has_run_long_from_the_threshold_on() {
        assert!(!ran_at_least(true, 1_000, 1_000 + SPREAD_AFTER_NS - 1, SPREAD_AFTER_NS));
        assert!(ran_at_least(true, 1_000, 1_000 + SPREAD_AFTER_NS, SPREAD_AFTER_NS));
        assert!(!ran_at_least(false, 0, u64::MAX, SPREAD_AFTER_NS));
    }

    #[test]
    fn a_handler_overruns_past_the_timeout_not_at_it() {
        let seen = HeadOfLine::look(&mut None, 7, 100);
        assert!(!seen.overran(100 + 500, 500));
        assert!(seen.overran(100 + 501, 500));
    }

    #[test]
    fn the_head_request_keeps_the_time_it_was_first_seen() {
        let mut seen = None;
        let first = HeadOfLine::look(&mut seen, 7, 100);
        assert_eq!(first, HeadOfLine { seq: 7, since_ns: 100 });
        assert_eq!(HeadOfLine::look(&mut seen, 7, 400), first);
        assert_eq!(seen, Some(first));
    }

    #[test]
    fn an_answer_puts_the_next_request_at_the_head_from_now() {
        let mut seen = Some(HeadOfLine { seq: 7, since_ns: 100 });
        let next = HeadOfLine { seq: 8, since_ns: 400 };
        assert_eq!(HeadOfLine::look(&mut seen, 8, 400), next);
        assert_eq!(seen, Some(next));
        let mut wrapped = Some(HeadOfLine { seq: u32::MAX, since_ns: 100 });
        assert_eq!(HeadOfLine::look(&mut wrapped, 0, 400), HeadOfLine { seq: 0, since_ns: 400 });
    }

    #[test]
    fn a_first_sighting_starts_at_the_first_look_wherever_the_cursor_is() {
        for read in [0, 7, u32::MAX] {
            let mut seen = None;
            assert_eq!(HeadOfLine::look(&mut seen, read, 400), HeadOfLine { seq: read, since_ns: 400 });
            assert_eq!(seen, Some(HeadOfLine { seq: read, since_ns: 400 }));
        }
    }

    #[test]
    fn a_host_refusal_names_the_lane_cause_unless_the_run_ends() {
        let cause = || Arc::from("boom");
        assert!(matches!(
            no_usable_lane(false, None, cause),
            UnansweredError::NoUsableLane { cause: c, .. } if &*c == "boom"
        ));
        assert!(matches!(
            no_usable_lane(true, None, cause),
            UnansweredError::WorkersGone(WorkerGroup::Lanes)
        ));
        // A full lane is refused as overloaded even while the run is ending.
        assert!(matches!(
            pick_lane(lanes(&[(true, 4)]), 4, || UnansweredError::WorkersGone(WorkerGroup::Lanes)),
            Err(UnansweredError::Overloaded(Capacity::WorkQueue(WorkerGroup::Lanes)))
        ));
    }

    const MS: u64 = 1_000_000;
    const B: CrashBackoff = CrashBackoff::DEFAULT;

    #[test]
    fn the_fork_waits_from_the_third_consecutive_crash_on() {
        assert_eq!(respawn_after(&B, 1, 0, 0), RespawnStep::Fork);
        assert_eq!(respawn_after(&B, 2, 0, 0), RespawnStep::Fork, "the second crash forks at once");
        assert_eq!(
            respawn_after(&B, 3, 0, 0),
            RespawnStep::Delay(Duration::from_millis(100)),
            "the third waits the first wait"
        );
    }

    #[test]
    fn the_wait_doubles_per_crash_up_to_the_cap() {
        let waits: Vec<_> = (3..=10).map(|c| B.wait(c)).collect();
        let ms = |n| Duration::from_millis(n);
        assert_eq!(
            waits,
            [ms(100), ms(200), ms(400), ms(800), ms(1600), ms(3200), ms(5000), ms(5000)]
        );
        // At 3.2 s the next doubling, 6.4 s, passes the cap and is reported as the cap.
        let under = CrashBackoff { cap: ms(3200), ..B };
        assert_eq!(under.wait(8), ms(3200), "exactly the cap is the cap");
        assert_eq!(under.wait(9), ms(3200), "past the cap is the cap");
        assert_eq!(B.wait(u32::MAX), B.cap, "no overflow however many crashes");
    }

    #[test]
    fn the_wait_counts_from_the_crash_and_forks_once_it_is_over() {
        let died = 1_000 * MS;
        assert_eq!(
            respawn_after(&B, 3, died, died + 40 * MS),
            RespawnStep::Delay(Duration::from_millis(60))
        );
        assert_eq!(
            respawn_after(&B, 3, died, died + 100 * MS - 1),
            RespawnStep::Delay(Duration::from_nanos(1)),
            "1 ns short of the wait still waits"
        );
        assert_eq!(respawn_after(&B, 3, died, died + 100 * MS), RespawnStep::Fork);
        assert_eq!(
            respawn_after(&B, 3, died, 0),
            RespawnStep::Delay(Duration::from_millis(100)),
            "a clock read before the crash waits the whole wait"
        );
    }

    #[test]
    fn an_answer_ends_the_run_and_the_next_crash_counts_from_one() {
        let mut run = CrashStreak::first(MS, WorkerExit::Exited(3));
        assert_eq!(run.crashes(), 1);
        for n in 2..=3 {
            assert_eq!(run.died(n * MS, WorkerExit::Exited(3)), n as u32);
        }
        assert!(matches!(run.respawn(&B, 3 * MS), RespawnStep::Delay(_)));
        assert!(run.alert_due(&B, 3 * MS));
        assert!(run.alerted());
        assert_eq!(run.answered(), Some(3), "an alerted run that ends is a recovery to say");
        // The next crash starts a new streak at one.
        let run = CrashStreak::first(10 * MS, WorkerExit::Exited(3));
        assert_eq!(run.respawn(&B, 10 * MS), RespawnStep::Fork);
        assert_eq!(run.answered(), None, "a run never alerted ends without a notice");
    }

    #[test]
    fn the_alert_is_said_at_the_third_crash_and_again_each_period() {
        let mut run = CrashStreak::first(0, WorkerExit::Exited(3));
        run.died(MS, WorkerExit::Exited(3));
        assert!(!run.alert_due(&B, MS), "two crashes are not a loop");
        assert_eq!(run.alert_at_ns(&B), None);
        run.died(2 * MS, WorkerExit::Exited(3));
        let at = 2 * MS;
        assert_eq!(run.alert_at_ns(&B), Some(0), "due at once when it starts looping");
        assert!(run.alert_due(&B, at));
        let period = 60_000 * MS;
        assert_eq!(run.alert_at_ns(&B), Some(at + period));
        assert!(!run.alert_due(&B, at + period - 1), "1 ns short of the period");
        assert!(run.alert_due(&B, at + period));
        assert!(!run.alert_due(&B, at + period + 1), "said again only a period later");
    }

    #[test]
    fn a_wait_is_refused_with_its_facts_and_the_time_left() {
        let mut run = CrashStreak::first(0, WorkerExit::Exited(3));
        for n in 1..3 {
            run.died(n * MS, WorkerExit::Exited(3));
        }
        let wait = run.wait(LaneIndex(2), &B);
        assert_eq!(wait.fork_at_ns, 2 * MS + 100 * MS);
        match wait.clone().refusal(52 * MS) {
            UnansweredError::LaneCrashLooping {
                lane: LaneIndex(2),
                crashes: 3,
                exit: WorkerExit::Exited(3),
                retry_in,
            } => assert_eq!(retry_in, Duration::from_millis(50)),
            other => panic!("{other:?}"),
        }
        match wait.refusal(u64::MAX) {
            UnansweredError::LaneCrashLooping { retry_in, .. } => assert_eq!(retry_in, Duration::ZERO),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_crash_looping_host_refuses_with_its_wait_unless_the_run_ends() {
        let wait = || CrashLoopWait {
            lane: LaneIndex(0),
            crashes: 3,
            exit: WorkerExit::Exited(3),
            fork_at_ns: 100,
        };
        let cause = || Arc::from("unused");
        assert!(matches!(
            no_usable_lane(false, Some((wait(), 40)), cause),
            UnansweredError::LaneCrashLooping { retry_in, .. } if retry_in == Duration::from_nanos(60)
        ));
        assert!(matches!(
            no_usable_lane(true, Some((wait(), 40)), cause),
            UnansweredError::WorkersGone(WorkerGroup::Lanes)
        ));
        let later = CrashLoopWait { fork_at_ns: 200, ..wait() };
        assert_eq!(later.sooner(wait()).fork_at_ns, 100);
        assert_eq!(wait().sooner(CrashLoopWait { fork_at_ns: 200, ..wait() }).fork_at_ns, 100);
    }

    #[test]
    fn with_no_host_left_a_lane_that_comes_back_beats_the_cause() {
        let waiting = || {
            Some(UnansweredError::LaneCrashLooping {
                lane: LaneIndex(0),
                crashes: 3,
                exit: WorkerExit::Exited(3),
                retry_in: Duration::from_millis(5),
            })
        };
        assert!(matches!(
            no_host_left(false, waiting(), Some(Arc::from("lane 1 broke"))),
            UnansweredError::LaneCrashLooping { .. }
        ));
        assert!(matches!(
            no_host_left(true, waiting(), None),
            UnansweredError::WorkersGone(WorkerGroup::Lanes)
        ));
    }

    #[test]
    fn with_no_host_left_the_refusal_carries_the_cause() {
        assert!(matches!(
            no_host_left(false, None, Some(Arc::from("lane 0 broke"))),
            UnansweredError::WorkersDied { cause, .. } if &*cause == "lane 0 broke"
        ));
        assert!(matches!(
            no_host_left(true, None, Some(Arc::from("lane 0 broke"))),
            UnansweredError::WorkersGone(WorkerGroup::Lanes)
        ));
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    fn the_crash_wait_only_grows_and_stays_under_the_cap() {
        let (first_ns, cap_ns, after): (u64, u64, u32) = (kani::any(), kani::any(), kani::any());
        let (fewer, more): (u32, u32) = (kani::any(), kani::any());
        kani::assume(fewer <= more);
        let shorter = crash_wait_ns(first_ns, cap_ns, after, fewer);
        let longer = crash_wait_ns(first_ns, cap_ns, after, more);
        assert!(shorter <= longer);
        assert!(longer <= cap_ns);
    }

    #[kani::proof]
    fn a_respawn_waits_at_most_the_backoff() {
        let (wait_ns, died_ns, now_ns): (u64, u64, u64) = (kani::any(), kani::any(), kani::any());
        let waited_ns = now_ns.saturating_sub(died_ns);
        let left = left_ns(wait_ns, died_ns, now_ns);
        assert!(left <= wait_ns);
        assert_eq!(left == 0, waited_ns >= wait_ns);
        if left > 0 {
            assert_eq!(left, wait_ns - waited_ns);
        }
    }

    const LANES: usize = 4;

    #[kani::proof]
    #[kani::unwind(6)]
    fn the_rule_picks_the_first_hot_lane_else_the_least_loaded() {
        let loads: [Option<usize>; LANES] = kani::any();
        let picked = picked_by_load(loads.iter().copied());
        let least = loads.iter().filter_map(|&load| load).min();
        let first_hot = loads.iter().position(|&l| l.is_some_and(|l| l < HOT_LOAD));
        match picked {
            Some((k, load)) => {
                assert!(k < LANES);
                assert_eq!(loads[k], Some(load));
                match first_hot {
                    Some(hot) => assert_eq!(k, hot),
                    None => {
                        assert_eq!(Some(load), least);
                        assert!(loads[..k].iter().all(|&l| l != Some(load)), "the first of equals");
                    }
                }
            }
            None => assert!(least.is_none()),
        }
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn a_request_goes_to_the_picked_lane_only_with_room() {
        let loads: [Option<usize>; LANES] = kani::any();
        let queue_per_worker: usize = kani::any();
        match (picked_by_load(loads.iter().copied()), lane_pick(loads.iter().copied(), queue_per_worker)) {
            (Some((k, load)), LanePick::Lane(lane)) => {
                assert!(lane == k && lane < LANES && load < queue_per_worker)
            }
            (Some((_, load)), LanePick::Full) => assert!(load >= queue_per_worker),
            (None, LanePick::NoLane) => {}
            _ => panic!("lane_pick did not follow the rule"),
        }
    }
}
