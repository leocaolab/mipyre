//! The host's decisions over its lanes, apart from the IO that carries them out: what each
//! lane's worker is, as data the host's shell acts on. It names a worker by its tag, never by
//! its process: the process (its pidfd, its exit watch) is the shell's.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::BytesMut;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_shm::adapter::doorbell::FutexDoorbell;
use mipyre_shm::adapter::mapped::MappedMemory;
use mipyre_shm::channel::{
    Attempt, DeadPeerAnswer, DeadPeerError, PeerFault, RequestSlot, SlotRoomError, StartOutcome,
};
use mipyre_shm::plan::ring::{RingDepth, SeqRange};

use std::time::Duration;

use super::health::{self, CrashBackoff, CrashLoopWait, CrashStreak, HeadOfLine, LanePick, RespawnStep};
use super::host::LaneLossError;
use super::lane_failure::{LaneIndex, LaneUnansweredError, WorkerExit};
use super::pid::Pid;
use super::route::{LaneJob, LaneReplySender, LaneRequestError};
use super::run::LaneEnd;
use super::wire;

/// A lane's worker as the host's decisions name it: its lane, the start attempt it was forked as,
/// and its pid (for what the host says about it). Each attempt number is taken once per server
/// (`ServerLanes::take_attempt`), so no two workers share a tag, and a death is matched to the
/// worker it is about by it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct WorkerTag {
    pub(super) lane: LaneIndex,
    pub(super) attempt: Attempt,
    pub(super) pid: Pid,
}

/// A replacement worker not yet reported ready: when its predecessor's death was seen (the
/// respawn latency), in monotonic nanoseconds, and the start attempt it reports under.
#[derive(Clone, Copy, Debug)]
pub(super) struct Replacement {
    pub(super) since_ns: u64,
    pub(super) attempt: Attempt,
}

/// Where a lane's worker is.
pub(super) enum LaneWorker {
    /// A worker runs the lane's requests.
    Running {
        worker: WorkerTag,
        /// Set while it is a replacement whose start is still being watched.
        starting: Option<Replacement>,
        /// What the host last saw at the head of the lane while this worker runs it; `None`
        /// until the host first looks at a request in flight. It is kept while the lane is
        /// idle, since a request once answered never comes back to the head, and it goes with
        /// the worker: a replacement's time on a request starts when the host sees it running.
        head: Option<HeadOfLine>,
    },
    /// Killed for running a handler past the timeout (once): its death answers 504.
    /// `head_since_ns` is when the host first saw that handler's request at the head.
    Overrun {
        worker: WorkerTag,
        starting: Option<Replacement>,
        head_since_ns: u64,
    },
    /// The worker died and a replacement is being forked as start attempt `attempt`; `since_ns`
    /// is when the death was seen.
    Forking { since_ns: u64, attempt: Attempt },
    /// The worker died while the server was stopping: no replacement comes.
    Gone,
    /// The lane could not get a worker and takes no requests (`break_lane`). It stays broken for
    /// the run, and the run's `broken_lanes` is counted from this state.
    Broken { why: Arc<LaneLossError> },
    /// The lane's workers crash-loop: it waits before forking the next one, as long as its crash
    /// streak says, and refuses its requests with 503 meanwhile.
    CrashWait,
    /// Killed for writing what no correct worker writes into its channel. Its requests were
    /// answered 502 when that was seen, and none of them runs again; until it is dead the lane
    /// takes none.
    Faulted { worker: WorkerTag },
}

impl LaneWorker {
    #[inline]
    pub(super) fn takes_requests(&self) -> bool {
        !matches!(
            self,
            LaneWorker::Broken { .. } | LaneWorker::CrashWait | LaneWorker::Faulted { .. }
        )
    }
}

/// One lane as its host keeps it; its place on the ring is its channel end's ([`LaneEnd`]).
pub(super) struct LocalLane {
    pub(super) index: LaneIndex,
    /// Holds who waits for sequence `k`, at `k`'s slot.
    pub(super) waiting: Vec<Option<LaneReplySender>>,
    pub(super) queued: VecDeque<LaneJob>,
    pub(super) worker: LaneWorker,
}

/// A host's lanes and, at the same place in another list, their channel ends (the ends are one
/// slice, which the host's park hook hands to `sleep_all` whole), and the crash streaks of the
/// lanes that have one: everything the host decides with, and nothing it does IO with.
pub(super) struct HostCore {
    pub(super) lanes: Vec<LocalLane>,
    pub(super) ends: Vec<LaneEnd>,
    /// Holds only the lanes in a crash run, so it is empty while they all serve.
    pub(super) crash_runs: Vec<LaneCrashStreak>,
}

impl LocalLane {
    pub(super) fn load(&self, end: &LaneEnd) -> usize {
        end.in_flight() as usize + self.queued.len()
    }

    pub(super) fn broken_why(&self) -> Option<&LaneLossError> {
        match &self.worker {
            LaneWorker::Broken { why } => Some(why),
            LaneWorker::CrashWait
            | LaneWorker::Running { .. }
            | LaneWorker::Overrun { .. }
            | LaneWorker::Faulted { .. }
            | LaneWorker::Forking { .. }
            | LaneWorker::Gone => None,
        }
    }

    /// Returns why `dead`, the worker this lane runs now, dies, as far as the host caused it, at
    /// `now_ns`; `None` for a worker the lane no longer runs (an earlier worker's late event).
    pub(super) fn death_cause(&self, dead: WorkerTag, now_ns: u64) -> Option<DeathCause> {
        match &self.worker {
            LaneWorker::Running { worker, head, .. } if *worker == dead => Some(DeathCause::Crashed { seen: *head }),
            LaneWorker::Overrun {
                worker, head_since_ns, ..
            } if *worker == dead => Some(DeathCause::Overran {
                ran: Duration::from_nanos(now_ns.saturating_sub(*head_since_ns)),
            }),
            LaneWorker::Faulted { worker } if *worker == dead => Some(DeathCause::Faulted),
            _ => None,
        }
    }

    /// This lane, whose worker is dead, waits out its crash loop: it takes no requests, and those
    /// already in it (moved from its queue into the ring after the dead worker's were given up)
    /// are taken back out of its ring and handed back with their refusal, so the next worker never
    /// runs them. `now_ns` is when.
    pub(super) fn close_for_crash_loop(
        &mut self,
        end: &mut LaneEnd,
        wait: &CrashLoopWait,
        now_ns: u64,
        depth: RingDepth,
    ) -> Vec<(LaneReplySender, LaneUnansweredError)> {
        self.worker = LaneWorker::CrashWait;
        // No worker runs the lane until the next fork, which starts after the requests given up.
        let given_up = end.abandon();
        let mut replies = self.take_replies(given_up, depth);
        replies.extend(self.queued.drain(..).map(|j| j.reply));
        replies
            .into_iter()
            .map(|reply| (reply, wait.clone().refusal(now_ns)))
            .collect()
    }

    /// Looks at the head of the lane at `now_ns` and, when the request its worker runs has run
    /// past `timeout_ns` since the host first saw it there, marks the worker overrun and returns
    /// it for the host to kill (its death answers 504).
    pub(super) fn overrun(&mut self, end: &LaneEnd, now_ns: u64, timeout_ns: u64) -> Option<WorkerTag> {
        let LaneWorker::Running {
            worker,
            starting,
            ref mut head,
        } = self.worker
        else {
            return None;
        };
        if end.in_flight() == 0 {
            return None;
        }
        let seen = HeadOfLine::look(head, end.window().read().get(), now_ns);
        if !seen.overran(now_ns, timeout_ns) {
            return None;
        }
        self.worker = LaneWorker::Overrun {
            worker,
            starting,
            head_since_ns: seen.since_ns,
        };
        Some(worker)
    }

    /// The lane's worker wrote what no correct worker writes (`fault`): a worker that runs is
    /// marked faulted and every request in the lane's slots is answered 502 into `done` (none runs
    /// again; the death that follows frees the slots); with no worker of its own the lane gives up
    /// the requests in it (503) and puts its counters back; while a replacement forks, or the lane
    /// is broken or faulted already, nothing changes.
    pub(super) fn fault(
        &mut self,
        end: &mut LaneEnd,
        fault: PeerFault,
        depth: RingDepth,
        done: &mut Completions,
    ) -> FaultStep {
        let i = self.index;
        match self.worker {
            LaneWorker::Running { worker, .. } | LaneWorker::Overrun { worker, .. } => {
                self.worker = LaneWorker::Faulted { worker };
                let refusal = || Err(LaneRequestError::Rejected(peer_corrupted(i, worker.pid, fault)));
                let held = self.take_replies(end.window().outstanding(), depth);
                done.extend(held.into_iter().map(|r| (r, refusal())));
                FaultStep::Killed(worker)
            }
            LaneWorker::Faulted { .. } | LaneWorker::Broken { .. } | LaneWorker::Forking { .. } => FaultStep::Kept,
            LaneWorker::Gone | LaneWorker::CrashWait => {
                // No worker serves the lane: it is stopping (gone), or its requests left it (waiting).
                let given_up = end.abandon();
                let refusal = || Err(LaneRequestError::Rejected(LaneUnansweredError::ShuttingDown));
                done.extend(self.take_replies(given_up, depth).into_iter().map(|r| (r, refusal())));
                FaultStep::GaveUp
            }
        }
    }

    /// Starts the lane's next worker as start attempt `attempt`, its predecessor dead, seen at
    /// `now_ns`: the channel takes the attempt's start report only, and the lane forks.
    pub(super) fn begin_fork(&mut self, end: &mut LaneEnd, attempt: Attempt, now_ns: u64) {
        end.begin_attempt(attempt);
        self.worker = LaneWorker::Forking {
            since_ns: now_ns,
            attempt,
        };
    }

    /// Looks, at `now_ns`, at the start report of a replacement the lane runs and has not yet
    /// seen ready: ready (with the respawn latency) clears the watch; a failed start, or a report
    /// that cannot be read, is why the lane breaks; nothing yet is `None`.
    pub(super) fn look_at_start(&mut self, end: &LaneEnd, now_ns: impl FnOnce() -> u64) -> Option<StartLook> {
        let (LaneWorker::Running { worker, starting, .. } | LaneWorker::Overrun { worker, starting, .. }) =
            &mut self.worker
        else {
            return None;
        };
        let replacement = (*starting)?;
        match end.start_report(replacement.attempt) {
            Ok(Some(StartOutcome::Ready)) => {
                *starting = None;
                Some(StartLook::Ready {
                    latency_us: now_ns().saturating_sub(replacement.since_ns) / 1_000,
                })
            }
            Ok(Some(StartOutcome::Failed(note))) => {
                let failure = String::from_utf8_lossy(&note).into_owned();
                Some(StartLook::Failed(LaneLossError::ReplacementNotStarted { failure }))
            }
            Err(source) => Some(StartLook::Failed(LaneLossError::ReplacementReportUnreadable {
                pid: worker.pid,
                source,
            })),
            Ok(None) => None,
        }
    }

    /// Breaks the lane for the run with `why`: it takes no more requests, and who waits on any of
    /// its requests, in its slots or queued, is handed back to be answered.
    pub(super) fn break_for_good(&mut self, why: &Arc<LaneLossError>) -> Breakage {
        let newly = self.broken_why().is_none();
        // A lane waiting out a crash loop was taken out of `usable_lanes` already.
        let was_waiting = self.waiting();
        let counted = self.worker.takes_requests();
        let before = std::mem::replace(&mut self.worker, LaneWorker::Broken { why: Arc::clone(why) });
        let was_forking = matches!(before, LaneWorker::Forking { .. });
        Breakage {
            newly,
            counted,
            was_waiting,
            was_forking,
            replies: self.take_every_reply(),
        }
    }

    /// Takes who waits on every request of the lane, in its slots or queued.
    pub(super) fn take_every_reply(&mut self) -> Vec<LaneReplySender> {
        let mut replies: Vec<_> = self.waiting.iter_mut().filter_map(Option::take).collect();
        replies.extend(self.queued.drain(..).map(|j| j.reply));
        replies
    }

    pub(super) fn waiting(&self) -> bool {
        matches!(self.worker, LaneWorker::CrashWait)
    }

    /// Takes who waits for each request in `range`, leaving the slots as they are.
    pub(super) fn take_replies(&mut self, range: SeqRange, depth: RingDepth) -> Vec<LaneReplySender> {
        (0..range.len())
            .filter_map(|k| self.waiting[range.start().after(k).slot_index(depth)].take())
            .collect()
    }
}

/// A lane of the host in a crash streak ([`CrashStreak`]), kept by the host beside its lanes
/// while the streak lasts: a lane that serves carries nothing, so the lanes the host scans per
/// request are as they were.
pub(super) struct LaneCrashStreak {
    pub(super) lane: LaneIndex,
    pub(super) run: CrashStreak,
    /// Where the lane's reading stood as its last death left it: an answer read since moves the
    /// end's `read` past it, which ends the run. Compared at the next death (and by the
    /// crash-loop watch), not counted per answer, so reading answers costs nothing more.
    pub(super) read_at_death: u32,
}

/// Of the runs in `runs` whose lane (in `owned`) waits out a crash loop, the one that forks
/// first.
pub(super) fn soonest_wait(
    owned: &[LocalLane],
    runs: &[LaneCrashStreak],
    backoff: &CrashBackoff,
) -> Option<CrashLoopWait> {
    runs.iter()
        .filter(|r| owned.iter().any(|l| l.index == r.lane && l.waiting()))
        .map(|r| r.run.wait(r.lane, backoff))
        .reduce(CrashLoopWait::sooner)
}

/// What one collect completes: who waits, and what to answer them.
pub(super) type Completions = Vec<(LaneReplySender, Result<HandlerResponse, LaneRequestError>)>;

/// Takes every answer `end`'s worker published into `buf` and decodes it, pushing it with who
/// waits for it onto `done`; returns how many it took, and the worker's fault when it found one
/// (the answers before it are taken).
#[inline]
pub(super) fn take_answers(
    end: &mut LaneEnd,
    buf: &mut BytesMut,
    waiting: &mut [Option<LaneReplySender>],
    depth: RingDepth,
    done: &mut Completions,
) -> (u64, Result<(), PeerFault>) {
    let mut ready = match end.answers() {
        Ok(ready) => ready,
        Err(fault) => return (0, Err(fault)),
    };
    let mut took = 0;
    loop {
        // An answer that does not fit what is left of `buf` makes room of at least
        // `ANSWER_BUFFER_BYTES`, the size `buf` started with (`BytesMut::reserve`).
        match ready.take_next(buf) {
            Ok(Some(seq)) => {
                let outcome = wire::decode_answer(buf.split().freeze());
                if let Some(reply) = waiting[seq.slot_index(depth)].take() {
                    done.push((reply, outcome));
                }
                took += 1;
            }
            Ok(None) => return (took, Ok(())),
            Err(fault) => return (took, Err(fault)),
        }
    }
}

/// Why a worker the host watches dies, as far as the host caused it.
#[derive(Clone, Copy)]
pub(super) enum DeathCause {
    /// `seen` is what the host last saw at the head of the lane, which times the request it ran.
    Crashed {
        seen: Option<HeadOfLine>,
    },
    Overran {
        ran: Duration,
    },
    /// Its fault was answered when it was seen.
    Faulted,
}

impl std::fmt::Display for DeathCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeathCause::Crashed { .. } => f.write_str("died"),
            DeathCause::Overran { ran } => write!(f, "was killed: its request ran {ran:?}, past the timeout"),
            DeathCause::Faulted => f.write_str("was killed: it wrote what no worker writes into its lane"),
        }
    }
}

/// The refusal a request gets when worker `pid` of lane `i` wrote what no worker writes.
pub(super) fn peer_corrupted(i: LaneIndex, pid: Pid, fault: PeerFault) -> LaneUnansweredError {
    LaneUnansweredError::PeerCorrupted {
        pid: pid.as_raw(),
        lane: i,
        fault,
    }
}

/// What the death of a lane's worker left: why it died, where the lane's reading stood (before the
/// request it died running is skipped: an answer since the last death ends a crash streak, a skip
/// is not one), the requests its death answers, and the fault in the counters it left, if any.
pub(super) struct Burial {
    pub(super) cause: DeathCause,
    pub(super) read_at_death: u32,
    pub(super) answered: Completions,
    pub(super) fault: Option<PeerFault>,
}

/// Takes over `lane`'s channel from `dead`, its worker, which ended with `exit` (seen at
/// `now_ns`): reads what it answered, answers 502 what it was running (504 when it overran
/// `timeout`) and what it never took (it could write their bytes, so none runs on the next
/// worker), and leaves the lane with no worker. `None` when `dead` is not the lane's worker.
pub(super) fn bury(
    lane: &mut LocalLane,
    end: &mut LaneEnd,
    dead: WorkerTag,
    exit: &WorkerExit,
    now_ns: u64,
    timeout: Duration,
    depth: RingDepth,
) -> Option<Burial> {
    let cause = lane.death_cause(dead, now_ns)?;
    let (i, pid) = (dead.lane, dead.pid);
    let mut fault = None;
    let read_before = end.window().read();
    let mut answered = Completions::new();
    // Where the lane's reading stood at the death, before the request it died running is
    // skipped: an answer since the last death ends the crash streak, a skip is not one.
    let read_at_death = match cause {
        DeathCause::Faulted => {
            // Its requests were answered when its fault was seen; none runs again.
            let given_up = end.abandon();
            let refusal = |r| (r, Err(LaneRequestError::Rejected(LaneUnansweredError::ShuttingDown)));
            answered.extend(lane.take_replies(given_up, depth).into_iter().map(refusal));
            read_before
        }
        DeathCause::Crashed { .. } | DeathCause::Overran { .. } => {
            let seen = match cause {
                DeathCause::Crashed { seen } => seen,
                DeathCause::Overran { .. } | DeathCause::Faulted => None,
            };
            let mut dead_answers = Vec::new();
            match end.take_over(&mut dead_answers) {
                Ok(split) => {
                    for DeadPeerAnswer { seq, message } in dead_answers {
                        let outcome = match message {
                            Ok(bytes) => wire::decode_answer(bytes),
                            Err(e) => Err(LaneRequestError::Rejected(peer_corrupted(i, pid, PeerFault::Length(e)))),
                        };
                        if let Some(reply) = lane.waiting[seq.slot_index(depth)].take() {
                            answered.push((reply, outcome));
                        }
                    }
                    if let Some(ran_seq) = split.running() {
                        let (raw_pid, exit) = (pid.as_raw(), exit.clone());
                        let refusal = match (cause, seen) {
                            (DeathCause::Overran { ran }, _) => LaneUnansweredError::LaneTimedOut {
                                pid: raw_pid,
                                lane: i,
                                ran,
                                timeout,
                                exit,
                            },
                            // How long it ran is what the host saw, if it saw this request at the head.
                            (DeathCause::Crashed { .. } | DeathCause::Faulted, seen) => {
                                let ran = seen
                                    .filter(|h| h.seq == ran_seq.get())
                                    .map(|h| Duration::from_nanos(now_ns.saturating_sub(h.since_ns)));
                                LaneUnansweredError::WorkerDied {
                                    pid: raw_pid,
                                    lane: i,
                                    ran,
                                    exit,
                                }
                            }
                        };
                        if let Some(reply) = lane.waiting[ran_seq.slot_index(depth)].take() {
                            answered.push((reply, Err(LaneRequestError::Rejected(refusal))));
                        }
                    }
                    // The dead worker could write their bytes, so none is run on the next one.
                    let untaken = |reply| {
                        let refusal = LaneUnansweredError::WorkerDiedBeforeTaking {
                            pid: pid.as_raw(),
                            lane: i,
                            exit: exit.clone(),
                        };
                        (reply, Err(LaneRequestError::Rejected(refusal)))
                    };
                    answered.extend(lane.take_replies(split.handed(), depth).into_iter().map(untaken));
                    split.answered().end()
                }
                Err(DeadPeerError {
                    cause: counters,
                    abandoned,
                }) => {
                    let counter_fault = PeerFault::Counter(counters);
                    fault = Some(counter_fault);
                    // Whether it ran them is unknown: they are answered, never run again.
                    let refusal = |r| {
                        (
                            r,
                            Err(LaneRequestError::Rejected(peer_corrupted(i, pid, counter_fault))),
                        )
                    };
                    answered.extend(lane.take_replies(abandoned, depth).into_iter().map(refusal));
                    read_before
                }
            }
        }
    };
    lane.worker = LaneWorker::Gone;
    Some(Burial {
        cause,
        read_at_death: read_at_death.get(),
        answered,
        fault,
    })
}

/// Takes lane `lane`'s crash streak out of `runs` for its worker's death with `exit` at `now_ns`,
/// its answers read up to `read_at_death`: the streak grows when the lane answered nothing since
/// its last death, else a new one starts, and the one that ended says how many crashes it held
/// (for the recovery to be logged).
pub(super) fn streak_after_death(
    runs: &mut Vec<LaneCrashStreak>,
    lane: LaneIndex,
    read_at_death: u32,
    exit: &WorkerExit,
    now_ns: u64,
) -> (CrashStreak, Option<u32>) {
    match runs.iter().position(|r| r.lane == lane) {
        Some(k) if runs[k].read_at_death == read_at_death => {
            let mut streak = runs.swap_remove(k).run;
            streak.died(now_ns, exit.clone());
            (streak, None)
        }
        // A streak whose lane answered since its last death is over.
        found => {
            let recovered = found.and_then(|k| runs.swap_remove(k).run.answered());
            (CrashStreak::first(now_ns, exit.clone()), recovered)
        }
    }
}

/// What a lane did with a fault its worker wrote ([`LocalLane::fault`]).
pub(super) enum FaultStep {
    /// The worker is faulted and must be killed.
    Killed(WorkerTag),
    /// Nothing changed: the lane has no worker that runs it yet, or is out of service.
    Kept,
    /// The lane had no worker and gave its requests up.
    GaveUp,
}

/// What one look at a host's crash runs found ([`tend_crash_runs`]).
pub(super) struct Tend {
    /// The lanes whose crash-loop wait is over: the host forks their next worker.
    pub(super) forks: Vec<LaneIndex>,
    /// The lanes that answered since their last death, and how many crashes their run held.
    pub(super) recovered: Vec<(LaneIndex, u32)>,
    /// The lanes whose crash-loop alert is due again.
    pub(super) alerts: Vec<LaneIndex>,
    /// When to look again, `None` once no lane loops.
    pub(super) next_ns: Option<u64>,
}

/// Looks once at a host's crash runs at `now_ns`: a lane that answered ends its run, a waiting
/// lane whose wait is over is due to fork, and a looping lane's alert comes due again.
pub(super) fn tend_crash_runs(owned: &mut HostCore, backoff: &CrashBackoff, now: u64) -> Tend {
    let mut tend = Tend {
        forks: Vec::new(),
        recovered: Vec::new(),
        alerts: Vec::new(),
        next_ns: None,
    };
    let HostCore {
        lanes,
        ends,
        crash_runs: runs,
    } = owned;
    let mut k = 0;
    while k < runs.len() {
        let entry = &mut runs[k];
        let Some(at) = lanes.iter().position(|l| l.index == entry.lane) else {
            k += 1;
            continue;
        };
        let (lane, end) = (&lanes[at], &ends[at]);
        if end.window().read().get() != entry.read_at_death {
            let ended = runs.swap_remove(k);
            if let Some(crashes) = ended.run.answered() {
                tend.recovered.push((ended.lane, crashes));
            }
            continue;
        }
        let forks = lane.waiting() && entry.run.respawn(backoff, now) == RespawnStep::Fork;
        if forks {
            tend.forks.push(entry.lane);
        }
        if entry.run.alert_due(backoff, now) {
            tend.alerts.push(entry.lane);
        }
        if let Some(alert_at) = entry.run.alert_at_ns(backoff) {
            // A looping lane is looked at again when it forks (waiting), else after the first
            // wait (has it answered yet?), and at its next alert at the latest.
            let look_at = if lane.waiting() && !forks {
                entry.run.wait(entry.lane, backoff).fork_at_ns
            } else {
                now.saturating_add(backoff.first.as_nanos().min(u128::from(u64::MAX)) as u64)
            };
            let at = look_at.min(alert_at);
            tend.next_ns = Some(tend.next_ns.map_or(at, |n| n.min(at)));
        }
        k += 1;
    }
    tend
}

/// What a look at a replacement's start found ([`LocalLane::look_at_start`]).
pub(super) enum StartLook {
    Ready {
        latency_us: u64,
    },
    /// It cannot start, or its report cannot be read: the lane breaks (no more forks).
    Failed(LaneLossError),
}

/// What breaking a lane changed ([`LocalLane::break_for_good`]).
pub(super) struct Breakage {
    /// It was not broken before.
    pub(super) newly: bool,
    /// It counted among the host's lanes that take requests.
    pub(super) counted: bool,
    pub(super) was_waiting: bool,
    /// A replacement was forking: its fork, once it answers, is for a lane that has no worker.
    pub(super) was_forking: bool,
    pub(super) replies: Vec<LaneReplySender>,
}

/// What the core says happened, for the shell to log, count and list (each told where it is
/// decided, with the facts of that moment).
pub(super) enum LaneNotice {
    /// A worker died (logged for every death while the run serves).
    WorkerDied {
        worker: WorkerTag,
        cause: DeathCause,
        exit: WorkerExit,
    },
    /// A worker wrote what no worker writes; it is killed.
    PeerFault { worker: WorkerTag, fault: PeerFault },
    /// A lane with no worker found what no worker writes in its channel, and gave its requests up.
    FaultWithoutWorker { lane: LaneIndex, fault: PeerFault },
    /// A lane answered after a crash streak of `crashes`.
    Recovered { lane: LaneIndex, crashes: u32 },
    /// A lane crash-loops (its alert, logged at the start and again every `alert_every`).
    CrashLoop {
        lane: LaneIndex,
        crashes: u32,
        exit: WorkerExit,
        wait: Duration,
        waiting: bool,
    },
    /// A crash-loop alert that was raised ended with its run (the lane broke, or the run stopped).
    LoopEnded { lane: LaneIndex },
    /// A lane lost its worker for good.
    LaneLost { lane: LaneIndex, why: Arc<LaneLossError> },
    /// A worker was killed for running past the timeout.
    TimedOut,
    /// A replacement is ready, `latency_us` after its predecessor's death was seen.
    Respawned { latency_us: u64 },
}

/// What the host does for its core: everything that is IO, a clock or another thread. The core
/// calls these and gets no result back but the attempt number, which the shell draws from the
/// server's counter: a fork's answer, a worker's death, the time all come back as calls into the
/// core. The shell implements it over tokio and the processes; the simulation over a seeded world.
pub(super) trait HostEffects {
    /// The number of a new start attempt (`ServerLanes::take_attempt`).
    fn take_attempt(&mut self) -> Attempt;
    /// Kills `worker`.
    fn kill(&mut self, worker: WorkerTag);
    /// Asks for lane `lane`'s next worker, as start attempt `attempt`; the answer comes back as
    /// [`HostCore::forked`].
    fn fork(&mut self, lane: LaneIndex, attempt: Attempt);
    /// Lane `lane` broke, or its host closed, while its replacement forked: the worker that fork
    /// makes is killed.
    fn drop_fork(&mut self, lane: LaneIndex);
    /// `worker` is dead and its lane forgets it.
    fn forget(&mut self, worker: WorkerTag);
    /// A request published into a lane woke its sleeping worker.
    fn woke_worker(&mut self);
    /// A queued request nobody waited for any more was dropped instead of run.
    fn skipped_unawaited(&mut self);
    /// Answers whoever waits on `reply`.
    fn reply(&mut self, reply: LaneReplySender, outcome: Result<HandlerResponse, LaneRequestError>);
    fn say(&mut self, notice: LaneNotice);
    /// Lane `lane` broke for good with `why` (`newly`: the first time); the run decides what it
    /// does to the server.
    fn lane_lost(&mut self, lane: LaneIndex, why: Arc<LaneLossError>, newly: bool);
    /// The host has `delta` more (or fewer) lanes that take requests.
    fn usable_lanes(&mut self, delta: isize);
    /// The requests in the host's lanes and queued behind them now (for other threads' spread).
    fn load(&mut self, load: usize);
    /// The host's lane waiting out a crash loop that forks first, or none; `watch` when a lane
    /// started waiting and the crash-loop watch must run.
    fn crash_wait(&mut self, soonest: Option<CrashLoopWait>, watch: bool);
}

/// Writes `job` into `slot` and publishes it, recording who waits for it; a request that does not
/// fit is not published and comes back with why.
#[expect(
    clippy::result_large_err,
    reason = "a refused job comes back by value, as it went in, to be forwarded or answered"
)]
#[inline(always)]
pub(super) fn publish(
    waiting: &mut [Option<LaneReplySender>],
    mut slot: RequestSlot<'_, MappedMemory, FutexDoorbell>,
    job: LaneJob,
    fx: &mut impl HostEffects,
) -> Result<(), (SlotRoomError, LaneJob)> {
    if let Err(e) = slot.write(|w| wire::encode_request(w, &job.request)) {
        return Err((e, job));
    }
    let index = slot.index();
    let rang = match slot.publish() {
        Ok(rang) => rang,
        Err(never) => match never {},
    };
    if rang.is_some() {
        fx.woke_worker();
    }
    waiting[index] = Some(job.reply);
    Ok(())
}

/// Returns whether a request taken from a queue still runs: one whose reply is closed has nobody
/// waiting for it. A request already in a ring or running is not asked again.
pub(super) fn runs_queued(reply_closed: bool) -> bool {
    !reply_closed
}

/// Moves `lane`'s queued requests into its ring while it has free slots, answering one that does
/// not fit with 413 and dropping one nobody waits for.
pub(super) fn refill(lane: &mut LocalLane, end: &mut LaneEnd, fx: &mut impl HostEffects) {
    while !lane.queued.is_empty() {
        let Some(slot) = end.request_slot() else {
            break;
        };
        let Some(job) = lane.queued.pop_front() else {
            break;
        };
        if !runs_queued(job.reply.is_closed()) {
            fx.skipped_unawaited();
            continue;
        }
        if let Err((e, job)) = publish(&mut lane.waiting, slot, job, fx) {
            fx.reply(job.reply, Err(LaneRequestError::TooLarge(e)));
        }
    }
}

/// What the core needs to know of the host's settings.
#[derive(Clone, Copy)]
pub(super) struct CoreConfig {
    pub(super) depth: RingDepth,
    /// A handler running longer than this is killed with its worker (504).
    pub(super) timeout: Duration,
    pub(super) crash_backoff: CrashBackoff,
}

impl HostCore {
    /// Lane `i` of this host and its channel end; `None` when the host does not own it.
    pub(super) fn lane(&mut self, i: LaneIndex) -> Option<(&mut LocalLane, &mut LaneEnd)> {
        let k = self.lanes.iter().position(|l| l.index == i)?;
        Some((&mut self.lanes[k], &mut self.ends[k]))
    }

    /// The requests in this host's lanes and queued behind them.
    pub(super) fn load(&self) -> usize {
        self.lanes.iter().zip(self.ends.iter()).map(|(l, e)| l.load(e)).sum()
    }

    /// Puts a request into one of the host's lanes ([`health::lane_pick`]): published at once when
    /// the lane has a free slot and nothing queued, else queued behind it. A request it cannot place
    /// comes back with why: the lanes are full, it does not fit a slot, or no lane takes requests
    /// ([`HostCore::no_lane_refusal`] says why, off the busy path).
    // Always inlined into the shell's submit: it is the busy path, and out of line it costs the request a move.
    #[inline(always)]
    #[expect(
        clippy::result_large_err,
        reason = "a request that is not placed comes back by value, as it went in, to be forwarded or answered"
    )]
    pub(super) fn submit(
        &mut self,
        job: LaneJob,
        queue_per_worker: usize,
        fx: &mut impl HostEffects,
    ) -> Result<(), Rejection> {
        let HostCore { lanes, ends, .. } = self;
        let loads = lanes
            .iter()
            .zip(ends.iter())
            .map(|(l, e)| l.worker.takes_requests().then(|| l.load(e)));
        let k = match health::lane_pick(loads, queue_per_worker) {
            LanePick::Lane(k) => k,
            LanePick::Full => {
                return Err(Rejection::Refused(
                    LaneRequestError::Rejected(health::lanes_full()),
                    job,
                ));
            }
            LanePick::NoLane => return Err(Rejection::NoLane(job)),
        };
        let (lane, end) = (&mut lanes[k], &mut ends[k]);
        match end.request_slot() {
            Some(slot) if lane.queued.is_empty() => publish(&mut lane.waiting, slot, job, fx)
                .map_err(|(e, job)| Rejection::Refused(LaneRequestError::TooLarge(e), job))?,
            Some(_) | None => lane.queued.push_back(job),
        }
        Ok(())
    }

    /// Why no lane of the host takes a request: the run is `stopping`, or a lane waits out a crash
    /// loop (said with when it forks, from `now_ns`), or the first broken lane's cause.
    pub(super) fn no_lane_refusal(
        &self,
        backoff: &CrashBackoff,
        stopping: bool,
        now_ns: u64,
        host: usize,
    ) -> LaneUnansweredError {
        let wait = soonest_wait(&self.lanes, &self.crash_runs, backoff).map(|wait| (wait, now_ns));
        let cause = || {
            let cause = match self.lanes.iter().find_map(LocalLane::broken_why) {
                Some(why) => why.to_string(),
                None => format!("process lane host {host} has no lane"),
            };
            Arc::from(cause)
        };
        health::no_usable_lane(stopping, wait, cause)
    }

    /// Reads every published answer of the host's lanes into `done` (each copied out of its slot
    /// into `buf`, [`take_answers`]) and refills each lane read; returns how many it read. A lane
    /// with nothing new costs one atomic load. A worker that wrote what no worker writes is
    /// handled where it is found ([`fault`]).
    pub(super) fn collect(
        &mut self,
        buf: &mut BytesMut,
        depth: RingDepth,
        done: &mut Completions,
        fx: &mut impl HostEffects,
    ) -> u64 {
        let mut read = 0u64;
        for (lane, end) in self.lanes.iter_mut().zip(self.ends.iter_mut()) {
            if !end.has_news() {
                continue;
            }
            match take_answers(end, buf, &mut lane.waiting, depth, done) {
                (took, Ok(())) => {
                    read += took;
                    refill(lane, end, fx);
                }
                (took, Err(f)) => {
                    read += took;
                    fault(lane, end, f, depth, done, fx);
                }
            }
        }
        read
    }

    /// The host's lane waiting out a crash loop that forks first.
    pub(super) fn soonest_wait(&self, backoff: &CrashBackoff) -> Option<CrashLoopWait> {
        soonest_wait(&self.lanes, &self.crash_runs, backoff)
    }

    /// `dead`, which the host saw die, ended with `exit` (the host read it after the death, at
    /// `now_ns`): takes over its lane's channel ([`bury`]), refills the lane for the next worker,
    /// and decides what follows — while the run serves, the next worker now or after a crash-loop
    /// wait ([`HostCore::crashed`]), or the lane breaks when the worker refused to start; while it
    /// stops (`stopping`), none. A worker its lane no longer runs changes nothing.
    pub(super) fn worker_exited(
        &mut self,
        dead: WorkerTag,
        exit: &WorkerExit,
        now_ns: u64,
        stopping: bool,
        config: &CoreConfig,
        fx: &mut impl HostEffects,
    ) {
        let i = dead.lane;
        let Some((lane, end)) = self.lane(i) else {
            return;
        };
        let Some(burial) = bury(lane, end, dead, exit, now_ns, config.timeout, config.depth) else {
            return;
        };
        if let Some(fault) = burial.fault {
            fx.say(LaneNotice::PeerFault { worker: dead, fault });
        }
        fx.forget(dead);
        refill(lane, end, fx);
        if !stopping {
            fx.say(LaneNotice::WorkerDied {
                worker: dead,
                cause: burial.cause,
                exit: exit.clone(),
            });
        }
        if refused_start(exit) {
            self.break_lane(i, LaneLossError::StartRefused { pid: dead.pid }, config, fx);
        } else if !stopping {
            self.crashed(i, burial.read_at_death, exit, now_ns, config, fx);
        }
        fx.load(self.load());
        for (reply, outcome) in burial.answered {
            fx.reply(reply, outcome);
        }
    }

    /// Lane `i`'s worker died with `exit` at `now_ns`, its answers read up to `read_at_death`:
    /// the lane's crash streak grows, or a new one starts (saying the recovery) if it answered
    /// since the last death; the crash-loop alert is said when due; and the next worker is forked
    /// now, or after the lane waits out the crash loop with its requests refused.
    fn crashed(
        &mut self,
        i: LaneIndex,
        read_at_death: u32,
        exit: &WorkerExit,
        now_ns: u64,
        config: &CoreConfig,
        fx: &mut impl HostEffects,
    ) {
        let backoff = config.crash_backoff;
        let (mut streak, recovered) = streak_after_death(&mut self.crash_runs, i, read_at_death, exit, now_ns);
        if let Some(crashes) = recovered {
            fx.say(LaneNotice::Recovered { lane: i, crashes });
        }
        let alert = streak.alert_due(&backoff, now_ns);
        let Some((lane, end)) = self.lane(i) else {
            return;
        };
        let refused = match streak.respawn(&backoff, now_ns) {
            RespawnStep::Fork => {
                let attempt = fx.take_attempt();
                lane.begin_fork(end, attempt, now_ns);
                fx.fork(i, attempt);
                None
            }
            RespawnStep::Delay(_) => {
                let wait = streak.wait(i, &backoff);
                Some(lane.close_for_crash_loop(end, &wait, now_ns, config.depth))
            }
        };
        let read_at_death = end.window().read().get();
        self.crash_runs.push(LaneCrashStreak {
            lane: i,
            run: streak,
            read_at_death,
        });
        if refused.is_some() {
            fx.usable_lanes(-1);
            fx.crash_wait(self.soonest_wait(&backoff), true);
        }
        if alert {
            self.say_crash_loop(i, &backoff, fx);
        }
        for (reply, refusal) in refused.into_iter().flatten() {
            fx.reply(reply, Err(LaneRequestError::Rejected(refusal)));
        }
    }

    /// Says lane `i`'s crash-loop alert, with its streak's facts.
    fn say_crash_loop(&mut self, i: LaneIndex, backoff: &CrashBackoff, fx: &mut impl HostEffects) {
        let waiting = self.lane(i).is_some_and(|(lane, _)| lane.waiting());
        let Some(run) = self.crash_runs.iter().find(|r| r.lane == i) else {
            return;
        };
        let crashes = run.run.crashes();
        fx.say(LaneNotice::CrashLoop {
            lane: i,
            crashes,
            exit: run.run.last_exit().clone(),
            wait: backoff.wait(crashes),
            waiting,
        });
    }

    /// Breaks lane `i`, which could not get a worker again ([`HostCore::mark_broken`]), and tells
    /// the run.
    pub(super) fn break_lane(
        &mut self,
        i: LaneIndex,
        why: LaneLossError,
        config: &CoreConfig,
        fx: &mut impl HostEffects,
    ) {
        let why = Arc::new(why);
        let newly = self.mark_broken(i, &why, config, fx);
        fx.lane_lost(i, why, newly);
    }

    /// Latches lane `i` broken with `why`: its requests are answered with why, it takes no more,
    /// its crash run ends. Returns whether it was not broken before, so a lane is counted broken
    /// once; a lane this host does not own changes nothing.
    pub(super) fn mark_broken(
        &mut self,
        i: LaneIndex,
        why: &Arc<LaneLossError>,
        config: &CoreConfig,
        fx: &mut impl HostEffects,
    ) -> bool {
        fx.say(LaneNotice::LaneLost {
            lane: i,
            why: Arc::clone(why),
        });
        let Some((lane, _)) = self.lane(i) else {
            return false;
        };
        let Breakage {
            newly,
            counted,
            was_waiting,
            was_forking,
            replies,
        } = lane.break_for_good(why);
        if was_forking {
            fx.drop_fork(i);
        }
        if counted {
            fx.usable_lanes(-1);
        }
        // A broken lane's crash run is over: it forks no more.
        if let Some(k) = self.crash_runs.iter().position(|r| r.lane == i) {
            if self.crash_runs.swap_remove(k).run.alerted() {
                fx.say(LaneNotice::LoopEnded { lane: i });
            }
        }
        if was_waiting {
            fx.crash_wait(self.soonest_wait(&config.crash_backoff), false);
        }
        fx.load(self.load());
        let cause: Arc<str> = Arc::from(why.to_string());
        for reply in replies {
            fx.reply(
                reply,
                Err(LaneRequestError::Rejected(LaneUnansweredError::LaneLost {
                    lane: i,
                    cause: Arc::clone(&cause),
                })),
            );
        }
        newly
    }

    /// Looks once at the crash runs at `now_ns` ([`tend_crash_runs`]): forks the lanes whose wait
    /// is over, says the recoveries and the alerts due, and returns when to look again (`None`
    /// once no lane loops).
    pub(super) fn tend(&mut self, now_ns: u64, config: &CoreConfig, fx: &mut impl HostEffects) -> Option<u64> {
        let Tend {
            forks,
            recovered,
            alerts,
            next_ns,
        } = tend_crash_runs(self, &config.crash_backoff, now_ns);
        let forked = forks.len();
        for i in forks {
            if let Some((lane, end)) = self.lane(i) {
                let attempt = fx.take_attempt();
                lane.begin_fork(end, attempt, now_ns);
                fx.fork(i, attempt);
            }
        }
        if forked > 0 {
            fx.usable_lanes(forked as isize);
            fx.crash_wait(self.soonest_wait(&config.crash_backoff), false);
        }
        for (lane, crashes) in recovered {
            fx.say(LaneNotice::Recovered { lane, crashes });
        }
        for i in alerts {
            self.say_crash_loop(i, &config.crash_backoff, fx);
        }
        next_ns
    }

    /// Lane `i`'s fork answered: with its new worker, which runs the lane and has its start
    /// watched; or with why it could not, which breaks the lane. An answer for a lane that no
    /// longer forks (it broke meanwhile) changes nothing here.
    pub(super) fn forked(
        &mut self,
        i: LaneIndex,
        answer: Result<WorkerTag, LaneLossError>,
        config: &CoreConfig,
        fx: &mut impl HostEffects,
    ) {
        let Some((lane, _)) = self.lane(i) else {
            return;
        };
        let LaneWorker::Forking { since_ns, attempt } = lane.worker else {
            return;
        };
        match answer {
            Ok(worker) => {
                lane.worker = LaneWorker::Running {
                    worker,
                    starting: Some(Replacement { since_ns, attempt }),
                    head: None,
                };
            }
            Err(why) => {
                lane.worker = LaneWorker::Gone;
                self.break_lane(i, why, config, fx);
            }
        }
    }

    /// Looks at the start reports of the replacements not yet seen ready (the time, `now_ns`, is
    /// read only for one that is): a ready
    /// one is said with its latency; one that cannot start breaks its lane (it would fail again).
    pub(super) fn look_at_starts(&mut self, now_ns: impl Fn() -> u64, config: &CoreConfig, fx: &mut impl HostEffects) {
        let mut failed = Vec::new();
        for (lane, end) in self.lanes.iter_mut().zip(self.ends.iter()) {
            match lane.look_at_start(end, &now_ns) {
                Some(StartLook::Ready { latency_us }) => fx.say(LaneNotice::Respawned { latency_us }),
                Some(StartLook::Failed(why)) => failed.push((lane.index, why)),
                None => {}
            }
        }
        for (i, why) in failed {
            self.break_lane(i, why, config, fx);
        }
    }

    /// Kills, at `now_ns`, the worker of any lane whose running handler is past the timeout.
    pub(super) fn check_timeouts(&mut self, now_ns: u64, config: &CoreConfig, fx: &mut impl HostEffects) {
        let timeout = config.timeout.as_nanos() as u64;
        for (lane, end) in self.lanes.iter_mut().zip(self.ends.iter()) {
            if let Some(worker) = lane.overrun(end, now_ns, timeout) {
                fx.kill(worker);
                fx.say(LaneNotice::TimedOut);
            }
        }
    }

    /// The run stopped and the host ends: every request still in its lanes or queued is answered
    /// 503, the crash runs end with it, and a replacement still forking is dropped, since no answer
    /// of its fork is read once the host ends.
    pub(super) fn close(&mut self, fx: &mut impl HostEffects) {
        for ended in self.crash_runs.drain(..) {
            if ended.run.alerted() {
                fx.say(LaneNotice::LoopEnded { lane: ended.lane });
            }
        }
        for lane in &mut self.lanes {
            if matches!(lane.worker, LaneWorker::Forking { .. }) {
                lane.worker = LaneWorker::Gone;
                fx.drop_fork(lane.index);
            }
            for reply in lane.take_every_reply() {
                fx.reply(
                    reply,
                    Err(LaneRequestError::Rejected(LaneUnansweredError::ShuttingDown)),
                );
            }
        }
    }
}

/// `lane`'s worker wrote what no correct worker writes (`fault`), found while reading its answers
/// ([`LocalLane::fault`]): a worker that runs is killed, and what the lane gives up goes into
/// `done`. It is kept out of line, so the reading loop pays only for the comparison.
#[cold]
pub(super) fn fault(
    lane: &mut LocalLane,
    end: &mut LaneEnd,
    fault: PeerFault,
    depth: RingDepth,
    done: &mut Completions,
    fx: &mut impl HostEffects,
) {
    match lane.fault(end, fault, depth, done) {
        FaultStep::Killed(worker) => {
            fx.say(LaneNotice::PeerFault { worker, fault });
            fx.kill(worker);
        }
        FaultStep::Kept => {}
        FaultStep::GaveUp => fx.say(LaneNotice::FaultWithoutWorker {
            lane: lane.index,
            fault,
        }),
    }
}

/// Is sysexits.h's EX_CONFIG, the status of a worker that refuses to start because it cannot trust what it would
/// start with: another build's `mipyre`, or a lane it cannot map. It writes nothing into the lane, and the server
/// breaks the lane for good instead of starting another. `START_REFUSED` in `mipyre._process_lane.server` is the same.
pub const START_REFUSED: i32 = 78;

/// Whether a worker that ended with `exit` refused to start ([`START_REFUSED`]): it cannot trust
/// what it would start with, and no replacement could either.
pub(super) fn refused_start(exit: &WorkerExit) -> bool {
    *exit == WorkerExit::Exited(START_REFUSED)
}

/// A request [`HostCore::submit`] could not place, handed back.
pub(super) enum Rejection {
    /// No lane of the host takes requests: [`HostCore::no_lane_refusal`] says why.
    NoLane(LaneJob),
    /// Refused, with why: the lanes are full, or it does not fit a slot.
    Refused(LaneRequestError, LaneJob),
}
