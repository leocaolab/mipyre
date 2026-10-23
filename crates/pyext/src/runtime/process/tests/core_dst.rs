//! Deterministic simulation of a host's lanes (b/743, `docs/design/lane-host-core.md` §4): the host's core
//! ([`HostCore`]) driven by a seeded world — a client sending requests, a worker per lane that takes and answers
//! them through its channel's own peer end, crashes at any step (idle, or holding a request it took), writes what no
//! worker writes, or overruns; forks that answer with a worker, fail, or bring one that refuses to start; a clock
//! that moves; and the run's stop, at a seeded step, as the shell meets it: new requests refused, the rest of the
//! poller's round (deaths, forks, timeouts, answers, but no crash-loop fork), then the close, while the workers still
//! run and hold requests; after it the workers go on with no host, until the run kills them. Every step picks the next
//! thing from the seed, so a failing seed replays the same schedule. The judgments are b/720's invariants by name
//! (`docs/design/lane-lifecycle/`), each checked after every step: `NoDuplicate` (no request taken by two workers;
//! the oneshot gives each at most one answer), `SlotsConserved`, and `AnsweredOrFailed` (no request dropped
//! unanswered; once the host closed, every one answered); at the close, `EndedHostLeavesNothing`.

use std::collections::HashMap;

use super::*;

/// A seeded generator (xorshift64*): the world's only source of choice.
struct Dice(u64);

impl Dice {
    fn new(seed: u64) -> Self {
        Dice(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// What a step's choice found: each the fact the world's next event is.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Event {
    Send,
    Collect,
    Serve { lane: usize },
    CrashHolding { lane: usize },
    CrashIdle { lane: usize },
    Corrupt { lane: usize },
    Overrun { lane: usize },
    Deaths,
    Forks,
    Tick { ns: u64 },
}

/// A lane's worker as the world plays it.
struct SimWorker {
    tag: WorkerTag,
    end: PeerEnd<MappedMemory, NoBell, NoSleep>,
    /// It refuses to start: it reports nothing and exits with `START_REFUSED`.
    refuses: bool,
}

/// What the world knows of a worker that is gone, for the host to be told.
struct Death {
    tag: WorkerTag,
    exit: WorkerExit,
}

/// The host's effects, as the world carries them out.
#[derive(Default)]
struct SimFx {
    seen: Seen,
    attempts: u32,
    kills: Vec<WorkerTag>,
    forks: Vec<(LaneIndex, Attempt)>,
    lost: Vec<(LaneIndex, bool)>,
}

impl HostEffects for SimFx {
    fn take_attempt(&mut self) -> Attempt {
        self.attempts += 1;
        Attempt::new(self.attempts)
    }
    fn kill(&mut self, worker: WorkerTag) {
        self.kills.push(worker);
    }
    fn fork(&mut self, lane: LaneIndex, attempt: Attempt) {
        self.forks.push((lane, attempt));
    }
    fn drop_fork(&mut self, lane: LaneIndex) {
        self.forks.retain(|&(l, _)| l != lane);
    }
    fn forget(&mut self, _worker: WorkerTag) {}
    fn woke_worker(&mut self) {}
    fn reply(&mut self, reply: LaneReplySender, outcome: Result<HandlerResponse, LaneRequestError>) {
        answer_if_awaited(reply, outcome);
    }
    fn say(&mut self, notice: LaneNotice) {
        let seen = &mut self.seen;
        match notice {
            LaneNotice::WorkerDied { .. } => seen.deaths += 1,
            LaneNotice::PeerFault { .. } | LaneNotice::FaultWithoutWorker { .. } => seen.faults += 1,
            LaneNotice::CrashLoop { .. } => seen.crash_loops += 1,
            LaneNotice::Recovered { .. } => seen.recoveries += 1,
            LaneNotice::LaneLost { .. } => seen.lanes_lost += 1,
            LaneNotice::TimedOut => seen.timeouts += 1,
            LaneNotice::Respawned { .. } => seen.respawns += 1,
            LaneNotice::LoopEnded { .. } => {}
        }
    }
    fn lane_lost(&mut self, lane: LaneIndex, _why: Arc<LaneLossError>, newly: bool) {
        self.lost.push((lane, newly));
    }
    fn usable_lanes(&mut self, _delta: isize) {}
    fn load(&mut self, _load: usize) {}
    fn crash_wait(&mut self, _soonest: Option<CrashLoopWait>, _watch: bool) {}
}

/// What the runs went through, summed over seeds: the simulation is worth its seeds only if they reach each of
/// these.
#[derive(Default, Debug, Clone, Copy, PartialEq)]
struct Seen {
    deaths: u32,
    faults: u32,
    crash_loops: u32,
    recoveries: u32,
    lanes_lost: u32,
    timeouts: u32,
    respawns: u32,
    /// Closes that found requests still held, which the close answered.
    closes_holding: u32,
    /// Deaths the host was told of while the run stopped.
    deaths_while_stopping: u32,
}

impl Seen {
    fn add(&mut self, b: Seen) {
        self.deaths += b.deaths;
        self.faults += b.faults;
        self.crash_loops += b.crash_loops;
        self.recoveries += b.recoveries;
        self.lanes_lost += b.lanes_lost;
        self.timeouts += b.timeouts;
        self.respawns += b.respawns;
        self.closes_holding += b.closes_holding;
        self.deaths_while_stopping += b.deaths_while_stopping;
    }
}

/// A request the client sent: who waits for its answer, until it has it.
enum Answer {
    Waiting(Sent),
    Got { served: bool },
}

/// What one seed's run went through and how it went: two runs of a seed are the same run only if all of it is
/// equal.
#[derive(Debug, PartialEq)]
struct Ran {
    seen: Seen,
    events: Vec<Event>,
    /// Whether each request, by its number, got a worker's answer rather than a refusal or a 5xx.
    served: Vec<bool>,
}

/// A request the client sent: who waits for its answer.
type Sent = tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>>;

/// One seeded run of a host with `LANES` lanes.
struct World {
    rng: Dice,
    now: u64,
    config: CoreConfig,
    core: HostCore,
    fds: Vec<OwnedFd>,
    workers: Vec<Option<SimWorker>>,
    deaths: Vec<Death>,
    fx: SimFx,
    sent: Vec<Answer>,
    /// The workers that took each request (by its number). A worker that took one and dropped it unanswered
    /// takes it again at its next wait (its cursor moves only when it answers): still one worker.
    runs: HashMap<u32, Vec<WorkerTag>>,
    buf: BytesMut,
    done: Completions,
    stopping: bool,
    next_pid: i32,
    events: Vec<Event>,
}

const LANES: usize = 2;
const REQUESTS: u32 = 40;
const STEPS: usize = 600;
/// The most host steps the rest of the poller's round takes after the stop, before its close.
const ROUND_REST: u64 = 6;
/// The workers' steps after the close, before the run kills them.
const AFTER_CLOSE: usize = 8;
/// A handler that runs this long is killed: well inside what a tick moves the clock.
const TIMEOUT: Duration = Duration::from_millis(50);
/// Crash loops that start after two deaths in a row and wait about as long as a tick moves the clock, so a run sees
/// lanes enter and leave them.
const BACKOFF: CrashBackoff = CrashBackoff {
    after: 2,
    first: Duration::from_millis(30),
    cap: Duration::from_millis(200),
    alert_every: Duration::from_secs(1),
};

impl World {
    fn new(seed: u64) -> Self {
        let geometry = lane_geometry(DEPTH, MIN_SLOT_BYTES).unwrap();
        let mut fx = SimFx::default();
        let (mut lanes, mut ends, mut fds, mut workers) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for i in 0..LANES {
            let (memory, fd) = MappedMemory::create(&geometry).unwrap();
            let channel = Arc::new(Channel::new(memory));
            let mut end = HostEnd::new(Arc::clone(&channel), FutexDoorbell);
            let attempt = fx.take_attempt();
            end.begin_attempt(attempt);
            let tag = WorkerTag {
                lane: LaneIndex(i),
                attempt,
                pid: Pid::new(1000 + i as i32).unwrap(),
            };
            lanes.push(LocalLane {
                index: LaneIndex(i),
                waiting: (0..DEPTH).map(|_| None).collect(),
                queued: Default::default(),
                worker: LaneWorker::Running {
                    worker: tag,
                    starting: None,
                    head: None,
                },
            });
            ends.push(end);
            fds.push(fd);
            workers.push(Some(SimWorker {
                tag,
                end: PeerEnd::new(channel, NoBell, NoSleep),
                refuses: false,
            }));
        }
        let config = CoreConfig {
            depth: geometry.depth(),
            timeout: TIMEOUT,
            crash_backoff: BACKOFF,
        };
        World {
            rng: Dice::new(seed),
            now: 1_000_000_000,
            config,
            core: HostCore {
                lanes,
                ends,
                crash_runs: Vec::new(),
            },
            fds,
            workers,
            deaths: Vec::new(),
            fx,
            sent: Vec::new(),
            runs: HashMap::new(),
            buf: BytesMut::with_capacity(4096),
            done: Completions::new(),
            stopping: false,
            next_pid: 2000,
            events: Vec::with_capacity(STEPS),
        }
    }

    fn pick(&mut self) -> Event {
        let lane = self.rng.below(LANES as u64) as usize;
        match self.rng.below(100) {
            0..=19 => Event::Send,
            20..=34 => Event::Collect,
            35..=59 => Event::Serve { lane },
            60..=62 => Event::CrashHolding { lane },
            63..=64 => Event::CrashIdle { lane },
            65 => Event::Corrupt { lane },
            66..=67 => Event::Overrun { lane },
            68..=79 => Event::Deaths,
            80..=89 => Event::Forks,
            _ => Event::Tick {
                ns: self.rng.below(2 * TIMEOUT.as_nanos() as u64),
            },
        }
    }

    fn step(&mut self, event: Event) {
        self.events.push(event);
        match event {
            Event::Send => self.send(),
            Event::Collect => {
                self.core
                    .collect(&mut self.buf, self.config.depth, &mut self.done, &mut self.fx);
                for (reply, outcome) in self.done.drain(..) {
                    answer_if_awaited(reply, outcome);
                }
            }
            Event::Serve { lane } => self.serve(lane, true),
            Event::CrashHolding { lane } => {
                self.serve(lane, false);
                self.die(
                    lane,
                    WorkerExit::Signaled {
                        signal: libc::SIGSEGV,
                        name: "Segmentation fault".into(),
                    },
                );
            }
            Event::CrashIdle { lane } => self.die(lane, WorkerExit::Exited(1)),
            Event::Corrupt { lane } => {
                if self.workers[lane].is_some() {
                    let raw = RawChannel::map(self.fds[lane].as_fd(), lane_geometry(DEPTH, MIN_SLOT_BYTES).unwrap());
                    raw.store(HeaderWord::RespHead, 0xDEAD);
                }
            }
            Event::Overrun { lane } => {
                // The worker takes a request and holds it; the clock then runs past the timeout.
                self.serve(lane, false);
            }
            Event::Deaths => self.deliver_deaths(),
            Event::Forks => self.answer_forks(),
            Event::Tick { ns } => {
                self.now += ns;
                self.core.check_timeouts(self.now, &self.config, &mut self.fx);
                // The crash-loop watch ends with the run (`watch_crash_loops`), so a stopping host forks no lane
                // out of its wait.
                if !self.stopping {
                    self.core.tend(self.now, &self.config, &mut self.fx);
                }
                let now = self.now;
                self.core.look_at_starts(|| now, &self.config, &mut self.fx);
            }
        }
        self.carry_out_kills();
    }

    fn send(&mut self) {
        let id = self.sent.len() as u32;
        if id >= REQUESTS {
            return;
        }
        let (job, answer) = numbered_job(id);
        self.sent.push(Answer::Waiting(answer));
        // The shell refuses a request once the run stops, before the core sees it (`ServerLanes::submit`).
        if self.stopping {
            answer_if_awaited(
                job.reply,
                Err(LaneRequestError::Rejected(UnansweredError::WorkersGone(
                    WorkerGroup::Lanes,
                ))),
            );
            return;
        }
        match self.core.submit(job, DEPTH as usize, &mut self.fx) {
            Ok(()) => {}
            Err(Rejection::Refused(why, job)) => answer_if_awaited(job.reply, Err(why)),
            Err(Rejection::NoLane(job)) => {
                let refusal = self
                    .core
                    .no_lane_refusal(&self.config.crash_backoff, self.stopping, self.now, 0);
                answer_if_awaited(job.reply, Err(LaneRequestError::Rejected(refusal)));
            }
        }
    }

    /// Lane `lane`'s worker takes its next request; with `answer`, it answers it.
    fn serve(&mut self, lane: usize, answer: bool) {
        let Some(worker) = &mut self.workers[lane] else {
            return;
        };
        if worker.refuses {
            return;
        }
        let Ok(Wait::Request(request)) = worker.end.wait_for_request(SpinWindow::until(0), || 0, 0) else {
            return;
        };
        let id = {
            let view = request.bytes().expect("the host writes whole requests");
            let req = wire::decode_request(&view).expect("the host encodes what a worker decodes");
            req.route_key
                .path
                .trim_start_matches("/r")
                .parse::<u32>()
                .expect("a numbered request")
        };
        let tag = worker.tag;
        let takers = self.runs.entry(id).or_default();
        if !takers.contains(&tag) {
            takers.push(tag);
        }
        if answer {
            let ok = HandlerResponse::new(
                hyper::StatusCode::OK,
                bytes::Bytes::from_static(b"ok"),
                mipyre_http::plan::response::JSON,
                mipyre_http::plan::response::HeaderLines::new(),
            );
            let Ok(_) = request.answer(|w| wire::encode_response(w, &ok).expect("fits"));
        }
    }

    fn die(&mut self, lane: usize, exit: WorkerExit) {
        if let Some(worker) = self.workers[lane].take() {
            self.deaths.push(Death { tag: worker.tag, exit });
        }
    }

    /// The host's kills take: the worker it named is gone.
    fn carry_out_kills(&mut self) {
        for tag in std::mem::take(&mut self.fx.kills) {
            let lane = tag.lane.0;
            if self.workers[lane].as_ref().is_some_and(|w| w.tag == tag) {
                self.die(
                    lane,
                    WorkerExit::Signaled {
                        signal: libc::SIGKILL,
                        name: "Killed".into(),
                    },
                );
            }
        }
    }

    fn deliver_deaths(&mut self) {
        for death in std::mem::take(&mut self.deaths) {
            if self.stopping {
                self.fx.seen.deaths_while_stopping += 1;
            }
            self.core.worker_exited(
                death.tag,
                &death.exit,
                self.now,
                self.stopping,
                &self.config,
                &mut self.fx,
            );
        }
    }

    /// Answers every fork the host asked for: a worker that starts, one that refuses to, or none.
    fn answer_forks(&mut self) {
        for (lane, attempt) in std::mem::take(&mut self.fx.forks) {
            let i = lane.0;
            match self.rng.below(10) {
                0 => {
                    let why = LaneLossError::NotRespawned(ForkError::NoTemplate);
                    self.core.forked(lane, Err(why), &self.config, &mut self.fx);
                }
                k => {
                    self.next_pid += 1;
                    let tag = WorkerTag {
                        lane,
                        attempt,
                        pid: Pid::new(self.next_pid).unwrap(),
                    };
                    let channel = Arc::clone(self.core.ends[i].channel());
                    let end = PeerEnd::new(channel, NoBell, NoSleep);
                    let refuses = k == 1;
                    if refuses {
                        self.deaths.push(Death {
                            tag,
                            exit: WorkerExit::Exited(START_REFUSED),
                        });
                    } else {
                        end.report_ready(attempt);
                        self.workers[i] = Some(SimWorker { tag, end, refuses });
                    }
                    self.core.forked(lane, Ok(tag), &self.config, &mut self.fx);
                }
            }
        }
    }

    /// `SlotsConserved`: no lane holds more requests in its slots than its depth, and who waits is
    /// recorded for no more than that.
    fn slots_conserved(&self) -> Result<(), String> {
        for (lane, end) in self.core.lanes.iter().zip(self.core.ends.iter()) {
            let waiting = lane.waiting.iter().filter(|w| w.is_some()).count();
            if end.in_flight() > DEPTH || waiting > DEPTH as usize {
                return Err(format!(
                    "lane {}: {} in flight, {waiting} waiting, depth {DEPTH}",
                    lane.index,
                    end.in_flight()
                ));
            }
        }
        Ok(())
    }

    /// Closes the host as its poller does once the run stopped: whatever it still holds is answered.
    fn close(&mut self) {
        if held(&self.core) > 0 {
            self.fx.seen.closes_holding += 1;
        }
        self.core.close(&mut self.fx);
    }

    /// Plays one step of the workers after their host closed: they take, answer and die with nobody reading.
    fn after_close(&mut self) {
        let lane = self.rng.below(LANES as u64) as usize;
        match self.rng.below(3) {
            0 => self.serve(lane, true),
            1 => self.serve(lane, false),
            _ => self.die(lane, WorkerExit::Exited(1)),
        }
    }

    fn judge(&mut self, closed: bool) -> Result<(), String> {
        no_duplicate(&self.runs)
            .and(self.slots_conserved())
            .and(answered_or_failed(&mut self.sent, closed))
    }
}

/// Builds request number `id` (`GET /r<id>`) and who waits for it.
fn numbered_job(id: u32) -> (LaneJob, Sent) {
    let (job, answer) = job();
    let LaneJob { mut request, reply } = job;
    request.route_key.path = format!("/r{id}");
    (LaneJob { request, reply }, answer)
}

/// `NoDuplicate`: no request was taken by two workers.
fn no_duplicate(runs: &HashMap<u32, Vec<WorkerTag>>) -> Result<(), String> {
    match runs.iter().find(|(_, takers)| takers.len() > 1) {
        Some((id, takers)) => Err(format!(
            "request {id} was taken by {} workers: {takers:?}",
            takers.len()
        )),
        None => Ok(()),
    }
}

/// Counts the requests `core` still holds: in a lane's slots or queued for one.
fn held(core: &HostCore) -> usize {
    core.lanes
        .iter()
        .map(|l| l.waiting.iter().flatten().count() + l.queued.len())
        .sum()
}

/// `EndedHostLeavesNothing`: a closed host holds no request, and waits for no fork it asked for: once it is closed
/// no fork's answer is read, so a fork it neither saw answer nor dropped makes a worker nobody kills (b/940).
fn ended_host_leaves_nothing(core: &HostCore, forks: &[(LaneIndex, Attempt)]) -> Result<(), String> {
    match (held(core), forks) {
        (0, []) => Ok(()),
        (0, forks) => Err(format!("closed with forks asked and never dropped: {forks:?}")),
        (n, _) => Err(format!("the closed host still holds {n} requests")),
    }
}

/// `AnsweredOrFailed`, after every step: no request sent is lost — each has its answer (a 5xx counting) or is still
/// held by the host, never dropped unanswered; and once the host `closed`, each has its answer.
fn answered_or_failed(sent: &mut [Answer], closed: bool) -> Result<(), String> {
    for (id, answer) in sent.iter_mut().enumerate() {
        let Answer::Waiting(receiver) = answer else {
            continue;
        };
        match receiver.try_recv() {
            Ok(outcome) => {
                *answer = Answer::Got {
                    served: outcome.is_ok(),
                }
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) if !closed => {}
            Err(e) => return Err(format!("request {id} got no answer: {e:?}")),
        }
    }
    Ok(())
}

/// Runs seed `seed` to its end and returns the first invariant it broke, with the step; else how it went.
fn run(seed: u64) -> Result<Ran, String> {
    let mut world = World::new(seed);
    let stop_at = world.rng.below(STEPS as u64) as usize;
    for step in 0..stop_at {
        let event = world.pick();
        world.step(event);
        world
            .judge(false)
            .map_err(|e| format!("seed {seed}, step {step} ({event:?}): {e}"))?;
    }

    world.stopping = true;
    let rest = world.rng.below(ROUND_REST + 1);
    for step in 0..rest {
        let event = world.pick();
        world.step(event);
        world
            .judge(false)
            .map_err(|e| format!("seed {seed}, stopping, step {step} ({event:?}): {e}"))?;
    }
    world.close();
    ended_host_leaves_nothing(&world.core, &world.fx.forks)
        .and(world.judge(true))
        .map_err(|e| format!("seed {seed}, at the close after {stop_at} steps and {rest} stopping: {e}"))?;
    for step in 0..AFTER_CLOSE {
        world.after_close();
        world
            .judge(true)
            .map_err(|e| format!("seed {seed}, closed, step {step}: {e}"))?;
    }

    let served = world
        .sent
        .iter()
        .map(|a| matches!(a, Answer::Got { served: true }))
        .collect();
    Ok(Ran {
        seen: world.fx.seen,
        events: world.events,
        served,
    })
}

/// The merge gate's seeds: the same schedules every run.
const SEEDS: std::ops::Range<u64> = 0..64;

/// Seeds a random run found failing; each runs every time.
const REGRESSIONS: &[u64] = &[];

#[test]
fn a_host_under_crashes_faults_timeouts_failed_forks_and_a_stop_answers_every_request_once() {
    let mut seen = Seen::default();
    for seed in SEEDS.chain(REGRESSIONS.iter().copied()) {
        match run(seed) {
            Ok(one) => seen.add(one.seen),
            Err(e) => panic!("{e}"),
        }
    }
    // The seeds reach every kind of trouble, so a green run says something about each.
    let Seen {
        deaths,
        faults,
        crash_loops,
        recoveries,
        lanes_lost,
        timeouts,
        respawns,
        closes_holding,
        deaths_while_stopping,
    } = seen;
    let reached = [
        deaths,
        faults,
        crash_loops,
        recoveries,
        lanes_lost,
        timeouts,
        respawns,
        closes_holding,
        deaths_while_stopping,
    ];
    assert!(reached.iter().all(|&n| n > 0), "{seen:?}");
}

/// A seed is its schedule: run twice, it makes the same choices and ends the same way, so a seed a run prints
/// replays its failure.
#[test]
fn a_seed_replays_the_same_run() {
    for seed in [0, 17, u64::MAX] {
        assert_eq!(run(seed), run(seed), "seed {seed}");
    }
}

/// The nightly run (b/743): seeds from the clock, many of them, built only under `--cfg lane_dst_random`; a failing
/// seed is printed for [`REGRESSIONS`].
#[cfg(lane_dst_random)]
#[test]
fn a_host_under_random_schedules_answers_every_request_once() {
    let base = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    for k in 0..10_000 {
        if let Err(e) = run(base.wrapping_add(k)) {
            panic!("{e}");
        }
    }
}

/// The judgments themselves, fed what a broken host would leave: a request taken twice (as a host that handed a dead
/// worker's requests to the next one would leave, b/925) is `NoDuplicate`'s red; one taken once is green.
#[test]
fn the_duplicate_judgment_is_red_for_a_request_taken_twice_and_green_once() {
    let worker = |attempt| WorkerTag {
        lane: LaneIndex(0),
        attempt: Attempt::new(attempt),
        pid: Pid::new(1000 + attempt as i32).unwrap(),
    };
    assert!(no_duplicate(&HashMap::from([(0, vec![worker(1)]), (1, vec![worker(1)])])).is_ok());
    assert!(no_duplicate(&HashMap::from([(0, vec![worker(1)]), (1, vec![worker(1), worker(2)])])).is_err());
}

/// A request whose sender went unanswered, or that a closed host still holds, is `AnsweredOrFailed`'s red; an
/// answered one, a refusal included, and one the serving host holds, green.
#[test]
fn the_lost_request_judgment_is_red_for_a_dropped_sender_and_green_for_a_refusal() {
    let (job, answer) = numbered_job(0);
    answer_if_awaited(
        job.reply,
        Err(LaneRequestError::Rejected(UnansweredError::WorkersGone(
            WorkerGroup::Lanes,
        ))),
    );
    assert!(answered_or_failed(&mut [Answer::Waiting(answer)], true).is_ok());
    let (held, answer) = numbered_job(1);
    let mut sent = [Answer::Waiting(answer)];
    assert!(answered_or_failed(&mut sent, false).is_ok(), "still held by the host");
    assert!(
        answered_or_failed(&mut sent, true).is_err(),
        "the host closed and never answered it"
    );
    drop(held);
    assert!(answered_or_failed(&mut sent, false).is_err(), "dropped unanswered");
}

/// A host that closed with a request queued is `EndedHostLeavesNothing`'s red; one that holds none, green.
#[test]
fn the_ended_host_judgment_is_red_for_a_request_left_queued_and_green_for_none() {
    let mut world = World::new(0);
    assert!(ended_host_leaves_nothing(&world.core, &[]).is_ok());
    let (job, _answer) = numbered_job(0);
    world.core.lanes[0].queued.push_back(job);
    assert!(ended_host_leaves_nothing(&world.core, &[]).is_err());
}

/// A host that closed with a fork it asked for and never dropped is `EndedHostLeavesNothing`'s red; one that waits
/// for none, green.
#[test]
fn the_ended_host_judgment_is_red_for_a_fork_left_asked_and_green_for_none() {
    let world = World::new(0);
    assert!(ended_host_leaves_nothing(&world.core, &[]).is_ok());
    assert!(ended_host_leaves_nothing(&world.core, &[(LaneIndex(0), Attempt::new(8))]).is_err());
}

/// The close drops the fork of a lane whose replacement is forking: the judgment's red above is what a close that
/// did not would leave (seed 2 before b/940).
#[test]
fn a_close_mid_fork_drops_the_fork() {
    let mut world = World::new(0);
    world.die(0, WorkerExit::Exited(1));
    world.deliver_deaths();
    assert!(matches!(world.core.lanes[0].worker, LaneWorker::Forking { .. }));
    assert_eq!(world.fx.forks.len(), 1);
    world.close();
    assert!(world.fx.forks.is_empty());
    assert!(matches!(world.core.lanes[0].worker, LaneWorker::Gone));
}
