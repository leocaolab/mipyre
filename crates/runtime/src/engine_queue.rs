//! Hands `async def` requests to the async engines of one interpreter kind (main's one engine, or the async pool's
//! workers) and counts the ones in flight, queued or running, against one limit.
//!
//! Each engine's asyncio loop waits on its own doorbell (`crate::sync::Doorbell`) as one more reader on its selector; a push rings
//! the engine that has waited longest, and only an engine that is waiting, so a burst while the engines are busy
//! costs no ring at all. An engine takes everything queued when it wakes and waits again as it takes the last job.
//! No wake-up is lost because an engine starts waiting only under the lock and only when the queue is empty then,
//! and quiets its doorbell before it looks, so a ring after that is never read away unseen.
//!
//! It does not know Python, the loop or its tasks, which backend it serves, or why an engine stopped: the engine's
//! owner says that through what it does with the jobs [`EngineInbox::leave`] hands back.

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;

use mipyre_util::throttle::LogThrottle;

use super::job_queue::JobSendError;
use super::main_threads::MainReplySender;
use super::pool::{PoolJob, PoolReplySender, ReplySender};
use crate::exchange::error::{refuse, HandlerError, UnansweredError};
use crate::exchange::request::Request;
use crate::sync::Doorbell;
use crate::sync::Mutex;
use mipyre_http::io::request_head::RequestLabel;

/// Holds back repeats of a doorbell the OS would not ring, which would otherwise log once per request.
const RING_FAILED_LOG_PERIOD: Duration = Duration::from_secs(60);

/// One request for an async engine.
pub(crate) struct EngineJob {
    /// Indexes the engine's handlers: a worker's by route, main's by handler.
    pub(crate) handler: usize,
    pub(crate) request: Request,
    pub(crate) reply: EngineReply,
}

/// Where an async engine's answer to one request goes, which decides what the answer may be: only main can answer
/// with a `Stream`.
pub(crate) enum EngineReply {
    Pool(PoolReplySender),
    Main(MainReplySender),
}

impl EngineReply {
    fn is_closed(&self) -> bool {
        match self {
            EngineReply::Pool(reply) => reply.is_closed(),
            EngineReply::Main(reply) => reply.is_closed(),
        }
    }

    /// Answers the request `label` names with `error`, logged.
    pub(crate) fn fail(self, label: &RequestLabel, error: HandlerError) {
        match self {
            EngineReply::Pool(reply) => send_answer(reply, label, Err(error)),
            EngineReply::Main(reply) => send_answer(reply, label, Err(error)),
        }
    }
}

impl EngineJob {
    /// Answers the job with `refusal` without running it.
    pub fn refuse(self, refusal: UnansweredError) {
        self.reply.fail(&self.request.label(), refuse(refusal));
    }
}

/// Logs an error with the request's label, then sends `result` to the waiting caller.
///
/// A caller that stopped waiting already said why, so the dropped result is noted only at debug.
pub(crate) fn send_answer<T>(reply: ReplySender<T>, label: &RequestLabel, result: Result<T, HandlerError>) {
    let result = result.map_err(|e| e.log(label));
    if reply.send(result).is_err() {
        tracing::debug!(target: "mipyre::server", request_id = %label.id, "nobody waits any more (the caller's budget ran out, or its client went away); dropping the result");
    }
    PoolJob::inc_completed();
}

/// How the queue sees one engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EngineState {
    /// In `waiting`: the next push rings its doorbell.
    Waiting,
    /// Rung, or taking: it looks at the queue again before it waits.
    Awake,
    /// Its loop ended; it takes nothing more.
    Gone,
}

/// What one look at the queue gives an engine.
#[derive(Debug, PartialEq, Eq)]
enum QueueFront<T> {
    /// A job whose caller still waits, now counted running; `last` when it emptied the queue, so the engine now
    /// waits.
    Job { job: T, last: bool },
    /// A job whose caller gave up: not run and not counted.
    Dropped(T),
    /// Nothing queued; the engine now waits.
    Empty,
    /// Nothing more will be taken: the sender is gone, or the engine is.
    Closed,
}

struct QueueState<T> {
    /// Is allocated for `limit` at the start and never grows, so nothing allocates under the lock.
    jobs: VecDeque<T>,
    limit: usize,
    /// Counts the jobs taken whose [`TaskPlace`] has not been dropped.
    running: usize,
    closed: bool,
    engines: Box<[EngineState]>,
    /// Holds each `Waiting` engine once, longest waiting first; allocated for every engine at the start.
    waiting: VecDeque<usize>,
    /// Counts the engines not `Gone`.
    present: usize,
}

/// Returns whether one more job may be queued: an engine must be left to take it, and the jobs queued and running
/// together must stay under `limit`.
fn admit(present: usize, queued: usize, running: usize, limit: usize) -> Result<(), JobSendError> {
    if present == 0 {
        Err(JobSendError::NoReceiverLeft)
    } else if queued + running >= limit {
        Err(JobSendError::QueueFull)
    } else {
        Ok(())
    }
}

impl<T> QueueState<T> {
    fn new(limit: usize, engines: usize) -> Self {
        QueueState {
            jobs: VecDeque::with_capacity(limit),
            limit,
            running: 0,
            closed: false,
            engines: vec![EngineState::Waiting; engines].into_boxed_slice(),
            waiting: (0..engines).collect(),
            present: engines,
        }
    }

    /// Queues `job` and returns the engine to ring, if one waits; a refused job comes back with why.
    fn push(&mut self, job: T) -> Result<Option<usize>, (JobSendError, T)> {
        if let Err(refused) = admit(self.present, self.jobs.len(), self.running, self.limit) {
            return Err((refused, job));
        }
        self.jobs.push_back(job);
        let rung = self.waiting.pop_front();
        if let Some(engine) = rung {
            self.engines[engine] = EngineState::Awake;
        }
        Ok(rung)
    }

    /// Gives `engine` the next job; `gone` says whether a job's caller stopped waiting.
    fn front(&mut self, engine: usize, gone: impl Fn(&T) -> bool) -> QueueFront<T> {
        if self.closed || self.engines[engine] == EngineState::Gone {
            self.unwait(engine);
            return QueueFront::Closed;
        }
        match self.jobs.pop_front() {
            Some(job) if gone(&job) => QueueFront::Dropped(job),
            Some(job) => {
                self.running += 1;
                let last = self.jobs.is_empty();
                if last {
                    self.wait(engine);
                }
                QueueFront::Job { job, last }
            }
            None => {
                self.wait(engine);
                QueueFront::Empty
            }
        }
    }

    fn wait(&mut self, engine: usize) {
        if self.engines[engine] == EngineState::Awake {
            self.engines[engine] = EngineState::Waiting;
            self.waiting.push_back(engine);
        }
    }

    fn unwait(&mut self, engine: usize) {
        if self.engines[engine] == EngineState::Waiting {
            self.waiting.retain(|&waiting| waiting != engine);
            self.engines[engine] = EngineState::Awake;
        }
    }

    /// Takes `engine` out for good and returns every queued job when it was the last engine, so its owner answers
    /// them rather than leave them to their budgets.
    fn leave(&mut self, engine: usize) -> VecDeque<T> {
        if self.engines[engine] == EngineState::Gone {
            return VecDeque::new();
        }
        self.unwait(engine);
        self.engines[engine] = EngineState::Gone;
        self.present -= 1;
        if self.present == 0 {
            std::mem::take(&mut self.jobs)
        } else {
            VecDeque::new()
        }
    }

    /// Closes the queue and returns the engines to ring so they see it.
    fn close(&mut self) -> VecDeque<usize> {
        self.closed = true;
        let rung = std::mem::take(&mut self.waiting);
        for &engine in &rung {
            self.engines[engine] = EngineState::Awake;
        }
        rung
    }

    fn release(&mut self) {
        self.running -= 1;
    }

    /// Puts back to waiting an engine whose ring did not go out, so the next push rings it again.
    fn rewait(&mut self, engine: usize) {
        if !self.closed {
            self.wait(engine);
        }
    }
}

struct EngineQueue {
    state: Mutex<QueueState<EngineJob>>,
    /// Is indexed by engine.
    doorbells: Box<[Doorbell]>,
    ring_failed_log: LogThrottle,
}

impl EngineQueue {
    /// Rings `engine`. A ring the OS refuses cannot happen while the queue owns the doorbell and it holds at most one
    /// unread ring; if it does, the engine waits again, so the job is taken at the next push or answered by its
    /// budget.
    fn ring(&self, engine: usize) {
        let Err(error) = self.doorbells[engine].ring() else {
            return;
        };
        self.state.lock().rewait(engine);
        if let Some(held) = self.ring_failed_log.admit(mipyre_util::clock::now()) {
            tracing::error!(
                target: "mipyre::server",
                engine,
                error = %error,
                held,
                "could not ring an async engine's doorbell; its queued requests wait for the next request or their budget"
            );
        }
    }
}

/// Makes the queue of `engines` async engines, admitting at most `limit` jobs in flight, and each engine's end.
///
/// # Errors
///
/// Fails when the OS will not make a doorbell.
pub(crate) fn engine_queue(limit: usize, engines: usize) -> io::Result<(EngineSender, Vec<EngineInbox>)> {
    let queue = new_queue(limit, engines)?;
    let inboxes = (0..engines).map(|engine| EngineInbox::of(&queue, engine)).collect();
    Ok((EngineSender(queue), inboxes))
}

/// Makes the queue of one async engine, as [`engine_queue`] does.
///
/// # Errors
///
/// Fails when the OS will not make the doorbell.
pub(crate) fn one_engine_queue(limit: usize) -> io::Result<(EngineSender, EngineInbox)> {
    let queue = new_queue(limit, 1)?;
    let inbox = EngineInbox::of(&queue, 0);
    Ok((EngineSender(queue), inbox))
}

fn new_queue(limit: usize, engines: usize) -> io::Result<Arc<EngineQueue>> {
    let doorbells = (0..engines)
        .map(|_| Doorbell::new())
        .collect::<io::Result<Box<[_]>>>()?;
    Ok(Arc::new(EngineQueue {
        state: Mutex::new(QueueState::new(limit, engines)),
        doorbells,
        ring_failed_log: LogThrottle::new(RING_FAILED_LOG_PERIOD),
    }))
}

/// Is the one sending end; dropping it closes the queue, and every engine then takes nothing more.
pub(crate) struct EngineSender(Arc<EngineQueue>);

impl EngineSender {
    /// Queues `job` and rings an engine that waits; a refused job is dropped here, after the lock is released.
    pub(crate) fn submit(&self, job: EngineJob) -> Result<(), JobSendError> {
        let pushed = self.0.state.lock().push(job);
        match pushed {
            Ok(None) => Ok(()),
            Ok(Some(engine)) => {
                self.0.ring(engine);
                Ok(())
            }
            Err((refused, job)) => {
                drop(job);
                Err(refused)
            }
        }
    }

    /// Returns how many jobs may be in flight at once, queued or running.
    pub(crate) fn capacity(&self) -> usize {
        self.0.state.lock().limit
    }
}

impl Drop for EngineSender {
    fn drop(&mut self) {
        let rung = self.0.state.lock().close();
        for engine in rung {
            self.0.ring(engine);
        }
    }
}

/// Is one engine's end of the queue; dropping it leaves the queue.
pub(crate) struct EngineInbox {
    queue: Arc<EngineQueue>,
    engine: usize,
}

/// Holds one taken job's place in the count of jobs in flight until it is dropped, which is when the task running the
/// job ends.
pub(crate) struct TaskPlace(Arc<EngineQueue>);

impl Drop for TaskPlace {
    fn drop(&mut self) {
        self.0.state.lock().release();
    }
}

impl EngineInbox {
    fn of(queue: &Arc<EngineQueue>, engine: usize) -> Self {
        EngineInbox {
            queue: Arc::clone(queue),
            engine,
        }
    }

    /// Returns the fd the engine's loop waits on: readable when the engine should take.
    pub(crate) fn doorbell(&self) -> RawFd {
        self.queue.doorbells[self.engine].fd()
    }

    /// Takes every job queued whose caller still waits, each with its place in flight, leaving the engine waiting;
    /// `None` once the queue is closed or the engine left, when jobs taken in the same call are dropped unrun.
    ///
    /// # Errors
    ///
    /// Fails when the doorbell cannot be read, which leaves it readable: the engine must stop, not look again.
    pub(crate) fn take(&self) -> io::Result<Option<Vec<(EngineJob, TaskPlace)>>> {
        self.queue.doorbells[self.engine].quiet()?;
        let mut taken = Vec::new();
        loop {
            let front = self.queue.state.lock().front(self.engine, |job| job.reply.is_closed());
            match front {
                QueueFront::Job { job, last } => {
                    taken.push((job, TaskPlace(Arc::clone(&self.queue))));
                    if last {
                        return Ok(Some(taken));
                    }
                }
                QueueFront::Dropped(job) => {
                    drop(job);
                    PoolJob::inc_completed();
                }
                QueueFront::Empty => return Ok(Some(taken)),
                QueueFront::Closed => return Ok(None),
            }
        }
    }

    /// Leaves the queue for good and returns the jobs still queued when this was the last engine; the caller answers
    /// them, or drops them, which tells their callers the engine lost them.
    pub(crate) fn leave(&self) -> VecDeque<EngineJob> {
        self.queue.state.lock().leave(self.engine)
    }
}

impl Drop for EngineInbox {
    fn drop(&mut self) {
        drop(self.leave());
    }
}

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;

    fn state(engines: usize, limit: usize) -> QueueState<u32> {
        QueueState::new(limit, engines)
    }

    fn live(_: &u32) -> bool {
        false
    }

    fn queue(s: &mut QueueState<u32>, job: u32) {
        assert!(s.push(job).is_ok(), "job {job} was refused");
    }

    #[test]
    fn one_place_short_of_the_limit_admits_and_at_it_refuses_counting_running_ones() {
        assert_eq!(admit(1, 2, 1, 4), Ok(()));
        assert_eq!(admit(1, 3, 1, 4), Err(JobSendError::QueueFull));
        assert_eq!(admit(1, 0, 4, 4), Err(JobSendError::QueueFull));
    }

    #[test]
    fn with_one_engine_left_it_admits_and_with_none_it_refuses_before_it_is_full() {
        assert_eq!(admit(1, 0, 0, 4), Ok(()));
        assert_eq!(admit(0, 0, 0, 4), Err(JobSendError::NoReceiverLeft));
        assert_eq!(admit(0, 4, 0, 4), Err(JobSendError::NoReceiverLeft));
    }

    #[test]
    fn a_push_rings_the_engine_waiting_longest_and_only_a_waiting_one() {
        let mut s = state(2, 8);
        assert_eq!(s.push(1).ok(), Some(Some(0)));
        assert_eq!(s.push(2).ok(), Some(Some(1)));
        assert_eq!(s.push(3).ok(), Some(None), "both engines are awake");
    }

    #[test]
    fn an_engine_takes_to_the_last_job_and_waits_as_it_takes_it() {
        let mut s = state(1, 8);
        queue(&mut s, 1);
        queue(&mut s, 2);
        assert_eq!(s.front(0, live), QueueFront::Job { job: 1, last: false });
        assert_eq!(s.push(3).ok(), Some(None), "the engine is taking, so it is not rung");
        assert_eq!(s.front(0, live), QueueFront::Job { job: 2, last: false });
        assert_eq!(s.front(0, live), QueueFront::Job { job: 3, last: true });
        assert_eq!(s.running, 3);
        assert_eq!(s.push(4).ok(), Some(Some(0)), "it waits again");
    }

    #[test]
    fn an_engine_rung_for_nothing_finds_the_queue_empty_and_waits_once() {
        let mut s = state(1, 8);
        queue(&mut s, 1);
        assert_eq!(s.front(0, live), QueueFront::Job { job: 1, last: true });
        assert_eq!(s.front(0, live), QueueFront::Empty);
        assert_eq!(s.waiting, [0], "it is in the waiting list once");
    }

    #[test]
    fn a_job_whose_caller_left_is_dropped_uncounted() {
        let mut s = state(1, 8);
        queue(&mut s, 7);
        assert_eq!(s.front(0, |&job| job == 7), QueueFront::Dropped(7));
        assert_eq!(s.running, 0);
        assert_eq!(s.front(0, live), QueueFront::Empty);
    }

    #[test]
    fn running_jobs_hold_their_places_until_released() {
        let mut s = state(1, 2);
        queue(&mut s, 1);
        queue(&mut s, 2);
        s.front(0, live);
        s.front(0, live);
        assert!(matches!(s.push(3), Err((JobSendError::QueueFull, 3))));
        s.release();
        assert_eq!(s.push(3).ok(), Some(Some(0)));
    }

    #[test]
    fn closing_rings_every_waiting_engine_and_each_then_sees_closed_with_jobs_left() {
        let mut s = state(3, 8);
        queue(&mut s, 1);
        assert_eq!(s.close(), [1, 2]);
        assert_eq!(s.front(0, live), QueueFront::Closed);
        assert_eq!(s.front(1, live), QueueFront::Closed);
        assert!(s.waiting.is_empty());
    }

    #[test]
    fn the_last_engine_to_leave_takes_the_queued_jobs_and_one_before_it_does_not() {
        let mut s = state(2, 8);
        queue(&mut s, 1);
        queue(&mut s, 2);
        assert!(s.leave(0).is_empty());
        assert_eq!(s.present, 1);
        assert_eq!(s.leave(1), [1, 2]);
        assert!(matches!(s.push(3), Err((JobSendError::NoReceiverLeft, 3))));
        assert!(s.leave(1).is_empty(), "leaving twice is leaving once");
        assert_eq!(s.present, 0);
    }

    #[test]
    fn a_waiting_engine_that_leaves_is_never_rung() {
        let mut s = state(2, 8);
        s.leave(0);
        assert_eq!(s.push(1).ok(), Some(Some(1)));
        assert_eq!(s.front(0, live), QueueFront::Closed);
    }

    #[test]
    fn an_engine_whose_ring_failed_is_rung_again_by_the_next_push() {
        let mut s = state(1, 8);
        assert_eq!(s.push(1).ok(), Some(Some(0)));
        s.rewait(0);
        s.rewait(0);
        assert_eq!(s.waiting, [0]);
        assert_eq!(s.push(2).ok(), Some(Some(0)));
    }

    #[test]
    fn a_job_pushed_while_the_engine_waits_is_never_left_without_a_ring() {
        // Every order of one push and one engine's take: after both, either the engine took the job or it was rung.
        let mut s = state(1, 8);
        assert_eq!(s.front(0, live), QueueFront::Empty);
        assert_eq!(s.push(1).ok(), Some(Some(0)));

        let mut s = state(1, 8);
        queue(&mut s, 1);
        assert_eq!(s.front(0, live), QueueFront::Job { job: 1, last: true });
        assert_eq!(s.push(2).ok(), Some(Some(0)));
    }

    #[test]
    fn a_take_with_nothing_queued_is_empty_and_once_the_sender_is_gone_is_closed() {
        let (sender, inbox) = one_engine_queue(4).expect("a doorbell");
        assert!(inbox.take().expect("a take").is_some_and(|taken| taken.is_empty()));
        drop(sender);
        assert!(inbox.take().expect("a take").is_none());
    }
}
