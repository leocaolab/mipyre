//! Hands a request to a lane host: on this thread's own lanes when it hosts some, else through another host's inbox,
//! and carries its answer back to whoever waits for it.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use super::health::{self, CrashLoopWait, HostLoad};
use super::host::{LaneHost, HOST};
use super::lane_failure::{LaneDecodeError, LaneUnansweredError, WorkerFailure};
use super::os::monotonic_ns;
use super::run::{RunPhase, RunState, ServerLanes};
use super::wire::LaneRequest;
use crate::sync::Ordering::*;
use mipyre_core::HandoffBackend;
use mipyre_http::io::request_head::RequestHead;
use mipyre_http::plan::response::HandlerResponse;
use mipyre_shm::channel::SlotRoomError;

/// Why a lane request got no response. The request's side turns it into its error and
/// counts a refusal there, once.
#[derive(Debug)]
pub enum LaneRequestError {
    /// The lanes gave up on it: they did not take it (full, no run serving them, no usable
    /// lane), its worker died running it, it ran past the lane timeout, or its lane lost its
    /// worker for good.
    Rejected(LaneUnansweredError),
    TooLarge(SlotRoomError),
    WorkerFailed(WorkerFailure),
    /// The worker's answer could not be decoded, which only a bug causes.
    Malformed(LaneDecodeError),
}

impl From<LaneDecodeError> for LaneRequestError {
    fn from(malformed: LaneDecodeError) -> Self {
        LaneRequestError::Malformed(malformed)
    }
}

pub(super) type LaneReplySender = tokio::sync::oneshot::Sender<Result<HandlerResponse, LaneRequestError>>;

/// Hands `outcome` to whoever waits on `reply`. The waiter is gone only when it gave up on the
/// request (it timed out, its client went away, the server stopped): there is nobody left to
/// tell, so the outcome is dropped.
pub(super) fn answer_lane(reply: LaneReplySender, outcome: Result<HandlerResponse, LaneRequestError>) {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "a closed oneshot means the waiter gave up on the request; see the doc above"
    )]
    let _ = reply.send(outcome);
}

pub(crate) struct LaneJob {
    pub(crate) request: LaneRequest,
    pub(crate) reply: LaneReplySender,
}

/// One run of a handler on the lanes: the receiving end of its job's reply. Dropped while its job
/// is still queued on a host, the job is not run; dropped once the job is in a ring, the job runs
/// on and its answer is dropped.
pub struct LaneRun(tokio::sync::oneshot::Receiver<Result<HandlerResponse, LaneRequestError>>);

impl Future for LaneRun {
    type Output = Result<HandlerResponse, LaneRequestError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match ready!(Pin::new(&mut self.0).poll(cx)) {
            Ok(reply) => reply,
            Err(_dropped_unanswered) => Err(LaneRequestError::Rejected(LaneUnansweredError::Dropped)),
        })
    }
}

/// A refusal is only what the lanes would not take now; what they refuse after taking an ask
/// (another host forwarded to was full, the run stopped) is in the run's reply.
impl HandoffBackend for ServerLanes {
    type Ask = LaneRequest;
    type Reply = Result<HandlerResponse, LaneRequestError>;
    type Refusal = LaneRequestError;
    type Run = LaneRun;

    fn submit(&self, request: LaneRequest) -> Result<LaneRun, LaneRequestError> {
        let (reply, run) = tokio::sync::oneshot::channel();
        match ServerLanes::submit(self, LaneJob { request, reply }) {
            Ok(()) => Ok(LaneRun(run)),
            Err((why, _reply)) => Err(why),
        }
    }
}

thread_local! {

    /// Where this thread's next round-robin pick over a run's hosts starts. Per thread, so the
    /// threads that submit do not all write one shared line; the picks
    /// of each thread still go round every host.
    static TURN: Cell<usize> = const { Cell::new(0) };
}

/// Takes this thread's next round-robin turn ([`TURN`]).
fn next_turn() -> usize {
    TURN.with(|t| {
        let turn = t.get();
        t.set(turn.wrapping_add(1));
        turn
    })
}

/// The bytes a request takes in a slot, over-counted: its body, URI, headers and path
/// parameters, with a margin for the fields' framing.
fn approx_message_len(head: &RequestHead, body_len: usize) -> usize {
    body_len
        + head.parts.uri.to_string().len()
        + head
            .parts
            .headers
            .iter()
            .map(|(k, v)| k.as_str().len() + v.len() + 8)
            .sum::<usize>()
        + head.params.located().map(|(name, _)| name.len() + 12).sum::<usize>()
        + 256
}

fn fits(len: usize, room: u32) -> Result<(), SlotRoomError> {
    if len as u64 > u64::from(room) {
        return Err(SlotRoomError {
            room,
            needed: len as u64,
        });
    }
    Ok(())
}

impl ServerLanes {
    /// Builds the refusal for a request no host can take, from whether the run ended, the
    /// crash-looping lane that is back first, and the run's first failure (else a host's).
    fn no_host_left(&self, run: &RunState) -> LaneUnansweredError {
        let waiting = run
            .hosts
            .iter()
            .filter_map(|h| h.crash_wait.lock().clone())
            .reduce(CrashLoopWait::sooner)
            .map(|wait| wait.refusal(monotonic_ns()));
        let first = self.first_failure.lock().as_ref().map(ToString::to_string);
        let cause = first
            .or_else(|| run.hosts.iter().find_map(|h| h.failure.get().map(ToString::to_string)))
            .map(Arc::from);
        health::no_host_left(run.stop.is_cancelled(), waiting, cause)
    }

    /// Queues a request: on this thread's own lanes when it hosts some (no hand-off), else
    /// through a host's inbox. Never waits.
    pub(crate) fn submit(&self, job: LaneJob) -> Result<(), (LaneRequestError, LaneReplySender)> {
        if let Err(e) = self.check_fits(&job.request.head, job.request.body.len()) {
            return Err((LaneRequestError::TooLarge(e), job.reply));
        }
        let local = HOST.with(|h| {
            h.borrow()
                .as_ref()
                .filter(|host| std::ptr::eq(&*host.lanes, self))
                .cloned()
        });
        // A thread hosting lanes of the current run reads the run from its host: no shared
        // lock on this path. Its run stopped once its stop token is cancelled.
        if let Some(host) = local {
            if host.stop.is_cancelled() {
                return Err((LaneRequestError::Rejected(LaneUnansweredError::ShuttingDown), job.reply));
            }
            let run = &host.run;
            // This thread's own lanes, unless they are busy and another host's are clearly
            // less so: then that host's inbox (a cross-thread hop, taken only when it buys
            // parallelism; e.g. macOS hands every connection of a SO_REUSEPORT group to one
            // thread). When its own lanes are full, another host too, before 503.
            let job = match self.spread_target(&host) {
                Some(k) => {
                    host.counts().spread_forwards.add(1);
                    match run.hosts[k].send(job) {
                        Ok(()) => return Ok(()),
                        // Its inbox went since it was checked: this host's own lanes take it.
                        Err(job) => job,
                    }
                }
                None => job,
            };
            return match host.submit(job) {
                Ok(()) => Ok(()),
                // Its lanes are full, or none is usable: another host that still serves takes it.
                Err((refused @ LaneRequestError::Rejected(_), job)) => {
                    let start = next_turn() % run.hosts.len();
                    match run.hand_to(start, Some(host.index), job) {
                        Ok(()) => {
                            host.counts().overflow_forwards.add(1);
                            Ok(())
                        }
                        Err(job) => Err((refused, job.reply)),
                    }
                }
                Err((failed, job)) => Err((failed, job.reply)),
            };
        }
        let run = match &*self.run.read() {
            RunPhase::Serving(run) => Arc::clone(run),
            RunPhase::Idle | RunPhase::Starting => {
                return Err((LaneRequestError::Rejected(LaneUnansweredError::ShuttingDown), job.reply))
            }
        };
        let start = next_turn() % run.hosts.len();
        match run.hand_to(start, None, job) {
            Ok(()) => Ok(()),
            Err(job) => Err((LaneRequestError::Rejected(self.no_host_left(&run)), job.reply)),
        }
    }

    /// Another host to give a request to instead of `host` ([`health::loaded_to_spread`],
    /// [`health::spread_to`]), while `host`'s handlers run long.
    fn spread_target(&self, host: &LaneHost) -> Option<usize> {
        let run = &host.run;
        if run.hosts.len() < 2 {
            return None;
        }
        let own = run.hosts[host.index].load.load(Relaxed);
        if !health::loaded_to_spread(own, host.owned.borrow().lanes.len()) || !host.running_long() {
            return None;
        }
        let start = next_turn() % run.hosts.len();
        let others = (0..run.hosts.len())
            .map(|i| (start + i) % run.hosts.len())
            .filter(|&i| i != host.index && run.hosts[i].serving())
            .map(|i| HostLoad {
                index: i,
                load: run.hosts[i].load.load(Relaxed),
            });
        health::spread_to(own, others)
    }

    /// Checks that a request fits a slot, so one that does not gets 413 before it is queued.
    fn check_fits(&self, head: &RequestHead, body_len: usize) -> Result<(), SlotRoomError> {
        fits(approx_message_len(head, body_len), self.geometry.max_message())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane_set::MIN_SLOT_BYTES;

    /// A message exactly a slot's room fits it; one byte more is refused before it is queued, with
    /// both sizes.
    #[test]
    fn a_message_fits_a_slot_up_to_its_room_and_one_byte_more_is_too_large() {
        let room = (MIN_SLOT_BYTES - 8) as u32;
        assert!(fits(room as usize, room).is_ok());
        assert!(fits(0, room).is_ok());
        match fits(room as usize + 1, room) {
            Err(SlotRoomError { needed, room: r }) => assert_eq!((needed, r), (u64::from(room) + 1, room)),
            Ok(()) => panic!("a message one byte past the slot fits"),
        }
    }

    fn poll_once(run: LaneRun) -> Poll<Result<HandlerResponse, LaneRequestError>> {
        std::pin::pin!(run).poll(&mut Context::from_waker(std::task::Waker::noop()))
    }

    /// A run whose job's reply was dropped unanswered is the lanes losing the request.
    #[test]
    fn a_run_whose_reply_was_dropped_unanswered_is_a_lost_request() {
        let (reply, run) = tokio::sync::oneshot::channel();
        drop(reply);
        assert!(matches!(
            poll_once(LaneRun(run)),
            Poll::Ready(Err(LaneRequestError::Rejected(LaneUnansweredError::Dropped)))
        ));
    }

    /// A run is pending until the lanes answer its job, and then it is that answer.
    #[test]
    fn a_run_is_pending_until_its_job_is_answered_and_then_is_the_answer() {
        let (reply, run) = tokio::sync::oneshot::channel();
        let mut run = std::pin::pin!(LaneRun(run));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(run.as_mut().poll(&mut cx).is_pending());
        answer_lane(
            reply,
            Err(LaneRequestError::Rejected(LaneUnansweredError::ShuttingDown)),
        );
        assert!(matches!(
            run.poll(&mut cx),
            Poll::Ready(Err(LaneRequestError::Rejected(LaneUnansweredError::ShuttingDown)))
        ));
    }
}
