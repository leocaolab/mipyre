//! Runs a pool worker's loop on its own thread: a sync worker takes requests off its queue, an
//! async worker runs its engine.

use std::time::Duration;

use super::watchdog::SyncLane;
use super::{answer_if_awaited, answer_queue_if_last, PoolJob};
use crate::exchange::error::{refuse, HandlerError, UnansweredError};
use crate::runtime::job_queue::JobReceiver;
use crate::runtime::live_workers::WorkerLife;
use crate::runtime::worker::async_engine::AsyncEngine;
use crate::runtime::worker::sync_runner::{ServeOutcome, UnitOutcome};
use crate::runtime::worker::SubInterpreterWorker;
use crate::runtime::worker_api::AsyncInbox;

/// Turns an error caused by the watchdog's interruption into the budget's 504
/// (`UnansweredError::Interrupted`), so the client gets the same answer whether the worker's reply
/// or the caller's timer arrives first. An error the handler raised by itself is kept.
fn settle_interrupted<T>(
    response: Result<T, HandlerError>,
    interrupted: bool,
    budget: Duration,
) -> Result<T, HandlerError> {
    match response {
        Err(raised) if interrupted => Err(refuse(UnansweredError::Interrupted {
            budget,
            raised: Box::new(raised),
        })),
        other => other,
    }
}

/// It stops serving with its lane's life, unless the watchdog abandoned it and gave that life to
/// its replacement.
pub(super) fn serve_sync(
    mut worker: SubInterpreterWorker,
    rx: JobReceiver<PoolJob>,
    lane: &SyncLane,
    budget: Duration,
) {
    loop {
        // The watchdog abandoned this worker mid-request, and its replacement serves the queue.
        if lane.retired() {
            break;
        }
        let Some(req) = rx.recv() else { break };
        // Nobody waits for this request any more, so a backlog's CPU is not spent on it.
        if req.reply.is_closed() {
            PoolJob::inc_completed();
            continue;
        }

        let (route, request, reply) = req.into_request();
        let label = request.label();

        lane.begin(route.index());
        let UnitOutcome {
            served: ServeOutcome { response, retired },
            interrupted,
        } = worker.serve(route, request, |unit| lane.publish_unit(unit));
        lane.end();

        let answer = settle_interrupted(response, interrupted, budget);
        answer_if_awaited(reply, answer.map_err(|e| e.log(&label)));
        PoolJob::inc_completed();
        // Its interpreter may be damaged, so it takes no more requests and the others take its
        // share of the queue.
        if let Some(retirement) = retired {
            if let Some(life) = lane.take_life() {
                answer_queue_if_last(life.retired(&retirement), &rx);
            }
            worker.end_on_own_thread();
            return;
        }
    }

    if let Some(life) = lane.take_life() {
        life.stopped();
    }
    worker.end_on_own_thread();
}

/// Runs the async engine (`_async_engine.py`) until its inbox closes or it stops; requests it
/// still held were answered as it let go of them (see `AsyncJob`). The last async worker to die
/// answers `queued`.
pub(super) fn serve_async(
    mut worker: SubInterpreterWorker<AsyncEngine>,
    inbox: AsyncInbox,
    queued: &JobReceiver<PoolJob>,
    life: WorkerLife,
) {
    match worker.run_async_engine(inbox) {
        Ok(()) => life.stopped(),
        Err(e) => {
            tracing::error!(
                target: "mipyre::server",
                worker = worker.worker_id().get(),
                error = %e,
                "async worker stopped serving"
            );
            answer_queue_if_last(life.died(e.to_string()), queued);
        }
    }

    worker.end_on_own_thread();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raised() -> HandlerError {
        HandlerError::panic(Box::new("stands in for the KeyboardInterrupt the handler raised"))
    }

    #[test]
    fn an_error_from_our_interruption_is_the_budget_timeout_whatever_arrives_first() {
        // The 504 matches what the caller's timer gives, so the client cannot see which won.
        let budget = Duration::from_secs(4);
        let settled = settle_interrupted::<u8>(Err(raised()), true, budget);
        let err = settled.expect_err("an error stays an error");
        assert!(
            format!("{err}").starts_with("the handler was interrupted at its 4s request budget"),
            "got: {err}"
        );
        assert!(matches!(err, HandlerError::Refused(_)));
    }

    #[test]
    fn an_error_the_handler_raised_by_itself_stays_a_handler_error() {
        let settled = settle_interrupted::<u8>(Err(raised()), false, Duration::from_secs(4));
        assert!(matches!(settled.err(), Some(HandlerError::Panicked { .. })));
    }

    #[test]
    fn a_reply_that_is_not_an_error_is_untouched_even_if_interrupted() {
        let settled = settle_interrupted(Ok(7u8), true, Duration::from_secs(4));
        assert_eq!(settled.ok(), Some(7));
    }
}
