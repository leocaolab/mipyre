//! Runs a pool worker's loop on its own thread: a sync worker takes requests off its queue, an
//! async worker runs its engine.

use std::time::Duration;

use super::watchdog::SyncLane;
use super::{answer_if_awaited, answer_queue_if_last, PoolJob};
use std::sync::Arc;

use crate::engine_queue::{EngineInbox, EngineJob};
use crate::exchange::error::WorkerGroup;
use crate::exchange::error::{refuse, HandlerError, UnansweredError};
use crate::exchange::loop_wait::LoopMailbox;
use crate::job_queue::JobReceiver;
use crate::live_workers::WorkerLife;
use crate::websocket::WsConnection;
use crate::worker::async_engine::AsyncEngine;
use crate::worker::sync_runner::{ServeOutcome, UnitOutcome};
use crate::worker::SubInterpreterWorker;

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

/// Runs the async engine (`_worker/async_engine.py`) until its queue closes or it stops; requests it
/// had taken were answered as it let go of them (see `AsyncJob`). The last async worker to die
/// answers the requests still queued with why.
/// Serves until the engine ends; it then stops taking WebSocket connections, so one handed later
/// goes to another engine or is refused at once instead of waiting out its handshake's budget.
pub(super) fn serve_async(
    mut worker: SubInterpreterWorker<AsyncEngine>,
    inbox: EngineInbox,
    connections: Option<Arc<LoopMailbox<WsConnection>>>,
    life: WorkerLife,
) {
    let (ended, left) = worker.run_async_engine(inbox, connections.clone());
    if let Some(connections) = connections {
        drop(connections.close());
    }
    match ended {
        Ok(()) => life.stopped(),
        Err(e) => {
            tracing::error!(
                target: "mipyre::server",
                worker = worker.worker_id().get(),
                error = %e,
                "async worker stopped serving"
            );
            if let Some(cause) = life.died(e.to_string()) {
                refuse_left(left, &cause);
            }
        }
    }

    worker.end_on_own_thread();
}

fn refuse_left(left: impl IntoIterator<Item = EngineJob>, cause: &std::sync::Arc<str>) {
    for job in left {
        job.refuse(UnansweredError::WorkersDied {
            group: WorkerGroup::Subinterp,
            cause: std::sync::Arc::clone(cause),
        });
    }
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
