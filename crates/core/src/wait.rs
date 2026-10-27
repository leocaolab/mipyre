//! Waits for a backend's run within the request's budget. The budget starts once `start` is
//! ready, so a request whose body is still streaming to its handler is bounded by the body's own
//! limit until then, not by the handler's. The clock is set only once the run has to be waited
//! for: a run that is ready at its first poll never touches the timer.

use std::future::Future;
use std::pin::{pin, Pin};
use std::task::{ready, Context, Poll};

use mipyre_http::plan::limits::RequestBudget;
use pin_project_lite::pin_project;
use tokio::task::coop;
use tokio::time::Sleep;

use crate::backend::HandoffBackend;

/// Hands `ask` to `backend` now and returns the wait for its reply; the ask is gone when this
/// returns, so a request's future awaiting the wait does not carry it. The budget starts once
/// `start` is ready; a request whose body is already read passes `std::future::ready(())`.
///
/// # Errors
///
/// Returns the backend's refusal when it would not take the ask.
// One backend can be handed to from several call sites (the main interpreter's threads are, from
// TPC and from the multi-thread servers), and with several callers LLVM keeps this out of line,
// which copies the ask in and returns the wait through memory.
#[inline(always)]
pub fn dispatch<B: HandoffBackend, Start: Future>(
    backend: &B,
    ask: B::Ask,
    budget: RequestBudget,
    start: Start,
) -> Result<ReplyWait<B::Run, Start>, B::Refusal> {
    let run = backend.submit(ask)?;
    Ok(ReplyWait {
        run,
        clock: Clock::Before { start, budget },
    })
}

pin_project! {
    /// The wait for a run's reply, at most until the budget's deadline. It yields `None` when the
    /// deadline came first; a reply that is there by the poll that sees the deadline still counts.
    /// Dropping it drops the run.
    ///
    /// # Panics
    ///
    /// Once it has to wait, polling it outside a tokio runtime with time enabled panics.
    #[must_use = "the run is dropped, unanswered, with the wait"]
    pub struct ReplyWait<Run, Start> {
        #[pin]
        run: Run,
        #[pin]
        clock: Clock<Start>,
    }
}

pin_project! {
    #[project = ClockProj]
    enum Clock<Start> {
        Before { #[pin] start: Start, budget: RequestBudget },
        Running { #[pin] deadline: Sleep },
    }
}

impl<Run: Future, Start: Future> Future for ReplyWait<Run, Start> {
    type Output = Option<Run::Output>;

    // Out of line, each instance returns the reply through memory and the awaiting future copies it
    // into its own state, which costs more than awaiting tokio's `Timeout`.
    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Poll::Ready(reply) = this.run.poll(cx) {
            return Poll::Ready(Some(reply));
        }
        loop {
            match this.clock.as_mut().project() {
                ClockProj::Before { start, budget } => {
                    let budget = *budget;
                    ready!(start.poll(cx));
                    this.clock.set(Clock::Running {
                        deadline: tokio::time::sleep(budget.get()),
                    });
                }
                ClockProj::Running { deadline } => {
                    ready!(poll_deadline(deadline, cx));
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// Polls the deadline even when the run has spent the task's coop budget, as tokio's `Timeout`
/// does: a run that spends it at every poll would otherwise keep its deadline from ever firing.
#[inline(always)]
fn poll_deadline(deadline: Pin<&mut Sleep>, cx: &mut Context<'_>) -> Poll<()> {
    if coop::has_budget_remaining() {
        deadline.poll(cx)
    } else {
        pin!(coop::unconstrained(deadline)).poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::{ready, Ready};
    use std::rc::Rc;
    use std::task::Waker;
    use std::time::Duration;

    use super::*;

    fn budget(seconds: f64) -> RequestBudget {
        RequestBudget::from_secs(seconds).unwrap()
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Full;

    /// A backend whose run replies `after` the ask was taken; `full` refuses every ask.
    /// `dropped_unready` counts the runs dropped before they replied: the runs nobody waited for.
    struct TimerBackend {
        after: Duration,
        full: bool,
        dropped_unready: Rc<Cell<u32>>,
    }

    impl TimerBackend {
        fn replying_after(after: Duration) -> Self {
            Self {
                after,
                full: false,
                dropped_unready: Rc::default(),
            }
        }
    }

    struct TimerRun {
        sleep: Pin<Box<Sleep>>,
        reply: Option<&'static str>,
        dropped_unready: Rc<Cell<u32>>,
    }

    impl Future for TimerRun {
        type Output = &'static str;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<&'static str> {
            ready!(self.sleep.as_mut().poll(cx));
            Poll::Ready(self.reply.take().expect("a run is polled until it replies, not after"))
        }
    }

    impl Drop for TimerRun {
        fn drop(&mut self) {
            if self.reply.is_some() {
                self.dropped_unready.set(self.dropped_unready.get() + 1);
            }
        }
    }

    impl HandoffBackend for TimerBackend {
        type Ask = &'static str;
        type Reply = &'static str;
        type Refusal = Full;
        type Run = TimerRun;

        fn submit(&self, ask: &'static str) -> Result<TimerRun, Full> {
            if self.full {
                return Err(Full);
            }
            Ok(TimerRun {
                sleep: Box::pin(tokio::time::sleep(self.after)),
                reply: Some(ask),
                dropped_unready: Rc::clone(&self.dropped_unready),
            })
        }
    }

    /// A backend whose run has replied by its first poll.
    struct ReadyBackend;

    impl HandoffBackend for ReadyBackend {
        type Ask = &'static str;
        type Reply = &'static str;
        type Refusal = Full;
        type Run = Ready<&'static str>;

        fn submit(&self, ask: &'static str) -> Result<Ready<&'static str>, Full> {
            Ok(ready(ask))
        }
    }

    async fn reply<B: HandoffBackend>(backend: &B, ask: B::Ask, budget: RequestBudget) -> Option<B::Reply> {
        let Ok(wait) = dispatch(backend, ask, budget, ready(())) else {
            panic!("the backend takes the ask");
        };
        wait.await
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_within_the_budget_is_the_answer_and_its_run_was_waited_for() {
        let backend = TimerBackend::replying_after(Duration::from_secs(1));
        assert_eq!(reply(&backend, "ok", budget(1.0)).await, Some("ok"));
        assert_eq!(backend.dropped_unready.get(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_past_the_budget_times_out_and_its_run_is_dropped_unready() {
        let backend = TimerBackend::replying_after(Duration::from_millis(1001));
        assert_eq!(reply(&backend, "late", budget(1.0)).await, None);
        assert_eq!(backend.dropped_unready.get(), 1);
    }

    #[test]
    fn a_full_backend_refuses_without_a_run() {
        let backend = TimerBackend {
            full: true,
            ..TimerBackend::replying_after(Duration::ZERO)
        };
        assert!(matches!(dispatch(&backend, "ask", budget(1.0), ready(())), Err(Full)));
        assert_eq!(backend.dropped_unready.get(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_caller_that_leaves_mid_run_drops_the_run_unready() {
        let backend = TimerBackend::replying_after(Duration::from_secs(10));
        let left = tokio::time::timeout(Duration::from_secs(1), reply(&backend, "ask", budget(30.0))).await;
        assert!(left.is_err());
        assert_eq!(backend.dropped_unready.get(), 1);
    }

    #[test]
    fn a_run_ready_at_the_first_poll_answers_without_a_runtime() {
        let Ok(wait) = dispatch(&ReadyBackend, "ok", budget(1.0), ready(())) else {
            panic!("the backend takes the ask");
        };
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(pin!(wait).poll(&mut cx), Poll::Ready(Some("ok")));
    }

    #[tokio::test(start_paused = true)]
    async fn the_budget_starts_once_the_start_is_ready() {
        let body_read = tokio::time::sleep(Duration::from_secs(5));
        let backend = TimerBackend::replying_after(Duration::from_secs(6));
        let Ok(wait) = dispatch(&backend, "ok", budget(1.0), body_read) else {
            panic!("the backend takes the ask");
        };
        assert_eq!(wait.await, Some("ok"));

        let body_read = tokio::time::sleep(Duration::from_secs(5));
        let backend = TimerBackend::replying_after(Duration::from_millis(6001));
        let Ok(wait) = dispatch(&backend, "late", budget(1.0), body_read) else {
            panic!("the backend takes the ask");
        };
        assert_eq!(wait.await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_reply_before_the_start_is_ready_is_the_answer() {
        let backend = TimerBackend::replying_after(Duration::from_secs(2));
        let never = std::future::pending::<()>();
        let Ok(wait) = dispatch(&backend, "early", budget(1.0), never) else {
            panic!("the backend takes the ask");
        };
        assert_eq!(wait.await, Some("early"));
    }

    /// A run that spends the task's whole coop budget at every poll and never replies.
    struct GreedyRun;

    impl Future for GreedyRun {
        type Output = &'static str;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<&'static str> {
            loop {
                if pin!(coop::consume_budget()).poll(cx).is_pending() {
                    return Poll::Pending;
                }
            }
        }
    }

    struct GreedyBackend;

    impl HandoffBackend for GreedyBackend {
        type Ask = ();
        type Reply = &'static str;
        type Refusal = Full;
        type Run = GreedyRun;

        fn submit(&self, (): ()) -> Result<GreedyRun, Full> {
            Ok(GreedyRun)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_that_spends_the_coop_budget_still_times_out() {
        let waiting = tokio::spawn(reply(&GreedyBackend, (), budget(1.0)));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..256 {
            if waiting.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(waiting.is_finished(), "the deadline never fired");
        assert_eq!(waiting.await.unwrap(), None);
    }
}
