//! The backends and the ways of running a request that the bench and the tests share: a
//! queued run awaited through `dispatch`, or through `tokio::time::timeout` as `run_on_pool` does
//! today; a handler run on the calling thread, as a TPC thread's worker runs it.

use std::future::{ready, Future, Ready};
use std::hint::black_box;
use std::pin::{pin, Pin};
use std::task::{Context, Poll, Waker};

use mipyre_core::{dispatch, HandoffBackend};
use mipyre_http::plan::limits::RequestBudget;

/// The size of a reply that is a response, not a word: a run's reply is moved through the wait.
pub type Body = [u64; 8];

pub struct Full;

/// A backend whose run has replied by its first poll, as a pool run whose reply is in its oneshot
/// by the time the request's task is polled.
pub struct ReadyBackend;

impl HandoffBackend for ReadyBackend {
    type Ask = Body;
    type Reply = Body;
    type Refusal = Full;
    type Run = Ready<Body>;

    fn submit(&self, ask: Body) -> Result<Ready<Body>, Full> {
        Ok(ready(ask))
    }
}

/// A backend whose run replies at its second poll: the request waits once, so its deadline is set
/// and registered with the timer.
pub struct SecondPollBackend;

pub struct SecondPollRun {
    body: Body,
    polled: bool,
}

impl Future for SecondPollRun {
    type Output = Body;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Body> {
        if self.polled {
            return Poll::Ready(self.body);
        }
        self.polled = true;
        Poll::Pending
    }
}

impl HandoffBackend for SecondPollBackend {
    type Ask = Body;
    type Reply = Body;
    type Refusal = Full;
    type Run = SecondPollRun;

    fn submit(&self, ask: Body) -> Result<SecondPollRun, Full> {
        Ok(SecondPollRun {
            body: ask,
            polled: false,
        })
    }
}

/// Runs a handler on a TPC thread's own worker: it checks that the worker still serves, then runs
/// the handler, whose reply is a result known only once it returns.
pub fn serve_inline(ask: Body) -> Result<Body, Full> {
    if black_box(false) {
        return Err(Full);
    }
    black_box(Ok(ask))
}

/// Awaits a request's run the way the core's callers will: the wait taken out of the result
/// before it is awaited, so the result is not held across the await.
pub async fn through_wait<B: HandoffBackend>(
    backend: &B,
    ask: B::Ask,
    budget: RequestBudget,
) -> Result<Option<B::Reply>, B::Refusal> {
    let wait = dispatch(backend, ask, budget, ready(()))?;
    Ok(wait.await)
}

/// Awaits a request's run the way `run_on_pool` does today.
pub async fn through_timeout<B: HandoffBackend>(
    backend: &B,
    ask: B::Ask,
    budget: RequestBudget,
) -> Result<Option<B::Reply>, B::Refusal> {
    let run = backend.submit(ask)?;
    match tokio::time::timeout(budget.get(), run).await {
        Ok(reply) => Ok(Some(reply)),
        Err(_elapsed) => Ok(None),
    }
}

/// Polls `future` until it is ready; every future here is ready by its second poll.
pub fn poll_to_ready<F: Future>(future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
}
