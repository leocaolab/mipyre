//! Says which backends the multi-thread server (`MIPYRE_TPC=0`) hands its requests to, on a Tokio
//! thread: a worker route goes to the sub-interpreter pool, and a `runtime="main"` route or the
//! fallback to the main interpreter's threads, which every handler of the GIL-mode server is. Every
//! server's main-interpreter routes are run from here.

use std::convert::Infallible;
use std::future::{ready, Future};
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::Response;
use tokio_util::task::AbortOnDropHandle;

use crate::site::Site;
use mipyre_core::{dispatch, HandoffBackend};
use mipyre_http::io::body::{body_channel, stream_body_feeder, ResponseBody};
use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::limits::RequestBudget;
use mipyre_http::plan::route::{BodyMode, HandlerTarget, MainHandler, RouteId};
use mipyre_runtime::exchange;
use mipyre_runtime::exchange::RequestBody;
use mipyre_runtime::exchange::{refuse, UnansweredError};
use mipyre_runtime::{InterpreterPool, MainThreads};

use super::pipeline::{collect_body, fail, finish, AcceptEncoding, HandlerRequest, Requester, ResponseSource};
use super::request::{Backends, WorkerBackend};
use super::respond::http_response;

/// Holds what one IO thread of the multi-thread server hands its requests, so a request takes one
/// reference, counted on that thread's cache line, to all of it. Each backend is `None` when no
/// route runs on it.
pub struct MultiThreadServer {
    /// Is this IO thread's own copy ([`Site::for_io_thread`]).
    pub site: Arc<Site>,
    pub pool: Option<Arc<InterpreterPool>>,
    pub main: Option<Arc<MainThreads>>,
}

impl Backends for MultiThreadServer {
    type Inline = Infallible;

    #[inline]
    fn site(&self) -> &Site {
        &self.site
    }

    #[inline]
    fn sync_worker(&self) -> Option<WorkerBackend<'_, Infallible>> {
        self.pool.as_deref().map(WorkerBackend::Pool)
    }

    #[inline]
    fn async_pool(&self) -> Option<&InterpreterPool> {
        self.pool.as_deref()
    }

    async fn run_inline(worker: &Infallible, _: &Site, _: HandlerRequest, _: RouteId) -> Response<ResponseBody> {
        match *worker {}
    }

    #[inline]
    fn main(&self) -> Option<&MainThreads> {
        self.main.as_deref()
    }

    #[inline]
    fn spawn_feeder<F: Future<Output = ()> + Send + 'static>(feeder: F) -> AbortOnDropHandle<()> {
        AbortOnDropHandle::new(tokio::spawn(feeder))
    }
}

/// Runs a main-interpreter route on `main` and finishes its response.
pub(super) async fn run_on_main<B: Backends>(
    site: &Site,
    main: &MainThreads,
    prepared: HandlerRequest,
    target: HandlerTarget,
    handler: MainHandler,
) -> Response<ResponseBody> {
    let HandlerRequest {
        head,
        body: incoming,
        timer,
        origin,
        ..
    } = prepared;
    // The `Request` takes the head; the log lines and the response keep their own copies.
    let label = head.label();
    let client = head.client;
    let resp = dispatch_to_main::<B>(main, head, incoming, target, handler, site, &label).await;
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::Main,
    )
}

/// Hands a main-interpreter route to `main` and builds its response; a streamed body is fed by a
/// task `B` spawns while the handler reads it.
async fn dispatch_to_main<B: Backends>(
    main: &MainThreads,
    head: RequestHead,
    incoming: Incoming,
    target: HandlerTarget,
    handler: MainHandler,
    site: &Site,
    label: &RequestLabel,
) -> Response<ResponseBody> {
    let limits = &site.rules.settings.limits;
    let (body, feeder) = match handler.body() {
        BodyMode::Streamed => {
            let (tx, rx) = body_channel();
            let feeder = B::spawn_feeder(stream_body_feeder(
                incoming,
                tx,
                limits.max_body.as_usize(),
                limits.body_time(),
                limits.request_budget,
            ));
            (RequestBody::streamed(rx), Some(feeder))
        }
        BodyMode::Buffered => match collect_body(incoming, limits).await {
            Ok(frames) => (RequestBody::buffered(frames), None),
            Err(e) => return fail(e, label, &site.counters),
        },
    };

    let accept_encoding = AcceptEncoding::of(&head, site.rules.settings.compression.as_ref());
    let ask = (target, handler, exchange::Request::new(head, body));
    let budget = limits.request_budget;
    let failure = 'failed: {
        let reply = match feeder {
            Some(feeder) => match await_streamed(main, ask, budget, feeder).await {
                Ok(reply) => reply,
                Err(refusal) => break 'failed refuse(refusal),
            },
            None => {
                let wait = match dispatch(main, ask, budget, ready(())) {
                    Ok(wait) => wait,
                    Err(refusal) => break 'failed refuse(refusal),
                };
                wait.await
            }
        };
        match reply {
            Some(Ok(result)) => return http_response(result, &accept_encoding, &site.counters).await,
            Some(Err(e)) => e,
            None => refuse(UnansweredError::TimedOut(budget.get())),
        }
    };
    fail(failure, label, &site.counters)
}

/// Hands `ask` to `backend` and waits for its reply, the budget starting once `feeder` has fed the
/// body. The feeder is stopped unless the run answers, since it does not see its reader leave while
/// it waits on the client's next frame; an answer that streams may still be reading the body.
async fn await_streamed<H: HandoffBackend>(
    backend: &H,
    ask: H::Ask,
    budget: RequestBudget,
    mut feeder: AbortOnDropHandle<()>,
) -> Result<Option<H::Reply>, H::Refusal> {
    let wait = dispatch(backend, ask, budget, &mut feeder)?;
    let reply = wait.await;
    if reply.is_some() {
        drop(feeder.detach());
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use std::future::Ready;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use hyper::body::{Body, Frame};
    use mipyre_http::io::body::BodyReceiver;
    use mipyre_http::plan::limits::{BodyGrace, BodyTimeAllowance};

    use super::*;

    /// A client that has sent the head and never sends its body; it counts the feeder's polls.
    struct SilentClient(Arc<AtomicUsize>);

    impl Body for SilentClient {
        type Data = Bytes;
        type Error = hyper::Error;

        fn poll_frame(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, hyper::Error>>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Poll::Pending
        }
    }

    #[derive(Debug)]
    struct Full;

    struct FullBackend;

    /// A backend whose run has answered by its first poll, keeping the body's reader as a handler
    /// whose response streams does.
    struct ReadyBackend(std::sync::Mutex<Option<BodyReceiver>>);

    impl HandoffBackend for ReadyBackend {
        type Ask = BodyReceiver;
        type Reply = ();
        type Refusal = Full;
        type Run = Ready<()>;

        fn submit(&self, rx: BodyReceiver) -> Result<Ready<()>, Full> {
            *self.0.lock().unwrap() = Some(rx);
            Ok(ready(()))
        }
    }

    impl HandoffBackend for FullBackend {
        type Ask = BodyReceiver;
        type Reply = ();
        type Refusal = Full;
        type Run = Ready<()>;

        fn submit(&self, _: BodyReceiver) -> Result<Ready<()>, Full> {
            Err(Full)
        }
    }

    const HOUR: f64 = 3600.0;

    /// Spawns the feeder of a silent client's body and waits until it waits on the client.
    async fn feeding_a_silent_client() -> (AbortOnDropHandle<()>, BodyReceiver, Arc<AtomicUsize>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = body_channel();
        let feeder = AbortOnDropHandle::new(tokio::spawn(stream_body_feeder(
            SilentClient(Arc::clone(&polls)),
            tx,
            usize::MAX,
            BodyTimeAllowance::absolute(BodyGrace::from_secs(HOUR).unwrap()),
            RequestBudget::from_secs(HOUR).unwrap(),
        )));
        while polls.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        (feeder, rx, polls)
    }

    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn a_refused_ask_stops_its_feeder_waiting_on_a_silent_client() {
        let (feeder, rx, polls) = feeding_a_silent_client().await;
        let abort = feeder.abort_handle();

        let refused = await_streamed(&FullBackend, rx, RequestBudget::from_secs(HOUR).unwrap(), feeder).await;
        settle().await;

        assert!(matches!(refused, Err(Full)));
        assert!(abort.is_finished(), "the feeder ends with the refusal");
        assert_eq!(Arc::strong_count(&polls), 1, "the feeder dropped the client's body");
        assert_eq!(
            polls.load(Ordering::Relaxed),
            1,
            "the client's body is not polled again"
        );
    }

    #[tokio::test]
    async fn an_answered_ask_keeps_its_feeder_for_a_response_that_still_reads_the_body() {
        let (feeder, rx, polls) = feeding_a_silent_client().await;
        let abort = feeder.abort_handle();
        let backend = ReadyBackend(std::sync::Mutex::new(None));

        let answered = await_streamed(&backend, rx, RequestBudget::from_secs(HOUR).unwrap(), feeder).await;
        settle().await;

        assert!(matches!(answered, Ok(Some(()))));
        assert!(!abort.is_finished(), "the feeder still feeds the body");
        assert_eq!(Arc::strong_count(&polls), 2, "the feeder still holds the client's body");
    }
}
