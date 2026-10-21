//! Dispatches worker routes to the shared sub-interpreter pool when TPC is off;
//! `runtime="main"` routes and the fallback run on main.

use mipyre_http::plan::limits::ByteLimit;
use hyper::body::Incoming;
use hyper::{Request, Response};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::exchange;
use crate::exchange::{
    refuse, Capacity, HandlerError, ReplyHolder, RequestLabel, UnansweredError, WorkerGroup,
};
use crate::runtime::PoolJob;
use crate::serve::site::Site;
use mipyre_http::plan::route::{HandlerCall, HandlerKind, RouteId};
use crate::exchange::RequestBody;
use mipyre_http::io::body::{collect, BodyGate, ResponseBody};

use super::on_main::run_on_main;
use super::pipeline::{
    await_reply, fail, finish, preprocess, reply_or_lost, AcceptEncoding, Disposition,
    HandlerRequest, ResponseSource,
};
use super::respond::http_response;

/// Bodies up to this size skip the admission gate. HTTP/2 multiplexes hundreds of streams
/// per connection, so tens of thousands of small requests can be in flight at once: a
/// permit budget sized for "the queue" would reject most of them, and one sized for the
/// worst-case RAM of a body flood would not protect against it. Only large bodies need a
/// permit.
const ADMISSION_SKIP_BYTES: ByteLimit = ByteLimit(64 * 1024);

/// Returns whether a request takes its admission permit before any byte of the body is read,
/// which it does when its `Content-Length` declares more than `ADMISSION_SKIP_BYTES`. A missing
/// or unusable `Content-Length` declares nothing, and the collector's gate bounds that body as it
/// arrives.
#[inline]
fn takes_upfront_permit(headers: &hyper::HeaderMap) -> bool {
    let declared = headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    ADMISSION_SKIP_BYTES.is_exceeded_by(declared)
}

/// A body past `skip_bytes` needs a permit from the pool's admission gate, held until the
/// response is ready so admitted bodies bound the memory in flight. `Content-Length` is the
/// client's word (chunked and HTTP/2 bodies carry none, and it can be under-declared), so the
/// collector re-checks against the bytes actually buffered.
struct Admission<'a> {
    semaphore: &'a Arc<Semaphore>,
    skip_bytes: ByteLimit,
    /// It is taken upfront for an honestly declared large `Content-Length`.
    permit: Option<OwnedSemaphorePermit>,
}

impl BodyGate for Admission<'_> {
    type Error = HandlerError;
    fn admit(&mut self, buffered: usize) -> Result<(), HandlerError> {
        if self.permit.is_none() && self.skip_bytes.is_exceeded_by(buffered as u64) {
            let permit = self
                .semaphore
                .clone()
                .try_acquire_owned()
                .map_err(admission_refused)?;
            self.permit = Some(permit);
        }
        Ok(())
    }
}

/// Maps a refused permit to overload when none is free, or to the pool shutting down when the
/// gate is closed.
fn admission_refused(e: TryAcquireError) -> HandlerError {
    refuse(match e {
        TryAcquireError::NoPermits => UnansweredError::Overloaded(Capacity::AdmissionPermits),
        TryAcquireError::Closed => UnansweredError::WorkersGone(WorkerGroup::Subinterp),
    })
}

pub(crate) async fn handle_request_subinterp(
    req: Request<Incoming>,
    pool: Arc<crate::runtime::InterpreterPool>,
    site: Arc<Site>,
    client: mipyre_transport::driver::Peer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let prepared = match preprocess(req, &site, client).await? {
        Disposition::Answered(r) => return Ok(r),
        Disposition::ForHandler(p) => p,
    };
    let resp = match prepared.call {
        HandlerCall::Main(target, body) => {
            run_on_main(&site, prepared, target, body, ResponseSource::Main).await
        }
        HandlerCall::Worker(route, kind) => {
            serve_on_pool(&pool, &site, prepared, route, kind).await
        }
        HandlerCall::Lane(route) => super::on_lane::serve_on_lane(&site, prepared, route).await,
    };
    Ok(resp)
}

/// Runs a worker route on `pool` and finishes its response. A TPC thread serves its
/// `async def` routes this way too, on the async pool.
pub(crate) async fn serve_on_pool(
    pool: &Arc<crate::runtime::InterpreterPool>,
    site: &Site,
    mut prepared: HandlerRequest,
    route: RouteId,
    kind: HandlerKind,
) -> Response<ResponseBody> {
    let label = prepared.head.label();
    let timer = prepared.timer;
    let origin = prepared.origin.take();
    let resp = run_on_pool(pool, prepared, route, kind, site, &label).await;
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        timer,
        origin,
        ResponseSource::Pool,
    )
}

async fn run_on_pool(
    pool: &Arc<crate::runtime::InterpreterPool>,
    prepared: HandlerRequest,
    route: RouteId,
    kind: HandlerKind,
    site: &Site,
    label: &RequestLabel,
) -> Response<ResponseBody> {
    let settings = &site.rules.settings;
    let HandlerRequest { head, body, .. } = prepared;
    // An honestly declared large body takes its permit before a byte is read, so it can be
    // refused upfront; one that under-declares is caught by the collector. hyper answers 400 to a
    // malformed Content-Length, and when Transfer-Encoding is also present the collector bounds
    // the body.
    let upfront = if takes_upfront_permit(&head.parts.headers) {
        match pool.submit_semaphore.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(e) => return fail(admission_refused(e), label, &site.counters),
        }
    } else {
        None
    };
    let mut admission = Admission {
        semaphore: &pool.submit_semaphore,
        skip_bytes: ADMISSION_SKIP_BYTES,
        permit: upfront,
    };
    let accept_encoding = AcceptEncoding::of(&head, settings.compression.as_ref());
    let limits = &settings.limits;
    let collected = collect(body, limits.max_body.as_usize(), &mut admission, || {
        limits.body_time()
    });
    let body = match collected.await {
        Ok(body) => body,
        Err(e) => return fail(e, label, &site.counters),
    };
    let _permit = admission.permit;

    let (reply, response_rx) = tokio::sync::oneshot::channel();
    let submitted = pool.submit(PoolJob {
        route,
        kind,
        request: exchange::Request::new(head, RequestBody::Buffered(body)),
        reply,
    });
    if let Err(rejection) = submitted {
        return fail(refuse(rejection), label, &site.counters);
    }
    PoolJob::inc_created();

    let reply = reply_or_lost(response_rx, ReplyHolder::PoolWorker);
    match await_reply(settings.limits.request_budget, reply).await {
        Ok(result) => http_response(result, &accept_encoding, &site.counters).await,
        Err(e) => fail(e, label, &site.counters),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::{HeaderValue, CONTENT_LENGTH};

    const SKIP: u64 = 64 * 1024;

    fn declaring(content_length: &'static str) -> hyper::HeaderMap {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static(content_length));
        headers
    }

    #[test]
    fn a_declared_body_past_64_kib_takes_its_permit_upfront() {
        assert_eq!(ADMISSION_SKIP_BYTES.0, SKIP);
        assert!(!takes_upfront_permit(&declaring("65536")));
        assert!(takes_upfront_permit(&declaring("65537")));
    }

    /// No `Content-Length`, or one that is not a number, declares nothing: the collector's
    /// gate bounds that body.
    #[test]
    fn a_body_that_declares_nothing_takes_no_upfront_permit() {
        assert!(!takes_upfront_permit(&hyper::HeaderMap::new()));
        assert!(!takes_upfront_permit(&declaring("lots")));
    }

    /// Checks that no free permit is overload (503) and that a closed gate is the pool shutting
    /// down, not overload.
    #[test]
    fn admission_refused_says_overloaded_or_shutting_down() {
        assert!(matches!(
            admission_refused(TryAcquireError::NoPermits),
            HandlerError::Refused(UnansweredError::Overloaded(Capacity::AdmissionPermits))
        ));
        assert!(matches!(
            admission_refused(TryAcquireError::Closed),
            HandlerError::Refused(UnansweredError::WorkersGone(WorkerGroup::Subinterp))
        ));
    }

    /// The collector's gate: up to 64 KiB buffered needs no permit (so with none free it still
    /// passes); one byte more needs one, and with none free it is refused as overloaded.
    #[test]
    fn the_collector_asks_for_a_permit_only_past_64_kib() {
        let none_free = Arc::new(Semaphore::new(0));
        let mut gate = Admission {
            semaphore: &none_free,
            skip_bytes: ADMISSION_SKIP_BYTES,
            permit: None,
        };
        assert!(gate.admit(SKIP as usize).is_ok());
        assert!(gate.permit.is_none());
        assert!(matches!(
            gate.admit(SKIP as usize + 1),
            Err(HandlerError::Refused(UnansweredError::Overloaded(
                Capacity::AdmissionPermits
            )))
        ));

        let one_free = Arc::new(Semaphore::new(1));
        let mut gate = Admission {
            semaphore: &one_free,
            skip_bytes: ADMISSION_SKIP_BYTES,
            permit: None,
        };
        assert!(gate.admit(SKIP as usize + 1).is_ok());
        assert!(gate.permit.is_some());
    }
}
