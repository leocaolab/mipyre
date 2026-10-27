//! Dispatches worker routes to the shared sub-interpreter pool when TPC is off, and a TPC thread's
//! `async def` routes to the async pool.

use hyper::Response;
use mipyre_core::dispatch;
use mipyre_http::io::request_head::RequestLabel;
use mipyre_http::plan::limits::ByteLimit;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::site::Site;
use mipyre_http::io::body::{collect, BodyGate, ResponseBody};
use mipyre_http::plan::route::{HandlerKind, RouteId};
use mipyre_runtime::exchange;
use mipyre_runtime::exchange::RequestBody;
use mipyre_runtime::exchange::{refuse, Capacity, HandlerError, UnansweredError};
use mipyre_runtime::InterpreterPool;

use super::pipeline::{fail, finish, AcceptEncoding, HandlerRequest, Requester, ResponseSource};
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

/// The run's permits for large bodies buffered for pool routes, one per body until its response
/// is ready, shared by every serving thread's copy of the site. There are none until the pool's
/// start adds one per queue slot.
#[derive(Clone)]
pub struct LargeBodyPermits(Arc<Semaphore>);

impl LargeBodyPermits {
    pub fn none() -> Self {
        LargeBodyPermits(Arc::new(Semaphore::new(0)))
    }

    pub fn add(&self, n: usize) {
        self.0.add_permits(n);
    }

    /// Takes a free permit without waiting, or refuses the request as overload.
    #[inline]
    fn take(&self) -> Result<OwnedSemaphorePermit, HandlerError> {
        Arc::clone(&self.0).try_acquire_owned().map_err(admission_refused)
    }
}

/// A body past `skip_bytes` needs one of the site's large-body permits, held until the
/// response is ready so admitted bodies bound the memory in flight. `Content-Length` is the
/// client's word (chunked and HTTP/2 bodies carry none, and it can be under-declared), so the
/// collector re-checks against the bytes actually buffered.
struct Admission<'a> {
    permits: &'a LargeBodyPermits,
    skip_bytes: ByteLimit,
    /// It is taken upfront for an honestly declared large `Content-Length`.
    permit: Option<OwnedSemaphorePermit>,
}

impl BodyGate for Admission<'_> {
    type Error = HandlerError;
    fn admit(&mut self, buffered: usize) -> Result<(), HandlerError> {
        if self.permit.is_none() && self.skip_bytes.is_exceeded_by(buffered as u64) {
            self.permit = Some(self.permits.take()?);
        }
        Ok(())
    }
}

/// Refuses a large body as overload. Nothing closes the site's permits, so a refusal always means
/// none is free.
fn admission_refused(_: TryAcquireError) -> HandlerError {
    refuse(UnansweredError::Overloaded(Capacity::AdmissionPermits))
}

/// Runs a worker route on `pool` and finishes its response. A TPC thread serves its
/// `async def` routes this way too, on the async pool.
pub(super) async fn serve_on_pool(
    pool: &InterpreterPool,
    site: &Site,
    mut prepared: HandlerRequest,
    route: RouteId,
    kind: HandlerKind,
) -> Response<ResponseBody> {
    let label = prepared.head.label();
    let client = prepared.head.client;
    let timer = prepared.timer;
    let origin = prepared.origin.take();
    let resp = run_on_pool(pool, prepared, route, kind, site, &label).await;
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        Requester::Given(&label.id, &client),
        timer,
        origin,
        ResponseSource::Pool,
    )
}

async fn run_on_pool(
    pool: &InterpreterPool,
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
        match site.large_body_permits.take() {
            Ok(permit) => Some(permit),
            Err(e) => return fail(e, label, &site.counters),
        }
    } else {
        None
    };
    let mut admission = Admission {
        permits: &site.large_body_permits,
        skip_bytes: ADMISSION_SKIP_BYTES,
        permit: upfront,
    };
    let accept_encoding = AcceptEncoding::of(&head, settings.compression.as_ref());
    let collected = collect(body, &mut admission, &settings.limits);
    let body = match collected.await {
        Ok(body) => body,
        Err(e) => return fail(e, label, &site.counters),
    };
    let _permit = admission.permit;

    let ask = (route, kind, exchange::Request::new(head, RequestBody::buffered(body)));
    let budget = settings.limits.request_budget;
    let failure = 'failed: {
        let wait = match dispatch(pool, ask, budget, std::future::ready(())) {
            Ok(wait) => wait,
            Err(refusal) => break 'failed refuse(refusal),
        };
        match wait.await {
            Some(Ok(result)) => return http_response(result, &accept_encoding, &site.counters).await,
            Some(Err(failure)) => failure,
            None => refuse(UnansweredError::TimedOut(budget.get())),
        }
    };
    fail(failure, label, &site.counters)
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

    #[test]
    fn admission_refused_says_overloaded() {
        assert!(matches!(
            admission_refused(TryAcquireError::NoPermits),
            HandlerError::Refused(UnansweredError::Overloaded(Capacity::AdmissionPermits))
        ));
    }

    /// The collector's gate: up to 64 KiB buffered needs no permit (so with none free it still
    /// passes); one byte more needs one, and with none free it is refused as overloaded.
    #[test]
    fn the_collector_asks_for_a_permit_only_past_64_kib() {
        let none_free = LargeBodyPermits::none();
        let mut gate = Admission {
            permits: &none_free,
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

        let one_free = LargeBodyPermits::none();
        one_free.add(1);
        let mut gate = Admission {
            permits: &one_free,
            skip_bytes: ADMISSION_SKIP_BYTES,
            permit: None,
        };
        assert!(gate.admit(SKIP as usize + 1).is_ok());
        assert!(gate.permit.is_some());
    }
}
