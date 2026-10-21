//! Runs the parts of a request every serving path shares, so the paths differ only in where the
//! handler runs: preprocessing, body collection under one budget, and one `finish` that applies
//! CORS and writes the access-log line.

use mipyre_http::plan::limits::{Limits, RequestBudget};
use std::time::Instant;

use bytes::Bytes;

use hyper::body::{Body as _, Incoming};
use hyper::header::HeaderValue;
use hyper::{Method, Request, Response};
use futures_util::FutureExt as _;
use tokio::sync::oneshot;

use crate::exchange::{
    refuse, HandlerError, UnansweredError, ReplyHolder, RequestHead, RequestLabel,
};
use crate::serve::site::{in_sample, next_roll, samples, Site};
use crate::telemetry::RunCounters;
use mipyre_http::io::static_files::try_static_file;
use mipyre_http::plan::request::{decide, RequestDecision, StaticMissAnswer};
use mipyre_http::plan::route::{GrpcMethod, HandlerCall, Params};
use mipyre_http::plan::cors::CorsOrigin;
use mipyre_http::plan::site::AccessLog;
use mipyre_http::io::body::{full_body, ResponseBody};
use mipyre_http::io::request_id::RequestId;

/// It is written to the access log as the `mode` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResponseSource {
    /// No Python ran, as for the fast path, a static file, a 404 or a rejected body.
    Engine,
    /// A main-interpreter handler ran, dispatched from a Tokio thread.
    Main,
    /// A sub-interpreter pool worker ran it.
    Pool,
    /// The TPC thread's own sub-interpreter ran it.
    Inline,
    /// The main interpreter ran it through the TPC bridge.
    Bridge,
    /// It is a WebSocket upgrade's 101 or its rejection.
    WebSocket,
    /// A process-lane worker ran it.
    Process,
}

impl ResponseSource {
    fn label(self) -> &'static str {
        match self {
            ResponseSource::Engine => "rust",
            ResponseSource::Main => "gil",
            ResponseSource::Pool => "subinterp",
            ResponseSource::Inline => "tpc-inline",
            ResponseSource::Bridge => "tpc-gil-bridge",
            ResponseSource::WebSocket => "websocket",
            ResponseSource::Process => "process",
        }
    }
}

/// It decides once, when the request starts, whether its access-log line may be written, so the
/// clock is read only for a request that may get one. It is empty when the log is off, or when the
/// request is outside the sample and no status floor could still bring it in. With a status floor
/// the app's rule is kept, since only the response can be held to it and a response the floor logs
/// takes no place in the sample; once the sample has kept the request, every response is logged.
#[derive(Clone, Copy)]
pub(crate) struct AccessTimer(Option<(AccessLog, Instant)>);

impl AccessTimer {
    pub(crate) fn start(site: &Site) -> Self {
        AccessTimer(site.rules.access_log.and_then(|log| {
            let rule = match log.always_status {
                Some(_) => log,
                None if in_sample(log.sample_n, next_roll) => AccessLog::EVERY_RESPONSE,
                None => return None,
            };
            Some((rule, Instant::now()))
        }))
    }
}

/// The origin the response to a request with `headers` names by the app's CORS rule; `None`
/// without CORS. It is decided while the request's headers are at hand, before they move into
/// the handler's `Request`.
#[inline]
pub(crate) fn cors_origin(site: &Site, headers: &hyper::HeaderMap) -> Option<CorsOrigin> {
    site.rules.settings.cors.as_ref()?.origin_of(headers)
}

/// Counts the answer in the run's counters, applies CORS for `origin` (from [`cors_origin`]),
/// drops the body of a `HEAD` response, and writes the access-log line. Every response of every
/// path goes through here exactly once, so this is where a request is counted.
#[inline]
pub(crate) fn finish(
    mut resp: Response<ResponseBody>,
    site: &Site,
    method: &Method,
    path: &str,
    timer: AccessTimer,
    origin: Option<CorsOrigin>,
    served: ResponseSource,
) -> Response<ResponseBody> {
    site.counters.count_request();
    if let Some(cors) = &site.rules.settings.cors {
        cors.apply(resp.headers_mut(), origin);
    }
    if *method == Method::HEAD {
        resp = without_body(resp);
    }
    let status = resp.status();
    if let AccessTimer(Some((log, start))) = timer {
        if samples(&log, status, next_roll) {
            crate::telemetry::log_access(
                method.as_str(),
                path,
                status.as_u16(),
                start.elapsed().as_micros() as u64,
                served.label(),
            );
        }
    }
    resp
}

/// Strips a `HEAD` response's body but keeps the headers a `GET` would get, its length included;
/// HTTP/1.1 would drop the body itself, but HTTP/2 would send it.
fn without_body(resp: Response<ResponseBody>) -> Response<ResponseBody> {
    let (mut parts, body) = resp.into_parts();
    mipyre_http::plan::response::head_headers(
        &mut parts.headers,
        parts.status,
        body.size_hint().exact(),
    );
    let empty = ResponseBody::empty();
    Response::from_parts(parts, empty)
}

#[expect(
    clippy::large_enum_variant,
    reason = "moved once per request, never stored: boxing `HandlerRequest` would add an allocation per request"
)]
pub(crate) enum Disposition {
    /// It was answered without a handler and is already finished.
    Answered(Response<ResponseBody>),
    ForHandler(HandlerRequest),
}

/// Its head is moved out of hyper's request into the handler's `Request`, not copied.
pub(crate) struct HandlerRequest {
    pub(crate) head: RequestHead,
    pub(crate) body: Incoming,
    pub(crate) call: HandlerCall,
    pub(crate) timer: AccessTimer,
    /// The origin its response names, from [`cors_origin`].
    pub(crate) origin: Option<CorsOrigin>,
}

/// It keeps the request's `Accept-Encoding` for the response after the headers move into the
/// handler's `Request`. Keeping it costs a refcount copy and no allocation, and with compression
/// off the headers are not looked at.
pub(crate) struct AcceptEncoding<'s>(Option<(HeaderValue, &'s CompressionSettings)>);

impl<'s> AcceptEncoding<'s> {
    #[inline]
    pub(crate) fn of(head: &RequestHead, compression: Option<&'s CompressionSettings>) -> Self {
        Self::from_headers(&head.parts.headers, compression)
    }

    #[inline]
    pub(crate) fn from_headers(
        headers: &hyper::HeaderMap,
        compression: Option<&'s CompressionSettings>,
    ) -> Self {
        Self(
            mipyre_http::plan::compression::accept_encoding(headers, compression)
                .map(|(value, settings)| (value.clone(), settings)),
        )
    }

    /// Returns the codings the client accepts and the settings to compress with. It returns
    /// `None` when compression is off or the client sent no `Accept-Encoding`, or one that is not
    /// visible ASCII, since then nothing is known to be accepted.
    #[inline]
    pub(crate) fn negotiable(&self) -> Option<(&str, &'s CompressionSettings)> {
        let (value, settings) = self.0.as_ref()?;
        Some((value.to_str().ok()?, *settings))
    }
}

use mipyre_http::plan::compression::CompressionSettings;

/// Answers the request as `mipyre_http::plan::request` decides, or resolves it to a handler.
pub(crate) async fn preprocess(
    req: Request<Incoming>,
    site: &Site,
    client: mipyre_transport::driver::Peer,
) -> Result<Disposition, hyper::Error> {
    let timer = AccessTimer::start(site);
    // The decision, and any answer to a request with no route, use the request as hyper handed
    // it; it is taken apart only for a handler.
    let (call, params) = match decide(
        req.method(),
        req.uri().path(),
        req.headers(),
        &site.rules.routes,
    ) {
        RequestDecision::Grpc(GrpcMethod::BenchmarkGetSum) => {
            return grpc_get_sum(req, site, timer)
                .await
                .map(Disposition::Answered)
        }
        RequestDecision::Fast(fast) => {
            let resp = full_body(fast.to_response().map(http_body_util::Full::new));
            return Ok(Disposition::Answered(finish(
                resp,
                site,
                req.method(),
                req.uri().path(),
                timer,
                cors_origin(site, req.headers()),
                ResponseSource::Engine,
            )));
        }
        RequestDecision::Route { call, params } => (call, params),
        // No route: a static file (GET/HEAD) answers it, else a CORS preflight is answered,
        // else the fallback handler's call runs, else 405 when the path has routes for other
        // methods, else 404. A preflight to a path with its own OPTIONS route is that route's.
        RequestDecision::NoRoute(no_route) => {
            if let Some(resp) = try_static_file(no_route.static_lookups()).await {
                return Ok(Disposition::Answered(finish(
                    full_body(resp),
                    site,
                    req.method(),
                    req.uri().path(),
                    timer,
                    cors_origin(site, req.headers()),
                    ResponseSource::Engine,
                )));
            }
            let cors = site.rules.settings.cors.as_ref();
            if let Some(resp) = cors.and_then(|c| c.preflight(req.method(), req.headers())) {
                return Ok(Disposition::Answered(finish(
                    full_body(resp.map(http_body_util::Full::new)),
                    site,
                    req.method(),
                    req.uri().path(),
                    timer,
                    cors_origin(site, req.headers()),
                    ResponseSource::Engine,
                )));
            }
            match no_route.after_static_miss() {
                StaticMissAnswer::Fallback(call) => (call, Params::new()),
                StaticMissAnswer::Miss(miss) => {
                    let resp = full_body(
                        mipyre_http::plan::response::reject_response(miss)
                            .map(http_body_util::Full::new),
                    );
                    return Ok(Disposition::Answered(finish(
                        resp,
                        site,
                        req.method(),
                        req.uri().path(),
                        timer,
                        cors_origin(site, req.headers()),
                        ResponseSource::Engine,
                    )));
                }
            }
        }
    };

    let origin = cors_origin(site, req.headers());
    let (parts, body) = req.into_parts();
    let request_id = RequestId::of(
        &parts.headers,
        site.rules.settings.request_id_header.as_ref(),
    );
    Ok(Disposition::ForHandler(HandlerRequest {
        head: RequestHead {
            parts,
            params,
            client,
            request_id,
        },
        body,
        call,
        timer,
        origin,
    }))
}

/// gRPC needs HTTP/2 trailers (`grpc-status`) the normal response path can't model, so the
/// benchmark method is answered outside the route table.
async fn grpc_get_sum(
    req: Request<Incoming>,
    site: &Site,
    timer: AccessTimer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let origin = cors_origin(site, req.headers());
    let resp = crate::serve::grpc::handle_get_sum(req, &site.rules.settings.limits).await?;
    Ok(finish(
        resp,
        site,
        &Method::POST,
        GrpcMethod::BenchmarkGetSum.path(),
        timer,
        origin,
        ResponseSource::Engine,
    ))
}

/// Reads the whole body, at most the app's `max_body` bytes, within [`Limits::body_time`].
pub(crate) async fn collect_body(body: Incoming, limits: &Limits) -> Result<Bytes, HandlerError> {
    Ok(mipyre_http::io::body::read_body(body, limits.max_body.as_usize(), || limits.body_time()).await?)
}

/// Logs an error that happened at the edge (no reply, no body, no capacity) and renders its
/// response.
pub(crate) fn fail(error: HandlerError, request: &RequestLabel, counters: &RunCounters) -> Response<ResponseBody> {
    full_body(error.log(request).into_response(counters))
}

/// Answers 500 for a request the route table sent to workers this server was built without,
/// logged and finished like every response.
pub(crate) fn without_workers(
    site: &Site,
    prepared: HandlerRequest,
    group: crate::exchange::WorkerGroup,
    served: ResponseSource,
) -> Response<ResponseBody> {
    let label = prepared.head.label();
    let resp = fail(refuse(UnansweredError::NoWorkers(group)), &label, &site.counters);
    finish(
        resp,
        site,
        &label.method,
        label.uri.path(),
        prepared.timer,
        prepared.origin,
        served,
    )
}

/// Waits at most `budget` for a handler's reply; `reply` already maps a reply that never came (a
/// closed channel, a panicked task) to an error.
pub(crate) async fn await_reply<T>(
    budget: RequestBudget,
    reply: impl std::future::Future<Output = Result<T, HandlerError>>,
) -> Result<T, HandlerError> {
    match tokio::time::timeout(budget.get(), reply).await {
        Ok(answer) => answer,
        Err(_) => Err(refuse(UnansweredError::TimedOut(budget.get()))),
    }
}

/// Works like [`await_reply`] for a request whose body is streamed to its handler: the budget
/// starts only once `body_read` ends, as a buffered request's starts once its body is in. Until
/// then the feeder's own deadline bounds the wait, so a client that stalls is answered 408 by the
/// body's budget, not 504 by the handler's.
pub(crate) async fn await_streamed_reply<T>(
    budget: RequestBudget,
    body_read: &mut (impl std::future::Future + Unpin),
    reply: impl std::future::Future<Output = Result<T, HandlerError>>,
) -> Result<T, HandlerError> {
    tokio::pin!(reply);
    tokio::select! {
        replied = &mut reply => return replied,
        _ = body_read => {}
    }
    await_reply(budget, reply).await
}

/// Waits for the reply `holder` was handed the sender of; the sender dropped unsent means the
/// holder lost the request.
pub(crate) fn reply_or_lost<T>(
    reply: oneshot::Receiver<T>,
    holder: ReplyHolder,
) -> impl std::future::Future<Output = Result<T, HandlerError>> {
    reply.map(move |reply| reply.map_err(|_| refuse(UnansweredError::WorkerLost(holder))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::CONTENT_LENGTH;

    fn head_with(accept_encoding: Option<&[u8]>) -> RequestHead {
        let (mut parts, ()) = Request::new(()).into_parts();
        if let Some(value) = accept_encoding {
            parts.headers.insert(
                hyper::header::ACCEPT_ENCODING,
                HeaderValue::from_bytes(value).unwrap(),
            );
        }
        RequestHead {
            parts,
            params: Params::new(),
            client: mipyre_transport::driver::Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: RequestId::mint(),
        }
    }

    fn compression_on() -> mipyre_http::plan::compression::CompressionSettings {
        mipyre_http::plan::compression::CompressionSettings::parse(0, true, true, 6, 4).unwrap()
    }

    fn accepted<'a>(accept: &'a AcceptEncoding<'_>) -> Option<&'a str> {
        accept.negotiable().map(|(accepted, _)| accepted)
    }

    #[test]
    fn accept_encoding_is_absent_not_empty() {
        let on = compression_on();
        assert_eq!(accepted(&AcceptEncoding::of(&head_with(None), Some(&on))), None);
        assert_eq!(
            accepted(&AcceptEncoding::of(&head_with(Some(b"gzip, br")), Some(&on))),
            Some("gzip, br")
        );
        // A value that is not visible ASCII says nothing about what is accepted.
        assert_eq!(
            accepted(&AcceptEncoding::of(&head_with(Some(b"gzip\xff")), Some(&on))),
            None
        );
    }

    #[test]
    fn with_compression_off_the_accept_encoding_is_not_kept() {
        assert_eq!(
            accepted(&AcceptEncoding::of(&head_with(Some(b"gzip, br")), None)),
            None
        );
    }

    #[test]
    fn a_head_response_keeps_the_length_and_drops_the_body() {
        let resp = full_body(Response::new(http_body_util::Full::new(
            Bytes::from_static(b"hello"),
        )));
        let resp = without_body(resp);
        assert_eq!(resp.headers()[CONTENT_LENGTH], "5");
        assert_eq!(resp.body().size_hint().exact(), Some(0));
    }
}
