//! Runs the parts of a request every serving path shares, so the paths differ only in where the
//! handler runs: preprocessing, body collection under one budget, and one `finish` that applies
//! CORS and writes the access-log line.

use mipyre_http::io::request_head::{RequestHead, RequestLabel};
use mipyre_http::plan::limits::{Limits, RequestBudget};

use futures_util::FutureExt as _;
use hyper::body::{Body as _, Incoming};
use hyper::header::HeaderValue;
use hyper::{Method, Request, Response};
use tokio::sync::oneshot;

use crate::error_response::render;
use crate::site::{in_sample, next_roll, samples, Site};
use crate::SiteCounters;
use mipyre_http::io::body::{full_body, BodyFrames, ResponseBody};
use mipyre_http::io::events::{self as http_events, HttpEvent};
use mipyre_http::io::static_files::try_static_file;
use mipyre_http::plan::cors::CorsOrigin;
use mipyre_http::plan::cross_origin::site_miss;
use mipyre_http::plan::forwarded::{ClientAddress, Proxies};
use mipyre_http::plan::reject::SiteMiss;
use mipyre_http::plan::request::{decide, RequestDecision, StaticMissAnswer};
use mipyre_http::plan::request_id::RequestId;
use mipyre_http::plan::response::site_miss_response;
use mipyre_http::plan::route::{GrpcMethod, HandlerCall, Params};
use mipyre_http::plan::site::{AccessLog, ServeSettings};
use mipyre_runtime::exchange::{refuse, HandlerError, ReplyHolder, UnansweredError};
use mipyre_transport::driver::Peer;

/// It is written to the access log as the `source` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResponseSource {
    /// No Python ran, as for the fast path, a static file, a 404 or a rejected body.
    Engine,
    /// A main-interpreter handler ran.
    Main,
    /// A sub-interpreter pool worker ran it.
    Pool,
    /// The TPC thread's own sub-interpreter ran it.
    Inline,
    /// It is a WebSocket upgrade's 101 or its rejection.
    WebSocket,
    /// A process-lane worker ran it.
    Process,
}

impl ResponseSource {
    fn label(self) -> &'static str {
        match self {
            ResponseSource::Engine => "rust",
            ResponseSource::Main => "main",
            ResponseSource::Pool => "subinterp",
            ResponseSource::Inline => "tpc-inline",
            ResponseSource::WebSocket => "websocket",
            ResponseSource::Process => "process",
        }
    }
}

/// It decides once, when the request starts, whether its answer may be timed, so the clock is read
/// only for a request whose answer the run counts or whose access-log line may be written. It is
/// empty when neither may happen: the run does not count its answers, and the log is off or the
/// request is outside its sample with no status floor that could still bring it in.
///
/// It holds when the request started, as a raw reading of the site's clock, and the access-log
/// rule its response is held to, which is
/// `None` when no line may be written. With a status floor the app's rule is kept, since only the
/// response can be held to it and a response the floor logs takes no place in the sample; once the
/// sample has kept the request, every response is logged.
#[derive(Clone, Copy)]
pub(crate) struct AccessTimer(Option<(u64, Option<AccessLog>)>);

impl AccessTimer {
    pub(crate) fn start(site: &Site) -> Self {
        let log = site.rules.access_log.and_then(|log| match log.always_status {
            Some(_) => Some(log),
            None if in_sample(log.sample_n, next_roll) => Some(AccessLog::EVERY_RESPONSE),
            None => None,
        });
        if log.is_none() && !site.counters.counts_answers() {
            return AccessTimer(None);
        }
        AccessTimer(Some((site.clock.raw(), log)))
    }
}

/// Returns the whole microseconds between two raw readings of `clock`: 0 when `end` is not after
/// `started`, which a counter read on another core can make it.
#[inline]
fn elapsed_us(clock: &quanta::Clock, started: u64, end: u64) -> u64 {
    clock.delta_as_nanos(started, end) / 1_000
}

/// The origin the response to a request with `headers` names by the app's CORS rule; `None`
/// without CORS. It is decided while the request's headers are at hand, before they move into
/// the handler's `Request`.
#[inline]
pub(crate) fn cors_origin(site: &Site, headers: &hyper::HeaderMap) -> Option<CorsOrigin> {
    site.rules.settings.cors.as_ref()?.origin_of(headers)
}

/// Who asked: the request's id, or the headers to take it from, and the client's address. The
/// response's request-id header and its access line name the request by these.
#[derive(Clone, Copy)]
pub(crate) enum Requester<'a> {
    /// The id a request handed to a handler was given when it was taken apart.
    Given(&'a RequestId, &'a Peer),
    /// The headers of a request the engine answered by itself, which has no id until one is
    /// written.
    Headers(&'a hyper::HeaderMap, &'a Peer),
}

/// Counts the answer in the run's counters, writes the access-log line, writes the request's id
/// in the app's request-id header, applies CORS for `origin` (from [`cors_origin`]) and drops the
/// body of a `HEAD` response. Every response of every path goes through here exactly once, so this
/// is where an answer is counted and labelled.
///
/// `requester` is who asked: the id a handler's request was given, or the headers of a request the
/// engine answered by itself, read only when the app names a request-id header or the response is
/// logged; and the client's address, for the access line.
#[inline]
#[expect(
    clippy::too_many_arguments,
    reason = "each is a fact a different step of the request produced, and the paths hold them in \
              different places (hyper's request, the handler's label, the prepared request)"
)]
pub(crate) fn finish(
    mut resp: Response<ResponseBody>,
    site: &Site,
    method: &Method,
    path: &str,
    requester: Requester<'_>,
    timer: AccessTimer,
    origin: Option<CorsOrigin>,
    served: ResponseSource,
) -> Response<ResponseBody> {
    let status = resp.status();
    let settings = &site.rules.settings;
    let mut id_written = false;
    if let AccessTimer(Some((at, log))) = timer {
        let logged = log.is_some_and(|log| samples(&log, status, next_roll));
        let counted = site.counters.counts_answers();
        if logged || counted {
            // The time runs to the response's head: a streamed body is still being sent.
            let elapsed_us = elapsed_us(&site.clock, at, site.clock.raw());
            site.counters.count_answer(method, status, elapsed_us);
            if logged {
                let line = AccessLine {
                    method,
                    path,
                    status,
                    elapsed_us,
                    served,
                };
                id_written = log_access(&mut resp, settings, line, requester);
            }
        }
    }
    // One test for the headers an answer may get: an app with neither pays one branch.
    if settings.request_id_header.is_some() | settings.cors.is_some() {
        if let (Some(header), false) = (&settings.request_id_header, id_written) {
            // It replaces a value the handler set, so the response carries one id, the request's.
            let value = match requester {
                Requester::Given(id, _) => id.header_value(),
                Requester::Headers(headers, _) => RequestId::of(headers, Some(header)).header_value(),
            };
            resp.headers_mut().insert(header.clone(), value);
        }
        if let Some(cors) = &settings.cors {
            cors.apply(resp.headers_mut(), origin);
        }
    }
    if *method == Method::HEAD {
        resp = without_body(resp);
    }
    resp
}

/// What a response's access line says besides its request's id.
struct AccessLine<'a> {
    method: &'a Method,
    path: &'a str,
    status: hyper::StatusCode,
    elapsed_us: u64,
    served: ResponseSource,
}

/// Writes the access line of a response the sample keeps, named by its request's id, and writes
/// that id in the app's request-id header too; returns whether it did. A request the engine
/// answered by itself has no id until here, and the line and the header must name the same one.
/// Out of line: a response not logged pays only the call's branch, and nothing of the id.
#[inline(never)]
fn log_access(
    resp: &mut Response<ResponseBody>,
    settings: &ServeSettings,
    line: AccessLine<'_>,
    requester: Requester<'_>,
) -> bool {
    let header = settings.request_id_header.as_ref();
    let made;
    let (id, client) = match requester {
        Requester::Given(id, client) => (id, *client),
        Requester::Headers(headers, client) => {
            made = RequestId::of(headers, header);
            (&made, *client)
        }
    };
    id.with_text(|text| {
        crate::log_access(
            line.method.as_str(),
            line.path,
            line.status.as_u16(),
            line.elapsed_us,
            line.served.label(),
            text,
            client,
        );
    });
    let Some(header) = header else {
        return false;
    };
    resp.headers_mut().insert(header.clone(), id.header_value());
    true
}

/// Strips a `HEAD` response's body but keeps the headers a `GET` would get, its length included;
/// HTTP/1.1 would drop the body itself, but HTTP/2 would send it.
#[inline]
fn without_body(resp: Response<ResponseBody>) -> Response<ResponseBody> {
    let (mut parts, body) = resp.into_parts();
    mipyre_http::plan::response::head_headers(&mut parts.headers, parts.status, body.size_hint().exact());
    let empty = ResponseBody::empty();
    Response::from_parts(parts, empty)
}

#[expect(
    clippy::large_enum_variant,
    reason = "moved once per request, never stored: boxing `HandlerRequest` would add an allocation per request"
)]
pub enum Disposition {
    /// It was answered without a handler and is already finished.
    Answered(Response<ResponseBody>),
    ForHandler(HandlerRequest),
}

/// Its head is moved out of hyper's request into the handler's `Request`, not copied.
pub struct HandlerRequest {
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
    pub(crate) fn from_headers(headers: &hyper::HeaderMap, compression: Option<&'s CompressionSettings>) -> Self {
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
pub(crate) async fn preprocess(req: Request<Incoming>, site: &Site, peer: Peer) -> Result<Disposition, hyper::Error> {
    let timer = AccessTimer::start(site);
    let client = client_of(peer, req.headers(), site);
    if let Some(miss) = refused_site(&req, false, site) {
        let resp = full_body(site_miss_response(miss).map(http_body_util::Full::new));
        return Ok(Disposition::Answered(finish(
            resp,
            site,
            req.method(),
            req.uri().path(),
            Requester::Headers(req.headers(), &client),
            timer,
            cors_origin(site, req.headers()),
            ResponseSource::Engine,
        )));
    }
    // The decision, and any answer to a request with no route, use the request as hyper handed
    // it; it is taken apart only for a handler.
    let (call, params) = match decide(req.method(), req.uri().path(), req.headers(), &site.rules.routes) {
        RequestDecision::Grpc(GrpcMethod::BenchmarkGetSum) => {
            return grpc_get_sum(req, site, timer, client).await.map(Disposition::Answered)
        }
        RequestDecision::Fast(fast) => {
            let resp = full_body(fast.to_response().map(http_body_util::Full::new));
            return Ok(Disposition::Answered(finish(
                resp,
                site,
                req.method(),
                req.uri().path(),
                Requester::Headers(req.headers(), &client),
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
                    resp,
                    site,
                    req.method(),
                    req.uri().path(),
                    Requester::Headers(req.headers(), &client),
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
                    Requester::Headers(req.headers(), &client),
                    timer,
                    cors_origin(site, req.headers()),
                    ResponseSource::Engine,
                )));
            }
            match no_route.after_static_miss() {
                StaticMissAnswer::Fallback(call) => (call, Params::new()),
                StaticMissAnswer::Miss(miss) => {
                    let resp =
                        full_body(mipyre_http::plan::response::reject_response(miss).map(http_body_util::Full::new));
                    return Ok(Disposition::Answered(finish(
                        resp,
                        site,
                        req.method(),
                        req.uri().path(),
                        Requester::Headers(req.headers(), &client),
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
    let request_id = RequestId::of(&parts.headers, site.rules.settings.request_id_header.as_ref());
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

/// Returns why the engine turns `req` away before routing it by the site's rules (its host, and
/// for a request that acts the page it comes from), or `None` to serve it.
#[inline]
pub(crate) fn refused_site<'r, B>(req: &'r Request<B>, upgrade: bool, site: &Site) -> Option<SiteMiss<'r>> {
    let settings = &site.rules.settings;
    site_miss(
        req.method(),
        upgrade,
        req.uri(),
        req.headers(),
        &settings.names,
        settings.cors.as_ref(),
    )
}

/// Returns the client of a request that came from `peer`: the address a trusted proxy names in
/// `X-Forwarded-For`, else the peer. With no proxy trusted, the default, only the one test is on
/// the request path; the rest is kept out of line so the caller's code is not grown by it.
#[inline]
pub(crate) fn client_of(peer: Peer, headers: &hyper::HeaderMap, site: &Site) -> Peer {
    let proxies = &site.rules.settings.proxies;
    if proxies.is_empty() {
        return peer;
    }
    forwarded_client(peer, headers, proxies)
}

/// Returns the client a trusted proxy names, else the peer. A trusted proxy's header that cannot
/// be read is counted and logged once a minute with its value, and the proxy is the client.
#[inline(never)]
fn forwarded_client(peer: Peer, headers: &hyper::HeaderMap, proxies: &Proxies) -> Peer {
    let Peer::Ip(ip) = peer else {
        return peer;
    };
    match proxies.client(ip, headers) {
        ClientAddress::Peer => peer,
        ClientAddress::Forwarded(client) => Peer::Ip(client),
        ClientAddress::Unreadable(line) => {
            if let Some(held) = http_events::degrade(HttpEvent::ForwardedForUnreadable) {
                tracing::warn!(
                    target: "mipyre::server",
                    proxy = %ip,
                    x_forwarded_for = ?line,
                    held,
                    "a trusted proxy's X-Forwarded-For has an entry that is no IP address where the \
                     client was looked for; the header is ignored and the proxy is taken for the client"
                );
            }
            peer
        }
    }
}

/// gRPC needs HTTP/2 trailers (`grpc-status`) the normal response path can't model, so the
/// benchmark method is answered outside the route table.
async fn grpc_get_sum(
    req: Request<Incoming>,
    site: &Site,
    timer: AccessTimer,
    client: Peer,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let origin = cors_origin(site, req.headers());
    // The request moves into the call, so its id is taken first.
    let request_id = RequestId::of(req.headers(), site.rules.settings.request_id_header.as_ref());
    let resp = crate::grpc::handle_get_sum(req, &site.rules.settings.limits).await?;
    Ok(finish(
        resp,
        site,
        &Method::POST,
        GrpcMethod::BenchmarkGetSum.path(),
        Requester::Given(&request_id, &client),
        timer,
        origin,
        ResponseSource::Engine,
    ))
}

/// Reads the whole body, at most the app's `max_body` bytes, within [`Limits::body_time`].
pub(crate) async fn collect_body(body: Incoming, limits: &Limits) -> Result<BodyFrames, HandlerError> {
    Ok(mipyre_http::io::body::read_body(body, limits).await?)
}

/// Logs an error that happened at the edge (no reply, no body, no capacity) and renders its
/// response.
#[inline]
pub(crate) fn fail(error: HandlerError, request: &RequestLabel, counters: &SiteCounters) -> Response<ResponseBody> {
    full_body(render(error.log(request), counters))
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
            parts
                .headers
                .insert(hyper::header::ACCEPT_ENCODING, HeaderValue::from_bytes(value).unwrap());
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
    fn elapsed_is_whole_microseconds_between_two_readings() {
        let (clock, mock) = quanta::Clock::mock();
        let started = clock.raw();
        mock.increment(100_999);
        assert_eq!(elapsed_us(&clock, started, clock.raw()), 100);
        mock.increment(1);
        assert_eq!(elapsed_us(&clock, started, clock.raw()), 101);
    }

    #[test]
    fn a_reading_not_after_the_start_is_no_time() {
        let (clock, mock) = quanta::Clock::mock();
        mock.increment(5_000);
        let started = clock.raw();
        assert_eq!(elapsed_us(&clock, started, started), 0);
        assert_eq!(elapsed_us(&clock, started, started - 1_000), 0);
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
        assert_eq!(accepted(&AcceptEncoding::of(&head_with(Some(b"gzip, br")), None)), None);
    }

    #[test]
    fn a_head_response_keeps_the_length_and_drops_the_body() {
        let resp = full_body(Response::new(http_body_util::Full::new(bytes::Bytes::from_static(
            b"hello",
        ))));
        let resp = without_body(resp);
        assert_eq!(resp.headers()[CONTENT_LENGTH], "5");
        assert_eq!(resp.body().size_hint().exact(), Some(0));
    }
}
