//! Holds the registered routes, hooks, static mounts and pre-built responses, and resolves a
//! request to the handler that runs it and where.
//!
//! The table is generic over the handler `H` and the static mount `D` without bounds, because
//! nothing here calls a handler or reads a file.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;

use bytes::Bytes;
use http::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, SERVER};
use http::{Method, Response, StatusCode};
use matchit::Router;

use crate::plan::header::{own_header_name, own_header_value};
use crate::plan::response::MAX_HANDLER_LINES;

/// A response built in full at registration, served for an exact `(method, path)` without
/// calling Python.
///
/// Serving one increments the `Bytes` refcounts of its body and header values, so every thread
/// serves its own copy of the table, made by [`FastResponse::own_copy`], to keep those counters
/// on one thread. It is not `Clone` on purpose, because a clone would share the counters.
#[derive(Debug)]
pub struct FastResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum FastResponseError {
    #[error("invalid status code {0}: an HTTP status is 100..=999")]
    InvalidStatus(u16),
    #[error("invalid header name {name:?}: {source}")]
    InvalidHeaderName {
        name: String,
        source: http::header::InvalidHeaderName,
    },
    #[error("invalid value for header {name:?}: {source}")]
    InvalidHeaderValue {
        name: String,
        source: http::header::InvalidHeaderValue,
    },
    #[error("{given} headers, past the {MAX_HANDLER_LINES} lines a response carries")]
    TooManyHeaderLines { given: usize },
}

impl FastResponse {
    /// # Errors
    ///
    /// Returns an error when the status or a header is invalid, so serving it cannot fail.
    pub fn parse(
        body: Bytes,
        content_type: &str,
        status: u16,
        headers: &HashMap<String, String>,
    ) -> Result<Self, FastResponseError> {
        let status = StatusCode::from_u16(status).map_err(|_| FastResponseError::InvalidStatus(status))?;
        if headers.len() > MAX_HANDLER_LINES {
            return Err(FastResponseError::TooManyHeaderLines { given: headers.len() });
        }
        let mut map = HeaderMap::with_capacity(headers.len() + 2);
        map.insert(CONTENT_TYPE, header_value("content-type", content_type)?);
        map.insert(SERVER, HeaderValue::from_static(crate::plan::response::SERVER_HEADER));
        for (name, value) in headers {
            let header =
                HeaderName::from_bytes(name.as_bytes()).map_err(|source| FastResponseError::InvalidHeaderName {
                    name: name.clone(),
                    source,
                })?;
            map.append(header, header_value(name, value)?);
        }
        Ok(FastResponse {
            status,
            headers: map,
            body,
        })
    }

    /// Returns the response, cloning the body and so incrementing its refcount.
    #[inline]
    pub fn to_response(&self) -> Response<Bytes> {
        let mut resp = Response::new(self.body.clone());
        *resp.status_mut() = self.status;
        *resp.headers_mut() = self.headers.clone();
        resp
    }

    /// Copies the response into allocations of its own, so the copy shares no refcount with
    /// `self`.
    pub fn own_copy(&self) -> FastResponse {
        let mut headers = HeaderMap::with_capacity(self.headers.len());
        for (name, value) in &self.headers {
            headers.append(own_header_name(name), own_header_value(value));
        }
        FastResponse {
            status: self.status,
            headers,
            body: Bytes::copy_from_slice(&self.body),
        }
    }
}

fn header_value(name: &str, value: &str) -> Result<HeaderValue, FastResponseError> {
    HeaderValue::from_str(value).map_err(|source| FastResponseError::InvalidHeaderValue {
        name: name.to_string(),
        source,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("{method} {path}: not an HTTP method: {source}")]
    NotAnHttpMethod {
        method: String,
        path: String,
        #[source]
        source: http::method::InvalidMethod,
    },
    #[error("{route}: {source}")]
    MisreadPath {
        route: RouteKey,
        #[source]
        source: TemplateError,
    },
    #[error("{route}: {source}")]
    RouterRefused {
        route: RouteKey,
        #[source]
        source: matchit::InsertError,
    },
}

/// A route path the router would read differently from what it says.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    /// matchit reads an Express-style `:name` as literal text, so the route would match only
    /// the path `:name` itself.
    #[error("`:{name}` is not a path parameter; write it as `{{{name}}}`")]
    ColonParam { name: String },
}

/// Returns the parameter names of a route path in order, a catch-all's without its `*`.
/// Malformed braces are left to the router's insert, which rejects them.
///
/// # Errors
///
/// Returns an error when a segment starts with `:name`.
pub fn template_params(path: &str) -> Result<Vec<&str>, TemplateError> {
    if let Some(name) = path.split('/').find_map(colon_param) {
        return Err(TemplateError::ColonParam { name: name.to_string() });
    }
    let mut names = Vec::new();
    let mut rest = path;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        if let Some(escaped) = after.strip_prefix('{') {
            rest = escaped;
            continue;
        }
        let Some(close) = after.find('}') else {
            break;
        };
        let name = &after[..close];
        names.push(name.strip_prefix('*').unwrap_or(name));
        rest = &after[close + 1..];
    }
    Ok(names)
}

fn colon_param(segment: &str) -> Option<&str> {
    let name = segment.strip_prefix(':')?;
    let starts_ident = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    starts_ident.then_some(name)
}

#[derive(Debug, thiserror::Error)]
pub enum WsRouteError {
    #[error("websocket {path}: {source}")]
    MisreadPath {
        path: String,
        #[source]
        source: TemplateError,
    },
    #[error("websocket {path}: {source}")]
    RouterRefused {
        path: String,
        #[source]
        source: matchit::InsertError,
    },
}

/// A WebSocket route's index in its [`RouteTable`]; only the table makes one, so it always names
/// one of its WebSocket handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WsRouteId(usize);

/// Where a WebSocket route's handler runs: on a sub-interpreter worker's async engine, or on
/// main's (`runtime="main"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsPlace {
    Worker,
    Main,
}

/// A WebSocket route as a worker must have it at the same index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WsRoute {
    pub path: String,
    pub place: WsPlace,
}

impl WsRouteId {
    /// Returns the position among the table's WebSocket routes in registration order.
    pub fn index(self) -> usize {
        self.0
    }
}

/// A route's index in its [`RouteTable`]; only the table makes one, so it always names one of its
/// routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteId(usize);

impl RouteId {
    /// Returns the position in registration order, which is the same in every interpreter
    /// because each worker's table is checked against main's [`RouteManifest`].
    pub fn index(self) -> usize {
        self.0
    }
}

/// A handler's index in its [`RouteTable`], covering every route's handler and the fallback's.
/// Only the table makes one, so it always names one of its handlers, also in a
/// [`RouteTable::copy_with`] copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandlerTarget(usize);

impl HandlerTarget {
    /// Returns the handler's position among the table's route and fallback handlers.
    pub fn index(self) -> usize {
        self.0
    }
}

/// Where a route's handler runs; Python's `runtime="process"` is [`HandlerPlace::Lane`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandlerPlace {
    Worker(HandlerKind),
    Main(MainHandler),
    Lane,
}

/// How a handler on the main interpreter runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MainHandler {
    /// A `def` handler, reading its body as the mode says.
    Sync(BodyMode),
    /// An `async def` handler, which always reads a buffered body.
    Async,
}

impl MainHandler {
    /// Returns how the handler reads its body.
    pub fn body(self) -> BodyMode {
        match self {
            MainHandler::Sync(body) => body,
            MainHandler::Async => BodyMode::Buffered,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandlerKind {
    Sync,
    Async,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyMode {
    Buffered,
    /// The body is fed to `req.stream` chunk by chunk while the handler runs.
    Streamed,
}

/// The `runtime=` a registration asks for, before the handler's kind and `stream=` are checked
/// against it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeChoice {
    Worker,
    Main,
    Lane,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteRegistration {
    pub runtime: RuntimeChoice,
    pub stream: bool,
    pub kind: HandlerKind,
}

/// A combination of `runtime=`, a streamed body (`body=BodyStream`) and handler kind that no serving path runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RouteRegistrationError {
    #[error(
        "body=BodyStream needs runtime='main': a streamed request body is only fed to handlers on the main interpreter"
    )]
    StreamOffMain,
    #[error("body=BodyStream is not supported on `async def` handlers; use a `def` handler")]
    StreamAsync,
    #[error(
        "runtime='process' does not take body=BodyStream: a process route gets the whole body at \
         once; use runtime='main' for a streamed body"
    )]
    LaneStream,
    #[error("runtime='process' does not run `async def` handlers; use a `def` handler")]
    LaneAsync,
}

impl TryFrom<RouteRegistration> for HandlerPlace {
    type Error = RouteRegistrationError;

    fn try_from(registration: RouteRegistration) -> Result<Self, RouteRegistrationError> {
        let RouteRegistration { runtime, stream, kind } = registration;
        match (runtime, stream, kind) {
            (RuntimeChoice::Lane, true, _) => Err(RouteRegistrationError::LaneStream),
            (RuntimeChoice::Lane, false, HandlerKind::Async) => Err(RouteRegistrationError::LaneAsync),
            (RuntimeChoice::Lane, false, HandlerKind::Sync) => Ok(HandlerPlace::Lane),
            (_, true, HandlerKind::Async) => Err(RouteRegistrationError::StreamAsync),
            (RuntimeChoice::Worker, true, HandlerKind::Sync) => Err(RouteRegistrationError::StreamOffMain),
            (RuntimeChoice::Main, true, HandlerKind::Sync) => {
                Ok(HandlerPlace::Main(MainHandler::Sync(BodyMode::Streamed)))
            }
            (RuntimeChoice::Main, false, HandlerKind::Sync) => {
                Ok(HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)))
            }
            (RuntimeChoice::Main, false, HandlerKind::Async) => Ok(HandlerPlace::Main(MainHandler::Async)),
            (RuntimeChoice::Worker, false, kind) => Ok(HandlerPlace::Worker(kind)),
        }
    }
}

/// The handler a request resolved to, and where it runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandlerCall {
    Worker(RouteId, HandlerKind),
    /// A `runtime="main"` route, or the fallback handler, which is a `def` handler reading a
    /// buffered body.
    Main(HandlerTarget, MainHandler),
    Lane(RouteId),
}

pub struct Route {
    pub handler: HandlerTarget,
    pub name: String,
    pub key: RouteKey,
    pub runtime: HandlerPlace,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RouteKey {
    pub method: Method,
    pub path: String,
}

impl std::fmt::Display for RouteKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.method, self.path)
    }
}

/// The path parameters of a matched route: each one's name, and where its value sits in the
/// path, still percent-encoded, so a value is decoded only when a handler reads it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Params {
    found: Vec<Param>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Param {
    name: Box<str>,
    value: Range<usize>,
}

impl Params {
    pub fn new() -> Self {
        Params::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.found.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.found.is_empty()
    }

    /// Adds the parameter `name`, whose value is the bytes at `value` in the path.
    pub fn push(&mut self, name: &str, value: Range<usize>) {
        self.found.push(Param {
            name: name.into(),
            value,
        });
    }

    /// Returns each parameter's name and where its value sits in the path, in the template's order.
    pub fn located(&self) -> impl Iterator<Item = (&str, Range<usize>)> {
        self.found.iter().map(|p| (&*p.name, p.value.clone()))
    }

    /// Returns each parameter's name and its value in `path` percent-decoded, invalid UTF-8 as
    /// U+FFFD so a literal `%` from `%25` cannot be confused with a failed decode; a value is
    /// `None` when its place is not in `path`, which is not the path the route matched.
    pub fn decoded<'a>(&'a self, path: &'a str) -> impl Iterator<Item = (&'a str, Option<Cow<'a, str>>)> {
        self.located().map(move |(name, at)| {
            let value = path
                .get(at)
                .map(|raw| percent_encoding::percent_decode_str(raw).decode_utf8_lossy());
            (name, value)
        })
    }

    /// Records where each matched value sits in `path`. matchit hands every value as a slice of
    /// the path it matched; were one not, its place would lie outside the path, and reading it
    /// says so rather than handing out bytes from elsewhere.
    fn of_match(path: &str, matched: &matchit::Params<'_, '_>) -> Params {
        let mut params = Params::new();
        if matched.is_empty() {
            return params;
        }
        params.found.reserve_exact(matched.len());
        for (name, value) in matched.iter() {
            let start = value.as_ptr().addr().wrapping_sub(path.as_ptr().addr());
            params.push(name, start..start.wrapping_add(value.len()));
        }
        params
    }
}

const GET_SLOT: usize = 0;
const HEAD_SLOT: usize = 5;

static STANDARD_METHODS: [Method; 9] = [
    Method::GET,
    Method::POST,
    Method::PUT,
    Method::DELETE,
    Method::PATCH,
    Method::HEAD,
    Method::OPTIONS,
    Method::CONNECT,
    Method::TRACE,
];

/// Holds values keyed by method: a standard method in a fixed slot, without hashing, and any
/// other in a short list.
#[derive(Clone, Debug)]
struct ByMethod<T> {
    standard: [Option<T>; STANDARD_METHODS.len()],
    other: Vec<(Method, T)>,
}

impl<T> Default for ByMethod<T> {
    fn default() -> Self {
        ByMethod {
            standard: std::array::from_fn(|_| None),
            other: Vec::new(),
        }
    }
}

#[inline]
fn standard_slot(method: &Method) -> Option<usize> {
    STANDARD_METHODS.iter().position(|m| m == method)
}

impl<T> ByMethod<T> {
    /// Finds what `at` finds under `method` spelled exactly as registered, since a method is
    /// case-sensitive (RFC 9110 §9.1); a `HEAD` it finds nothing for takes the `GET` one, whose
    /// body the pipeline drops (RFC 9110 §9.3.2).
    // The engine's request pipeline is one large async fn, where the inliner otherwise leaves
    // this a call per request. The two rare branches stay calls, so only the lookup that hits
    // lands in that function.
    #[inline(always)]
    fn serving<'s, R>(&'s self, method: &Method, at: impl Fn(&'s T) -> Option<R>) -> Option<R> {
        let Some(slot) = standard_slot(method) else {
            return self.serving_other(method, at);
        };
        match self.standard[slot].as_ref().and_then(&at) {
            None if slot == HEAD_SLOT => self.serving_head_as_get(at),
            found => found,
        }
    }

    #[cold]
    fn serving_other<'s, R>(&'s self, method: &Method, at: impl Fn(&'s T) -> Option<R>) -> Option<R> {
        let (_, value) = self.other.iter().find(|(m, _)| m == method)?;
        at(value)
    }

    #[cold]
    fn serving_head_as_get<'s, R>(&'s self, at: impl Fn(&'s T) -> Option<R>) -> Option<R> {
        self.standard[GET_SLOT].as_ref().and_then(at)
    }

    fn get_or_default(&mut self, method: &Method) -> &mut T
    where
        T: Default,
    {
        match standard_slot(method) {
            Some(i) => self.standard[i].get_or_insert_with(T::default),
            None => {
                let at = match self.other.iter().position(|(m, _)| m == method) {
                    Some(at) => at,
                    None => {
                        self.other.push((method.clone(), T::default()));
                        self.other.len() - 1
                    }
                };
                &mut self.other[at].1
            }
        }
    }

    fn map<U>(&self, f: impl Fn(&T) -> U) -> ByMethod<U> {
        ByMethod {
            standard: std::array::from_fn(|i| self.standard[i].as_ref().map(&f)),
            other: self.other.iter().map(|(m, v)| (m.clone(), f(v))).collect(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&Method, &T)> {
        STANDARD_METHODS
            .iter()
            .zip(&self.standard)
            .filter_map(|(m, v)| v.as_ref().map(|v| (m, v)))
            .chain(self.other.iter().map(|(m, v)| (m, v)))
    }
}

/// The fast responses of one method, sorted by path for a binary search without hashing.
#[derive(Debug, Default)]
struct FastPaths(Vec<(Box<str>, FastResponse)>);

impl FastPaths {
    fn own_copy(&self) -> Self {
        FastPaths(self.0.iter().map(|(p, r)| (p.clone(), r.own_copy())).collect())
    }

    #[inline]
    fn get(&self, path: &str) -> Option<&FastResponse> {
        self.0
            .binary_search_by(|(p, _)| (**p).cmp(path))
            .ok()
            .map(|i| &self.0[i].1)
    }

    fn contains(&self, path: &str) -> bool {
        self.get(path).is_some()
    }

    /// Adds `response` for `path`, or returns `false` and adds nothing when `path` has one.
    fn insert(&mut self, path: &str, response: FastResponse) -> bool {
        match self.0.binary_search_by(|(p, _)| (**p).cmp(path)) {
            Ok(_) => false,
            Err(at) => {
                self.0.insert(at, (path.into(), response));
                true
            }
        }
    }
}

pub struct RouteTable<H, D> {
    /// It is only added to, a second fallback replacing the first in its slot, so every
    /// [`HandlerTarget`] handed out stays valid.
    handlers: Vec<H>,
    routes: Vec<Route>,
    routers: ByMethod<Router<RouteId>>,
    fallback: Option<HandlerTarget>,
    /// WebSocket routes have their own router because one may share a path with a `GET` route;
    /// the `Upgrade` header picks between them.
    ws_handlers: Vec<H>,
    /// Is indexed like `ws_handlers`.
    ws_routes: Vec<WsRoute>,
    ws_router: Router<WsRouteId>,
    /// It is private and only added to, so the sealed counts never exceed the hooks.
    before_hooks: Vec<H>,
    after_hooks: Vec<H>,
    /// A request asks the mounts in registration order.
    static_mounts: Vec<D>,
    fast_responses: ByMethod<FastPaths>,
    /// The counts when `Mipyre.run()` began, which mark what the script registered; what main
    /// adds after them exists only on main. It is private, so the counts never exceed the table.
    sealed: Option<RouteCounts>,
    grpc_methods: Vec<GrpcMethod>,
}

/// A gRPC method the engine answers itself, outside the route table, because it needs HTTP/2
/// trailers that a route's response cannot carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrpcMethod {
    BenchmarkGetSum,
}

impl GrpcMethod {
    pub const fn path(self) -> &'static str {
        match self {
            GrpcMethod::BenchmarkGetSum => "/benchmark.BenchmarkService/GetSum",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RouteCounts {
    routes: usize,
    websockets: usize,
    before_hooks: usize,
    after_hooks: usize,
}

/// What a worker's route at the same index of a [`RouteManifest`] must be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedRoute {
    pub key: RouteKey,
    pub on_worker: bool,
}

impl ExpectedRoute {
    pub fn of(route: &Route) -> Self {
        ExpectedRoute {
            key: route.key.clone(),
            on_worker: matches!(route.runtime, HandlerPlace::Worker(_)),
        }
    }
}

/// The routes and hook counts a worker must have, computed by main from its sealed table.
/// It holds plain values, so a worker's init, running with the worker's thread state current,
/// never touches main's handlers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteManifest {
    pub routes: Vec<ExpectedRoute>,
    pub websockets: Vec<WsRoute>,
    pub before_hooks: usize,
    pub after_hooks: usize,
}

impl RouteManifest {
    /// Describes `table` up to its seal, or all of it when unsealed, as a worker's table is.
    pub fn of<H, D>(table: &RouteTable<H, D>) -> Self {
        let sealed = table.sealed.unwrap_or_else(|| table.counts());
        RouteManifest {
            routes: table.routes[..sealed.routes].iter().map(ExpectedRoute::of).collect(),
            websockets: table.ws_routes[..sealed.websockets].to_vec(),
            before_hooks: sealed.before_hooks,
            after_hooks: sealed.after_hooks,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty() && self.websockets.is_empty() && self.before_hooks == 0 && self.after_hooks == 0
    }

    /// Lists both manifests one route per line, marking the first differing index.
    pub fn describe_mismatch(expected: &Self, got: &Self) -> String {
        let first_diff = expected
            .routes
            .iter()
            .zip(&got.routes)
            .position(|(a, b)| a != b)
            .unwrap_or_else(|| expected.routes.len().min(got.routes.len()));
        let list = |s: &Self| {
            let mut out = String::new();
            for (i, route) in s.routes.iter().enumerate() {
                let ExpectedRoute { key, on_worker } = route;
                let mark = if i == first_diff { "  <-- first difference" } else { "" };
                let place = if *on_worker { "" } else { " (not on a sub-interpreter)" };
                out.push_str(&format!("    [{i}] {key}{place}{mark}\n"));
            }
            for (i, WsRoute { path, place }) in s.websockets.iter().enumerate() {
                let place = if *place == WsPlace::Worker {
                    ""
                } else {
                    " (not on a sub-interpreter)"
                };
                out.push_str(&format!("    websocket [{i}] {path}{place}\n"));
            }
            out.push_str(&format!(
                "    before_request hooks: {}, after_request hooks: {}\n",
                s.before_hooks, s.after_hooks
            ));
            out
        };
        format!(
            "the main interpreter registered:\n{}this worker's script registered:\n{}",
            list(expected),
            list(got)
        )
    }
}

impl<H, D> Default for RouteTable<H, D> {
    fn default() -> Self {
        RouteTable {
            handlers: Vec::new(),
            routes: Vec::new(),
            routers: ByMethod::default(),
            fallback: None,
            ws_handlers: Vec::new(),
            ws_routes: Vec::new(),
            ws_router: Router::new(),
            before_hooks: Vec::new(),
            after_hooks: Vec::new(),
            static_mounts: Vec::new(),
            fast_responses: ByMethod::default(),
            sealed: None,
            grpc_methods: Vec::new(),
        }
    }
}

impl<H, D> RouteTable<H, D> {
    /// Marks what is registered so far as the script's, the part workers compare against;
    /// later calls keep the first mark.
    pub fn seal(&mut self) {
        if self.sealed.is_none() {
            self.sealed = Some(self.counts());
        }
    }

    /// Registers a gRPC method the engine answers itself; registering it again changes nothing.
    pub fn add_grpc_method(&mut self, method: GrpcMethod) {
        if !self.grpc_methods.contains(&method) {
            self.grpc_methods.push(method);
        }
    }

    #[inline]
    pub fn grpc_method(&self, path: &str) -> Option<GrpcMethod> {
        self.grpc_methods.iter().copied().find(|m| m.path() == path)
    }

    /// Adds a hook that runs before every route's handler, after the hooks added earlier.
    pub fn add_before_hook(&mut self, hook: H) {
        self.before_hooks.push(hook);
    }

    /// Adds a hook that runs after every route's handler, after the hooks added earlier.
    pub fn add_after_hook(&mut self, hook: H) {
        self.after_hooks.push(hook);
    }

    /// Adds a static-file mount, asked after the mounts added earlier.
    pub fn add_static_mount(&mut self, mount: D) {
        self.static_mounts.push(mount);
    }

    pub fn before_hooks(&self) -> &[H] {
        &self.before_hooks
    }

    pub fn after_hooks(&self) -> &[H] {
        &self.after_hooks
    }

    pub fn static_mounts(&self) -> &[D] {
        &self.static_mounts
    }

    pub fn static_mounts_mut(&mut self) -> &mut [D] {
        &mut self.static_mounts
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed.is_some()
    }

    fn counts(&self) -> RouteCounts {
        RouteCounts {
            routes: self.routes.len(),
            websockets: self.ws_routes.len(),
            before_hooks: self.before_hooks.len(),
            after_hooks: self.after_hooks.len(),
        }
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// Returns every handler this table holds, including the fallback, WebSocket handlers and
    /// hooks.
    pub fn handlers(&self) -> impl Iterator<Item = &H> {
        self.handlers
            .iter()
            .chain(self.ws_handlers.iter())
            .chain(self.before_hooks.iter())
            .chain(self.after_hooks.iter())
    }

    /// Copies the table with every handler copied by `copy` and fast-response bytes of its own.
    pub fn copy_with(&self, copy: impl Fn(&H) -> H) -> Self
    where
        D: Clone,
    {
        let copy_all = |v: &[H]| v.iter().map(&copy).collect();
        RouteTable {
            // The order is kept so every `HandlerTarget` names the same handler in the copy.
            handlers: copy_all(&self.handlers),
            routes: self
                .routes
                .iter()
                .map(|r| Route {
                    handler: r.handler,
                    name: r.name.clone(),
                    key: r.key.clone(),
                    runtime: r.runtime,
                })
                .collect(),
            routers: self.routers.clone(),
            fallback: self.fallback,
            ws_handlers: copy_all(&self.ws_handlers),
            ws_routes: self.ws_routes.clone(),
            ws_router: self.ws_router.clone(),
            before_hooks: copy_all(&self.before_hooks),
            after_hooks: copy_all(&self.after_hooks),
            static_mounts: self.static_mounts.clone(),
            // The bytes are copied rather than cloned, for the reason on `FastResponse`.
            fast_responses: self.fast_responses.map(FastPaths::own_copy),
            sealed: self.sealed,
            grpc_methods: self.grpc_methods.clone(),
        }
    }

    /// Registers a route; a refused one leaves the table untouched.
    ///
    /// # Errors
    ///
    /// Returns an error when `method` is not an HTTP token, which no request could carry, or the
    /// router refuses `path` as a duplicate, a conflict or malformed.
    pub fn insert(
        &mut self,
        method: &str,
        path: &str,
        handler: H,
        name: String,
        runtime: HandlerPlace,
    ) -> Result<(), RouteError> {
        // The fallible steps go first, so a refused route leaves the table untouched.
        let id = RouteId(self.routes.len());
        let key = RouteKey {
            method: parse_method(method, path)?,
            path: path.to_string(),
        };
        template_params(path).map_err(|source| RouteError::MisreadPath {
            route: key.clone(),
            source,
        })?;
        let router = self.routers.get_or_default(&key.method);
        router.insert(path, id).map_err(|source| RouteError::RouterRefused {
            route: key.clone(),
            source,
        })?;
        let target = HandlerTarget(self.handlers.len());
        self.handlers.push(handler);
        self.routes.push(Route {
            handler: target,
            name,
            key,
            runtime,
        });
        Ok(())
    }

    /// Registers `response` for exactly `(method, path)`, returning `false` and changing nothing
    /// when that pair has one already.
    ///
    /// # Errors
    ///
    /// Returns an error when `method` is not an HTTP token.
    pub fn add_fast_response(&mut self, method: &str, path: &str, response: FastResponse) -> Result<bool, RouteError> {
        let method = parse_method(method, path)?;
        Ok(self.fast_responses.get_or_default(&method).insert(path, response))
    }

    /// Registers the WebSocket handler for `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when `path` already has a handler, or the router would read it
    /// differently from what it says.
    pub fn add_websocket(&mut self, path: &str, handler: H, place: WsPlace) -> Result<(), WsRouteError> {
        template_params(path).map_err(|source| WsRouteError::MisreadPath {
            path: path.to_string(),
            source,
        })?;
        let id = WsRouteId(self.ws_handlers.len());
        self.ws_router
            .insert(path, id)
            .map_err(|source| WsRouteError::RouterRefused {
                path: path.to_string(),
                source,
            })?;
        self.ws_handlers.push(handler);
        self.ws_routes.push(WsRoute {
            path: path.to_string(),
            place,
        });
        Ok(())
    }

    /// Returns whether any WebSocket route is registered; an app without one never upgrades a
    /// connection, so its connections stay on one thread unlocked.
    #[inline]
    pub fn has_websocket_routes(&self) -> bool {
        !self.ws_handlers.is_empty()
    }

    #[inline]
    pub fn websocket(&self, path: &str) -> Option<(WsRouteId, Params)> {
        // The match is exhaustive, so a new matchit error variant fails to compile instead of
        // being read as no route.
        let matched = match self.ws_router.at(path) {
            Ok(matched) => matched,
            Err(matchit::MatchError::NotFound) => return None,
        };
        Some((*matched.value, Params::of_match(path, &matched.params)))
    }

    pub fn ws_handler(&self, id: WsRouteId) -> &H {
        &self.ws_handlers[id.0]
    }

    /// Returns the WebSocket handlers, indexed by [`WsRouteId::index`].
    pub fn ws_handlers(&self) -> &[H] {
        &self.ws_handlers
    }

    /// Returns the WebSocket routes, indexed by [`WsRouteId::index`].
    pub fn ws_routes(&self) -> &[WsRoute] {
        &self.ws_routes
    }

    pub fn ws_place(&self, id: WsRouteId) -> WsPlace {
        self.ws_routes[id.0].place
    }

    fn has_websockets_on(&self, place: WsPlace) -> bool {
        self.ws_routes.iter().any(|r| r.place == place)
    }

    /// Sets the handler for a request no route matched; a second one replaces the first.
    pub fn set_fallback(&mut self, handler: H) {
        match self.fallback {
            Some(target) => self.handlers[target.0] = handler,
            None => {
                self.fallback = Some(HandlerTarget(self.handlers.len()));
                self.handlers.push(handler);
            }
        }
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    /// Returns the id of the route at `index` in registration order.
    ///
    /// # Panics
    ///
    /// Panics when `index` is out of range.
    pub fn route_id(&self, index: usize) -> RouteId {
        assert!(index < self.routes.len(), "route index {index} out of range");
        RouteId(index)
    }

    pub fn route(&self, id: RouteId) -> &Route {
        &self.routes[id.0]
    }

    /// Returns whether any route runs on a sub-interpreter worker; only then does a server build
    /// sub-interpreters.
    /// Returns whether any request or WebSocket connection runs on a sub-interpreter worker.
    pub fn uses_workers(&self) -> bool {
        self.has_websockets_on(WsPlace::Worker)
            || self.routes.iter().any(|r| matches!(r.runtime, HandlerPlace::Worker(_)))
    }

    pub fn uses_lane(&self) -> bool {
        self.routes.iter().any(|r| r.runtime == HandlerPlace::Lane)
    }

    /// Returns whether any request can run on the main interpreter, counting the fallback and the
    /// `runtime="main"` WebSocket handlers, which run on main's async engine.
    pub fn uses_main(&self) -> bool {
        self.fallback.is_some()
            || self.has_websockets_on(WsPlace::Main)
            || self.routes.iter().any(|r| matches!(r.runtime, HandlerPlace::Main(_)))
    }

    #[inline]
    pub fn handler(&self, target: HandlerTarget) -> &H {
        &self.handlers[target.0]
    }

    /// Resolves `(method, path)` to its handler and path parameters; a `HEAD` with no `HEAD`
    /// route runs the `GET` route (RFC 9110 §9.3.2), and the pipeline drops the body.
    #[inline]
    pub fn resolve(&self, method: &Method, path: &str) -> Option<(HandlerCall, Params)> {
        let (id, params) = self.routers.serving(method, |router| matched_route(router, path))?;
        let route = self.route(id);
        let call = match route.runtime {
            HandlerPlace::Worker(kind) => HandlerCall::Worker(id, kind),
            HandlerPlace::Main(handler) => HandlerCall::Main(route.handler, handler),
            HandlerPlace::Lane => HandlerCall::Lane(id),
        };
        Some((call, params))
    }

    /// Returns the methods that have a route for `path` as an `Allow` value; only a request
    /// that matched no route asks, so the scan is off the hot path.
    pub(crate) fn allowed_methods(&self, path: &str) -> Option<HeaderValue> {
        let routed = self
            .routers
            .iter()
            .filter(|(_, router)| router.at(path).is_ok())
            .map(|(method, _)| method.as_str());
        let prebuilt = self
            .fast_responses
            .iter()
            .filter(|(_, paths)| paths.contains(path))
            .map(|(method, _)| method.as_str());
        allow_value(routed.chain(prebuilt))
    }

    pub fn fallback_call(&self) -> Option<HandlerCall> {
        self.fallback
            .map(|target| HandlerCall::Main(target, MainHandler::Sync(BodyMode::Buffered)))
    }

    /// Returns the pre-built response for exactly `(method, path)`; a `HEAD` with none gets the
    /// `GET` one, and the pipeline drops the body.
    #[inline]
    pub fn fast_response(&self, method: &Method, path: &str) -> Option<&FastResponse> {
        self.fast_responses.serving(method, |paths| paths.get(path))
    }
}

#[inline]
fn matched_route(router: &Router<RouteId>, path: &str) -> Option<(RouteId, Params)> {
    // The match is exhaustive, so a new matchit error variant fails to compile instead of
    // being read as no route.
    let matched = match router.at(path) {
        Ok(matched) => matched,
        Err(matchit::MatchError::NotFound) => return None,
    };
    Some((*matched.value, Params::of_match(path, &matched.params)))
}

/// Parses `method` from its uppercase spelling, so a lowercase registration names the standard
/// method; a name that is not a token could never match a request, so it is refused.
fn parse_method(method: &str, path: &str) -> Result<Method, RouteError> {
    let upper = method.to_uppercase();
    Method::from_bytes(upper.as_bytes()).map_err(|source| RouteError::NotAnHttpMethod {
        method: upper,
        path: path.to_string(),
        source,
    })
}

/// Returns `methods` as a sorted `Allow` value with `HEAD` wherever `GET` is, because the `GET`
/// route serves it.
#[expect(
    clippy::expect_used,
    reason = "method names are tokens (RFC 9110 §9.1), and tokens joined by \", \" are a valid header value"
)]
fn allow_value<'a>(methods: impl Iterator<Item = &'a str>) -> Option<HeaderValue> {
    let mut methods: Vec<&str> = methods.collect();
    if methods.contains(&"GET") && !methods.contains(&"HEAD") {
        methods.push("HEAD");
    }
    if methods.is_empty() {
        return None;
    }
    methods.sort_unstable();
    methods.dedup();
    Some(HeaderValue::from_str(&methods.join(", ")).expect("HTTP method names are tokens, valid in a header value"))
}

/// How many of a table's routes run in each place; a WebSocket route counts as an `async def`
/// one where it runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlaceCounts {
    pub worker_sync: usize,
    pub worker_async: usize,
    pub main_buffered: usize,
    pub main_streamed: usize,
    pub main_async: usize,
    pub lane: usize,
}

impl PlaceCounts {
    pub fn of<H, D>(table: &RouteTable<H, D>) -> Self {
        let mut counts = PlaceCounts::default();
        for route in table.routes() {
            let count = match route.runtime {
                HandlerPlace::Worker(HandlerKind::Sync) => &mut counts.worker_sync,
                HandlerPlace::Worker(HandlerKind::Async) => &mut counts.worker_async,
                HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)) => &mut counts.main_buffered,
                HandlerPlace::Main(MainHandler::Sync(BodyMode::Streamed)) => &mut counts.main_streamed,
                HandlerPlace::Main(MainHandler::Async) => &mut counts.main_async,
                HandlerPlace::Lane => &mut counts.lane,
            };
            *count += 1;
        }
        // A WebSocket handler is an `async def` that runs on an async engine.
        for route in &table.ws_routes {
            match route.place {
                WsPlace::Worker => counts.worker_async += 1,
                WsPlace::Main => counts.main_async += 1,
            }
        }
        counts
    }

    pub fn main(&self) -> usize {
        self.main_buffered + self.main_streamed + self.main_async
    }

    pub fn total(&self) -> usize {
        self.worker_sync + self.worker_async + self.main() + self.lane
    }

    /// Returns whether no route is on the main interpreter and none is `async def`.
    pub fn all_inline_sync(&self) -> bool {
        self.main() == 0 && self.worker_async == 0
    }
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod route_model;

#[cfg(test)]
mod runtime_tests {
    use super::*;

    fn place(runtime: RuntimeChoice, stream: bool, kind: HandlerKind) -> Result<HandlerPlace, RouteRegistrationError> {
        HandlerPlace::try_from(RouteRegistration { runtime, stream, kind })
    }

    #[test]
    fn a_streamed_body_needs_a_sync_main_interpreter_handler() {
        use HandlerPlace::{Main, Worker};
        assert_eq!(
            place(RuntimeChoice::Main, true, HandlerKind::Sync),
            Ok(Main(MainHandler::Sync(BodyMode::Streamed)))
        );
        assert_eq!(
            place(RuntimeChoice::Worker, true, HandlerKind::Sync),
            Err(RouteRegistrationError::StreamOffMain)
        );
        assert_eq!(
            place(RuntimeChoice::Worker, true, HandlerKind::Async),
            Err(RouteRegistrationError::StreamAsync)
        );
        assert_eq!(
            place(RuntimeChoice::Main, true, HandlerKind::Async),
            Err(RouteRegistrationError::StreamAsync)
        );
        assert_eq!(
            place(RuntimeChoice::Main, false, HandlerKind::Async),
            Ok(Main(MainHandler::Async))
        );
        assert_eq!(
            place(RuntimeChoice::Worker, false, HandlerKind::Sync),
            Ok(Worker(HandlerKind::Sync))
        );
        assert_eq!(
            place(RuntimeChoice::Worker, false, HandlerKind::Async),
            Ok(Worker(HandlerKind::Async))
        );
    }
}

#[cfg(test)]
mod allow_tests {
    use super::*;

    #[test]
    fn allow_lists_sorted_methods_with_head_for_get() {
        let allow = |m: &[&str]| allow_value(m.iter().copied());
        assert_eq!(allow(&["POST", "GET"]).unwrap(), "GET, HEAD, POST");
        assert_eq!(allow(&["GET", "HEAD"]).unwrap(), "GET, HEAD");
        assert_eq!(allow(&["DELETE"]).unwrap(), "DELETE");
        // A route and a pre-built response for the same method list it once.
        assert_eq!(allow(&["GET", "GET"]).unwrap(), "GET, HEAD");
        assert!(allow(&[]).is_none());
    }
}

#[cfg(test)]
mod template_tests {
    use super::*;

    #[test]
    fn params_are_named_in_order_catch_all_without_its_star() {
        assert_eq!(template_params("/a/{x}/b/{y}"), Ok(vec!["x", "y"]));
        assert_eq!(template_params("/img{id}.png"), Ok(vec!["id"]));
        assert_eq!(template_params("/files/{*rest}"), Ok(vec!["rest"]));
        assert_eq!(template_params("/static"), Ok(vec![]));
    }

    #[test]
    fn escaped_braces_are_literal() {
        assert_eq!(template_params("/{{x}}/{y}"), Ok(vec!["y"]));
    }

    #[test]
    fn a_colon_segment_names_the_brace_form() {
        let err = template_params("/items/:item_id").unwrap_err();
        assert_eq!(err, TemplateError::ColonParam { name: "item_id".into() });
        assert_eq!(
            err.to_string(),
            "`:item_id` is not a path parameter; write it as `{item_id}`"
        );
    }

    #[test]
    fn a_colon_inside_a_segment_is_literal() {
        assert_eq!(template_params("/v1/items:batchGet"), Ok(vec![]));
        assert_eq!(template_params("/{id}:archive"), Ok(vec!["id"]));
        assert_eq!(template_params("/:/x"), Ok(vec![]));
        assert_eq!(template_params("/:8080"), Ok(vec![]));
    }
}

#[cfg(test)]
mod template_proptests {
    use super::*;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    struct Segment {
        template: String,
        path: String,
        param: Option<(String, String)>,
    }

    fn literal() -> impl Strategy<Value = Segment> {
        prop_oneof![
            "[a-z0-9._-]{1,8}".prop_map(|s| (s.clone(), s)),
            // Escaped braces match literal braces.
            "[a-z]{1,4}".prop_map(|s| (format!("{s}{{{{x}}}}"), format!("{s}{{x}}"))),
        ]
        .prop_map(|(template, path)| Segment {
            template,
            path,
            param: None,
        })
    }

    fn param(index: usize) -> impl Strategy<Value = Segment> {
        (
            "[a-z]{0,3}",
            "[a-z_][a-z0-9_]{0,6}",
            proptest::option::of("\\.[a-z]{1,3}"),
            "[a-zA-Z0-9]{1,8}",
        )
            .prop_map(move |(prefix, name, suffix, value)| {
                // A name bound twice is the router's error, not ours, so names are unique.
                let name = format!("{name}{index}");
                let suffix = suffix.unwrap_or_default();
                Segment {
                    template: format!("{prefix}{{{name}}}{suffix}"),
                    path: format!("{prefix}{value}{suffix}"),
                    param: Some((name, value)),
                }
            })
    }

    type Route = (Vec<Segment>, Option<(String, String)>);

    fn route() -> impl Strategy<Value = Route> {
        let segments = proptest::collection::vec(any::<bool>(), 0..6).prop_flat_map(|kinds| {
            kinds
                .into_iter()
                .enumerate()
                .map(
                    |(i, is_param)| {
                        if is_param {
                            param(i).boxed()
                        } else {
                            literal().boxed()
                        }
                    },
                )
                .collect::<Vec<_>>()
        });
        let catch_all = proptest::option::of(("[a-z]{1,6}", "[a-z0-9]{1,5}(/[a-z0-9]{1,5}){0,3}"));
        (segments, catch_all)
    }

    proptest! {
        #[test]
        fn template_params_names_what_the_router_binds((segments, catch_all) in route()) {
            let mut template: String = segments.iter().map(|s| format!("/{}", s.template)).collect();
            let mut path: String = segments.iter().map(|s| format!("/{}", s.path)).collect();
            let mut expected: Vec<(String, String)> =
                segments.iter().filter_map(|s| s.param.clone()).collect();
            if let Some((name, rest)) = catch_all {
                let name = format!("{name}_rest");
                template.push_str(&format!("/{{*{name}}}"));
                path.push_str(&format!("/{rest}"));
                expected.push((name, rest));
            }
            if template.is_empty() {
                template.push('/');
                path.push('/');
            }

            let names = template_params(&template).unwrap();
            let expected_names: Vec<&str> = expected.iter().map(|(n, _)| n.as_str()).collect();
            prop_assert_eq!(&names, &expected_names);

            let mut router = Router::new();
            router.insert(template.as_str(), ()).unwrap();
            let matched = router.at(&path).unwrap();
            let bound: Vec<(String, String)> = matched
                .params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            prop_assert_eq!(bound, expected);
        }

        #[test]
        fn a_colon_identifier_segment_is_refused_anywhere(
            before in "(/[a-z]{1,5}){0,3}",
            name in "[a-zA-Z_][a-zA-Z0-9_]{0,6}",
            after in "(/[a-z]{1,5}){0,3}",
        ) {
            let template = format!("{before}/:{name}{after}");
            prop_assert_eq!(
                template_params(&template),
                Err(TemplateError::ColonParam { name })
            );
        }

        #[test]
        fn any_path_yields_names_it_contains_without_panicking(path in "[/{}*:a-z0-9]{0,30}") {
            if let Ok(names) = template_params(&path) {
                for name in names {
                    prop_assert!(
                        path.contains(&format!("{{{name}}}")) || path.contains(&format!("{{*{name}}}")),
                        "{name:?} not in {path:?}"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod fast_response_tests {
    use super::*;

    fn with_headers(n: usize) -> Result<FastResponse, FastResponseError> {
        let headers = (0..n).map(|i| (format!("x-{i}"), String::new())).collect();
        FastResponse::parse(Bytes::from_static(b"ok"), "text/plain", 200, &headers)
    }

    /// One header past what a handler's response carries is refused at registration rather than
    /// panicking there or when the server adds its own lines.
    #[test]
    fn a_fast_response_carries_at_most_max_handler_lines() {
        assert!(with_headers(MAX_HANDLER_LINES).is_ok());
        assert!(matches!(
            with_headers(MAX_HANDLER_LINES + 1),
            Err(FastResponseError::TooManyHeaderLines { given }) if given == MAX_HANDLER_LINES + 1
        ));
    }

    fn health() -> FastResponse {
        let headers = HashMap::from([("X-Probe".to_string(), "yes".to_string())]);
        FastResponse::parse(Bytes::from(b"ok".to_vec()), "text/plain", 200, &headers).expect("a valid response")
    }

    /// A leaked `from_static` body is never unique, so this proves the registration owns its
    /// bytes.
    #[test]
    fn the_bytes_are_freed_with_the_response() {
        let fast = health();
        let served = fast.to_response();
        let body = fast.body.clone();
        assert!(!body.is_unique(), "the response still holds its body");
        drop(served);
        drop(fast);
        assert!(
            body.is_unique(),
            "the response's body was not released when it was dropped"
        );
    }

    #[test]
    fn a_copy_shares_nothing_with_the_original() {
        let original = health();
        let copy = original.own_copy();
        assert_ne!(copy.body.as_ptr(), original.body.as_ptr(), "the body is shared");
        let custom = HeaderName::from_static("x-probe");
        let (a, b) = (&original.headers[&custom], &copy.headers[&custom]);
        assert_ne!(a.as_bytes().as_ptr(), b.as_bytes().as_ptr(), "a header value is shared");
        let (a, b) = (&original.headers[CONTENT_TYPE], &copy.headers[CONTENT_TYPE]);
        assert_ne!(
            a.as_bytes().as_ptr(),
            b.as_bytes().as_ptr(),
            "the content type is shared"
        );

        let held = original.body.clone();
        let _served: Vec<_> = (0..4).map(|_| copy.to_response()).collect();
        drop(original);
        assert!(
            held.is_unique(),
            "serving the copy took a reference to the original's body"
        );
    }

    #[test]
    fn a_copy_serves_the_same_response() {
        let original = health();
        let (a, b) = (original.to_response(), original.own_copy().to_response());
        assert_eq!(a.status(), b.status());
        assert_eq!(a.headers(), b.headers());
    }

    struct DropCounter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl AsRef<[u8]> for DropCounter {
        fn as_ref(&self) -> &[u8] {
            b"constant"
        }
    }

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    const REGISTERED: usize = 200;

    /// Builds a table of counted bodies, each served once from it and once from a thread's copy;
    /// `keep` sees each body as it is registered.
    fn table_of_counted(
        drops: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
        mut keep: impl FnMut(&Bytes),
    ) -> RouteTable<u32, ()> {
        let mut table = RouteTable::<u32, ()>::new();
        for i in 0..REGISTERED {
            let body = Bytes::from_owner(DropCounter(drops.clone()));
            keep(&body);
            let fast = FastResponse::parse(body, "text/plain", 200, &HashMap::new()).expect("a valid response");
            let path = format!("/c{i}");
            assert!(table.add_fast_response("GET", &path, fast).expect("GET is a method"));
            let served = table.fast_response(&Method::GET, &path).expect("registered");
            drop(served.to_response());
        }
        let copy = table.copy_with(|h| *h);
        drop(copy.fast_response(&Method::GET, "/c0").expect("copied").to_response());
        drop(copy);
        table
    }

    /// Serving, a thread's copy and the copy's end free no body; the table's end frees them all.
    #[test]
    fn every_registered_body_is_freed_with_its_table() {
        let drops = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let table = table_of_counted(&drops, |_| {});
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 0);
        drop(table);
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), REGISTERED);
    }

    #[test]
    fn the_count_sees_a_body_kept_past_its_table() {
        let drops = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut kept = Vec::new();
        drop(table_of_counted(&drops, |b| kept.push(b.clone())));
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 0);
        kept.truncate(REGISTERED - 1);
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
mod insert_tests {
    use super::*;

    #[test]
    fn a_method_that_is_not_a_token_is_refused_at_registration() {
        let mut table: RouteTable<u32, ()> = RouteTable::new();
        let refused = table.insert("GE T", "/x", 1, "h".into(), HandlerPlace::Lane);
        assert!(
            matches!(refused, Err(RouteError::NotAnHttpMethod { .. })),
            "got {refused:?}"
        );
        assert!(table.routes().is_empty());

        table
            .insert("get", "/x", 2, "h".into(), HandlerPlace::Lane)
            .expect("a lowercase token is a method");
        assert_eq!(table.routes()[0].key.method, Method::GET);
    }

    #[test]
    fn a_fast_response_method_that_is_not_a_token_is_refused_at_registration() {
        let response = || {
            FastResponse::parse(
                Bytes::from_static(b"ok"),
                "text/plain",
                200,
                &std::collections::HashMap::new(),
            )
            .expect("a plain response parses")
        };
        let mut table: RouteTable<u32, ()> = RouteTable::new();
        let refused = table.add_fast_response("GE T", "/x", response());
        assert!(
            matches!(refused, Err(RouteError::NotAnHttpMethod { .. })),
            "got {refused:?}"
        );
        assert!(table.fast_response(&Method::GET, "/x").is_none());

        assert_eq!(table.add_fast_response("get", "/x", response()).ok(), Some(true));
        assert!(table.fast_response(&Method::GET, "/x").is_some());
    }
}

#[cfg(test)]
mod ws_route_tests {
    use super::route_model::decoded_pairs;
    use super::*;

    fn table() -> RouteTable<u32, ()> {
        RouteTable::new()
    }

    #[test]
    fn a_table_has_websocket_routes_once_one_is_added() {
        let mut t = table();
        assert!(!t.has_websocket_routes());
        t.add_websocket("/ws", 1, WsPlace::Main).unwrap();
        assert!(t.has_websocket_routes());
    }

    #[test]
    fn a_param_is_where_its_value_sits_in_the_path_and_decoded_only_when_read() {
        let mut t = table();
        t.add_websocket("/img{id}.png/{*rest}", 1, WsPlace::Main).unwrap();
        let path = "/img%2542.png/a/b";
        let (_, params) = t.websocket(path).unwrap();
        let located: Vec<(&str, Range<usize>)> = params.located().collect();
        assert_eq!(located, [("id", 4..9), ("rest", 14..17)]);
        assert_eq!(
            decoded_pairs(&params, path),
            [
                ("id".to_string(), "%42".to_string()),
                ("rest".to_string(), "a/b".to_string())
            ]
        );
        let short: Vec<Option<Cow<'_, str>>> = params.decoded("/img").map(|(_, v)| v).collect();
        assert_eq!(short, [None, None], "a path the route did not match holds no value");
    }

    #[test]
    fn a_websocket_path_is_a_template_with_decoded_params() {
        let mut t = table();
        t.add_websocket("/chat/{room}", 7, WsPlace::Main).unwrap();
        let (id, params) = t.websocket("/chat/a%20b").unwrap();
        assert_eq!(*t.ws_handler(id), 7);
        assert_eq!(
            decoded_pairs(&params, "/chat/a%20b"),
            vec![("room".to_string(), "a b".to_string())]
        );
        assert!(t.websocket("/chat").is_none());
    }

    #[test]
    fn a_duplicate_websocket_path_is_refused_and_keeps_the_first() {
        let mut t = table();
        t.add_websocket("/ws", 1, WsPlace::Main).unwrap();
        let err = t.add_websocket("/ws", 2, WsPlace::Main).unwrap_err();
        assert!(err.to_string().contains("/ws"), "{err}");
        let (id, _) = t.websocket("/ws").unwrap();
        assert_eq!(*t.ws_handler(id), 1);
    }

    #[test]
    fn a_colon_websocket_path_is_refused() {
        let mut t = table();
        let err = t.add_websocket("/ws/:id", 1, WsPlace::Main).unwrap_err();
        assert!(err.to_string().contains("{id}"), "{err}");
        assert!(t.websocket("/ws/:id").is_none());
    }

    #[test]
    fn a_websocket_route_and_a_get_route_share_a_path() {
        let mut t = table();
        t.insert(
            "GET",
            "/ws",
            1,
            "page".into(),
            HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)),
        )
        .unwrap();
        t.add_websocket("/ws", 2, WsPlace::Main).unwrap();
        let (id, _) = t.websocket("/ws").unwrap();
        assert_eq!(*t.ws_handler(id), 2);
        assert!(t.resolve(&Method::GET, "/ws").is_some());
    }

    #[test]
    fn a_copy_keeps_the_websocket_routes() {
        let mut t = table();
        t.add_websocket("/ws/{id}", 3, WsPlace::Main).unwrap();
        let copy = t.copy_with(|h| h + 10);
        let (id, params) = copy.websocket("/ws/9").unwrap();
        assert_eq!(*copy.ws_handler(id), 13);
        assert_eq!(
            decoded_pairs(&params, "/ws/9"),
            vec![("id".to_string(), "9".to_string())]
        );
    }
}

#[cfg(test)]
mod fallback_tests {
    use super::*;

    fn main_route(t: &mut RouteTable<u32, ()>, path: &str, handler: u32) {
        t.insert(
            "GET",
            path,
            handler,
            path.into(),
            HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)),
        )
        .unwrap();
    }

    fn main_target(call: HandlerCall) -> HandlerTarget {
        match call {
            HandlerCall::Main(target, MainHandler::Sync(BodyMode::Buffered)) => target,
            other => panic!("expected a buffered call on main: {other:?}"),
        }
    }

    #[test]
    fn a_request_no_route_matched_runs_the_fallback_by_its_id() {
        let mut t = RouteTable::<u32, ()>::new();
        main_route(&mut t, "/a", 1);
        t.set_fallback(9);
        assert!(t.resolve(&Method::GET, "/nowhere").is_none());
        let target = main_target(t.fallback_call().unwrap());
        assert_eq!(*t.handler(target), 9);
    }

    #[test]
    fn no_fallback_no_call() {
        let mut t = RouteTable::<u32, ()>::new();
        main_route(&mut t, "/a", 1);
        assert_eq!(t.fallback_call(), None);
    }

    #[test]
    fn the_fallback_and_the_routes_have_distinct_ids_wherever_it_was_registered() {
        let mut t = RouteTable::<u32, ()>::new();
        // Registered before, then between, the routes: its slot is taken once, and every
        // route still names its own handler.
        t.set_fallback(9);
        main_route(&mut t, "/a", 1);
        main_route(&mut t, "/b", 2);
        let fallback = main_target(t.fallback_call().unwrap());
        let a = main_target(t.resolve(&Method::GET, "/a").unwrap().0);
        let b = main_target(t.resolve(&Method::GET, "/b").unwrap().0);
        assert_eq!((*t.handler(fallback), *t.handler(a), *t.handler(b)), (9, 1, 2));
        assert!(fallback != a && fallback != b && a != b);
        // Workers index by route id, so the ids stay in the routes' registration order.
        assert_eq!(t.route(t.route_id(0)).key.path, "/a");
        assert_eq!(t.route(t.route_id(1)).key.path, "/b");
        assert_eq!(t.route(t.route_id(1)).handler, b);
    }

    #[test]
    fn a_second_fallback_replaces_the_first_in_its_slot() {
        let mut t = RouteTable::<u32, ()>::new();
        t.set_fallback(8);
        let first = main_target(t.fallback_call().unwrap());
        main_route(&mut t, "/a", 1);
        t.set_fallback(9);
        let second = main_target(t.fallback_call().unwrap());
        assert_eq!(first, second);
        assert_eq!(*t.handler(second), 9);
        // The first fallback leaves no orphaned handler behind for the GC to report.
        assert_eq!(t.handlers().copied().collect::<Vec<_>>(), [9, 1]);
    }

    #[test]
    fn a_copy_keeps_every_id() {
        let mut t = RouteTable::<u32, ()>::new();
        t.set_fallback(9);
        main_route(&mut t, "/a", 1);
        let copy = t.copy_with(|h| h + 10);
        let fallback = main_target(t.fallback_call().unwrap());
        let a = main_target(t.resolve(&Method::GET, "/a").unwrap().0);
        assert_eq!((*copy.handler(fallback), *copy.handler(a)), (19, 11));
        assert_eq!(copy.fallback_call(), t.fallback_call());
    }

    #[test]
    fn the_manifest_workers_check_against_has_no_fallback() {
        let mut with = RouteTable::<u32, ()>::new();
        with.set_fallback(9);
        main_route(&mut with, "/a", 1);
        let mut without = RouteTable::<u32, ()>::new();
        main_route(&mut without, "/a", 1);
        assert_eq!(RouteManifest::of(&with), RouteManifest::of(&without));
    }
}

#[cfg(test)]
mod method_tests {
    use super::*;

    fn method(name: &str) -> Method {
        Method::from_bytes(name.as_bytes()).expect("a token is a method")
    }

    #[test]
    fn the_get_and_head_slots_hold_their_methods() {
        assert_eq!(STANDARD_METHODS[GET_SLOT], Method::GET);
        assert_eq!(STANDARD_METHODS[HEAD_SLOT], Method::HEAD);
    }

    fn table() -> RouteTable<u32, ()> {
        let mut t = RouteTable::new();
        for (m, path, h) in [("GET", "/a", 1), ("PURGE", "/a", 2), ("POST", "/p", 3)] {
            t.insert(
                m,
                path,
                h,
                "h".into(),
                HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)),
            )
            .unwrap();
        }
        t
    }

    fn handler_of(t: &RouteTable<u32, ()>, m: &Method, path: &str) -> Option<u32> {
        match t.resolve(m, path)?.0 {
            HandlerCall::Main(target, _) => Some(*t.handler(target)),
            other => panic!("expected a main route, got {other:?}"),
        }
    }

    #[test]
    fn a_request_method_matches_its_route_only_in_the_spelling_registered() {
        let t = table();
        assert_eq!(handler_of(&t, &Method::GET, "/a"), Some(1));
        assert_eq!(handler_of(&t, &method("get"), "/a"), None);
        assert_eq!(handler_of(&t, &method("PURGE"), "/a"), Some(2));
        assert_eq!(handler_of(&t, &method("Purge"), "/a"), None);
        assert_eq!(handler_of(&t, &method("PURGEX"), "/a"), None);
        assert_eq!(handler_of(&t, &Method::POST, "/a"), None);
    }

    #[test]
    fn only_head_falls_back_to_get() {
        let mut t = table();
        assert_eq!(handler_of(&t, &Method::HEAD, "/a"), Some(1));
        assert_eq!(handler_of(&t, &method("head"), "/a"), None);
        assert_eq!(handler_of(&t, &Method::OPTIONS, "/a"), None);
        assert_eq!(handler_of(&t, &Method::HEAD, "/p"), None);
        // A `HEAD` route of its own wins.
        t.insert(
            "HEAD",
            "/a",
            4,
            "h".into(),
            HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)),
        )
        .unwrap();
        assert_eq!(handler_of(&t, &Method::HEAD, "/a"), Some(4));
    }

    /// The route and the fast response registered under one spelling at two paths answer every
    /// request spelling alike, so a request a fast response misses is never told its method is
    /// allowed.
    #[test]
    fn a_fast_response_matches_a_request_method_as_a_route_does() {
        let mut t: RouteTable<u32, ()> = RouteTable::new();
        for registered in ["GET", "PURGE"] {
            t.insert(
                registered,
                "/route",
                1,
                "h".into(),
                HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)),
            )
            .unwrap();
            let fast = FastResponse::parse(
                Bytes::from_static(b"ok"),
                "text/plain",
                200,
                &std::collections::HashMap::new(),
            )
            .expect("a plain response parses");
            assert_eq!(t.add_fast_response(registered, "/fast", fast).ok(), Some(true));
        }
        for requested in ["GET", "get", "HEAD", "head", "PURGE", "purge", "Purge", "POST"] {
            let m = method(requested);
            assert_eq!(
                t.fast_response(&m, "/fast").is_some(),
                t.resolve(&m, "/route").is_some(),
                "{requested}"
            );
        }
        assert!(t.fast_response(&method("PURGE"), "/fast").is_some());
        assert!(t.fast_response(&method("purge"), "/fast").is_none());
    }
}
