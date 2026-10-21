//! Decides what answers a request, in two steps.
//!
//! [`decide`] returns a [`NoRoute`] token when no route matches, and only spending that token
//! after the static files missed ([`NoRoute::after_static_miss`]) reaches the fallback, 405 or
//! 404, so the types enforce that static files come first and a static hit never computes the
//! `Allow` list.

use http::header::{HeaderMap, CONTENT_TYPE};
use http::Method;

use crate::plan::reject::RouteMiss;
use crate::plan::route::{FastResponse, GrpcMethod, HandlerCall, Params, RouteTable};
use crate::plan::static_plan::{self, StaticLookups, StaticMountRule};

/// What answers a request, as far as its line and headers tell.
pub enum RequestDecision<'r, 'q, H, D> {
    Grpc(GrpcMethod),
    Fast(&'r FastResponse),
    Route { call: HandlerCall, params: Params },
    NoRoute(NoRoute<'r, 'q, H, D>),
}

/// A request no route matched. Only [`decide`] makes one, so the second step cannot run on a
/// request the first step did not refuse, nor on another path.
pub struct NoRoute<'r, 'q, H, D> {
    routes: &'r RouteTable<H, D>,
    path: &'q str,
    headers: &'q HeaderMap,
    method: &'q Method,
}

/// What answers a request that no route and no static file answered.
#[derive(Debug, PartialEq, Eq)]
pub enum StaticMissAnswer {
    Fallback(HandlerCall),
    Miss(RouteMiss),
}

/// Tries a registered gRPC method, a pre-built response and a route, in that order.
///
/// A `HEAD` with no `HEAD` registration takes the `GET` one, whose body the engine drops.
#[inline]
pub fn decide<'r, 'q, H, D>(
    method: &'q Method,
    path: &'q str,
    headers: &'q HeaderMap,
    routes: &'r RouteTable<H, D>,
) -> RequestDecision<'r, 'q, H, D> {
    if *method == Method::POST {
        if let Some(grpc) = routes.grpc_method(path) {
            if is_grpc(headers) {
                return RequestDecision::Grpc(grpc);
            }
        }
    }
    if let Some(fast) = routes.fast_response(method, path) {
        return RequestDecision::Fast(fast);
    }
    match routes.resolve(method, path) {
        Some((call, params)) => RequestDecision::Route { call, params },
        None => RequestDecision::NoRoute(NoRoute {
            routes,
            path,
            headers,
            method,
        }),
    }
}

fn is_grpc(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/grpc"))
}

impl<'r, 'q, H, D: AsRef<StaticMountRule>> NoRoute<'r, 'q, H, D> {
    /// Returns the static files the request asks for, mount by mount; only `GET` and `HEAD` ask
    /// for any.
    pub fn static_lookups(&self) -> StaticLookups<'r, 'q, D> {
        if *self.method == Method::GET || *self.method == Method::HEAD {
            static_plan::lookups(self.path, self.headers, self.routes.static_mounts())
        } else {
            StaticLookups::none(self.headers)
        }
    }
}

impl<H, D> NoRoute<'_, '_, H, D> {
    /// Picks the fallback handler, else 405 when the path has routes for other methods, else 404.
    pub fn after_static_miss(self) -> StaticMissAnswer {
        if let Some(call) = self.routes.fallback_call() {
            return StaticMissAnswer::Fallback(call);
        }
        StaticMissAnswer::Miss(match self.routes.allowed_methods(self.path) {
            Some(allow) => RouteMiss::MethodNotAllowed(allow),
            None => RouteMiss::NotFound,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::route::{BodyMode, HandlerPlace};
    use crate::plan::static_plan::Copies;
    use http::header::{HeaderValue, ACCEPT_ENCODING};

    struct Mount(StaticMountRule);

    impl AsRef<StaticMountRule> for Mount {
        fn as_ref(&self) -> &StaticMountRule {
            &self.0
        }
    }

    type Table = RouteTable<u32, Mount>;

    fn table() -> Table {
        let mut t = Table::new();
        t.insert(
            "GET",
            "/users/{id}",
            1,
            "user".into(),
            HandlerPlace::Main(BodyMode::Buffered),
        )
        .unwrap();
        t.insert(
            "POST",
            "/items",
            2,
            "items".into(),
            HandlerPlace::Main(BodyMode::Buffered),
        )
        .unwrap();
        let fast = FastResponse::parse(
            bytes::Bytes::from_static(b"ok"),
            "text/plain",
            200,
            &Default::default(),
        )
        .unwrap();
        t.add_fast_response("GET", "/health", fast).unwrap();
        t.add_static_mount(Mount(
            StaticMountRule::new("/static", Copies::Precompressed).unwrap(),
        ));
        t
    }

    const GRPC_GET_SUM_PATH: &str = GrpcMethod::BenchmarkGetSum.path();

    fn grpc_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/grpc+proto"),
        );
        h
    }

    #[test]
    fn the_grpc_method_is_decided_first_and_only_when_on() {
        let off = table();
        let mut t = table();
        t.add_grpc_method(GrpcMethod::BenchmarkGetSum);
        let h = grpc_headers();
        assert!(matches!(
            decide(&Method::POST, GRPC_GET_SUM_PATH, &h, &t),
            RequestDecision::Grpc(GrpcMethod::BenchmarkGetSum)
        ));
        assert!(matches!(
            decide(&Method::POST, GRPC_GET_SUM_PATH, &h, &off),
            RequestDecision::NoRoute(_)
        ));
        let plain = HeaderMap::new();
        assert!(matches!(
            decide(&Method::POST, GRPC_GET_SUM_PATH, &plain, &t),
            RequestDecision::NoRoute(_)
        ));
        let mut json = HeaderMap::new();
        json.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(matches!(
            decide(&Method::POST, GRPC_GET_SUM_PATH, &json, &t),
            RequestDecision::NoRoute(_)
        ));
        assert!(matches!(
            decide(&Method::GET, GRPC_GET_SUM_PATH, &h, &t),
            RequestDecision::NoRoute(_)
        ));
    }

    #[test]
    fn a_fast_response_comes_before_routes_and_head_takes_get() {
        let t = table();
        let h = HeaderMap::new();
        assert!(matches!(
            decide(&Method::GET, "/health", &h, &t),
            RequestDecision::Fast(_)
        ));
        assert!(matches!(
            decide(&Method::HEAD, "/health", &h, &t),
            RequestDecision::Fast(_)
        ));
    }

    #[test]
    fn a_streamed_route_is_decided_with_its_body_streamed() {
        let mut t = table();
        t.insert(
            "POST",
            "/upload",
            3,
            "upload".into(),
            HandlerPlace::Main(BodyMode::Streamed),
        )
        .unwrap();
        let h = HeaderMap::new();
        match decide(&Method::POST, "/upload", &h, &t) {
            RequestDecision::Route { call, .. } => {
                assert!(
                    matches!(call, HandlerCall::Main(_, BodyMode::Streamed)),
                    "{call:?}"
                );
            }
            _ => panic!("expected a route"),
        }
    }

    #[test]
    fn a_route_carries_its_call_and_params() {
        let t = table();
        let h = HeaderMap::new();
        match decide(&Method::GET, "/users/7", &h, &t) {
            RequestDecision::Route { call, params } => {
                assert_eq!(
                    call,
                    HandlerCall::Main(t.route(t.route_id(0)).handler, BodyMode::Buffered)
                );
                assert_eq!(params, vec![("id".to_string(), "7".to_string())]);
            }
            _ => panic!("expected a route"),
        }
    }

    fn no_route<'r, 'q>(
        t: &'r Table,
        method: &'q Method,
        path: &'q str,
        h: &'q HeaderMap,
    ) -> NoRoute<'r, 'q, u32, Mount> {
        match decide(method, path, h, t) {
            RequestDecision::NoRoute(no_route) => no_route,
            _ => panic!("expected no route for {method} {path}"),
        }
    }

    #[test]
    fn only_get_and_head_ask_for_static_files() {
        let t = table();
        let mut h = HeaderMap::new();
        h.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
        for method in [Method::GET, Method::HEAD] {
            let lookups: Vec<_> = no_route(&t, &method, "/static/a.js", &h)
                .static_lookups()
                .map(|lookup| match lookup {
                    static_plan::Lookup::File(lookup) => lookup.accept_encoding(),
                    static_plan::Lookup::Refused(refusal) => panic!("{refusal}"),
                })
                .collect();
            assert_eq!(lookups, [Some("gzip")], "{method}");
        }
        for method in [Method::POST, Method::DELETE] {
            assert_eq!(
                no_route(&t, &method, "/static/a.js", &h)
                    .static_lookups()
                    .count(),
                0,
                "{method}"
            );
        }
    }

    #[test]
    fn after_a_static_miss_it_is_405_or_404() {
        let t = table();
        let h = HeaderMap::new();
        assert_eq!(
            no_route(&t, &Method::GET, "/items", &h).after_static_miss(),
            StaticMissAnswer::Miss(RouteMiss::MethodNotAllowed(HeaderValue::from_static(
                "POST"
            )))
        );
        assert_eq!(
            no_route(&t, &Method::GET, "/nowhere", &h).after_static_miss(),
            StaticMissAnswer::Miss(RouteMiss::NotFound)
        );
    }

    #[test]
    fn after_a_static_miss_the_fallback_answers_first() {
        let mut t = table();
        t.set_fallback(9);
        let h = HeaderMap::new();
        match no_route(&t, &Method::GET, "/items", &h).after_static_miss() {
            StaticMissAnswer::Fallback(HandlerCall::Main(target, BodyMode::Buffered)) => {
                assert_eq!(*t.handler(target), 9);
            }
            other => panic!("expected the fallback on main, buffered: {other:?}"),
        }
    }
}
