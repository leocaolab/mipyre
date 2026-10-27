//! The request decision fuzz target: whatever method, path and headers a client sends, a gRPC method answers
//! only a gRPC `POST`, a pre-built response comes before a route, a route resolves only what the table resolves, and
//! a request none answers asks for static files only with `GET` or `HEAD`, then reaches the fallback, a 405 that
//! does not allow its method, or a 404.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::request::fuzz::...` mutates it under libFuzzer,
//! and an input it finds that fails goes into the corpus as a regression.

use http::header::{HeaderName, ACCEPT_ENCODING, CONTENT_TYPE};
use http::Method;

use super::{decide, RequestDecision, StaticMissAnswer};
use crate::plan::fuzz_headers::header_map;
use crate::plan::reject::RouteMiss;
use crate::plan::route::{FastResponse, GrpcMethod, HandlerPlace, RouteTable};
use crate::plan::static_plan::{Copies, StaticMountRule};

const HEADERS: [HeaderName; 2] = [CONTENT_TYPE, ACCEPT_ENCODING];
const ROUTES: [(&str, &str); 3] = [
    ("GET", "/users/{id}"),
    ("POST", "/items"),
    ("DELETE", "/static/{*rest}"),
];
const FAST_PATH: &str = "/health";
const GRPC: GrpcMethod = GrpcMethod::BenchmarkGetSum;

type Input = (Vec<u8>, Vec<u8>, Vec<(u8, Vec<u8>)>, bool);

struct Mount(StaticMountRule);

impl AsRef<StaticMountRule> for Mount {
    fn as_ref(&self) -> &StaticMountRule {
        &self.0
    }
}

type Table = RouteTable<(), Mount>;

fn table(fallback: bool) -> Table {
    let mut table = Table::new();
    for (method, path) in ROUTES {
        table
            .insert(method, path, (), format!("{method} {path}"), HandlerPlace::Lane)
            .unwrap();
    }
    let ok = FastResponse::parse(bytes::Bytes::from_static(b"ok"), "text/plain", 200, &Default::default()).unwrap();
    assert!(table.add_fast_response("GET", FAST_PATH, ok).unwrap());
    table.add_static_mount(Mount(StaticMountRule::new("/static", Copies::Precompressed).unwrap()));
    table.add_grpc_method(GRPC);
    if fallback {
        table.set_fallback(());
    }
    table
}

fn decide_any((method, path, fields, fallback): &Input) {
    let Ok(method) = Method::from_bytes(method) else {
        return;
    };
    let path = String::from_utf8_lossy(path);
    let headers = header_map(&HEADERS, fields);
    let table = table(*fallback);
    let grpc = method == Method::POST
        && path == GRPC.path()
        && headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("application/grpc"));

    match decide(&method, &path, &headers, &table) {
        RequestDecision::Grpc(found) => {
            assert!(grpc, "{method} {path:?} {headers:?}");
            assert_eq!(found, GRPC);
        }
        RequestDecision::Fast(_) => {
            assert!(!grpc);
            assert!(table.fast_response(&method, &path).is_some(), "{method} {path:?}");
        }
        RequestDecision::Route { call, params } => {
            assert!(
                !grpc && table.fast_response(&method, &path).is_none(),
                "{method} {path:?}"
            );
            assert_eq!(table.resolve(&method, &path), Some((call, params)), "{method} {path:?}");
        }
        RequestDecision::NoRoute(no_route) => {
            assert!(
                !grpc && table.fast_response(&method, &path).is_none(),
                "{method} {path:?}"
            );
            assert_eq!(table.resolve(&method, &path), None, "{method} {path:?}");
            let asks = no_route.static_lookups().count();
            let may_ask = (method == Method::GET || method == Method::HEAD)
                && path.strip_prefix("/static/").is_some_and(|rel| !rel.is_empty());
            assert_eq!(asks, usize::from(may_ask), "{method} {path:?}");
            match no_route.after_static_miss() {
                StaticMissAnswer::Fallback(_) => assert!(*fallback),
                StaticMissAnswer::Miss(miss) => {
                    assert!(!*fallback);
                    match (miss, table.allowed_methods(&path)) {
                        (RouteMiss::MethodNotAllowed(allow), Some(expected)) => {
                            assert_eq!(allow, expected);
                            let allow = allow.to_str().unwrap();
                            assert!(
                                allow.split(", ").all(|m| m != method.as_str()),
                                "{method} {path:?} refused with Allow {allow:?}"
                            );
                        }
                        (RouteMiss::NotFound, None) => {}
                        (miss, allow) => panic!("{method} {path:?} missed as {miss:?}, Allow {allow:?}"),
                    }
                }
            }
        }
    }
}

#[test]
fn any_request_is_decided_in_order_and_a_miss_is_fallback_405_or_404() {
    bolero::check!()
        .with_type::<Input>()
        .with_iterations(0)
        .for_each(decide_any);
}
