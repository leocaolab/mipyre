//! Measures the route decision and the no-route steps after it.
use std::alloc::System;
use std::hint::black_box;

use bytes::Bytes;
use hyper::header::HeaderMap;
use hyper::Method;
use mipyre_http::plan::request::{decide, RequestDecision};
use mipyre_http::plan::route::{FastResponse, HandlerKind, HandlerPlace, RouteTable};
use mipyre_http::plan::static_plan::{Copies, Lookup, StaticMountRule};
use mipyre_microbench::{measure, run, AllocationCounter, Case, Must};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

/// The engine's mount without its directory, which `decide` never reads.
struct Mount(StaticMountRule);

impl AsRef<StaticMountRule> for Mount {
    fn as_ref(&self) -> &StaticMountRule {
        &self.0
    }
}

/// Builds the table `benchmarks/paths/app.py` serves for `BENCH_KIND=http`.
fn bench_routes() -> RouteTable<u32, Mount> {
    let mut t = RouteTable::new();
    let fast = FastResponse::parse(Bytes::from_static(b"ok"), "text/plain", 200, &Default::default())
        .must("a plain fast response");
    t.add_fast_response("GET", "/fast", fast).must("register /fast");
    for (path, h) in [("/json", 1), ("/async", 2)] {
        t.insert("GET", path, h, "h".into(), HandlerPlace::Worker(HandlerKind::Sync))
            .must("register a route");
    }
    t.add_static_mount(Mount(
        StaticMountRule::new("/static", Copies::Precompressed).must("a mount"),
    ));
    t
}

/// Decides `method path` and follows the decision as far as the engine goes without IO.
fn route_decide(name: &str, method: Method, path: &'static str, n: u64) {
    let routes = bench_routes();
    let headers = HeaderMap::new();
    measure(name, n, |_| {
        match decide(
            black_box(&method),
            black_box(path),
            black_box(&headers),
            black_box(&routes),
        ) {
            RequestDecision::Route { call, params } => {
                black_box((call, params));
            }
            RequestDecision::NoRoute(no_route) => {
                let mut lookups = no_route.static_lookups();
                match lookups.next() {
                    Some(lookup) => {
                        black_box(matches!(lookup, Lookup::File(_)));
                    }
                    None => {
                        black_box(no_route.after_static_miss());
                    }
                }
            }
            RequestDecision::Fast(fast) => {
                black_box(fast);
            }
            RequestDecision::Grpc(grpc) => {
                black_box(grpc);
            }
        }
    });
}

/// Decides the route the json-* paths take.
fn route_hit(n: u64) {
    route_decide("route_hit", Method::GET, "/json", n);
}

/// Finds no `HEAD` route and falls back to the `GET` one.
fn route_head(n: u64) {
    route_decide("route_head", Method::HEAD, "/json", n);
}

/// Decides the 404 the notfound path gets, since no route or mount covers it.
fn route_notfound(n: u64) {
    route_decide("route_notfound", Method::GET, "/nowhere", n);
}

/// Decides the static lookup the static-hit path gets.
fn route_static(n: u64) {
    route_decide("route_static", Method::GET, "/static/a.js", n);
}

fn main() {
    let cases: [Case; 4] = [
        ("route_hit", route_hit),
        ("route_head", route_head),
        ("route_notfound", route_notfound),
        ("route_static", route_static),
    ];
    run(&cases);
}
