//! The route-resolution fuzz target: whatever method and path a client sends, the table resolves it to a
//! route of that method (a `HEAD` with none to the `GET` one) whose parameters are the template's, the `Allow` value
//! lists every method that resolves, and a pre-built response answers its exact path only.
//!
//! `cargo test` replays the committed corpus (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::route::fuzz::...` mutates it under libFuzzer,
//! and an input it finds that fails goes into the corpus as a regression.

use http::Method;

use super::{template_params, HandlerCall, HandlerPlace, RouteTable, WsPlace};

const ROUTES: [(&str, &str); 7] = [
    ("GET", "/"),
    ("GET", "/users/{id}"),
    ("POST", "/users/{id}"),
    ("GET", "/users/{id}/posts/{post}"),
    ("GET", "/files/{*rest}"),
    ("GET", "/static/a.css"),
    ("PURGE", "/cache/{key}"),
];
const FAST_PATH: &str = "/health";

type Table = RouteTable<(), ()>;

fn table() -> Table {
    let mut table = Table::new();
    for (method, path) in ROUTES {
        table
            .insert(method, path, (), format!("{method} {path}"), HandlerPlace::Lane)
            .unwrap();
    }
    table.add_websocket("/ws/{room}", (), WsPlace::Worker).unwrap();
    let ok =
        super::FastResponse::parse(bytes::Bytes::from_static(b"ok"), "text/plain", 200, &Default::default()).unwrap();
    assert!(table.add_fast_response("GET", FAST_PATH, ok).unwrap());
    table
}

fn methods(asked: Option<Method>) -> impl Iterator<Item = Method> {
    [
        Method::GET,
        Method::HEAD,
        Method::POST,
        Method::from_bytes(b"PURGE").unwrap(),
    ]
    .into_iter()
    .chain(asked)
}

fn resolve_any((method, path): &(Vec<u8>, Vec<u8>)) {
    let table = table();
    let path = String::from_utf8_lossy(path);
    let asked = Method::from_bytes(method).ok();

    for method in methods(asked) {
        let Some((call, params)) = table.resolve(&method, &path) else {
            continue;
        };
        let HandlerCall::Lane(id) = call else {
            panic!("every route runs on the lane, yet {method} {path:?} resolved to {call:?}");
        };
        let route = table.route(id);
        assert!(
            route.key.method == method || method == Method::HEAD && route.key.method == Method::GET,
            "{method} {path:?} resolved to {}",
            route.key
        );
        let names: Vec<&str> = params.located().map(|(name, _)| name).collect();
        assert_eq!(names, template_params(&route.key.path).unwrap(), "{method} {path:?}");
        assert!(
            params.decoded(&path).all(|(_, value)| value.is_some()),
            "{method} {path:?}: a param's place is outside the path"
        );
        if !route.key.path.contains('{') {
            assert_eq!(path, route.key.path);
        }

        let allow = table
            .allowed_methods(&path)
            .expect("a path that resolves allows its method");
        let allow = allow.to_str().unwrap();
        assert!(
            allow.split(", ").any(|m| m == method.as_str()),
            "{allow:?} misses {method}"
        );
    }

    if table.allowed_methods(&path).is_none() {
        assert!(methods(None).all(|m| table.resolve(&m, &path).is_none() && table.fast_response(&m, &path).is_none()));
    }
    for method in methods(None) {
        let fast = table.fast_response(&method, &path).is_some();
        assert_eq!(
            fast,
            path == FAST_PATH && (method == Method::GET || method == Method::HEAD),
            "{method} {path:?}"
        );
    }
    if let Some((_, params)) = table.websocket(&path) {
        let names: Vec<&str> = params.located().map(|(name, _)| name).collect();
        assert_eq!(names, ["room"]);
        assert!(params.decoded(&path).all(|(_, value)| value.is_some()), "{path:?}");
    }
}

#[test]
fn any_method_and_path_resolve_to_a_route_of_that_method_or_to_none() {
    bolero::check!()
        .with_type::<(Vec<u8>, Vec<u8>)>()
        .with_iterations(0)
        .for_each(resolve_any);
}
