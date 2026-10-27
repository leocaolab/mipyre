//! Holds the lane's request encoding to what the host's client sent: every request target and header the
//! HTTP side accepts reaches the worker as it was, through a real slot.

use http::header::{HeaderName, HeaderValue};

use crate::wire::tests::written;
use crate::wire::{decode_request, encode_request, LaneRequest};
use mipyre_http::io::request_head::RequestHead;
use mipyre_http::plan::request_id::{RequestId, ServerRequestId};
use mipyre_http::plan::route::RouteKey;
use mipyre_transport::driver::Peer;

/// A slot of the lane's default size, 256 KiB, without its header.
const ROOM: usize = (256 << 10) - 8;

fn request(method: http::Method, target: &str) -> LaneRequest {
    let (parts, ()) = http::Request::builder()
        .method(method.clone())
        .uri(target)
        .body(())
        .unwrap()
        .into_parts();
    LaneRequest {
        route_key: RouteKey {
            method,
            path: "/r".into(),
        },
        head: RequestHead {
            parts,
            params: Default::default(),
            client: Peer::InMemory,
            request_id: RequestId::Server(ServerRequestId { process: 1, number: 2 }),
        },
        body: Default::default(),
    }
}

/// Every form of request target `http::Uri` parses comes back to the worker as the same URI.
#[test]
fn every_request_target_the_http_side_parses_reaches_the_worker_unchanged() {
    let targets = [
        ("GET", "/"),
        ("GET", "/a/b?c=d&e"),
        ("GET", "/%zz?%00"),
        ("GET", "//double//slash"),
        ("GET", "/?"),
        ("OPTIONS", "*"),
        ("CONNECT", "example.com:443"),
        ("CONNECT", "[::1]:8080"),
        ("GET", "http://example.com"),
        ("GET", "http://user:pw@example.com:8080/p?q=1"),
        ("GET", "https://EXAMPLE.com/P"),
        ("GET", "http://[::1]/x"),
    ];
    for (method, target) in targets {
        let method = http::Method::from_bytes(method.as_bytes()).unwrap();
        let uri: http::Uri = target.parse().unwrap_or_else(|e| panic!("{target}: {e}"));
        let sent = request(method, target);
        assert_eq!(sent.head.parts.uri, uri);
        let (fits, bytes) = written(ROOM, |w| encode_request(w, &sent));
        fits.unwrap();
        let back = decode_request(&bytes).unwrap_or_else(|e| panic!("{target}: {e}"));
        assert_eq!(back.head.parts.uri.to_string(), uri.to_string(), "{target}");
        assert_eq!(back.head.parts.uri, uri, "{target}");
    }
}

/// A request's header lines, the same name many times over and values of any byte a header value may hold, reach the
/// worker in their order, each with its own value.
#[test]
fn header_lines_of_one_name_and_any_value_reach_the_worker_in_order() {
    let mut sent = request(http::Method::GET, "/");
    let name = HeaderName::from_static("x-many");
    let values: Vec<HeaderValue> = (0..2000u32)
        .map(|i| {
            let mut v = format!("v{i}\t").into_bytes();
            v.extend([0x80u8, 0xff, b' ', b'"']);
            HeaderValue::from_bytes(&v).unwrap()
        })
        .collect();
    for v in &values {
        sent.head.parts.headers.append(name.clone(), v.clone());
    }
    let (fits, bytes) = written(ROOM, |w| encode_request(w, &sent));
    fits.unwrap();
    let back = decode_request(&bytes).unwrap();
    let got: Vec<&HeaderValue> = back.head.parts.headers.get_all(&name).iter().collect();
    assert_eq!(got.len(), values.len());
    assert!(got.iter().zip(&values).all(|(a, b)| *a == b));
}
