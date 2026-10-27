//! The CORS fuzz targets: an origin entry reads as a browser's origin or is refused, and what it reads as
//! reads as itself again; a request's `Origin` and method decide which origin the response names and whether it is a
//! preflight, and a response names only an origin the rule allows, byte for byte.
//!
//! `cargo test` replays the committed corpora (`__fuzz__/.../corpus`) and nothing else, so the merge runs the same
//! inputs every time; `cargo bolero test -p mipyre_http --lib plan::cors::fuzz::...` mutates them under libFuzzer,
//! and an input it finds that fails goes into the corpus as a regression.

use http::header::{HeaderMap, HeaderName, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN, VARY};
use http::{Method, StatusCode};

use super::{browser_origin, Cors, CorsOrigin};
use crate::plan::fuzz_headers::header_map;

const ORIGINS: [&str; 2] = ["https://a.example", "http://b.example:8080"];
const REQUEST_HEADERS: [HeaderName; 2] = [ORIGIN, ACCESS_CONTROL_REQUEST_METHOD];
const METHODS: [Method; 3] = [Method::OPTIONS, Method::GET, Method::POST];

fn read_origin_entry(raw: &[u8]) {
    let entry = String::from_utf8_lossy(raw);
    let read = browser_origin(&entry);
    let parsed = Cors::parse(&[entry.to_string()], "GET", "content-type", None, false, None);
    if entry == "*" {
        assert!(parsed.is_ok());
        return;
    }
    assert_eq!(parsed.is_ok(), read.is_some(), "{entry:?}");
    let Some(origin) = read else {
        return;
    };
    assert!(
        origin.starts_with("http://") || origin.starts_with("https://"),
        "{entry:?} read as {origin:?}"
    );
    assert_eq!(
        browser_origin(&origin).as_deref(),
        Some(origin.as_str()),
        "{entry:?} read as {origin:?}"
    );
}

fn answer_request((method, fields): &(u8, Vec<(u8, Vec<u8>)>)) {
    let method = &METHODS[usize::from(*method) % METHODS.len()];
    let request = header_map(&REQUEST_HEADERS, fields);
    let origins = ORIGINS.map(str::to_string);
    for rule in [&origins[..1], &origins[..]] {
        let cors = Cors::parse(rule, "GET, POST", "content-type", None, false, Some(600)).unwrap();
        answer_with(&cors, method, &request);
    }
}

fn answer_with(cors: &Cors, method: &Method, request: &HeaderMap) {
    let named = cors.origin_of(request);
    if let (Some(CorsOrigin(at)), [_, _, ..]) = (named, cors.origins.as_slice()) {
        let asked = request
            .get(ORIGIN)
            .expect("several origins name one only for a request that names one");
        assert_eq!(&cors.origins[at], asked);
        assert!(cors.trusts(asked));
    }
    if let Some(asked) = request.get(ORIGIN) {
        assert_eq!(cors.trusts(asked), cors.origins.contains(asked), "{asked:?}");
    }

    let mut response = HeaderMap::new();
    cors.apply(&mut response, named);
    let allowed = response.get(ACCESS_CONTROL_ALLOW_ORIGIN);
    assert_eq!(allowed, named.map(|CorsOrigin(at)| &cors.origins[at]));
    assert_eq!(response.contains_key(VARY), cors.origins.len() > 1);

    let preflight = cors.preflight(method, request);
    let is_preflight = *method == Method::OPTIONS && request.contains_key(ACCESS_CONTROL_REQUEST_METHOD);
    assert_eq!(
        preflight.as_ref().map(|r| r.status()),
        is_preflight.then_some(StatusCode::NO_CONTENT)
    );
}

#[test]
fn any_origin_entry_reads_as_a_browser_origin_or_is_refused() {
    bolero::check!().with_iterations(0).for_each(read_origin_entry);
}

#[test]
fn any_request_gets_only_an_origin_the_rule_allows() {
    bolero::check!()
        .with_type::<(u8, Vec<(u8, Vec<u8>)>)>()
        .with_iterations(0)
        .for_each(answer_request);
}
