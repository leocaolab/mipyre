//! The route table against a reference model: one sequence of registrations and requests goes to a
//! [`RouteTable`] and to a model a reader can check by eye, and every step's outcome must agree.
//!
//! The model keeps every registered route in a list. A request is matched by trying each route of its method in turn,
//! segment by segment, and the most specific match wins: at the first segment where two matches differ, a literal
//! beats a parameter and a parameter beats a catch-all. Two routes conflict when they have the same shape (parameter
//! names aside), or when one has a parameter where the other, after the same shape so far, has a catch-all.

use std::collections::BTreeSet;

use http::Method;
use percent_encoding::percent_decode_str;
use proptest::prelude::*;

use super::{
    BodyMode, HandlerCall, HandlerKind, HandlerPlace, MainHandler, RouteError, RouteTable, WsPlace, WsRouteError,
    WsRouteId,
};

/// Returns `params` decoded against `path` as owned pairs, each value there.
pub(super) fn decoded_pairs(params: &super::Params, path: &str) -> Vec<(String, String)> {
    params
        .decoded(path)
        .map(|(name, value)| (name.to_string(), value.expect("the value is in the path").into_owned()))
        .collect()
}

/// One segment of a registered route path.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Segment {
    Literal(&'static str),
    Param(String),
    /// It is only ever the last segment.
    CatchAll(String),
    /// An Express-style `:id`, which the table refuses.
    Colon,
}

impl Segment {
    /// Ranks how specific the segment is: a lower rank wins a request two routes both match.
    fn rank(&self) -> u8 {
        match self {
            Segment::Literal(_) => 0,
            Segment::Param(_) => 1,
            Segment::CatchAll(_) => 2,
            Segment::Colon => 3,
        }
    }

    fn same_shape(&self, other: &Segment) -> bool {
        match (self, other) {
            (Segment::Literal(a), Segment::Literal(b)) => a == b,
            (Segment::Param(_), Segment::Param(_)) | (Segment::CatchAll(_), Segment::CatchAll(_)) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Template(Vec<Segment>);

impl Template {
    fn render(&self) -> String {
        let segments: Vec<String> = self
            .0
            .iter()
            .map(|segment| match segment {
                Segment::Literal(text) => (*text).to_string(),
                Segment::Param(name) => format!("{{{name}}}"),
                Segment::CatchAll(name) => format!("{{*{name}}}"),
                Segment::Colon => ":id".to_string(),
            })
            .collect();
        format!("/{}", segments.join("/"))
    }

    fn misread(&self) -> bool {
        self.0.contains(&Segment::Colon)
    }

    fn conflicts(&self, other: &Template) -> bool {
        let shared = self.0.iter().zip(&other.0).take_while(|(a, b)| a.same_shape(b)).count();
        if shared == self.0.len() && shared == other.0.len() {
            return true;
        }
        matches!(
            (self.0.get(shared), other.0.get(shared)),
            (Some(Segment::Param(_)), Some(Segment::CatchAll(_)))
                | (Some(Segment::CatchAll(_)), Some(Segment::Param(_)))
        )
    }

    fn ranks(&self) -> Vec<u8> {
        self.0.iter().map(Segment::rank).collect()
    }

    /// Returns the parameters this route binds for `path`, or `None` if it does not match.
    fn bind(&self, path: &str) -> Option<Params> {
        let segments: Vec<&str> = path.strip_prefix('/')?.split('/').collect();
        bind(&self.0, &segments)
    }
}

fn bind(template: &[Segment], path: &[&str]) -> Option<Params> {
    let decoded = |raw: &str| percent_decode_str(raw).decode_utf8_lossy().into_owned();
    match (template, path) {
        ([], []) => Some(Vec::new()),
        ([Segment::CatchAll(name)], [_, ..]) => {
            let rest = path.join("/");
            (!rest.is_empty()).then(|| vec![(name.clone(), decoded(&rest))])
        }
        ([Segment::Literal(text), template @ ..], [segment, path @ ..]) if text == segment => bind(template, path),
        ([Segment::Param(name), template @ ..], [segment, path @ ..]) if !segment.is_empty() => {
            let mut params = vec![(name.clone(), decoded(segment))];
            params.extend(bind(template, path)?);
            Some(params)
        }
        _ => None,
    }
}

/// Returns the index and parameters of the most specific of `templates` that matches `path`.
fn most_specific<'a>(templates: impl Iterator<Item = (usize, &'a Template)>, path: &str) -> Option<(usize, Params)> {
    templates
        .filter_map(|(index, template)| Some((template.ranks(), index, template.bind(path)?)))
        .min_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, index, params)| (index, params))
}

/// The parameters a route binds, each value percent-decoded.
type Params = Vec<(String, String)>;

#[derive(Clone, Debug)]
enum Op {
    Insert {
        method: &'static str,
        template: Template,
        place: HandlerPlace,
    },
    FastResponse {
        method: &'static str,
        path: String,
    },
    WebSocket(Template),
    Ask {
        method: &'static str,
        path: String,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum Registration {
    Added,
    Misread,
    Refused,
}

#[derive(Debug, PartialEq, Eq)]
struct Resolution {
    route: usize,
    place: HandlerPlace,
    params: Params,
}

/// What a table answers for one request.
#[derive(Debug, PartialEq, Eq)]
struct Answer {
    resolved: Option<Resolution>,
    fast_response: bool,
    allow: Option<String>,
    websocket: Option<(usize, Params)>,
}

struct RouteModel {
    /// Each route's method is its uppercase spelling, which is how the table stores it.
    routes: Vec<(String, Template, HandlerPlace)>,
    fast_responses: BTreeSet<(String, String)>,
    websockets: Vec<Template>,
}

impl RouteModel {
    fn new() -> Self {
        RouteModel {
            routes: Vec::new(),
            fast_responses: BTreeSet::new(),
            websockets: Vec::new(),
        }
    }

    fn insert(&mut self, method: &str, template: &Template, place: HandlerPlace) -> Registration {
        let method = method.to_uppercase();
        if template.misread() {
            return Registration::Misread;
        }
        if self
            .routes
            .iter()
            .any(|(m, t, _)| *m == method && t.conflicts(template))
        {
            return Registration::Refused;
        }
        self.routes.push((method, template.clone(), place));
        Registration::Added
    }

    fn add_fast_response(&mut self, method: &str, path: &str) -> bool {
        self.fast_responses.insert((method.to_uppercase(), path.to_string()))
    }

    fn add_websocket(&mut self, template: &Template) -> Registration {
        if template.misread() {
            return Registration::Misread;
        }
        if self.websockets.iter().any(|t| t.conflicts(template)) {
            return Registration::Refused;
        }
        self.websockets.push(template.clone());
        Registration::Added
    }

    fn lookup(&self, method: &str, path: &str) -> Option<Resolution> {
        let of_method = self.routes.iter().enumerate().filter(|(_, (m, _, _))| m == method);
        let (route, params) = most_specific(of_method.map(|(index, (_, template, _))| (index, template)), path)?;
        Some(Resolution {
            route,
            place: self.routes[route].2,
            params,
        })
    }

    fn ask(&self, method: &str, path: &str) -> Answer {
        let resolved = self
            .lookup(method, path)
            .or_else(|| (method == "HEAD").then(|| self.lookup("GET", path))?);
        let fast = |m: &str| self.fast_responses.contains(&(m.to_string(), path.to_string()));
        let fast_response = fast(method) || (method == "HEAD" && fast("GET"));

        let mut allowed: BTreeSet<&str> = self
            .routes
            .iter()
            .filter(|(_, template, _)| template.bind(path).is_some())
            .map(|(m, _, _)| m.as_str())
            .chain(
                self.fast_responses
                    .iter()
                    .filter(|(_, p)| p == path)
                    .map(|(m, _)| m.as_str()),
            )
            .collect();
        if allowed.contains("GET") {
            allowed.insert("HEAD");
        }
        let allow = (!allowed.is_empty()).then(|| allowed.into_iter().collect::<Vec<_>>().join(", "));

        let websocket = most_specific(self.websockets.iter().enumerate(), path);
        Answer {
            resolved,
            fast_response,
            allow,
            websocket,
        }
    }
}

type Table = RouteTable<usize, ()>;

fn registration<E>(result: Result<(), E>, misread: impl Fn(&E) -> bool) -> Registration {
    match result {
        Ok(()) => Registration::Added,
        Err(error) if misread(&error) => Registration::Misread,
        Err(_) => Registration::Refused,
    }
}

fn ask_table(table: &Table, method: &str, path: &str) -> Answer {
    let method = Method::from_bytes(method.as_bytes()).expect("the generated methods are tokens");
    let resolved = table.resolve(&method, path).map(|(call, params)| {
        let (route, place) = match call {
            HandlerCall::Worker(id, kind) => (id.index(), HandlerPlace::Worker(kind)),
            HandlerCall::Main(target, handler) => (*table.handler(target), HandlerPlace::Main(handler)),
            HandlerCall::Lane(id) => (id.index(), HandlerPlace::Lane),
        };
        Resolution {
            route,
            place,
            params: decoded_pairs(&params, path),
        }
    });
    let allow = table
        .allowed_methods(path)
        .map(|value| value.to_str().expect("method names are visible ASCII").to_string());
    let websocket = table
        .websocket(path)
        .map(|(WsRouteId(index), params)| (index, decoded_pairs(&params, path)));
    Answer {
        resolved,
        fast_response: table.fast_response(&method, path).is_some(),
        allow,
        websocket,
    }
}

fn fast_body() -> super::FastResponse {
    super::FastResponse::parse(bytes::Bytes::from_static(b"ok"), "text/plain", 200, &Default::default())
        .expect("a 200 with no headers is a valid response")
}

fn run(ops: &[Op]) -> Result<(), TestCaseError> {
    let mut table = Table::new();
    let mut model = RouteModel::new();
    let mut asked = Vec::new();
    for op in ops {
        match op {
            Op::Insert {
                method,
                template,
                place,
            } => {
                let rendered = template.render();
                let index = table.routes().len();
                let got = registration(table.insert(method, &rendered, index, String::new(), *place), |e| {
                    matches!(e, RouteError::MisreadPath { .. })
                });
                prop_assert_eq!(got, model.insert(method, template, *place), "{} {}", method, rendered);
            }
            Op::FastResponse { method, path } => {
                let added = table
                    .add_fast_response(method, path, fast_body())
                    .expect("the generated methods are tokens");
                prop_assert_eq!(added, model.add_fast_response(method, path), "fast {} {}", method, path);
            }
            Op::WebSocket(template) => {
                let rendered = template.render();
                let index = table.handlers().count();
                let got = registration(table.add_websocket(&rendered, index, WsPlace::Main), |e| {
                    matches!(e, WsRouteError::MisreadPath { .. })
                });
                prop_assert_eq!(got, model.add_websocket(template), "websocket {}", rendered);
            }
            Op::Ask { method, path } => {
                let got = ask_table(&table, method, path);
                prop_assert_eq!(got, model.ask(method, path), "{} {}", method, path);
                asked.push((*method, path));
            }
        }
    }

    let copy = table.copy_with(|handler| *handler);
    for (method, path) in asked {
        prop_assert_eq!(
            ask_table(&copy, method, path),
            ask_table(&table, method, path),
            "the copy, {} {}",
            method,
            path
        );
    }
    Ok(())
}

const LITERALS: [&str; 3] = ["a", "b", "ab"];

fn segment(position: usize, last: bool) -> BoxedStrategy<Segment> {
    let literal = prop::sample::select(&LITERALS[..]).prop_map(Segment::Literal);
    let param = prop::sample::select(&["x", "y"][..]).prop_map(move |name| Segment::Param(format!("{name}{position}")));
    if !last {
        return prop_oneof![4 => literal, 3 => param, 1 => Just(Segment::Colon)].boxed();
    }
    let catch_all =
        prop::sample::select(&["r", "s"][..]).prop_map(move |name| Segment::CatchAll(format!("{name}{position}")));
    prop_oneof![
        4 => literal,
        3 => param,
        2 => catch_all,
        1 => Just(Segment::Literal("")),
        1 => Just(Segment::Colon),
    ]
    .boxed()
}

fn template() -> impl Strategy<Value = Template> {
    (1..=3usize)
        .prop_flat_map(|len| {
            (0..len)
                .map(|position| segment(position, position + 1 == len))
                .collect::<Vec<_>>()
        })
        .prop_map(Template)
}

/// Generates request paths from the routes' literals, an empty segment, and percent-encoded text, which a literal
/// does not match but a parameter decodes.
fn path() -> impl Strategy<Value = String> {
    let segment = prop::sample::select(&["a", "b", "ab", "c", "", "%61", "x%2Fy"][..]);
    prop::collection::vec(segment, 1..=4).prop_map(|segments| format!("/{}", segments.join("/")))
}

fn place() -> impl Strategy<Value = HandlerPlace> {
    prop::sample::select(
        &[
            HandlerPlace::Worker(HandlerKind::Sync),
            HandlerPlace::Worker(HandlerKind::Async),
            HandlerPlace::Main(MainHandler::Sync(BodyMode::Buffered)),
            HandlerPlace::Main(MainHandler::Sync(BodyMode::Streamed)),
            HandlerPlace::Main(MainHandler::Async),
            HandlerPlace::Lane,
        ][..],
    )
}

/// Registration spellings: lowercase names the standard method, and `PURGE` is an extension.
const REGISTERED: [&str; 5] = ["GET", "get", "HEAD", "POST", "PURGE"];
/// Request spellings: a method is case-sensitive, so `get`, `head` and `purge` match nothing registered.
const REQUESTED: [&str; 8] = ["GET", "HEAD", "POST", "PURGE", "OPTIONS", "get", "head", "purge"];

fn op() -> impl Strategy<Value = Op> {
    let registered = || prop::sample::select(&REGISTERED[..]);
    prop_oneof![
        4 => (registered(), template(), place()).prop_map(|(method, template, place)| Op::Insert { method, template, place }),
        1 => (registered(), path()).prop_map(|(method, path)| Op::FastResponse { method, path }),
        1 => template().prop_map(Op::WebSocket),
        5 => (prop::sample::select(&REQUESTED[..]), path()).prop_map(|(method, path)| Op::Ask { method, path }),
    ]
}

proptest! {
    /// Every registration and request of any sequence comes out of the table as it comes out of the model, and a
    /// copy of the table answers every request as the table does.
    #[test]
    fn the_route_table_answers_every_step_as_its_reference_model(ops in prop::collection::vec(op(), 0..48)) {
        run(&ops)?;
    }
}
