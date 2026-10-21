//! Defines the requests this package turns away by itself, as decisions the engine answers
//! rather than errors it logs.

use http::header::HeaderValue;

/// A request that no route, static file or fallback answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteMiss {
    NotFound,
    /// The path has routes but none for the method; the value is their `Allow` header.
    MethodNotAllowed(HeaderValue),
}
