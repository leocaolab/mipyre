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

/// A request the engine turns away before routing it, holding the request's own bytes so the
/// answer can quote them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiteMiss<'a> {
    /// The request's authority (`host[:port]`), whose host is none of this server's names.
    UnknownHost(&'a [u8]),
    /// The request's `Origin`, a site this server does not trust; `None` when the request has
    /// none and only `Sec-Fetch-Site` says it comes from another site.
    CrossSite(Option<&'a [u8]>),
}
