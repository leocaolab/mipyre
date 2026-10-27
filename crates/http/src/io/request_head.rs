//! Holds what a request is before its body: hyper's parts, the route's path parameters, the peer
//! and the request id. Every backend builds the handler's request from it.
//!
//! It sits in `io/`, not `plan/`, because it carries the connection's peer, a transport fact
//! `plan/` does not name.

use http::request::Parts;
use http::{Method, Uri};

use crate::plan::request_id::RequestId;
use crate::plan::route::Params;
use mipyre_transport::driver::Peer;

pub struct RequestHead {
    pub parts: Parts,
    pub params: Params,
    pub client: Peer,
    pub request_id: RequestId,
}

/// The label names a request in its log lines after the request moved into the handler's
/// `Request`; making one only bumps reference counts and never allocates.
#[derive(Clone, Debug)]
pub struct RequestLabel {
    pub id: RequestId,
    pub method: Method,
    pub uri: Uri,
}

impl RequestHead {
    pub fn label(&self) -> RequestLabel {
        RequestLabel {
            id: self.request_id.clone(),
            method: self.parts.method.clone(),
            uri: self.parts.uri.clone(),
        }
    }
}
