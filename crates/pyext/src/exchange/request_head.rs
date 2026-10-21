//! Builds every handler `Request`, on every path, from a [`RequestHead`] and a body.
//!
//! Nothing is copied: hyper's request parts move in, and a [`RequestLabel`] only bumps the
//! reference counts of the URI and id.

use hyper::http::request::Parts;

use crate::exchange::error::RequestLabel;
use crate::exchange::request::Request;
use mipyre_transport::driver::Peer;
use mipyre_http::plan::route::Params;
use crate::exchange::request::RequestBody;
use mipyre_http::io::request_id::RequestId;

pub(crate) struct RequestHead {
    pub(crate) parts: Parts,
    pub(crate) params: Params,
    pub(crate) client: Peer,
    pub(crate) request_id: RequestId,
}

impl RequestHead {
    pub(crate) fn label(&self) -> RequestLabel {
        RequestLabel {
            id: self.request_id.clone(),
            method: self.parts.method.clone(),
            uri: self.parts.uri.clone(),
        }
    }
}

impl Request {
    pub(crate) fn new(head: RequestHead, body: RequestBody) -> Self {
        Request { head, body }
    }

    pub(crate) fn label(&self) -> RequestLabel {
        self.head.label()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn head(uri: &str) -> RequestHead {
        let (parts, ()) = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("x-a", "1")
            .body(())
            .unwrap()
            .into_parts();
        RequestHead {
            parts,
            params: vec![("id".into(), "7".into())],
            client: Peer::Ip(std::net::Ipv4Addr::LOCALHOST.into()),
            request_id: RequestId::mint(),
        }
    }

    #[test]
    fn the_request_takes_the_head_as_is() {
        let request = Request::new(head("/items/7?q=1"), RequestBody::Buffered(Bytes::new()));
        assert_eq!(request.head.parts.method, hyper::Method::POST);
        assert_eq!(request.head.parts.uri.path(), "/items/7");
        assert_eq!(request.head.parts.uri.query(), Some("q=1"));
        assert_eq!(request.head.parts.headers["x-a"], "1");
        assert!(matches!(request.body, RequestBody::Buffered(_)));
    }

    #[test]
    fn the_label_shares_the_uri_bytes() {
        let head = head("/items/7?q=1");
        let label = head.label();
        let request = Request::new(head, RequestBody::Buffered(Bytes::new()));
        assert_eq!(label.uri.path().as_ptr(), request.head.parts.uri.path().as_ptr());
        assert_eq!(label.method.as_str(), "POST");
    }
}
