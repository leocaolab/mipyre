//! Holds the rules one app serves by: its route table and the settings every request is decided by.

use std::num::NonZeroU64;

use http::header::HeaderName;
use http::StatusCode;

use crate::plan::compression::CompressionSettings;
use crate::plan::cors::Cors;
use crate::plan::header::own_header_name;
use crate::plan::limits::Limits;
use crate::plan::route::RouteTable;

#[derive(Clone, Debug)]
pub struct ServeSettings {
    pub limits: Limits,
    pub cors: Option<Cors>,
    pub compression: Option<CompressionSettings>,
    /// The header a client's request id arrives in; with `None` the server mints every id.
    pub request_id_header: Option<HeaderName>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccessLog {
    /// The log keeps 1 in N responses.
    pub sample_n: NonZeroU64,
    /// A response with a status at or above this is always logged.
    pub always_status: Option<StatusCode>,
}

impl AccessLog {
    pub const EVERY_RESPONSE: AccessLog = AccessLog {
        sample_n: NonZeroU64::MIN,
        always_status: None,
    };
}

impl ServeSettings {
    /// Returns default limits with everything else off.
    pub fn defaults() -> Self {
        ServeSettings {
            limits: Limits::DEFAULT,
            cors: None,
            compression: None,
            request_id_header: None,
        }
    }

    /// Copies the settings, putting every header a response clones in an allocation of its own.
    pub fn own_copy(&self) -> Self {
        ServeSettings {
            limits: self.limits,
            cors: self.cors.as_ref().map(Cors::own_copy),
            compression: self.compression,
            request_id_header: self.request_id_header.as_ref().map(own_header_name),
        }
    }
}

pub struct SiteRules<H, M> {
    pub routes: RouteTable<H, M>,
    pub settings: ServeSettings,
    pub access_log: Option<AccessLog>,
}

impl<H, M: Clone> SiteRules<H, M> {
    /// Copies the rules for one serving thread, putting every byte a response clones in an
    /// allocation of its own so the thread's hot path writes no refcount another thread writes.
    pub fn copy_with(&self, copy: impl Fn(&H) -> H) -> Self {
        SiteRules {
            routes: self.routes.copy_with(copy),
            settings: self.settings.own_copy(),
            access_log: self.access_log,
        }
    }
}

/// Says which HTTP versions a server drives, which is what its TLS offers in ALPN; a server that
/// drives only HTTP/1.1 must not advertise `h2`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocols {
    /// It is for a server driven by hyper's `AutoBuilder`.
    Http2AndHttp1,
    /// It is for a server driven by `http1::Builder`; a client that offers only `h2` fails the
    /// handshake instead of speaking a protocol the server would misread.
    Http1Only,
}

impl Protocols {
    /// Returns the ALPN protocol ids, most preferred first.
    pub fn alpn(self) -> &'static [&'static [u8]] {
        match self {
            Protocols::Http2AndHttp1 => &[b"h2", b"http/1.1"],
            Protocols::Http1Only => &[b"http/1.1"],
        }
    }
}
