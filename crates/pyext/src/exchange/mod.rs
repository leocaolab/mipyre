//! Carries a request from hyper to a Python handler and the handler's answer back.
//!
//! Nothing here decides where Python runs: `runtime` does, and it depends on this layer, never
//! the reverse.

pub(crate) mod body_stream;
pub(crate) mod cookie;
pub(crate) mod error;
pub(crate) mod http_exception;
pub(crate) mod json;
pub(crate) mod lane_failure;
pub(crate) mod request;
pub(crate) mod request_head;
pub(crate) mod response;
pub(crate) mod stream;
pub(crate) mod to_response;
pub(crate) mod websocket;

pub(crate) use self::body_stream::{BodyStream, PyBodyReadError};
pub(crate) use self::cookie::{Cookie, SameSite};
#[cfg(test)]
pub(crate) use self::error::tests::{captured_log, captured_log_from, captured_log_value, label};
pub(crate) use self::error::{
    catch_panic, refuse, Capacity, ErrorRecord, HandlerError, PyException, ReplyHolder, RequestLabel, RequestStage,
    UnansweredError, WorkerGroup,
};
pub(crate) use self::http_exception::HttpException;
pub(crate) use self::json::{loads, require_json};
pub(crate) use self::lane_failure::{LaneDecodeError, LaneIndex, TaggedField, WorkerExit, WorkerFailure};
pub(crate) use self::request::{Request, RequestBody, RequestHeaders};
pub(crate) use self::request_head::RequestHead;
pub(crate) use self::response::Response;
pub(crate) use self::stream::ResponseStream;
pub(crate) use self::websocket::{
    connection_channels, ConnectionEnds, HandlerEnds, QueueEntry, QueueSender, WebSocket, WebSocketMessage,
};
