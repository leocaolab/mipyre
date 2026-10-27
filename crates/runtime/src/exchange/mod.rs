//! Carries a request from hyper to a Python handler and the handler's answer back.
//!
//! Nothing here decides where Python runs: `runtime` does, and it depends on this layer, never
//! the reverse.

pub mod body_stream;
pub mod cookie;
pub mod error;
pub mod http_exception;
pub mod json;
pub(crate) mod loop_wait;
pub mod request;
pub mod request_head;
pub mod response;
pub mod stream;
pub mod to_response;
pub mod websocket;

pub use self::body_stream::{BodyStream, PyBodyReadError};
pub use self::cookie::{Cookie, SameSite};
pub(crate) use self::error::catch_panic;
pub use self::error::{
    refuse, Capacity, ErrorRecord, HandlerError, PyException, ReplyHolder, RequestStage, UnansweredError,
};
pub use self::http_exception::HttpException;
pub use self::json::{loads, require_json};
pub use self::loop_wait::LoopWaits;
pub use self::request::{Request, RequestBody, RequestHeaders};
pub use self::response::{Response, VALIDATION_DETAIL_LIMIT};
pub use self::stream::{HandlerReply, ResponseStream, StreamReply};
pub use self::websocket::{
    connection_channels, ConnectionEnds, HandlerEnds, QueueEntry, QueueSender, WebSocket, WebSocketMessage,
};
