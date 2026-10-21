//! Turns a handler's answer into the hyper response the client gets, compressed or streamed as
//! SSE; every serving path ends here.

use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::header::{HeaderValue, CACHE_CONTROL, CONNECTION, CONTENT_TYPE, SERVER};
use hyper::Response;

use crate::exchange::ErrorRecord;
use crate::runtime::{MainReply, StreamReply};
use crate::telemetry::RunCounters;
use mipyre_http::plan::response::{build_response, HandlerResponse};
use mipyre_http::io::body::{full_body, ResponseBody};

use super::pipeline::AcceptEncoding;

/// Compresses the response as the app's `compression` settings allow when the client accepts it.
pub(crate) async fn http_response(
    result: Result<HandlerResponse, ErrorRecord>,
    accept_encoding: &AcceptEncoding<'_>,
    counters: &RunCounters,
) -> Response<ResponseBody> {
    let resp = match result {
        Ok(data) => {
            let data = match accept_encoding.negotiable() {
                Some((accepted, settings)) => {
                    crate::serve::compression::compress(data, accepted, settings).await
                }
                None => data,
            };
            build_response(data).map(Full::new)
        }
        Err(logged) => logged.into_response(counters),
    };
    full_body(resp)
}

pub(crate) async fn build_main_http_response(
    result: Result<MainReply, ErrorRecord>,
    accept_encoding: &AcceptEncoding<'_>,
    counters: &RunCounters,
) -> Response<ResponseBody> {
    match result {
        Ok(MainReply::Response(data)) => http_response(Ok(data), accept_encoding, counters).await,
        Ok(MainReply::Stream(info)) => build_stream_response(info),
        Err(logged) => http_response(Err(logged), accept_encoding, counters).await,
    }
}

#[inline]
pub(crate) fn build_stream_response(info: StreamReply) -> Response<ResponseBody> {
    use tokio_stream::StreamExt;

    let stream =
        tokio_stream::wrappers::ReceiverStream::new(info.rx).map(|result| result.map(Frame::data));

    let body = StreamBody::new(stream);
    // The channel's error type is `Infallible`: `match e {}` converts it without a panic path.
    let boxed = ResponseBody::boxed(body.map_err(|e| match e {}));

    // The stream's headers were validated when it was made; its own values replace the
    // defaults.
    let mut headers = info.head.headers;
    headers
        .entry(CONTENT_TYPE)
        .or_insert(info.head.content_type);
    headers
        .entry(CACHE_CONTROL)
        .or_insert(HeaderValue::from_static("no-cache"));
    headers
        .entry(CONNECTION)
        .or_insert(HeaderValue::from_static("keep-alive"));
    headers
        .entry(SERVER)
        .or_insert(HeaderValue::from_static(mipyre_http::plan::response::SERVER_HEADER));
    let mut resp = Response::new(boxed);
    *resp.status_mut() = info.head.status;
    *resp.headers_mut() = headers;
    resp
}
