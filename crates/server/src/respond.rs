//! Turns a handler's answer into the hyper response the client gets, compressed or streamed as
//! SSE; every serving path ends here.

use http_body_util::{Full, StreamBody};
use hyper::body::Frame;
use hyper::header::{HeaderValue, CACHE_CONTROL, CONNECTION, CONTENT_TYPE, SERVER};
use hyper::Response;

use crate::SiteCounters;
use mipyre_http::io::body::{full_body, ResponseBody};
use mipyre_http::plan::response::build_response;
use mipyre_runtime::exchange::{ErrorRecord, HandlerReply, StreamReply};

use super::error_response::render;
use super::pipeline::AcceptEncoding;

/// Compresses a whole response as the app's `compression` settings allow when the client accepts
/// it; a stream is sent as it comes, uncompressed.
///
/// It takes any answer that becomes a `HandlerReply` so that a backend that never streams passes
/// its `HandlerResponse` as it is: lifted before the call, the answer would be copied into this
/// future in the wider shape on every request.
pub(crate) async fn http_response(
    result: Result<impl Into<HandlerReply>, ErrorRecord>,
    accept_encoding: &AcceptEncoding<'_>,
    counters: &SiteCounters,
) -> Response<ResponseBody> {
    let resp = match result.map(Into::into) {
        Ok(HandlerReply::Response(data)) => {
            // Rare: a branch on a field the answer already carries.
            if let Some(errors) = data.validation_errors() {
                count_validation_failure(counters, errors);
            }
            let data = match accept_encoding.negotiable() {
                Some((accepted, settings)) => crate::compression::compress(data, accepted, settings).await,
                None => data,
            };
            build_response(data).map(Full::new)
        }
        Ok(HandlerReply::Stream(stream)) => return stream_response(stream),
        Err(logged) => render(logged, counters),
    };
    full_body(resp)
}

/// Counts a request that failed validation in the run that served it, and logs it at most once a
/// minute for that run, saying how many such lines were held back. A client sends these at will, hence the throttle.
#[cold]
#[inline(never)]
fn count_validation_failure(counters: &SiteCounters, errors: std::num::NonZeroU32) {
    if let Some(held) = counters.run.validation_failed(mipyre_util::clock::now()) {
        tracing::warn!(
            target: "mipyre::validation",
            "a request failed validation with {errors} error(s) ({held} more since the last line)"
        );
    }
}

#[cold]
#[inline(never)]
fn stream_response(info: StreamReply) -> Response<ResponseBody> {
    use tokio_stream::StreamExt;

    let stream = tokio_stream::wrappers::ReceiverStream::new(info.rx).map(|result| result.map(Frame::data));

    let body = StreamBody::new(stream);
    let boxed = ResponseBody::boxed(body);

    // The stream's headers were validated when it was made; its own values replace the
    // defaults.
    let mut headers = info.head.headers;
    headers.entry(CONTENT_TYPE).or_insert(info.head.content_type);
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
