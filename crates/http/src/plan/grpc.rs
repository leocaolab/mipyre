//! Names the gRPC status codes a call ends with (<https://grpc.github.io/grpc/core/md_doc_statuscodes.html>).

use http::header::HeaderValue;
use http::StatusCode;

/// The content type of every gRPC response; a request's may add a suffix (`application/grpc+proto`).
pub const APPLICATION_GRPC: HeaderValue = HeaderValue::from_static("application/grpc");

/// One of the 17 codes the `grpc-status` trailer carries, numbered as the wire numbers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum GrpcCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl GrpcCode {
    const ALL: [GrpcCode; 17] = [
        GrpcCode::Ok,
        GrpcCode::Cancelled,
        GrpcCode::Unknown,
        GrpcCode::InvalidArgument,
        GrpcCode::DeadlineExceeded,
        GrpcCode::NotFound,
        GrpcCode::AlreadyExists,
        GrpcCode::PermissionDenied,
        GrpcCode::ResourceExhausted,
        GrpcCode::FailedPrecondition,
        GrpcCode::Aborted,
        GrpcCode::OutOfRange,
        GrpcCode::Unimplemented,
        GrpcCode::Internal,
        GrpcCode::Unavailable,
        GrpcCode::DataLoss,
        GrpcCode::Unauthenticated,
    ];

    /// Returns the code numbered `number`, or `None` past 16.
    pub fn of(number: i64) -> Option<GrpcCode> {
        let index = usize::try_from(number).ok()?;
        Self::ALL.get(index).copied()
    }

    pub fn number(self) -> u8 {
        self as u8
    }

    /// Returns the `grpc-status` value, the code's number in decimal.
    pub fn header_value(self) -> HeaderValue {
        HeaderValue::from_static(match self {
            GrpcCode::Ok => "0",
            GrpcCode::Cancelled => "1",
            GrpcCode::Unknown => "2",
            GrpcCode::InvalidArgument => "3",
            GrpcCode::DeadlineExceeded => "4",
            GrpcCode::NotFound => "5",
            GrpcCode::AlreadyExists => "6",
            GrpcCode::PermissionDenied => "7",
            GrpcCode::ResourceExhausted => "8",
            GrpcCode::FailedPrecondition => "9",
            GrpcCode::Aborted => "10",
            GrpcCode::OutOfRange => "11",
            GrpcCode::Unimplemented => "12",
            GrpcCode::Internal => "13",
            GrpcCode::Unavailable => "14",
            GrpcCode::DataLoss => "15",
            GrpcCode::Unauthenticated => "16",
        })
    }

    /// Returns the name the gRPC docs and grpcio's `StatusCode` give it.
    pub fn name(self) -> &'static str {
        match self {
            GrpcCode::Ok => "OK",
            GrpcCode::Cancelled => "CANCELLED",
            GrpcCode::Unknown => "UNKNOWN",
            GrpcCode::InvalidArgument => "INVALID_ARGUMENT",
            GrpcCode::DeadlineExceeded => "DEADLINE_EXCEEDED",
            GrpcCode::NotFound => "NOT_FOUND",
            GrpcCode::AlreadyExists => "ALREADY_EXISTS",
            GrpcCode::PermissionDenied => "PERMISSION_DENIED",
            GrpcCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
            GrpcCode::FailedPrecondition => "FAILED_PRECONDITION",
            GrpcCode::Aborted => "ABORTED",
            GrpcCode::OutOfRange => "OUT_OF_RANGE",
            GrpcCode::Unimplemented => "UNIMPLEMENTED",
            GrpcCode::Internal => "INTERNAL",
            GrpcCode::Unavailable => "UNAVAILABLE",
            GrpcCode::DataLoss => "DATA_LOSS",
            GrpcCode::Unauthenticated => "UNAUTHENTICATED",
        }
    }

    /// Returns the code gRPC gives an HTTP status that is not its own
    /// (<https://grpc.github.io/grpc/core/md_doc_http-grpc-status-mapping.html>), for a gRPC method
    /// whose handler raised `HTTPException`.
    pub fn of_http(status: StatusCode) -> GrpcCode {
        match status.as_u16() {
            400 => GrpcCode::Internal,
            401 => GrpcCode::Unauthenticated,
            403 => GrpcCode::PermissionDenied,
            404 => GrpcCode::Unimplemented,
            429 | 502 | 503 | 504 => GrpcCode::Unavailable,
            _ => GrpcCode::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_numbers_run_from_0_to_16_and_stop() {
        assert_eq!(GrpcCode::of(-1), None);
        assert_eq!(GrpcCode::of(0), Some(GrpcCode::Ok));
        assert_eq!(GrpcCode::of(16), Some(GrpcCode::Unauthenticated));
        assert_eq!(GrpcCode::of(17), None);
        for (number, code) in GrpcCode::ALL.iter().enumerate() {
            assert_eq!(usize::from(code.number()), number, "{}", code.name());
            assert_eq!(code.header_value(), number.to_string().as_str(), "{}", code.name());
        }
    }

    #[test]
    fn an_http_status_takes_grpcs_table() {
        let code = |status: u16| GrpcCode::of_http(StatusCode::from_u16(status).unwrap());
        assert_eq!(code(400), GrpcCode::Internal);
        assert_eq!(code(401), GrpcCode::Unauthenticated);
        assert_eq!(code(403), GrpcCode::PermissionDenied);
        assert_eq!(code(404), GrpcCode::Unimplemented);
        assert_eq!(code(429), GrpcCode::Unavailable);
        assert_eq!(code(504), GrpcCode::Unavailable);
        assert_eq!(code(405), GrpcCode::Unknown);
        assert_eq!(code(500), GrpcCode::Unknown);
    }
}
