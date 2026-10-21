//! Defines the facts a process-lane request's error carries about the worker that ran it.
//!
//! The lane builds them, but they live here because `exchange` sits below `lane` and its
//! [`HandlerError`](super::HandlerError) and [`UnansweredError`](super::UnansweredError) hold them.

use std::time::Duration;

/// A lane's index counts from 0 among the server's process lanes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LaneIndex(pub(crate) usize);

impl std::fmt::Display for LaneIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A process-lane message failed to decode; the server wrote it, so this is a bug, never client input.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LaneDecodeError {
    #[error("malformed process-lane message: wanted {wanted} bytes at {at}, have {len}")]
    Truncated { wanted: usize, at: usize, len: usize },
    #[error("malformed process-lane message: the slot says its message is {len} bytes; a slot holds {max}")]
    PastSlot { len: usize, max: usize },
    #[error("malformed process-lane message: {0}")]
    NotUtf8(#[source] std::str::Utf8Error),
    #[error("malformed process-lane message: {0}")]
    BadMethod(#[source] hyper::http::method::InvalidMethod),
    #[error("malformed process-lane message: {0}")]
    BadUri(#[source] hyper::http::uri::InvalidUri),
    #[error("malformed process-lane message: {0}")]
    BadHeaderName(#[source] hyper::header::InvalidHeaderName),
    #[error("malformed process-lane message: {0}")]
    BadHeaderValue(#[source] hyper::header::InvalidHeaderValue),
    #[error("malformed process-lane message: {0}")]
    BadStatus(#[source] hyper::http::status::InvalidStatusCode),
    #[error("malformed process-lane message: {field} {tag}")]
    UnknownTag { field: TaggedField, tag: u8 },
    /// The content type travels in its own field, so no writer puts one among the header lines.
    #[error("malformed process-lane message: a content-type {0:?} among the response's header lines")]
    TypeAmongLines(hyper::header::HeaderValue),
    #[error("malformed process-lane message: client request id {0:?}")]
    BadClientRequestId(String),
}

/// Each of these fields starts with a byte that says which of its forms follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaggedField {
    PeerKind,
    RequestIdKind,
    TracebackFlag,
    AnswerKind,
}

impl std::fmt::Display for TaggedField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TaggedField::PeerKind => "peer kind",
            TaggedField::RequestIdKind => "request id kind",
            TaggedField::TracebackFlag => "traceback flag",
            TaggedField::AnswerKind => "answer kind",
        })
    }
}

/// The worker says why the request failed in its own words, kept as the other process sent them.
#[derive(Debug)]
pub(crate) struct WorkerFailure {
    pub(crate) error: String,
    pub(crate) traceback: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkerExit {
    Exited(i32),
    /// `name` is the OS's name for the signal, read where the worker was reaped.
    Signaled { signal: i32, name: String },
    Other(i32),
    Unreported(Duration),
}

impl std::fmt::Display for WorkerExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerExit::Exited(code) => write!(f, "exited with status {code}"),
            WorkerExit::Signaled { signal, name } => write!(f, "killed by signal {signal} ({name})"),
            WorkerExit::Other(status) => write!(f, "wait status {status:#x}"),
            WorkerExit::Unreported(waited) => write!(
                f,
                "its wait status was not reported by the template within {waited:?}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::UnansweredError;

    #[test]
    fn a_worker_exit_says_how_it_ended() {
        assert_eq!(WorkerExit::Exited(3).to_string(), "exited with status 3");
        // `lane::server`'s test pins how the lane reads the OS's name.
        let name = "Killed";
        let killed = WorkerExit::Signaled { signal: 9, name: name.to_owned() };
        assert_eq!(killed.to_string(), format!("killed by signal 9 ({name})"));
        assert_eq!(WorkerExit::Other(0x7f).to_string(), "wait status 0x7f");
        assert_eq!(
            WorkerExit::Unreported(Duration::from_millis(100)).to_string(),
            "its wait status was not reported by the template within 100ms"
        );
    }

    #[test]
    fn a_dead_or_overrun_worker_is_said_with_its_pid_lane_run_time_and_exit() {
        let died = UnansweredError::WorkerDied {
            pid: 42,
            lane: LaneIndex(3),
            ran: Duration::from_millis(5),
            exit: WorkerExit::Exited(1),
        };
        assert_eq!(
            died.to_string(),
            "the worker running the request (pid 42, lane 3) died after 5ms: exited with status 1"
        );
        let overran = UnansweredError::LaneTimedOut {
            pid: 42,
            lane: LaneIndex(3),
            ran: Duration::from_millis(1600),
            timeout: Duration::from_millis(1500),
            exit: WorkerExit::Signaled { signal: 9, name: "Killed".to_owned() },
        };
        let name = "Killed";
        assert_eq!(
            overran.to_string(),
            format!(
                "the handler ran 1.6s, past the process-lane timeout of 1.5s; its worker (pid 42, \
                 lane 3) was killed (killed by signal 9 ({name}))"
            )
        );
    }
}
