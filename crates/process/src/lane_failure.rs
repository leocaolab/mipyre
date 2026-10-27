//! Defines the facts a process-lane request's error carries: the worker that ran it, and why the lanes gave it up.
//!
//! `exchange`'s `HandlerError` and `UnansweredError` hold them, so they sit below both the lanes and `exchange`, a
//! layer of their own. They say nothing of status codes or log levels.

use std::sync::Arc;
use std::time::Duration;

use mipyre_shm::channel::PeerFault;

/// A lane's index counts from 0 among the server's process lanes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LaneIndex(pub usize);

impl std::fmt::Display for LaneIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A process-lane message failed to decode: the other end of the lane wrote bytes no encoder writes. It is never
/// client input.
#[derive(Debug, thiserror::Error)]
pub enum LaneDecodeError {
    #[error("malformed process-lane message: wanted {wanted} bytes at {at}, have {len}")]
    Truncated { wanted: usize, at: usize, len: usize },
    /// A count of `count` entries at `at`, each taking at least `least` bytes, which the rest of the message is too
    /// short to hold.
    #[error("malformed process-lane message: {count} entries of at least {least} bytes each at {at}, have {len}")]
    CountPastEnd {
        count: usize,
        least: usize,
        at: usize,
        len: usize,
    },
    #[error("malformed process-lane message: {count} header lines, past the {max} a header map takes room for")]
    TooManyLines { count: usize, max: usize },
    /// The worker read its request's length past its slot.
    #[error("malformed process-lane message: {0}")]
    PastSlot(#[source] mipyre_shm::plan::slot_len::SlotLengthError),
    #[error("malformed process-lane message: {0}")]
    NotUtf8(#[source] std::str::Utf8Error),
    #[error("malformed process-lane message: {0}")]
    BadMethod(#[source] http::method::InvalidMethod),
    #[error("malformed process-lane message: {0}")]
    BadUri(#[source] http::uri::InvalidUri),
    #[error("malformed process-lane message: {0}")]
    BadHeaderName(#[source] http::header::InvalidHeaderName),
    #[error("malformed process-lane message: {0}")]
    BadHeaderValue(#[source] http::header::InvalidHeaderValue),
    #[error("malformed process-lane message: {0}")]
    BadStatus(#[source] http::status::InvalidStatusCode),
    #[error("malformed process-lane message: {field} {tag}")]
    UnknownTag { field: TaggedField, tag: u8 },
    /// The content type travels in its own field, so no writer puts one among the header lines.
    #[error("malformed process-lane message: a content-type {0:?} among the response's header lines")]
    TypeAmongLines(http::header::HeaderValue),
    #[error("malformed process-lane message: client request id {0:?}")]
    BadClientRequestId(String),
    #[error("malformed process-lane message: path param {name:?} at {value:?} is not in the path {path:?}")]
    ParamPastPath {
        name: String,
        value: std::ops::Range<usize>,
        path: String,
    },
}

/// Each of these fields starts with a byte that says which of its forms follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaggedField {
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
pub struct WorkerFailure {
    pub error: String,
    pub traceback: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerExit {
    Exited(i32),
    /// `name` is the OS's name for the signal, read where the worker was reaped.
    Signaled {
        signal: i32,
        name: String,
    },
    Other(i32),
    Unreported(Duration),
}

impl std::fmt::Display for WorkerExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerExit::Exited(code) => write!(f, "exited with status {code}"),
            WorkerExit::Signaled { signal, name } => write!(f, "killed by signal {signal} ({name})"),
            WorkerExit::Other(status) => write!(f, "wait status {status:#x}"),
            WorkerExit::Unreported(waited) => {
                write!(f, "its wait status was not reported by the template within {waited:?}")
            }
        }
    }
}

/// The lanes gave the request up without an answer from its handler.
#[derive(Debug, thiserror::Error)]
pub enum LaneUnansweredError {
    #[error("no capacity for the request: process lane queue")]
    Full,
    #[error("process lanes: the server is shutting down")]
    ShuttingDown,
    /// No host is left serving; `cause` is why the first one stopped.
    #[error("process lanes: its workers stopped serving: {cause}")]
    WorkersDied { cause: Arc<str> },
    /// This concerns only the host that took the request, unlike [`LaneUnansweredError::WorkersDied`]: another host can
    /// still serve, and the run goes on; `cause` is why the first lane broke.
    #[error("process lanes: no lane of the host that took the request is usable: {cause}")]
    NoUsableLane { cause: Arc<str> },
    #[error("the process lane dropped the request without an answer")]
    Dropped,
    /// The request is answered 502. `ran` is how long the host had seen the request running, `None` when the worker
    /// died before the host looked.
    #[error(
        "the worker running the request (pid {pid}, lane {lane}) died{}: {exit}",
        ran.map(|ran| format!(" after {ran:?}")).unwrap_or_default()
    )]
    WorkerDied {
        pid: i32,
        lane: LaneIndex,
        ran: Option<Duration>,
        exit: WorkerExit,
    },
    /// The request is answered 502 and is not run on another worker: it waited in the lane's ring for a worker that
    /// died before taking it, and that worker could have rewritten its bytes in the shared memory.
    #[error(
        "the worker of process lane {lane} (pid {pid}) died before it took the request ({exit}); the request is not \
         run on another worker, since the dead one could have changed it"
    )]
    WorkerDiedBeforeTaking {
        pid: i32,
        lane: LaneIndex,
        exit: WorkerExit,
    },
    /// The request is answered 504.
    #[error(
        "the handler ran {ran:?}, past the process-lane timeout of {timeout:?}; its worker \
         (pid {pid}, lane {lane}) was killed ({exit})"
    )]
    LaneTimedOut {
        pid: i32,
        lane: LaneIndex,
        ran: Duration,
        timeout: Duration,
        exit: WorkerExit,
    },
    /// The request is answered 502 and is not run again: its worker wrote a counter it cannot
    /// have written honestly, so whether it ran the request is unknown. The worker is killed.
    #[error(
        "the worker of process lane {lane} (pid {pid}) corrupted the lane: {}",
        fault_said(fault)
    )]
    PeerCorrupted {
        pid: i32,
        lane: LaneIndex,
        fault: PeerFault,
    },
    /// The request is answered 503 with `Retry-After` from `retry_in`; the other lanes and hosts
    /// keep serving, and a lane that answers ends the wait for good.
    #[error(
        "process lane {lane} is waiting {retry_in:?} before it forks a new worker: its last \
         {crashes} workers died without answering a request, the last one {exit}"
    )]
    LaneCrashLooping {
        lane: LaneIndex,
        crashes: u32,
        exit: WorkerExit,
        retry_in: Duration,
    },
    /// The request is answered 502, with the lane's own words for why it has no worker.
    #[error("no worker for lane {lane}: {cause}")]
    LaneLost { lane: LaneIndex, cause: Arc<str> },
}

/// Says what a worker wrote that no correct worker writes: the error the lane's end measured it with.
pub(super) fn fault_said(fault: &PeerFault) -> &dyn std::fmt::Display {
    match fault {
        PeerFault::Counter(e) => e,
        PeerFault::Length(e) => e,
        PeerFault::Wakes(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_worker_exit_says_how_it_ended() {
        assert_eq!(WorkerExit::Exited(3).to_string(), "exited with status 3");
        // `server`'s test pins how the lane reads the OS's name.
        let name = "Killed";
        let killed = WorkerExit::Signaled {
            signal: 9,
            name: name.to_owned(),
        };
        assert_eq!(killed.to_string(), format!("killed by signal 9 ({name})"));
        assert_eq!(WorkerExit::Other(0x7f).to_string(), "wait status 0x7f");
        assert_eq!(
            WorkerExit::Unreported(Duration::from_millis(100)).to_string(),
            "its wait status was not reported by the template within 100ms"
        );
    }

    #[test]
    fn a_dead_or_overrun_worker_is_said_with_its_pid_lane_run_time_and_exit() {
        let died = LaneUnansweredError::WorkerDied {
            pid: 42,
            lane: LaneIndex(3),
            ran: Some(Duration::from_millis(5)),
            exit: WorkerExit::Exited(1),
        };
        assert_eq!(
            died.to_string(),
            "the worker running the request (pid 42, lane 3) died after 5ms: exited with status 1"
        );
        let unseen = LaneUnansweredError::WorkerDied {
            pid: 42,
            lane: LaneIndex(3),
            ran: None,
            exit: WorkerExit::Exited(1),
        };
        assert_eq!(
            unseen.to_string(),
            "the worker running the request (pid 42, lane 3) died: exited with status 1"
        );
        let untaken = LaneUnansweredError::WorkerDiedBeforeTaking {
            pid: 42,
            lane: LaneIndex(3),
            exit: WorkerExit::Exited(1),
        };
        assert_eq!(
            untaken.to_string(),
            "the worker of process lane 3 (pid 42) died before it took the request (exited with status 1); the request \
             is not run on another worker, since the dead one could have changed it"
        );
        let overran = LaneUnansweredError::LaneTimedOut {
            pid: 42,
            lane: LaneIndex(3),
            ran: Duration::from_millis(1600),
            timeout: Duration::from_millis(1500),
            exit: WorkerExit::Signaled {
                signal: 9,
                name: "Killed".to_owned(),
            },
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
