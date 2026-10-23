//! Lets a `body=BodyStream` route read `req.stream`, an iterator of the body's chunks as they arrive.
//!
//! The channel from the feeder is bounded, so a slow handler stops the feeder polling the body,
//! which closes the client's TCP window. The body keeps the size and time limits a buffered body
//! has, not counting the time it waits for the handler to read.
//!
//! A rejected body raises [`PyBodyReadError`], and a handler that lets it through gets the 413, 408
//! or 400 a buffered body would, logged as a rejection rather than a server error. A stream that
//! failed raises the same error on every later read.

use crate::sync::poisoning::{Mutex, MutexGuard};

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyStopIteration};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use mipyre_http::io::body::{BodyMessage, BodyReadError, BodyReceiver};

/// Python's `BodyReadError`, named as the Rust [`BodyReadError`] it carries.
mod py_error {
    pyo3::create_exception!(
        mipyre.engine,
        BodyReadError,
        pyo3::exceptions::PyOSError,
        "A streamed request body was rejected, as a buffered one would be: larger than \
         max_body_size, too slow, or a failed read. An OSError, so `except OSError` still \
         catches it; left uncaught, the request gets the buffered body's 413 / 408 / 400."
    );
}
pub(crate) use py_error::BodyReadError as PyBodyReadError;

/// The server hangs this on a `BodyReadError` it raises, as `_rejection`, so the dispatcher can
/// answer the rejection as what it is.
#[pyclass(frozen, name = "_BodyRejection", module = "mipyre.engine")]
struct Rejection(BodyReadError);

fn body_rejected(py: Python<'_>, reject: BodyReadError) -> PyErr {
    let err = PyBodyReadError::new_err(reject.to_string());
    let carried = Bound::new(py, Rejection(reject)).and_then(|r| err.value(py).setattr(intern!(py, "_rejection"), r));
    match carried {
        Ok(()) => err,
        Err(failed) => {
            failed.set_cause(py, Some(err));
            failed
        }
    }
}

/// Returns `None` for a handler's own `raise BodyReadError(...)`, which carries no rejection.
pub(crate) fn rejection_of(py: Python<'_>, err: &PyErr) -> Option<BodyReadError> {
    if !err.is_instance_of::<PyBodyReadError>(py) {
        return None;
    }
    let carried = err.value(py).getattr(intern!(py, "_rejection")).ok()?;
    let rejection = carried.cast::<Rejection>().ok()?;
    Some(rejection.get().0.clone())
}

enum StreamState {
    Open(BodyReceiver),
    Finished,
    /// Every later read raises this error again.
    Failed(BodyStreamError),
}

#[derive(Clone)]
enum BodyStreamError {
    Rejected(BodyReadError),
    /// The handler did not read the body within this request budget.
    Unread(std::time::Duration),
    /// The feeder was dropped without a last message, so reading on would silently truncate the body.
    CutOff,
}

impl BodyStreamError {
    fn to_py(&self, py: Python<'_>) -> PyErr {
        match self {
            BodyStreamError::Rejected(reject) => body_rejected(py, reject.clone()),
            BodyStreamError::Unread(budget) => PyOSError::new_err(format!(
                "the handler did not read its streamed request body within the request budget \
                 ({budget:?}); the server stopped reading the body"
            )),
            BodyStreamError::CutOff => PyOSError::new_err(
                "the request body stream ended before the body did: the server stopped \
                 feeding it (the request was abandoned)",
            ),
        }
    }
}

/// The state stays locked across each blocking receive, so concurrent readers take chunks in
/// turn instead of one seeing a spurious end.
///
/// The lock is only taken while detached: a reader waiting for it while attached would hold the
/// GIL that the holder needs back once its chunk arrives, and both would hang.
#[pyclass(name = "BodyStream", module = "mipyre.engine")]
pub(crate) struct BodyStream {
    state: Mutex<StreamState>,
}

impl BodyStream {
    pub(crate) fn new(rx: BodyReceiver) -> Self {
        BodyStream {
            state: Mutex::new(StreamState::Open(rx)),
        }
    }

    /// Must be called detached (see [`BodyStream`]).
    fn lock(&self) -> MutexGuard<'_, StreamState> {
        // A panic under the lock left the state whole, since each transition is one assignment,
        // and unwinding into Python must not happen.
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Waits for the next chunk, returning `None` at the body's end; must run detached.
    fn next_message(&self) -> Result<Option<bytes::Bytes>, BodyStreamError> {
        let mut state = self.lock();
        let rx = match &mut *state {
            StreamState::Open(rx) => rx,
            StreamState::Finished => return Ok(None),
            StreamState::Failed(failure) => return Err(failure.clone()),
        };
        let failure = match rx.blocking_recv() {
            Some(BodyMessage::Chunk(chunk)) => return Ok(Some(chunk)),
            Some(BodyMessage::End) => {
                *state = StreamState::Finished;
                return Ok(None);
            }
            Some(BodyMessage::Rejected(reject)) => BodyStreamError::Rejected(reject),
            Some(BodyMessage::Unread(budget)) => BodyStreamError::Unread(budget),
            None => BodyStreamError::CutOff,
        };
        *state = StreamState::Failed(failure.clone());
        Err(failure)
    }

    /// Takes the rest of the body and counts its bytes; must run detached.
    fn drain(&self) -> Result<u64, DrainError> {
        let mut state = self.lock();
        let rx = match &mut *state {
            StreamState::Open(rx) => rx,
            StreamState::Finished => return Err(DrainError::AlreadyRead),
            StreamState::Failed(failure) => return Err(DrainError::StreamFailed(failure.clone())),
        };
        let mut total: u64 = 0;
        let failure = loop {
            match rx.blocking_recv() {
                Some(BodyMessage::Chunk(chunk)) => total += chunk.len() as u64,
                Some(BodyMessage::End) => {
                    *state = StreamState::Finished;
                    return Ok(total);
                }
                Some(BodyMessage::Rejected(reject)) => break BodyStreamError::Rejected(reject),
                Some(BodyMessage::Unread(budget)) => break BodyStreamError::Unread(budget),
                None => break BodyStreamError::CutOff,
            }
        };
        *state = StreamState::Failed(failure.clone());
        Err(DrainError::StreamFailed(failure))
    }
}

enum DrainError {
    AlreadyRead,
    StreamFailed(BodyStreamError),
}

#[pymethods]
impl BodyStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Blocks until the next chunk arrives; raises `StopIteration` at the end and [`PyBodyReadError`]
    /// for a rejected body, again on every later read.
    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        match py.detach(|| self.next_message()) {
            Ok(Some(chunk)) => Ok(PyBytes::new(py, &chunk).unbind()),
            Ok(None) => Err(PyStopIteration::new_err(py.None())),
            Err(failure) => Err(failure.to_py(py)),
        }
    }

    /// Reads whole chunks until it has at least `n` bytes, or the whole body when `n` is `None`;
    /// chunks are never split, so the result may be longer than `n`, and it is `b""` at the end.
    #[pyo3(signature = (n=None))]
    fn read(&self, py: Python<'_>, n: Option<usize>) -> PyResult<Py<PyBytes>> {
        let mut buf = Vec::<u8>::new();
        loop {
            // The check comes first so `read(0)` consumes nothing, as `file.read(0)` does.
            if let Some(limit) = n {
                if buf.len() >= limit {
                    break;
                }
            }
            match self.__next__(py) {
                Ok(chunk) => {
                    buf.extend_from_slice(chunk.bind(py).as_bytes());
                }
                Err(e) if e.is_instance_of::<PyStopIteration>(py) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(PyBytes::new(py, &buf).unbind())
    }

    /// Consumes the rest of the stream and returns its byte count, without the GIL round trip and
    /// `bytes` object per chunk that iterating in Python pays.
    ///
    /// Raises [`PyBodyReadError`] for a rejected body, and `RuntimeError` on a stream already read
    /// to its end, where `0` would read as an empty body.
    fn drain_count(&self, py: Python<'_>) -> PyResult<u64> {
        match py.detach(|| self.drain()) {
            Ok(total) => Ok(total),
            Err(DrainError::AlreadyRead) => Err(PyRuntimeError::new_err(
                "the request body stream was already read to its end",
            )),
            Err(DrainError::StreamFailed(failure)) => Err(failure.to_py(py)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keeps an unread body out of `BodyReadError`, which the dispatcher would answer as the
    /// client's fault with a 408.
    #[test]
    fn an_unread_body_reads_as_an_oserror_naming_the_handler_not_a_rejection() {
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let budget = std::time::Duration::from_secs(7);
            let unread = || {
                let (tx, rx) = mipyre_http::io::body::body_channel();
                tx.try_send(BodyMessage::Unread(budget))
                    .unwrap_or_else(|_| panic!("an empty channel has room"));
                BodyStream::new(rx)
            };
            let assert_unread = |err: PyErr| {
                assert!(err.is_instance_of::<PyOSError>(py), "{err}");
                assert!(!err.is_instance_of::<PyBodyReadError>(py), "{err}");
                assert!(rejection_of(py, &err).is_none(), "{err}");
                let text = err.value(py).to_string();
                assert!(
                    text.contains("the handler did not read") && text.contains("7s"),
                    "{text}"
                );
            };

            let stream = unread();
            let first = stream.__next__(py).expect_err("an unread body is an error");
            assert_unread(first);
            let again = stream.read(py, None).expect_err("and stays one");
            assert_unread(again);

            let drained = unread().drain_count(py).expect_err("an unread body is an error");
            assert_unread(drained);
        });
    }

    /// Guards two past bugs: a second reader took an empty slot for the end and cut the body
    /// short, and a reader waiting for the lock while holding the GIL deadlocked.
    #[test]
    fn two_threads_reading_one_stream_split_it_without_cutting_it_short() {
        const CHUNKS: usize = 200;
        Python::initialize();
        let (tx, rx) = mipyre_http::io::body::body_channel();
        let stream = std::sync::Arc::new(BodyStream::new(rx));
        let feeder = std::thread::spawn(move || {
            for i in 0..CHUNKS {
                let chunk = bytes::Bytes::from(i.to_string());
                tx.blocking_send(BodyMessage::Chunk(chunk))
                    .unwrap_or_else(|_| panic!("the stream is still open"));
            }
            tx.blocking_send(BodyMessage::End)
                .unwrap_or_else(|_| panic!("the stream is still open"));
        });
        let reader = |stream: std::sync::Arc<BodyStream>| {
            std::thread::spawn(move || {
                mipyre_subinterp::attach::with_main(|py| {
                    let mut got = Vec::new();
                    loop {
                        match stream.__next__(py) {
                            Ok(chunk) => got.push(
                                std::str::from_utf8(chunk.bind(py).as_bytes())
                                    .expect("the feeder sends decimal text")
                                    .parse::<usize>()
                                    .expect("decimal"),
                            ),
                            Err(e) if e.is_instance_of::<PyStopIteration>(py) => break,
                            Err(e) => panic!("reading the stream raised {e}"),
                        }
                    }
                    let again = stream.__next__(py).expect_err("still at the end");
                    assert!(again.is_instance_of::<PyStopIteration>(py), "{again}");
                    got
                })
            })
        };
        let (a, b) = (reader(stream.clone()), reader(stream));
        feeder.join().expect("the feeder does not panic");
        let mut all = a.join().expect("reader a does not panic");
        all.extend(b.join().expect("reader b does not panic"));
        all.sort_unstable();
        assert_eq!(all, (0..CHUNKS).collect::<Vec<_>>());
    }

    /// Counts the per-chunk cost of a streamed body with `perf stat -e instructions:u`; `BENCH_N`
    /// sets the calls (default 1000), and the `BENCH_N=0` run is the baseline to subtract.
    #[test]
    fn next_on_a_waiting_chunk_counted() {
        let n: usize = std::env::var("BENCH_N")
            .ok()
            .and_then(|n| n.parse().ok())
            .unwrap_or(1000);
        Python::initialize();
        mipyre_subinterp::attach::with_main(|py| {
            let (tx, rx) = mipyre_http::io::body::body_channel();
            let stream = BodyStream::new(rx);
            let chunk = bytes::Bytes::from_static(b"chunk");
            for _ in 0..n {
                tx.try_send(BodyMessage::Chunk(chunk.clone()))
                    .unwrap_or_else(|_| panic!("one chunk at a time has room"));
                let got = std::hint::black_box(stream.__next__(py)).expect("the waiting chunk");
                assert_eq!(got.bind(py).as_bytes(), b"chunk");
            }
        });
    }
}
