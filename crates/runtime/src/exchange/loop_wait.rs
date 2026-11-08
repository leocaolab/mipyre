//! Lets an asyncio loop await a Rust wait that another thread ends, through a doorbell the loop
//! watches; the thread that ends the wait only rings, without the GIL. A `Stream`'s wait for room
//! has a doorbell of its own. It does not know what a wait is for or why it ended.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use pyo3::intern;
use pyo3::prelude::*;

use crate::sync::{Doorbell, Mutex};

type Room = Pin<Box<dyn Future<Output = PyResult<()>> + Send>>;

/// Returns a future on the running asyncio loop that is done when `room` ends, or fails with its
/// error; cancelling it stops the wait. Each call opens a doorbell fd (`OSError` when none can be
/// had) for the loop to watch until the future is done.
pub(super) fn on_running_loop<'py>(
    py: Python<'py>,
    room: impl Future<Output = PyResult<()>> + Send + 'static,
) -> PyResult<Bound<'py, PyAny>> {
    let event_loop = py
        .import(intern!(py, "asyncio"))?
        .call_method0(intern!(py, "get_running_loop"))?;
    let done = event_loop.call_method0(intern!(py, "create_future"))?;
    let wait = Bound::new(
        py,
        LoopRoomWait {
            room: Mutex::new(Some(Box::pin(room))),
            bell: Arc::new(BellWaker(Doorbell::new()?)),
        },
    )?;
    wait.get().poll(&done)?;
    if !is_done(&done)? {
        let fd = wait.get().bell.0.fd();
        event_loop.call_method1(
            intern!(py, "add_reader"),
            (fd, wait.getattr(intern!(py, "ring"))?, &done),
        )?;
        done.call_method1(
            intern!(py, "add_done_callback"),
            (wait.getattr(intern!(py, "finish"))?,),
        )?;
    }
    Ok(done)
}

/// A waker that rings a doorbell, which needs no GIL, so the thread that frees room (a connection's)
/// never waits for Python.
struct BellWaker(Doorbell);

impl Wake for BellWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Err(error) = self.0.ring() {
            tracing::warn!(target: "mipyre::server", %error, "a doorbell for room would not ring; its producer waits on");
        }
    }
}

/// One wait for room, polled on the loop's thread each time its doorbell rings. It holds neither
/// the loop nor the asyncio future, which come back with each callback, so no reference cycle
/// keeps it alive.
#[pyclass(frozen, module = "mipyre.engine")]
struct LoopRoomWait {
    /// Is `None` once the wait ended or its future is done, which lets go of what the wait holds.
    room: Mutex<Option<Room>>,
    bell: Arc<BellWaker>,
}

#[pymethods]
impl LoopRoomWait {
    /// Quiets the doorbell, then polls the wait.
    fn ring(&self, done: &Bound<'_, PyAny>) -> PyResult<()> {
        self.bell.0.quiet()?;
        self.poll(done)
    }

    /// Stops the loop watching the doorbell and lets go of the wait, once `done` is done or
    /// cancelled.
    fn finish(&self, done: &Bound<'_, PyAny>) -> PyResult<()> {
        drop(self.room.lock().take());
        let py = done.py();
        done.call_method0(intern!(py, "get_loop"))?
            .call_method1(intern!(py, "remove_reader"), (self.bell.0.fd(),))?;
        Ok(())
    }
}

impl LoopRoomWait {
    /// Polls the wait, registering the doorbell to be rung, and settles `done` once it ended.
    fn poll(&self, done: &Bound<'_, PyAny>) -> PyResult<()> {
        let waker = Waker::from(Arc::clone(&self.bell));
        let ended = {
            let mut room = self.room.lock();
            let Some(wait) = room.as_mut() else {
                return Ok(());
            };
            let Poll::Ready(ended) = wait.as_mut().poll(&mut Context::from_waker(&waker)) else {
                return Ok(());
            };
            *room = None;
            ended
        };
        if is_done(done)? {
            return Ok(());
        }
        let py = done.py();
        match ended {
            Ok(()) => done.call_method1(intern!(py, "set_result"), (py.None(),))?,
            Err(end) => done.call_method1(intern!(py, "set_exception"), (end.into_value(py),))?,
        };
        Ok(())
    }
}

fn is_done(future: &Bound<'_, PyAny>) -> PyResult<bool> {
    future.call_method0(intern!(future.py(), "done"))?.is_truthy()
}
