//! Lets an asyncio loop await Rust waits that another thread ends, and take items other threads hand
//! it, through doorbells the loop watches; the thread that ends a wait only rings, without the GIL.
//! A `Stream`'s wait for room has a doorbell of its own; a WebSocket's waits share one per loop
//! however many are pending. It does not know what a wait is for or why it ended.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::os::fd::RawFd;
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

/// Holds what other threads hand one loop until the loop takes it all, ringing the loop's doorbell
/// only for the first item after a take, so a burst costs one ring.
pub(crate) struct LoopMailbox<T> {
    state: Mutex<Mail<T>>,
    bell: Doorbell,
}

struct Mail<T> {
    items: Vec<T>,
    /// Is false once the loop stopped taking, after which nothing more is accepted.
    open: bool,
}

impl<T> LoopMailbox<T> {
    pub(crate) fn new() -> io::Result<Self> {
        Ok(LoopMailbox {
            state: Mutex::new(Mail {
                items: Vec::new(),
                open: true,
            }),
            bell: Doorbell::new()?,
        })
    }

    /// Hands `item` to the loop, or returns it when the loop has stopped taking.
    ///
    /// A ring the OS refuses cannot happen while the doorbell holds at most one unread ring; if
    /// it does, the item waits for the next ring.
    pub(crate) fn push(&self, item: T) -> Result<(), T> {
        let first = {
            let mut mail = self.state.lock();
            if !mail.open {
                return Err(item);
            }
            mail.items.push(item);
            mail.items.len() == 1
        };
        if first {
            if let Err(error) = self.bell.ring() {
                tracing::error!(target: "mipyre::server", %error,
                    "a loop's doorbell would not ring; what was handed to it waits for the next ring");
            }
        }
        Ok(())
    }

    /// Takes everything handed so far. The doorbell is quieted first, so an item pushed after the
    /// take rings it again.
    ///
    /// # Errors
    ///
    /// Fails when the doorbell cannot be read, which leaves it readable.
    pub(crate) fn take(&self) -> io::Result<Vec<T>> {
        self.bell.quiet()?;
        Ok(std::mem::take(&mut self.state.lock().items))
    }

    /// Stops accepting, and returns what was handed and never taken.
    pub(crate) fn close(&self) -> Vec<T> {
        let mut mail = self.state.lock();
        mail.open = false;
        std::mem::take(&mut mail.items)
    }

    pub(crate) fn doorbell(&self) -> RawFd {
        self.bell.fd()
    }
}

/// Polls a wait on the loop's thread with the interpreter attached, so its result is made there.
pub(crate) type LoopPoll = Box<dyn FnMut(Python<'_>, &mut Context<'_>) -> Poll<PyResult<Py<PyAny>>> + Send>;

/// Wakes a wait by handing its id to the loop.
struct WaitWaker {
    woken: Arc<LoopMailbox<u64>>,
    id: u64,
}

impl Wake for WaitWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.woken.push(self.id).is_err() {
            tracing::debug!(target: "mipyre::server", "a wait woke after its loop ended");
        }
    }
}

struct LoopWait {
    poll: LoopPoll,
    waker: Waker,
    /// Is the asyncio future the awaiting task holds; a wait whose future is done (cancelled) is
    /// dropped unpolled at its next wake, so what it would have taken stays queued.
    done: Py<PyAny>,
}

struct WaitTable {
    next: u64,
    waits: HashMap<u64, LoopWait>,
}

/// The waits pending on one asyncio loop. It lives on the loop's interpreter and is touched only on
/// the loop's thread.
#[pyclass(frozen, module = "mipyre.engine", name = "_LoopWaits")]
pub struct LoopWaits {
    event_loop: Py<PyAny>,
    /// Is a future already done with `None`, returned for every send that queued at once.
    sent: Py<PyAny>,
    woken: Arc<LoopMailbox<u64>>,
    pending: Mutex<WaitTable>,
}

#[pymethods]
impl LoopWaits {
    /// Makes the waits of `event_loop`, which must watch `doorbell` and call `run` when it is
    /// readable.
    #[new]
    fn py_new(event_loop: Bound<'_, PyAny>) -> PyResult<Self> {
        let py = event_loop.py();
        let sent = event_loop.call_method0(intern!(py, "create_future"))?;
        sent.call_method1(intern!(py, "set_result"), (py.None(),))?;
        Ok(LoopWaits {
            event_loop: event_loop.unbind(),
            sent: sent.unbind(),
            woken: Arc::new(LoopMailbox::new()?),
            pending: Mutex::new(WaitTable {
                next: 0,
                waits: HashMap::new(),
            }),
        })
    }

    #[getter]
    fn doorbell(&self) -> RawFd {
        self.woken.doorbell()
    }

    /// Polls every wait woken since the last run and settles the futures of those that ended.
    fn run(&self, py: Python<'_>) -> PyResult<()> {
        for id in self.woken.take()? {
            let Some(mut wait) = self.pending.lock().waits.remove(&id) else {
                continue;
            };
            if is_done(wait.done.bind(py))? {
                continue;
            }
            match (wait.poll)(py, &mut Context::from_waker(&wait.waker)) {
                Poll::Pending => {
                    self.pending.lock().waits.insert(id, wait);
                }
                Poll::Ready(ended) => settle(wait.done.bind(py), ended)?,
            }
        }
        Ok(())
    }
}

impl LoopWaits {
    /// Returns a future already done with `None`; awaiting a done future does not change it.
    pub(crate) fn sent<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.sent.bind(py).clone()
    }

    /// Returns a future on the loop that is done when `poll` is ready, with its result. A wait
    /// ready at once still costs one future, already done.
    /// The id is the wait's while it is pending.
    pub(crate) fn start<'py>(&self, py: Python<'py>, mut poll: LoopPoll) -> PyResult<(Bound<'py, PyAny>, Option<u64>)> {
        let done = self.event_loop.bind(py).call_method0(intern!(py, "create_future"))?;
        let id = {
            let mut pending = self.pending.lock();
            let id = pending.next;
            pending.next += 1;
            id
        };
        let waker = Waker::from(Arc::new(WaitWaker {
            woken: Arc::clone(&self.woken),
            id,
        }));
        match poll(py, &mut Context::from_waker(&waker)) {
            Poll::Ready(ended) => {
                settle(&done, ended)?;
                Ok((done, None))
            }
            Poll::Pending => {
                let wait = LoopWait {
                    poll,
                    waker,
                    done: done.clone().unbind(),
                };
                self.pending.lock().waits.insert(id, wait);
                Ok((done, Some(id)))
            }
        }
    }

    /// Returns whether wait `id` is still awaited; a cancelled one is dropped here.
    pub(crate) fn is_awaited(&self, py: Python<'_>, id: u64) -> PyResult<bool> {
        let mut pending = self.pending.lock();
        let Some(wait) = pending.waits.get(&id) else {
            return Ok(false);
        };
        if is_done(wait.done.bind(py))? {
            pending.waits.remove(&id);
            return Ok(false);
        }
        Ok(true)
    }
}

fn settle(done: &Bound<'_, PyAny>, ended: PyResult<Py<PyAny>>) -> PyResult<()> {
    let py = done.py();
    match ended {
        Ok(value) => done.call_method1(intern!(py, "set_result"), (value,))?,
        Err(end) => done.call_method1(intern!(py, "set_exception"), (end.into_value(py),))?,
    };
    Ok(())
}
