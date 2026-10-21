//! Runs TPC's `runtime="main"` routes on the main interpreter, for C extensions that cannot load
//! in a sub-interpreter.
//!
//! There are several bridge threads because a handler doing I/O releases the GIL but still blocks
//! its thread; with one thread, GIL work would queue behind that I/O while main sits idle.
//!
//! The shared channel is deliberately small: a saturated bridge answers 503 at once instead of
//! queueing memory behind a path slower than the inbound rate, and sub-interpreter routes keep
//! full speed.

use std::sync::Arc;
use std::time::Instant;

use mipyre_tpc::ShutdownDeadline;
use pyo3::prelude::*;
use tokio::sync::oneshot;

use crate::exchange::error::{Capacity, ErrorRecord, UnansweredError, WorkerGroup};
use crate::exchange::request::Request;
use super::job_queue::{job_queue, JobReceiver, JobSendError, JobSender};
use crate::runtime::{call_handler_with_hooks, MainReply};
use mipyre_http::plan::route::HandlerTarget;
use mipyre_http::plan::site::SiteRules;
use mipyre_tpc::{ThreadJoinError, OsThread, Spawner};

/// Sizes the bridge; `capacity` is the total queue before 503, not a per-thread one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BridgeConfig {
    pub(crate) workers: std::num::NonZeroUsize,
    pub(crate) capacity: std::num::NonZeroUsize,
}

/// Carries a request built on a TPC thread to the bridge and its answer back.
///
/// A streamed answer's receiver is `Send`, so it crosses back to the TPC thread, whose body writer
/// drives it.
pub(crate) struct BridgeJob {
    pub(crate) target: HandlerTarget,
    pub(crate) request: Request,
    pub(crate) reply: BridgeReplySender,
}

pub(crate) type BridgeReplySender = oneshot::Sender<Result<MainReply, ErrorRecord>>;

/// Reports a bridge thread the OS would not start; it fails the start, because serving with fewer
/// threads than configured would be a silent capacity cut.
#[derive(Debug, thiserror::Error)]
#[error("could not start main-interpreter bridge thread {worker} of {requested}: {source}")]
pub(crate) struct BridgeSpawnError {
    worker: usize,
    requested: usize,
    #[source]
    source: std::io::Error,
}

impl From<BridgeSpawnError> for PyErr {
    fn from(e: BridgeSpawnError) -> Self {
        pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
    }
}

/// Sends work to the bridge; its threads exit once every reference to it is gone.
pub(crate) struct MainInterpBridge {
    tx: JobSender<BridgeJob>,
}

/// Owns the bridge's threads; it is apart from [`MainInterpBridge`] because a thread's end signal
/// is not `Sync` and the bridge is shared.
pub(crate) struct BridgeThreads {
    threads: Vec<OsThread<()>>,
}

impl MainInterpBridge {
    /// Spawns the bridge threads on one shared channel.
    ///
    /// # Errors
    ///
    /// Fails when a thread does not start; the threads already started are joined within the
    /// app's `shutdown_timeout` before the error returns.
    pub(crate) fn spawn<M: Clone + Send + 'static>(
        py: Python<'_>,
        rules: &SiteRules<Py<PyAny>, M>,
        config: BridgeConfig,
        spawner: &mut impl Spawner,
    ) -> Result<(Arc<Self>, BridgeThreads), BridgeSpawnError> {
        let workers = config.workers.get();
        let shutdown_timeout = rules.settings.limits.shutdown_timeout.get();
        let (tx, rx) = job_queue::<BridgeJob>(config.capacity.get());

        // These are plain threads with no tokio runtime, because a handler reading `req.stream`
        // calls `Receiver::blocking_recv`, which panics inside a runtime context.
        let mut threads = Vec::with_capacity(workers);
        for i in 0..workers {
            // Each thread owns its copy so no reference count is shared between threads.
            let (rx, rules) = (rx.clone(), crate::runtime::copy_rules(rules, py));
            let builder = std::thread::Builder::new()
                .name(format!("mipyre-main-bridge-{i}"))
                .stack_size(mipyre_subinterp::PYTHON_THREAD_STACK);
            match spawner.spawn(i, builder, move || serve(i, rx, rules)) {
                Ok(handle) => threads.push(handle),
                Err(source) => {
                    drop(tx);
                    let started = BridgeThreads { threads };
                    // The GIL is released because the threads attach to main on their way out.
                    py.detach(|| started.join(&ShutdownDeadline::new(shutdown_timeout)));
                    return Err(BridgeSpawnError {
                        worker: i,
                        requested: workers,
                        source,
                    });
                }
            }
        }

        tracing::info!(
            target: "mipyre::server",
            workers,
            capacity = config.capacity.get(),
            "main-interp bridge spawned"
        );
        Ok((Arc::new(MainInterpBridge { tx }), BridgeThreads { threads }))
    }

    /// Queues `item` without blocking.
    ///
    /// # Errors
    ///
    /// Fails when the queue is full or every bridge thread has exited. The refused item is dropped
    /// here, and its closed reply channel is what stops the caller's body feeder.
    pub(crate) fn try_dispatch(&self, item: BridgeJob) -> Result<(), UnansweredError> {
        let group = WorkerGroup::MainBridge;
        self.tx.try_send(item).map_err(|e| match e {
            JobSendError::QueueFull => UnansweredError::Overloaded(Capacity::WorkQueue(group)),
            JobSendError::NoReceiverLeft => UnansweredError::WorkersGone(group),
        })
    }
}

impl BridgeThreads {
    /// Waits until `deadline` for every bridge thread to exit and returns the names of the ones
    /// still running.
    ///
    /// Call it with the GIL released, since the threads attach to main on their way out, and after
    /// every [`MainInterpBridge`] reference is gone; a reference still held keeps the channel open.
    /// A handler can block forever, so a thread still running at the deadline is abandoned rather
    /// than hang the stop.
    pub(crate) fn join(self, deadline: &ShutdownDeadline) -> Vec<String> {
        let mut abandoned = Vec::new();
        let deadline_at = deadline.fix(Instant::now());
        for handle in self.threads {
            let thread = handle.name();
            match handle.join_by(deadline_at) {
                Ok(()) => {}
                Err(ThreadJoinError::Panicked(message)) => tracing::error!(
                    target: "mipyre::server",
                    thread = %thread,
                    panic = %message,
                    "main-interp bridge worker panicked"
                ),
                Err(ThreadJoinError::StillRunning) => {
                    tracing::error!(
                        target: "mipyre::server",
                        "main-interp bridge thread {thread} did not exit within {:?}; abandoning it",
                        deadline.grace(),
                    );
                    abandoned.push(thread);
                }
            }
        }
        abandoned
    }
}

fn serve<M>(worker: usize, rx: JobReceiver<BridgeJob>, rules: SiteRules<Py<PyAny>, M>) {
    while let Some(item) = rx.recv() {
        dispatch_one(&rules, item);
    }
    // Python objects this thread owns must go while attached, before the thread state does.
    mipyre_subinterp::attach::with_main(move |py| {
        crate::runtime::close_thread_event_loop(py);
        drop(rules);
    });
    tracing::info!(
        target: "mipyre::server",
        worker,
        "main-interp bridge worker exiting (channel closed)"
    );
}

fn dispatch_one<M>(rules: &SiteRules<Py<PyAny>, M>, item: BridgeJob) {
    let BridgeJob {
        target,
        request,
        reply,
    } = item;

    // The caller already gave up, so the main GIL is not spent on a reply nobody reads.
    if reply.is_closed() {
        return;
    }

    let result = call_handler_with_hooks(rules, target, request);
    crate::runtime::answer_if_awaited(reply, result);
}

#[cfg(test)]
mod tests {
    use super::*;
    use mipyre_http::plan::route::RouteTable;
    use mipyre_http::plan::site::ServeSettings;
    use std::num::NonZeroUsize;

    /// Refuses the thread at `refuse_at`, since the OS cannot be made to refuse one on demand.
    struct RefuseAt {
        refuse_at: usize,
        started: usize,
    }

    impl Spawner for RefuseAt {
        fn spawn<R: Send + 'static>(
            &mut self,
            index: usize,
            builder: std::thread::Builder,
            body: impl FnOnce() -> R + Send + 'static,
        ) -> std::io::Result<OsThread<R>> {
            if index == self.refuse_at {
                return Err(std::io::Error::other("no thread for the bridge"));
            }
            self.started += 1;
            mipyre_tpc::spawn(builder, body)
        }
    }

    #[test]
    fn a_bridge_thread_that_cannot_start_fails_the_start_with_the_ones_started_joined() {
        Python::initialize();
        let rules = SiteRules::<Py<PyAny>, ()> {
            routes: RouteTable::new(),
            settings: ServeSettings::defaults(),
            access_log: None,
        };
        let config = BridgeConfig {
            workers: NonZeroUsize::new(2).unwrap(),
            capacity: NonZeroUsize::new(1).unwrap(),
        };
        let mut spawner = RefuseAt {
            refuse_at: 1,
            started: 0,
        };
        mipyre_subinterp::attach::with_main(|py| {
            let spawned = MainInterpBridge::spawn(py, &rules, config, &mut spawner);
            let Err(error) = spawned else {
                panic!("the second bridge thread was refused");
            };
            assert_eq!(spawner.started, 1, "thread 0 started, and was joined");
            assert_eq!(
                error.to_string(),
                "could not start main-interpreter bridge thread 1 of 2: no thread for the bridge"
            );
            let raised = PyErr::from(error);
            assert!(raised.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
        });
    }
}
