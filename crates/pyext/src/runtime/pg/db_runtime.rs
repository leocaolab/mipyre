//! Runs a run's (or one standalone handle's) Postgres I/O on a `mipyre-db` tokio runtime of its
//! own, off the server's.
//!
//! Sync callers wait for it on a std channel with the GIL released, never with
//! `Runtime::block_on`, which panics on a thread already inside a Tokio context such as a TPC
//! worker.

use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use tokio::runtime::Runtime;

const MIN_RUNTIME_THREADS: NonZeroUsize = NonZeroUsize::new(2).unwrap();
const CPUS_PER_RUNTIME_THREAD: usize = 4;

/// A failure of the DB runtime itself, not of a query.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error("mipyre-db task panicked: {0}")]
    Panicked(String),
    #[error("mipyre-db task was cancelled")]
    Cancelled,
    #[error("mipyre-db runtime shut down before the task finished")]
    RuntimeGone,
    /// The OS refused to build the runtime (threads or fds exhausted), or would not say how many
    /// CPUs to size it from. Nothing ran.
    #[error("mipyre-db runtime could not be built: {0}")]
    RuntimeNotBuilt(#[source] std::io::Error),
}

impl From<tokio::task::JoinError> for TaskError {
    fn from(e: tokio::task::JoinError) -> Self {
        match e.try_into_panic() {
            Ok(payload) => Self::Panicked(mipyre_tpc::panic_message(&*payload)),
            Err(_cancelled) => Self::Cancelled,
        }
    }
}

/// One `mipyre-db` runtime: a run's, shared by its pools, or a standalone handle's.
pub(crate) struct DbRuntime {
    /// Is `None` only while it is being dropped.
    rt: Option<Runtime>,
    threads: NonZeroUsize,
}

impl DbRuntime {
    /// Builds a runtime of `threads` threads, or sized from this process's CPUs when `None`.
    ///
    /// # Errors
    ///
    /// Returns [`TaskError::RuntimeNotBuilt`] when the CPU count is unknown or the OS refuses
    /// the threads.
    pub(crate) fn build(threads: Option<NonZeroUsize>) -> Result<Self, TaskError> {
        let threads = match threads {
            Some(threads) => threads,
            None => default_runtime_threads().map_err(TaskError::RuntimeNotBuilt)?,
        };
        let rt = db_runtime_of(threads).map_err(TaskError::RuntimeNotBuilt)?;
        Ok(DbRuntime { rt: Some(rt), threads })
    }

    pub(crate) fn threads(&self) -> NonZeroUsize {
        self.threads
    }

    fn rt(&self) -> &Runtime {
        match &self.rt {
            Some(rt) => rt,
            // Only `drop` takes it, and nothing uses a runtime being dropped.
            None => unreachable!("a DbRuntime is used only while it is alive"),
        }
    }

    /// Starts `fut` on this runtime.
    pub(crate) fn spawn<F>(&self, fut: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.rt().spawn(fut)
    }

    /// Runs `fut` on this runtime and blocks the calling thread until it finishes; callers
    /// release the GIL around it.
    ///
    /// It spawns and waits on a std channel because `Runtime::block_on` panics on a thread
    /// already inside a Tokio context, which is where TPC workers run handlers.
    ///
    /// # Errors
    ///
    /// Returns [`TaskError::Panicked`] with the message when `fut` panics, and
    /// [`TaskError::RuntimeGone`] when the runtime shuts down first.
    pub(crate) fn run<F, T>(&self, fut: F) -> Result<T, TaskError>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.spawn(async move {
            let outcome = AssertUnwindSafe(fut)
                .catch_unwind()
                .await
                .map_err(|payload| TaskError::Panicked(mipyre_tpc::panic_message(&*payload)));
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a closed channel means the caller is gone, and nobody wants the outcome"
            )]
            let _ = tx.send(outcome);
        });
        rx.recv().map_err(|_| TaskError::RuntimeGone)?
    }

    /// Waits for the next message on a cursor channel and returns it with up to `max` already
    /// buffered, handing the receiver back; an empty batch means the sender is gone.
    ///
    /// It receives on the DB runtime because `blocking_recv` panics inside a Tokio context
    /// such as a TPC worker.
    ///
    /// # Errors
    ///
    /// Returns [`TaskError`] when the DB runtime cannot run the receive.
    pub(crate) fn recv_batch<T: Send + 'static>(
        &self,
        mut rx: tokio::sync::mpsc::Receiver<T>,
        max: usize,
    ) -> Result<(tokio::sync::mpsc::Receiver<T>, Vec<T>), TaskError> {
        self.run(async move {
            let mut batch = Vec::new();
            if let Some(first) = rx.recv().await {
                batch.push(first);
                while batch.len() < max {
                    match rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(_) => break,
                    }
                }
            }
            (rx, batch)
        })
    }
}

impl Drop for DbRuntime {
    /// Stops the runtime without waiting: dropping a tokio runtime blocks, which panics on a
    /// thread inside a Tokio context (a TPC worker may drop the last handle).
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

/// Sizes a `mipyre-db` runtime from this process's CPUs when `PgPool.connect()` names no
/// count. Every query's protocol work and socket I/O runs on these threads, so a fixed count
/// would become a serial stage under load.
///
/// # Errors
///
/// Returns an error asking for `runtime_threads=` when the OS does not say how many CPUs the
/// process may use.
pub(crate) fn default_runtime_threads() -> std::io::Result<NonZeroUsize> {
    let cpus = std::thread::available_parallelism().map_err(unknown_cpu_count)?;
    Ok(threads_for_cpus(cpus))
}

fn threads_for_cpus(cpus: NonZeroUsize) -> NonZeroUsize {
    // `None` means the CPU share is below one thread.
    let share = NonZeroUsize::new(cpus.get() / CPUS_PER_RUNTIME_THREAD).unwrap_or(MIN_RUNTIME_THREADS);
    share.max(MIN_RUNTIME_THREADS)
}

/// Explains that the caller must name the thread count, because a guessed one would silently
/// serialise or oversubscribe every query.
fn unknown_cpu_count(e: std::io::Error) -> std::io::Error {
    std::io::Error::new(
        e.kind(),
        format!(
            "cannot size the mipyre-db runtime: the OS did not say how many CPUs this \
             process may use ({e}); pass runtime_threads= to PgPool.connect()"
        ),
    )
}

/// Builds a DB runtime. Its blocking pool attaches an interpreter to hand results to Python, so
/// every thread it starts gets `PYTHON_THREAD_STACK`.
fn db_runtime_of(threads: NonZeroUsize) -> std::io::Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.get())
        .thread_name("mipyre-db")
        .thread_stack_size(mipyre_subinterp::PYTHON_THREAD_STACK)
        .enable_all()
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one() -> DbRuntime {
        DbRuntime::build(Some(NonZeroUsize::MIN)).unwrap()
    }

    #[test]
    fn the_db_runtime_starts_its_threads_with_python_stacks() {
        crate::thread_stack::with_exact_stacks(
            concat!(module_path!(), "::the_db_runtime_starts_its_threads_with_python_stacks"),
            || {
                let python = crate::thread_stack::of_a_thread_with(mipyre_subinterp::PYTHON_THREAD_STACK);
                let rt = db_runtime_of(NonZeroUsize::MIN).expect("the DB runtime builds");
                let blocking = rt
                    .block_on(async { tokio::task::spawn_blocking(crate::thread_stack::current).await })
                    .expect("on the blocking pool");
                assert_eq!(blocking, python);
            },
        );
    }

    /// Runs `f` inside a `current_thread` runtime, where a TPC worker runs handlers.
    fn inside_current_thread_runtime<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn recv_batch_drains_a_cursor_channel_from_a_tokio_context() {
        const CAPACITY: usize = 8;
        let rt = one();
        let got = inside_current_thread_runtime(async {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(CAPACITY);
            rt.spawn(async move {
                for i in 0..100 {
                    tx.send(i).await.unwrap();
                }
            });
            let mut got = Vec::new();
            loop {
                let (r, batch) = rt.recv_batch(rx, CAPACITY).unwrap();
                assert!(batch.len() <= CAPACITY);
                if batch.is_empty() {
                    break;
                }
                got.extend(batch);
                rx = r;
            }
            got
        });
        assert_eq!(got, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn run_waits_from_a_tokio_context() {
        let rt = one();
        let v = inside_current_thread_runtime(async { rt.run(async { 7 }).unwrap() });
        assert_eq!(v, 7);
    }

    #[test]
    fn a_runtime_dropped_inside_a_tokio_context_stops_without_panicking() {
        let rt = one();
        inside_current_thread_runtime(async move { drop(rt) });
    }

    /// An async query holds its runtime until its result is delivered, so the last handle can go
    /// on one of the runtime's own threads.
    #[test]
    fn a_runtime_whose_last_handle_its_own_task_drops_stops_without_panicking() {
        let rt = std::sync::Arc::new(one());
        let (others_gone, wait_for_others) = tokio::sync::oneshot::channel::<()>();
        let (dropped, stopped) = std::sync::mpsc::channel();
        let last = std::sync::Arc::clone(&rt);
        rt.spawn(async move {
            wait_for_others.await.unwrap();
            drop(last);
            dropped.send(()).unwrap();
        });
        drop(rt);
        others_gone.send(()).unwrap();
        stopped.recv().expect("the task dropped its runtime and went on");
    }

    /// Proves why `recv_batch` exists.
    #[test]
    #[should_panic(expected = "Cannot block the current thread from within a runtime")]
    fn blocking_recv_panics_in_a_tokio_context() {
        inside_current_thread_runtime(async {
            let (_tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
            rx.blocking_recv()
        });
    }

    /// Proves why `run` exists.
    #[test]
    #[should_panic(expected = "Cannot start a runtime from within a runtime")]
    fn block_on_panics_in_a_tokio_context() {
        let rt = one();
        inside_current_thread_runtime(async { rt.rt().block_on(async { 7 }) });
    }

    #[test]
    fn a_panicking_task_reports_its_panic_message() {
        let err = one().run::<_, ()>(async { panic!("boom {}", 42) }).unwrap_err();
        match err {
            TaskError::Panicked(msg) => assert_eq!(msg, "boom 42"),
            other => panic!("expected Panicked, got {other:?}"),
        }
    }

    #[test]
    fn the_default_runtime_has_at_least_two_threads() {
        assert!(default_runtime_threads().unwrap() >= MIN_RUNTIME_THREADS);
        assert!(DbRuntime::build(None).unwrap().threads() >= MIN_RUNTIME_THREADS);
    }

    #[test]
    fn an_unknown_cpu_count_names_the_fix() {
        let err = unknown_cpu_count(std::io::Error::other("no cgroup quota file"));
        let text = err.to_string();
        assert!(text.contains("no cgroup quota file"), "{text}");
        assert!(text.contains("pass runtime_threads="), "{text}");
    }

    #[test]
    fn the_default_threads_formula_is_unchanged() {
        let n = |v: usize| NonZeroUsize::new(v).unwrap();
        assert_eq!(threads_for_cpus(n(1)), MIN_RUNTIME_THREADS);
        assert_eq!(threads_for_cpus(n(16)), n(4));
        assert_eq!(threads_for_cpus(n(64)), n(16));
    }

    #[test]
    fn a_runtime_the_os_refuses_is_said_in_its_words() {
        let refused = TaskError::RuntimeNotBuilt(std::io::Error::other("EAGAIN: threads"));
        assert_eq!(
            refused.to_string(),
            "mipyre-db runtime could not be built: EAGAIN: threads"
        );
    }
}
