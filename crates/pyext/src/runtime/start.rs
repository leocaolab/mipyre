//! Starts the workers of one server together, or none of them keeps running.
//!
//! Each worker is created on the thread that will serve it, so the memory its interpreter first
//! touches is that thread's. The threads create their workers one at a time in enlist order,
//! because creating one runs the user's script, and module-level code such as a `CREATE TABLE`
//! has always run one worker after another. At the first failure no further thread creates, and
//! the workers already created are ended on their own threads.

use std::panic::AssertUnwindSafe;

use crossbeam_channel::{bounded, Receiver, Sender};
use mipyre_tpc::panic_message;

use super::worker::start::WorkerStartError;
use super::worker::{RequestRunner, SubInterpreterWorker, WorkerIndex};

/// Holds the first failure of a start and every later one, in report order; each was already
/// logged. It is boxed because it travels in every `Result` on the way up.
#[derive(Debug, thiserror::Error)]
#[error("{}", describe(&self.0.0, &self.0.1))]
pub(crate) struct StartError(Box<(WorkerStartError, Vec<WorkerStartError>)>);

impl StartError {
    pub(crate) fn into_parts(self) -> (WorkerStartError, Vec<WorkerStartError>) {
        *self.0
    }

    /// Converts the failures into the exception main raises: a lone failure raises as its own,
    /// and several are one `RuntimeError` naming all.
    pub(crate) fn into_pyerr(self, py: pyo3::Python<'_>) -> pyo3::PyErr {
        let (first, rest) = self.into_parts();
        if rest.is_empty() {
            return first.into_pyerr(py);
        }
        pyo3::exceptions::PyRuntimeError::new_err(describe(&first, &rest))
    }
}

fn describe(first: &WorkerStartError, rest: &[WorkerStartError]) -> String {
    let mut text = first.to_string();
    for failure in rest {
        text.push_str("; also: ");
        text.push_str(&failure.to_string());
    }
    text
}

fn log_failures(failures: &[WorkerStartError]) {
    for failure in failures {
        tracing::error!(
            target: "mipyre::server",
            error_type = failure.kind(),
            error = %failure,
            "worker failed to start"
        );
    }
}

/// Logs every failure and turns them into the start's result.
fn into_result(failures: Vec<WorkerStartError>) -> Result<(), StartError> {
    log_failures(&failures);
    let mut failures = failures.into_iter();
    match failures.next() {
        None => Ok(()),
        Some(first) => Err(StartError(Box::new((first, failures.collect())))),
    }
}

/// Holds one thread's side of a gate.
///
/// A dropped turn sender means the set aborted before this thread's turn; a `None` verdict means
/// the set did not start and the thread must end its worker.
pub(crate) struct StartHandle<T> {
    turn: Receiver<()>,
    report: Sender<Result<(), WorkerStartError>>,
    verdict: Receiver<Option<T>>,
}

/// Holds the starter's side of a gate: one entry per enlisted thread in each list, in enlist
/// order.
pub(crate) struct StartGate<T> {
    turns: Vec<Sender<()>>,
    reports: Vec<Receiver<Result<(), WorkerStartError>>>,
    verdicts: Vec<Sender<Option<T>>>,
}

impl<T> StartGate<T> {
    pub(crate) fn new() -> Self {
        StartGate {
            turns: Vec::new(),
            reports: Vec::new(),
            verdicts: Vec::new(),
        }
    }

    /// Enlists the thread about to be spawned; its turn comes after every thread enlisted before
    /// it.
    pub(crate) fn enlist(&mut self) -> StartHandle<T> {
        // Each channel carries at most one message, so one slot means no send ever waits.
        let (turn_tx, turn) = bounded(1);
        let (report, report_rx) = bounded(1);
        let (verdict_tx, verdict) = bounded(1);
        self.turns.push(turn_tx);
        self.reports.push(report_rx);
        self.verdicts.push(verdict_tx);
        StartHandle {
            turn,
            report,
            verdict,
        }
    }

    /// Tells every enlisted thread to end without creating a worker, for when a thread of the set
    /// could not be spawned.
    ///
    /// No thread has had its turn yet, so there is nothing to end or wait for, and the caller's
    /// join returns at once.
    pub(crate) fn abort(&mut self) {
        self.turns.clear();
        self.reports.clear();
        self.verdicts.clear();
    }

    /// Lets each thread create its worker in turn and, if all succeed, serve with its payload.
    ///
    /// `payloads` is in enlist order. The caller joins the threads it spawned either way.
    ///
    /// # Errors
    ///
    /// Returns every failure when a start fails; the workers already created are then ended and
    /// no later thread creates one.
    pub(crate) fn serve_or_abort(self, payloads: Vec<T>) -> Result<(), StartError> {
        debug_assert_eq!(payloads.len(), self.turns.len());
        let mut created = 0;
        let mut failure = None;
        for (position, (turn, report)) in self.turns.iter().zip(&self.reports).enumerate() {
            #[cfg(feature = "startup_trace")]
            mipyre_subinterp::trace("turn_send");
            // A closed channel means the thread ended before its start was done.
            let reported = match turn.send(()) {
                Ok(()) => report.recv().ok(),
                Err(_) => None,
            };
            match reported {
                Some(Ok(())) => created += 1,
                Some(Err(failed)) => {
                    failure = Some(failed);
                    break;
                }
                None => {
                    failure = Some(WorkerStartError::ReportLost { position });
                    break;
                }
            }
        }
        let mut failures: Vec<WorkerStartError> = failure.into_iter().collect();
        if failures.is_empty() {
            for (position, (verdict, payload)) in self.verdicts.iter().zip(payloads).enumerate() {
                if verdict.send(Some(payload)).is_err() {
                    failures.push(WorkerStartError::ReportLost { position });
                }
            }
            return into_result(failures);
        }
        // Dropping the turns tells the threads that never had theirs to end.
        drop(self.turns);
        for (position, verdict) in self.verdicts.into_iter().enumerate() {
            if position < created {
                // A thread that cannot take its verdict ended without ending its worker.
                if verdict.send(None).is_err() {
                    failures.push(WorkerStartError::ReportLost { position });
                }
            }
            // The thread at `created` is the one that failed, and it ended on its own.
        }
        into_result(failures)
    }
}

/// Is what a thread serves with once its set has started; it must be ended on the thread that made
/// it.
pub(crate) trait ThreadResident: Sized {
    fn end_here(self);
}

impl<R: RequestRunner> ThreadResident for SubInterpreterWorker<R> {
    fn end_here(self) {
        self.end_on_own_thread()
    }
}

impl<W: ThreadResident> ThreadResident for Option<W> {
    fn end_here(self) {
        if let Some(worker) = self {
            worker.end_here();
        }
    }
}

/// Waits for this thread's turn, creates its worker, reports, and waits for the verdict.
///
/// A panic in `create` is reported as [`WorkerStartError::Panicked`]. It returns `None` when the
/// thread must not serve; a worker it created is then already ended here.
pub(crate) fn start_here<W: ThreadResident, T>(
    handle: StartHandle<T>,
    worker: WorkerIndex,
    create: impl FnOnce() -> Result<W, WorkerStartError>,
) -> Option<(W, T)> {
    let StartHandle {
        turn,
        report,
        verdict,
    } = handle;
    if turn.recv().is_err() {
        return None;
    }
    #[cfg(feature = "startup_trace")]
    mipyre_subinterp::trace("turn_recv");
    let created = match std::panic::catch_unwind(AssertUnwindSafe(create)) {
        Ok(created) => created,
        Err(payload) => Err(WorkerStartError::Panicked {
            worker,
            message: panic_message(&*payload),
        }),
    };
    let created = match created {
        Ok(created) => created,
        Err(failure) => {
            // If the starter is gone, nothing else will log the failure.
            if let Err(lost) = report.send(Err(failure)) {
                if let Err(failure) = lost.0 {
                    tracing::error!(
                        target: "mipyre::server",
                        error_type = failure.kind(),
                        error = %failure,
                        "worker failed to start, and its starter is gone"
                    );
                }
            }
            return None;
        }
    };
    #[cfg(feature = "startup_trace")]
    mipyre_subinterp::trace("report_send");
    if report.send(Ok(())).is_err() {
        tracing::error!(
            target: "mipyre::server",
            worker = %worker,
            "worker started, and its starter is gone; ending it"
        );
        created.end_here();
        return None;
    }
    let verdict = verdict.recv();
    #[cfg(feature = "startup_trace")]
    mipyre_subinterp::trace("verdict_recv");
    match verdict {
        Ok(Some(payload)) => Some((created, payload)),
        Ok(None) => {
            created.end_here();
            None
        }
        Err(_) => {
            tracing::error!(
                target: "mipyre::server",
                worker = %worker,
                "the starter dropped the verdict without one; ending the worker"
            );
            created.end_here();
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::super::worker::start::WorkerStartError;
    use super::*;
    use mipyre_subinterp::NewInterpreterError;

    #[test]
    fn a_refused_creation_is_creation_failed_and_anything_else_is_prepare_failed() {
        let worker = WorkerIndex::new(1);
        let refused = WorkerStartError::CreationFailed {
            worker,
            source: mipyre_subinterp::NewInterpreterError::new(
                Some("Py_NewInterpreterFromConfig".into()),
                Some("refused".into()),
            ),
        };
        assert!(refused.to_string().starts_with("worker 1: "), "{refused}");
        assert_eq!(refused.kind(), "creation_failed");

        let no_routes = WorkerStartError::NoRoutes {
            worker,
            expected: 2,
        };
        assert_eq!(no_routes.kind(), "prepare_failed");
    }

    /// Counts its ends, checking each happens on the thread that created it.
    struct Counted {
        ends: Arc<AtomicUsize>,
        created_on: std::thread::ThreadId,
    }

    impl ThreadResident for Counted {
        fn end_here(self) {
            assert_eq!(
                std::thread::current().id(),
                self.created_on,
                "a worker is ended on the thread that created it"
            );
            self.ends.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone, Copy)]
    enum Fate {
        Created,
        CreationRefused,
        PrepareRaised,
        Panics,
    }

    struct Outcome {
        result: Result<(), StartError>,
        ends: usize,
        /// Is sorted.
        served: Vec<usize>,
        /// Lists the positions whose creation ran, in the order they ran.
        turns: Vec<usize>,
        most_at_once: usize,
    }

    /// Starts one thread per fate through a gate; each serves with its position as payload.
    fn start_set(fates: &[Fate]) -> Outcome {
        let ends = Arc::new(AtomicUsize::new(0));
        let served = Arc::new(Mutex::new(Vec::new()));
        let turns = Arc::new(Mutex::new(Vec::new()));
        let (running, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let mut gate = StartGate::<usize>::new();
        let mut handles = Vec::new();
        let mut payloads = Vec::new();
        for (i, &fate) in fates.iter().enumerate() {
            let start_handle = gate.enlist();
            let (ends, served, turns) = (Arc::clone(&ends), Arc::clone(&served), Arc::clone(&turns));
            let (running, most) = (Arc::clone(&running), Arc::clone(&most));
            let index = WorkerIndex::new(i);
            handles.push(std::thread::spawn(move || {
                let started = start_here(start_handle, index, || {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    most.fetch_max(now, Ordering::SeqCst);
                    turns.lock().unwrap().push(i);
                    let created = match fate {
                        Fate::Created => Ok(Counted {
                            ends: Arc::clone(&ends),
                            created_on: std::thread::current().id(),
                        }),
                        Fate::CreationRefused => Err(WorkerStartError::CreationFailed {
                            worker: index,
                            source: NewInterpreterError::new(
                                Some("Py_NewInterpreterFromConfig".into()),
                                Some("out of memory".into()),
                            ),
                        }),
                        Fate::PrepareRaised => Err(WorkerStartError::NoRoutes {
                            worker: index,
                            expected: 1,
                        }),
                        Fate::Panics => {
                            running.fetch_sub(1, Ordering::SeqCst);
                            panic!("injected start panic")
                        }
                    };
                    running.fetch_sub(1, Ordering::SeqCst);
                    created
                });
                if let Some((_worker, payload)) = started {
                    served.lock().unwrap().push(payload);
                }
            }));
            payloads.push(i);
        }
        let result = gate.serve_or_abort(payloads);
        for handle in handles {
            handle.join().expect("a start thread returns");
        }
        let mut served = served.lock().unwrap().clone();
        served.sort_unstable();
        let turns = turns.lock().unwrap().clone();
        Outcome {
            result,
            ends: ends.load(Ordering::SeqCst),
            served,
            turns,
            most_at_once: most.load(Ordering::SeqCst),
        }
    }

    fn kinds(e: &StartError) -> Vec<&'static str> {
        std::iter::once(&e.0 .0)
            .chain(e.0 .1.iter())
            .map(WorkerStartError::kind)
            .collect()
    }

    #[test]
    fn a_set_that_starts_serves_every_thread_and_ends_none() {
        let out = start_set(&[Fate::Created, Fate::Created, Fate::Created]);
        assert!(out.result.is_ok());
        assert_eq!(out.ends, 0);
        assert_eq!(out.served, vec![0, 1, 2]);
    }

    #[test]
    fn the_workers_are_created_one_at_a_time_in_enlist_order() {
        let out = start_set(&[Fate::Created; 8]);
        assert!(out.result.is_ok());
        assert_eq!(out.turns, (0..8).collect::<Vec<_>>());
        assert_eq!(out.most_at_once, 1);
    }

    #[test]
    fn a_refused_creation_aborts_the_set_and_no_later_thread_creates() {
        let out = start_set(&[Fate::Created, Fate::CreationRefused, Fate::Created]);
        let aborted = out.result.expect_err("the set did not start");
        assert_eq!(kinds(&aborted), vec!["creation_failed"]);
        assert!(aborted.to_string().contains("out of memory"));
        assert_eq!(out.turns, vec![0, 1], "the thread after the failure creates nothing");
        assert_eq!(out.ends, 1, "the worker created before the failure is ended");
        assert!(out.served.is_empty());
    }

    #[test]
    fn a_setup_that_raises_aborts_the_set_before_any_other_creates() {
        let out = start_set(&[Fate::PrepareRaised, Fate::Created, Fate::Created]);
        let aborted = out.result.expect_err("the set did not start");
        assert_eq!(kinds(&aborted), vec!["prepare_failed"]);
        assert!(aborted.to_string().contains("registered no routes"));
        assert_eq!(out.turns, vec![0]);
        assert_eq!(out.ends, 0);
        assert!(out.served.is_empty());
    }

    #[test]
    fn a_panicking_start_is_reported_and_aborts_the_set() {
        let out = start_set(&[Fate::Created, Fate::Panics, Fate::Created]);
        let aborted = out.result.expect_err("the set did not start");
        assert_eq!(kinds(&aborted), vec!["panicked"]);
        assert!(aborted.to_string().contains("injected start panic"));
        assert_eq!(out.turns, vec![0, 1]);
        assert_eq!(out.ends, 1);
        assert!(out.served.is_empty());
    }

    #[test]
    fn a_spawn_that_fails_stops_the_spawned_threads_before_any_creates() {
        let created = Arc::new(AtomicUsize::new(0));
        let mut gate = StartGate::<()>::new();
        let mut handles = Vec::new();
        for i in 0..2 {
            let start_handle = gate.enlist();
            let created = Arc::clone(&created);
            handles.push(std::thread::spawn(move || {
                let started = start_here(start_handle, WorkerIndex::new(i), || {
                    created.fetch_add(1, Ordering::SeqCst);
                    Ok(None::<Counted>)
                });
                assert!(started.is_none(), "an aborted start serves nothing");
            }));
        }
        // This enlistment stands for a third thread that could not be spawned.
        let unspawned = gate.enlist();
        gate.abort();
        for handle in handles {
            handle.join().expect("a start thread returns");
        }
        drop(unspawned);
        assert_eq!(created.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_thread_that_ends_without_reporting_fails_the_start() {
        let mut gate = StartGate::<()>::new();
        let gone = gate.enlist();
        let kept = gate.enlist();
        let waiter = std::thread::spawn(move || {
            start_here(kept, WorkerIndex::new(1), || Ok(None::<Counted>)).is_none()
        });
        drop(gone);
        let aborted = gate.serve_or_abort(vec![(), ()]).expect_err("the start fails");
        assert_eq!(kinds(&aborted), vec!["report_lost"]);
        assert!(waiter.join().expect("returns"), "the other thread creates nothing");
    }
}
