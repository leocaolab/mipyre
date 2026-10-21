//! Counts what one server run does: its answers, its workers, where its TPC threads could not be
//! placed, the drains that ran out of time, the stop watchers that did not end cleanly, and the
//! blocking calls a stopping TPC thread left running.
//!
//! Each run holds its own [`RunCounters`] and the paths that count are handed it, so a test counts
//! on a handle of its own and reads exact numbers. [`process_counts`], which `get_gil_metrics()`
//! shows, adds up every run the process started. Counts that belong to the process rather than a
//! run stay in `monitor` and `degraded`.

use std::cell::Cell;
use std::sync::{Arc, Weak};
use std::thread::ThreadId;

use crossbeam_utils::CachePadded;

use crate::sync::{AtomicI64, AtomicU64, Mutex, Ordering};

/// One thread's request counts. Only that thread writes them, with a load and a store and no
/// read-modify-write.
#[derive(Default)]
struct RequestCounts {
    requests: AtomicU64,
    refused: AtomicU64,
}

/// A run's request counts take one cache line per thread that answered, because one shared
/// counter would have every serving core take the same line exclusively on every request.
#[derive(Default)]
struct PerThreadCounts {
    /// A thread id is never reused in a process, so a slot has one writer for good. A thread that
    /// answers for two runs has a slot in each.
    slots: Mutex<Vec<(ThreadId, Arc<CachePadded<RequestCounts>>)>>,
    /// A thread already tearing down its thread-locals counts here, atomically because several
    /// such threads may count at once.
    late: RequestCounts,
}

struct ThreadSlot {
    /// The run is compared by address, and a weak reference keeps the address from being reused
    /// while held.
    run: Weak<PerThreadCounts>,
    counts: Arc<CachePadded<RequestCounts>>,
}

thread_local! {
    static LAST_SLOT: Cell<Option<ThreadSlot>> = const { Cell::new(None) };
}

impl PerThreadCounts {
    /// Returns this thread's slot, making it on the thread's first answer for this run.
    fn slot_of(&self, thread: ThreadId) -> Arc<CachePadded<RequestCounts>> {
        let mut slots = self.slots.lock();
        if let Some((_, slot)) = slots.iter().find(|(owner, _)| *owner == thread) {
            return Arc::clone(slot);
        }
        let slot = Arc::new(CachePadded::new(RequestCounts::default()));
        slots.push((thread, Arc::clone(&slot)));
        slot
    }

    #[inline]
    fn bump(self: &Arc<Self>, field: fn(&RequestCounts) -> &AtomicU64) {
        let counted = LAST_SLOT.try_with(|last| {
            let slot = match last.take() {
                Some(slot) if std::ptr::eq(slot.run.as_ptr(), Arc::as_ptr(self)) => slot,
                _ => ThreadSlot {
                    run: Arc::downgrade(self),
                    counts: self.slot_of(std::thread::current().id()),
                },
            };
            // This thread is the slot's only writer.
            let counter = field(&slot.counts);
            counter.store(counter.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
            last.set(Some(slot));
        });
        if counted.is_err() {
            field(&self.late).fetch_add(1, Ordering::Relaxed);
        }
    }

    fn sum(&self, field: fn(&RequestCounts) -> &AtomicU64) -> u64 {
        self.slots
            .lock()
            .iter()
            .map(|(_, slot)| field(slot).load(Ordering::Relaxed))
            .sum::<u64>()
            + field(&self.late).load(Ordering::Relaxed)
    }
}

/// The counts of one server run, made once per start and handed to every path of the run that
/// counts.
pub(crate) struct RunCounters {
    /// This says whether the run counts requests, as `MIPYRE_METRICS` said at its start; refusals
    /// are always counted.
    metrics: bool,
    answers: Arc<PerThreadCounts>,
    workers_serving: AtomicI64,
    workers_died: AtomicU64,
    /// A `gc.collect()` that raised or panicked on the Rust side counts here and retires its
    /// worker.
    gc_failures: AtomicU64,
    workers_retired: AtomicU64,
    last_retirement: Mutex<Option<String>>,
    cpu_pin_failures: AtomicU64,
    cpu_qos_failures: AtomicU64,
    cpu_affinity_unreadable: AtomicU64,
    cpu_topology_unreadable: AtomicU64,
    cpu_threads_beyond_cpus: AtomicU64,
    drain_timeouts: AtomicU64,
    drain_closed_connections: AtomicU64,
    stop_watcher_failures: AtomicU64,
    blocking_calls_left: AtomicU64,
}

/// One run's counts, or every run's added up, read at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunCounts {
    /// It is `None` when the run does not count requests. Added up, it is what the counting runs
    /// counted, and `None` when the latest run does not count.
    pub(crate) requests: Option<u64>,
    pub(crate) refused: u64,
    pub(crate) workers_serving: i64,
    pub(crate) workers_died: u64,
    pub(crate) gc_failures: u64,
    pub(crate) workers_retired: u64,
    pub(crate) last_worker_retirement: Option<String>,
    pub(crate) cpu_pin_failures: u64,
    pub(crate) cpu_qos_failures: u64,
    pub(crate) cpu_affinity_unreadable: u64,
    pub(crate) cpu_topology_unreadable: u64,
    pub(crate) cpu_threads_beyond_cpus: u64,
    pub(crate) drain_timeouts: u64,
    pub(crate) drain_closed_connections: u64,
    pub(crate) stop_watcher_failures: u64,
    pub(crate) blocking_calls_left: u64,
}

impl RunCounts {
    /// Adds a later run's counts to these; the later run's last retirement stands when it has
    /// one, and requests are added only while the later run counts them.
    fn and(self, later: RunCounts) -> RunCounts {
        RunCounts {
            requests: later.requests.map(|counted| counted + self.requests.unwrap_or(0)),
            refused: self.refused + later.refused,
            workers_serving: self.workers_serving + later.workers_serving,
            workers_died: self.workers_died + later.workers_died,
            gc_failures: self.gc_failures + later.gc_failures,
            workers_retired: self.workers_retired + later.workers_retired,
            last_worker_retirement: later.last_worker_retirement.or(self.last_worker_retirement),
            cpu_pin_failures: self.cpu_pin_failures + later.cpu_pin_failures,
            cpu_qos_failures: self.cpu_qos_failures + later.cpu_qos_failures,
            cpu_affinity_unreadable: self.cpu_affinity_unreadable + later.cpu_affinity_unreadable,
            cpu_topology_unreadable: self.cpu_topology_unreadable + later.cpu_topology_unreadable,
            cpu_threads_beyond_cpus: self.cpu_threads_beyond_cpus + later.cpu_threads_beyond_cpus,
            drain_timeouts: self.drain_timeouts + later.drain_timeouts,
            drain_closed_connections: self.drain_closed_connections
                + later.drain_closed_connections,
            stop_watcher_failures: self.stop_watcher_failures + later.stop_watcher_failures,
            blocking_calls_left: self.blocking_calls_left + later.blocking_calls_left,
        }
    }
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

impl RunCounters {
    /// Makes a run's counters at zero; `metrics` says whether the run counts requests.
    pub(crate) fn new(metrics: bool) -> Self {
        RunCounters {
            metrics,
            answers: Arc::default(),
            workers_serving: AtomicI64::new(0),
            workers_died: AtomicU64::new(0),
            gc_failures: AtomicU64::new(0),
            workers_retired: AtomicU64::new(0),
            last_retirement: Mutex::new(None),
            cpu_pin_failures: AtomicU64::new(0),
            cpu_qos_failures: AtomicU64::new(0),
            cpu_affinity_unreadable: AtomicU64::new(0),
            cpu_topology_unreadable: AtomicU64::new(0),
            cpu_threads_beyond_cpus: AtomicU64::new(0),
            drain_timeouts: AtomicU64::new(0),
            drain_closed_connections: AtomicU64::new(0),
            stop_watcher_failures: AtomicU64::new(0),
            blocking_calls_left: AtomicU64::new(0),
        }
    }

    /// Counts one answered request, whatever its answer, when the run counts requests.
    #[inline]
    pub(crate) fn count_request(&self) {
        if self.metrics {
            self.answers.bump(|c| &c.requests);
        }
    }

    /// Counts one request the server gave up on (`exchange::refuse`), whether or not the run
    /// counts requests.
    #[inline]
    pub(crate) fn count_refused(&self) {
        self.answers.bump(|c| &c.refused);
    }

    pub(crate) fn workers_started(&self, count: usize) {
        self.workers_serving.fetch_add(
            i64::try_from(count).unwrap_or(i64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Counts a worker that stopped serving; `died` says it stopped on its own (its thread state
    /// could not be bound, its engine failed, it retired) rather than at shutdown.
    pub(crate) fn worker_stopped(&self, died: bool) {
        self.workers_serving.fetch_sub(1, Ordering::Relaxed);
        if died {
            self.workers_died.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Counts a worker retired because its `gc.collect()` failed, keeping `why` as the last
    /// retirement.
    pub(crate) fn worker_retired(&self, why: String) {
        self.gc_failures.fetch_add(1, Ordering::Relaxed);
        self.workers_retired.fetch_add(1, Ordering::Relaxed);
        *self.last_retirement.lock() = Some(why);
    }

    /// Counts a start whose TPC threads run unpinned because
    /// [`mipyre_cpu::HostCpus::pin_order`] could not say which CPUs to pin to.
    pub(crate) fn unpinned_start(&self, why: &mipyre_cpu::PinOrderError) {
        match why {
            mipyre_cpu::PinOrderError::Affinity(_) | mipyre_cpu::PinOrderError::EmptyMask => {
                &self.cpu_affinity_unreadable
            }
            mipyre_cpu::PinOrderError::Topology(_) => &self.cpu_topology_unreadable,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a thread that could not be pinned to its CPU and runs unpinned.
    pub(crate) fn pin_failure(&self) {
        self.cpu_pin_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a macOS thread whose QoS class could not be raised, so it may run on efficiency
    /// cores.
    pub(crate) fn qos_failure(&self) {
        self.cpu_qos_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a start with more TPC threads than CPUs to pin them to; the extra ones run unpinned.
    pub(crate) fn threads_beyond_cpus(&self) {
        self.cpu_threads_beyond_cpus.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a drain that ran out of time and the `remaining` connections it closed.
    pub(crate) fn drain_timeout(&self, remaining: usize) {
        self.drain_timeouts.fetch_add(1, Ordering::Relaxed);
        self.drain_closed_connections
            .fetch_add(to_u64(remaining), Ordering::Relaxed);
    }

    /// Counts a stop watcher that panicked or was still running when its run ended.
    pub(crate) fn stop_watcher_failure(&self) {
        self.stop_watcher_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts `calls` blocking calls a TPC thread's runtime left running when it shut down.
    pub(crate) fn blocking_calls_left(&self, calls: usize) {
        self.blocking_calls_left.fetch_add(to_u64(calls), Ordering::Relaxed);
    }

    pub(crate) fn read(&self) -> RunCounts {
        RunCounts {
            requests: self.metrics.then(|| self.answers.sum(|c| &c.requests)),
            refused: self.answers.sum(|c| &c.refused),
            workers_serving: self.workers_serving.load(Ordering::Relaxed),
            workers_died: self.workers_died.load(Ordering::Relaxed),
            gc_failures: self.gc_failures.load(Ordering::Relaxed),
            workers_retired: self.workers_retired.load(Ordering::Relaxed),
            last_worker_retirement: self.last_retirement.lock().clone(),
            cpu_pin_failures: self.cpu_pin_failures.load(Ordering::Relaxed),
            cpu_qos_failures: self.cpu_qos_failures.load(Ordering::Relaxed),
            cpu_affinity_unreadable: self.cpu_affinity_unreadable.load(Ordering::Relaxed),
            cpu_topology_unreadable: self.cpu_topology_unreadable.load(Ordering::Relaxed),
            cpu_threads_beyond_cpus: self.cpu_threads_beyond_cpus.load(Ordering::Relaxed),
            drain_timeouts: self.drain_timeouts.load(Ordering::Relaxed),
            drain_closed_connections: self.drain_closed_connections.load(Ordering::Relaxed),
            stop_watcher_failures: self.stop_watcher_failures.load(Ordering::Relaxed),
            blocking_calls_left: self.blocking_calls_left.load(Ordering::Relaxed),
        }
    }
}

/// A run's counters are kept after it stops, so its counts stay in the totals; each costs a
/// handful of words and a cache line per thread that answered for it.
static RUNS: parking_lot::Mutex<Vec<Arc<RunCounters>>> = parking_lot::const_mutex(Vec::new());

/// Makes the counters of a run starting now and adds them to the process's counts; `metrics`
/// says whether the run counts requests.
pub(crate) fn start_run(metrics: bool) -> Arc<RunCounters> {
    let run = Arc::new(RunCounters::new(metrics));
    RUNS.lock().push(Arc::clone(&run));
    run
}

/// Returns every run's counts added up, or `None` before any run started.
pub(crate) fn process_counts() -> Option<RunCounts> {
    RUNS.lock().iter().map(|run| run.read()).reduce(RunCounts::and)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_answer_is_counted_once_and_a_refusal_also_as_refused() {
        let run = RunCounters::new(true);
        run.count_request();
        run.count_request();
        run.count_refused();
        run.count_request();
        let counts = run.read();
        assert_eq!((counts.requests, counts.refused), (Some(3), 1));
    }

    #[test]
    fn without_metrics_only_refusals_are_counted() {
        let run = RunCounters::new(false);
        run.count_request();
        run.count_request();
        run.count_refused();
        let counts = run.read();
        assert_eq!((counts.requests, counts.refused), (None, 1));
    }

    #[test]
    fn the_answers_of_every_thread_add_up() {
        let run = Arc::new(RunCounters::new(true));
        // Sequential threads: each exits before the next counts, so its slot outlives it.
        for _ in 0..4 {
            let run = Arc::clone(&run);
            std::thread::spawn(move || (0..1000).for_each(|_| run.count_request()))
                .join()
                .unwrap();
        }
        (0..10).for_each(|_| run.count_request());
        assert_eq!(run.read().requests, Some(4010));
    }

    #[test]
    fn a_thread_answering_for_two_runs_counts_each_in_its_own() {
        let (a, b) = (RunCounters::new(true), RunCounters::new(true));
        for _ in 0..3 {
            a.count_request();
            b.count_request();
            b.count_refused();
        }
        a.count_request();
        assert_eq!((a.read().requests, a.read().refused), (Some(4), 0));
        assert_eq!((b.read().requests, b.read().refused), (Some(3), 3));
        // Switching runs keeps one slot per thread and run.
        assert_eq!(a.answers.slots.lock().len(), 1);
        assert_eq!(b.answers.slots.lock().len(), 1);
    }

    #[test]
    fn a_worker_that_stops_on_its_own_is_counted_as_died_and_no_longer_serving() {
        let run = RunCounters::new(false);
        run.workers_started(2);
        run.worker_stopped(false);
        run.worker_stopped(true);
        let counts = run.read();
        assert_eq!((counts.workers_serving, counts.workers_died), (0, 1));
    }

    #[test]
    fn a_retired_worker_is_counted_with_its_gc_failure_and_reason() {
        let run = RunCounters::new(false);
        run.worker_retired("worker 7 retired: gc.collect() raised MemoryError: ".into());
        let counts = run.read();
        assert_eq!((counts.gc_failures, counts.workers_retired), (1, 1));
        assert_eq!(
            counts.last_worker_retirement.as_deref(),
            Some("worker 7 retired: gc.collect() raised MemoryError: ")
        );
    }

    #[test]
    fn a_drain_that_ran_out_counts_once_with_its_open_connections() {
        let run = RunCounters::new(false);
        run.drain_timeout(3);
        let counts = run.read();
        assert_eq!((counts.drain_timeouts, counts.drain_closed_connections), (1, 3));
    }

    #[test]
    fn runs_add_up_and_the_later_retirement_stands() {
        let earlier = RunCounts {
            requests: Some(2),
            workers_retired: 1,
            last_worker_retirement: Some("earlier".into()),
            ..RunCounts::default()
        };
        let later = RunCounts {
            requests: Some(3),
            refused: 1,
            ..RunCounts::default()
        };
        let total = earlier.clone().and(later.clone());
        assert_eq!((total.requests, total.refused), (Some(5), 1));
        // A run that counted nothing adds nothing; the total is there while the latest run counts.
        let uncounted = RunCounts::default();
        assert_eq!(uncounted.clone().and(later.clone()).requests, Some(3));
        assert_eq!(later.clone().and(uncounted).requests, None);
        assert_eq!(total.last_worker_retirement.as_deref(), Some("earlier"));
        let later_retired = RunCounts {
            last_worker_retirement: Some("later".into()),
            ..later
        };
        assert_eq!(
            earlier.and(later_retired).last_worker_retirement.as_deref(),
            Some("later")
        );
    }
}
