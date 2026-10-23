//! Counts what one server run does: its answers, its workers, where its TPC threads could not be
//! placed, the drains that ran out of time, the stop watchers that did not end cleanly, and the
//! blocking calls a stopping TPC thread left running.
//!
//! Each run holds its own [`RunCounters`] and the paths that count are handed it, so a test counts
//! on a handle of its own and reads exact numbers; Python reads them from that server
//! (`Server.metrics()`, b/335 D15). Counts that belong to the process rather than a run stay in
//! `monitor` and `degraded`.

use std::cell::Cell;
use std::sync::{Arc, Weak};
use std::thread::ThreadId;
use std::time::Duration;

use crossbeam_utils::CachePadded;
use hyper::{Method, StatusCode};
use mipyre_http::plan::answer::{duration_bucket, MethodKind, StatusClass, DURATION_BUCKETS};
use mipyre_util::throttle::LogThrottle;

use crate::sync::{AtomicI64, AtomicU64, Mutex, Ordering};

const METHOD_KINDS: usize = MethodKind::ALL.len();
const STATUS_CLASSES: usize = StatusClass::ALL.len();

/// One thread's request counts. Only that thread writes them, with a load and a store and no
/// read-modify-write.
struct RequestCounts {
    refused: AtomicU64,
    /// It is `None` in a run that does not count its answers, which then carries no table; it is
    /// out of line and padded, so a slot stays small and no two threads' tables share a line.
    answers: Option<Box<CachePadded<AnswerCounters>>>,
}

impl RequestCounts {
    fn new(counts_answers: bool) -> Self {
        RequestCounts {
            refused: AtomicU64::new(0),
            answers: counts_answers.then(Box::default),
        }
    }
}

/// One thread's answers, by method kind and status class, and how long they took.
#[derive(Default)]
struct AnswerCounters {
    answers: [[AtomicU64; STATUS_CLASSES]; METHOD_KINDS],
    /// Each bucket counts the answers that fell in it alone, not those below it too.
    duration_buckets: [AtomicU64; DURATION_BUCKETS],
    duration_sum_us: AtomicU64,
}

impl AnswerCounters {
    #[inline]
    fn record(&self, kind: MethodKind, class: StatusClass, elapsed_us: u64, writer: Writer) {
        writer.add(&self.answers[kind.index()][class.index()], 1);
        writer.add(&self.duration_buckets[duration_bucket(elapsed_us)], 1);
        writer.add(&self.duration_sum_us, elapsed_us);
    }

    fn read(&self) -> AnswerCounts {
        AnswerCounts {
            answers: self
                .answers
                .each_ref()
                .map(|row| row.each_ref().map(|count| count.load(Ordering::Relaxed))),
            duration_buckets: self
                .duration_buckets
                .each_ref()
                .map(|count| count.load(Ordering::Relaxed)),
            duration_sum_us: self.duration_sum_us.load(Ordering::Relaxed),
        }
    }
}

/// Who writes a set of counts: a thread its slot belongs to, or any of the threads that count
/// late, which share one set.
#[derive(Clone, Copy)]
enum Writer {
    Sole,
    Shared,
}

impl Writer {
    #[inline]
    fn add(self, counter: &AtomicU64, n: u64) {
        match self {
            // No other thread writes it, so a load and a store do without a locked instruction.
            Writer::Sole => counter.store(counter.load(Ordering::Relaxed) + n, Ordering::Relaxed),
            Writer::Shared => {
                counter.fetch_add(n, Ordering::Relaxed);
            }
        }
    }
}

/// The answers a run gave, by method kind and status class, and how long they took, read at one
/// moment; indexed by [`MethodKind::index`] and [`StatusClass::index`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AnswerCounts {
    pub(crate) answers: [[u64; STATUS_CLASSES]; METHOD_KINDS],
    /// Each bucket holds the answers that fell in it alone; the bounds are
    /// [`mipyre_http::plan::answer::DURATION_BOUNDS_US`].
    pub(crate) duration_buckets: [u64; DURATION_BUCKETS],
    pub(crate) duration_sum_us: u64,
}

impl AnswerCounts {
    /// Returns how many answers there were, whatever their method and status.
    pub(crate) fn total(&self) -> u64 {
        self.answers.iter().flatten().sum()
    }

    fn and(mut self, other: &AnswerCounts) -> AnswerCounts {
        for (row, other_row) in self.answers.iter_mut().zip(&other.answers) {
            for (count, other_count) in row.iter_mut().zip(other_row) {
                *count += other_count;
            }
        }
        for (count, other_count) in self.duration_buckets.iter_mut().zip(&other.duration_buckets) {
            *count += other_count;
        }
        self.duration_sum_us += other.duration_sum_us;
        self
    }
}

/// A run's request counts take one cache line per thread that answered, because one shared
/// counter would have every serving core take the same line exclusively on every request.
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
    fn new(counts_answers: bool) -> Self {
        PerThreadCounts {
            slots: Mutex::default(),
            late: RequestCounts::new(counts_answers),
        }
    }

    /// Returns this thread's slot, making it on the thread's first answer for this run, with an
    /// answer table when the run counts its answers, as `late` has.
    fn slot_of(&self, thread: ThreadId) -> Arc<CachePadded<RequestCounts>> {
        let mut slots = self.slots.lock();
        if let Some((_, slot)) = slots.iter().find(|(owner, _)| *owner == thread) {
            return Arc::clone(slot);
        }
        let slot = Arc::new(CachePadded::new(RequestCounts::new(self.late.answers.is_some())));
        slots.push((thread, Arc::clone(&slot)));
        slot
    }

    /// Runs `record` on this thread's slot, or on `late` when the thread is tearing down.
    #[inline]
    fn count(self: &Arc<Self>, record: impl Fn(&RequestCounts, Writer)) {
        let counted = LAST_SLOT.try_with(|last| {
            let slot = match last.take() {
                Some(slot) if std::ptr::eq(slot.run.as_ptr(), Arc::as_ptr(self)) => slot,
                _ => ThreadSlot {
                    run: Arc::downgrade(self),
                    counts: self.slot_of(std::thread::current().id()),
                },
            };
            record(&slot.counts, Writer::Sole);
            last.set(Some(slot));
        });
        if counted.is_err() {
            record(&self.late, Writer::Shared);
        }
    }

    fn refused(&self) -> u64 {
        self.slots
            .lock()
            .iter()
            .map(|(_, slot)| slot.refused.load(Ordering::Relaxed))
            .sum::<u64>()
            + self.late.refused.load(Ordering::Relaxed)
    }

    /// Returns every thread's answers added up, or `None` when the run does not count them.
    fn answers(&self) -> Option<AnswerCounts> {
        let late = self.late.answers.as_ref()?.read();
        let slots = self.slots.lock();
        Some(slots.iter().fold(late, |sum, (_, slot)| match &slot.answers {
            Some(answers) => sum.and(&answers.read()),
            None => sum,
        }))
    }
}

const VALIDATION_LOG_PERIOD: Duration = Duration::from_secs(60);

/// The counts of one server run, made once per start and handed to every path of the run that
/// counts.
pub(crate) struct RunCounters {
    /// This says whether the run counts its answers, as `MIPYRE_METRICS` said at its start;
    /// refusals are always counted. It is read on every request, so it is kept here rather than
    /// behind the slots, whose tables are there exactly when it is true.
    metrics: bool,
    answers: Arc<PerThreadCounts>,
    workers_serving: AtomicI64,
    workers_died: AtomicU64,
    /// A `runtime="process"` worker that died (crashed, killed, timed out) while the run served
    /// counts here; the lane forks its replacement.
    process_workers_died: AtomicU64,
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
    /// A request answered 422 because its `body=` model or `query=` string failed validation
    /// counts here. A client sends these at will, so their log line is held to one a minute
    /// (`validation_log`), for every worker of the run together.
    validation_failures: AtomicU64,
    validation_log: LogThrottle,
    /// Why the CPU counts the run was sized from are a fallback, as the read said it when the
    /// run started; `None` when the OS gave them.
    cpu_sizing_fallback: Option<String>,
    /// Why this run's TPC threads run unpinned, when the CPUs to pin to could not be read.
    unpinned_because: Mutex<Option<String>>,
}

/// One run's counts, read at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunCounts {
    /// It is `None` when the run does not count its answers.
    pub(crate) answers: Option<AnswerCounts>,
    pub(crate) refused: u64,
    pub(crate) workers_serving: i64,
    pub(crate) workers_died: u64,
    pub(crate) process_workers_died: u64,
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
    pub(crate) validation_failures: u64,
    /// It is `None` when the OS gave the CPU counts the run is sized from.
    pub(crate) cpu_sizing_fallback: Option<String>,
    /// It is `None` unless the run's TPC threads run unpinned because the CPUs to pin to could
    /// not be read.
    pub(crate) unpinned_because: Option<String>,
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

impl RunCounters {
    /// Makes a run's counters at zero; `metrics` says whether the run counts its answers.
    pub(crate) fn new(metrics: bool) -> Self {
        RunCounters {
            metrics,
            answers: Arc::new(PerThreadCounts::new(metrics)),
            workers_serving: AtomicI64::new(0),
            workers_died: AtomicU64::new(0),
            process_workers_died: AtomicU64::new(0),
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
            validation_failures: AtomicU64::new(0),
            validation_log: LogThrottle::new(VALIDATION_LOG_PERIOD),
            cpu_sizing_fallback: None,
            unpinned_because: Mutex::new(None),
        }
    }

    /// Records why the CPU counts this run is sized from are a fallback (`ServerCpus::
    /// fallback_reason`), before the run starts.
    pub(crate) fn sized_from_fallback(mut self, reason: Option<String>) -> Self {
        self.cpu_sizing_fallback = reason;
        self
    }

    /// Says whether the run counts its answers; a request it counts has its start read.
    #[inline]
    pub(crate) fn counts_answers(&self) -> bool {
        self.metrics
    }

    /// Counts one answer to a `method` request, with its `status` and the whole microseconds
    /// `elapsed_us` from the request's start to the response's head, when the run counts its
    /// answers.
    #[inline]
    pub(crate) fn count_answer(&self, method: &Method, status: StatusCode, elapsed_us: u64) {
        if self.metrics {
            let (kind, class) = (MethodKind::of(method), StatusClass::of(status));
            self.answers.count(|counts, writer| {
                if let Some(answers) = &counts.answers {
                    answers.record(kind, class, elapsed_us, writer);
                }
            });
        }
    }

    /// Counts one request the server gave up on (`exchange::refuse`), whether or not the run
    /// counts its answers.
    #[inline]
    pub(crate) fn count_refused(&self) {
        self.answers.count(|counts, writer| writer.add(&counts.refused, 1));
    }

    pub(crate) fn workers_started(&self, count: usize) {
        self.workers_serving
            .fetch_add(i64::try_from(count).unwrap_or(i64::MAX), Ordering::Relaxed);
    }

    /// Counts a worker that stopped serving; `died` says it stopped on its own (its thread state
    /// could not be bound, its engine failed, it retired) rather than at shutdown.
    pub(crate) fn worker_stopped(&self, died: bool) {
        self.workers_serving.fetch_sub(1, Ordering::Relaxed);
        if died {
            self.workers_died.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Counts a `runtime="process"` worker that died while the run served.
    pub(crate) fn process_worker_died(&self) {
        self.process_workers_died.fetch_add(1, Ordering::Relaxed);
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
    /// The run says why it runs unpinned (`RunCounts::unpinned_because`).
    pub(crate) fn unpinned_start(&self, why: &mipyre_cpu::PinOrderError) {
        match why {
            mipyre_cpu::PinOrderError::Affinity(_) | mipyre_cpu::PinOrderError::EmptyMask => {
                &self.cpu_affinity_unreadable
            }
            mipyre_cpu::PinOrderError::Topology(_) => &self.cpu_topology_unreadable,
        }
        .fetch_add(1, Ordering::Relaxed);
        *self.unpinned_because.lock() = Some(format!("threads not pinned: {why}"));
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

    /// Counts a request that failed validation and returns, once a minute, how many such lines
    /// were held back since the last one went out; `None` while the line is held back.
    pub(crate) fn validation_failed(&self, now: Duration) -> Option<u64> {
        self.validation_failures.fetch_add(1, Ordering::Relaxed);
        self.validation_log.admit(now)
    }

    pub(crate) fn read(&self) -> RunCounts {
        RunCounts {
            answers: self.answers.answers(),
            refused: self.answers.refused(),
            workers_serving: self.workers_serving.load(Ordering::Relaxed),
            workers_died: self.workers_died.load(Ordering::Relaxed),
            process_workers_died: self.process_workers_died.load(Ordering::Relaxed),
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
            validation_failures: self.validation_failures.load(Ordering::Relaxed),
            cpu_sizing_fallback: self.cpu_sizing_fallback.clone(),
            unpinned_because: self.unpinned_because.lock().clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(run: &RunCounters) {
        run.count_answer(&Method::GET, StatusCode::OK, 300);
    }

    fn total(counts: &RunCounts) -> Option<u64> {
        counts.answers.as_ref().map(AnswerCounts::total)
    }

    #[test]
    fn an_answer_is_counted_in_its_cell_bucket_and_sum() {
        let run = RunCounters::new(true);
        run.count_answer(&Method::GET, StatusCode::OK, 100);
        run.count_answer(&Method::POST, StatusCode::INTERNAL_SERVER_ERROR, 101);
        run.count_answer(&Method::POST, StatusCode::BAD_GATEWAY, 60_000_000);
        let answers = run.read().answers.unwrap();
        let cell = |kind: MethodKind, code: u16| {
            answers.answers[kind.index()][StatusClass::of(StatusCode::from_u16(code).unwrap()).index()]
        };
        assert_eq!((cell(MethodKind::Get, 200), cell(MethodKind::Post, 500)), (1, 2));
        assert_eq!(answers.total(), 3);
        let mut buckets = [0; DURATION_BUCKETS];
        buckets[0] = 1;
        buckets[1] = 1;
        buckets[DURATION_BUCKETS - 1] = 1;
        assert_eq!(answers.duration_buckets, buckets);
        assert_eq!(answers.duration_sum_us, 100 + 101 + 60_000_000);
    }

    #[test]
    fn each_answer_is_counted_once_and_a_refusal_also_as_refused() {
        let run = RunCounters::new(true);
        answer(&run);
        answer(&run);
        run.count_refused();
        answer(&run);
        let counts = run.read();
        assert_eq!((total(&counts), counts.refused), (Some(3), 1));
    }

    #[test]
    fn without_metrics_only_refusals_are_counted() {
        let run = RunCounters::new(false);
        answer(&run);
        answer(&run);
        run.count_refused();
        let counts = run.read();
        assert_eq!((counts.answers, counts.refused), (None, 1));
    }

    #[test]
    fn the_answers_of_every_thread_add_up() {
        let run = Arc::new(RunCounters::new(true));
        // Sequential threads: each exits before the next counts, so its slot outlives it.
        for _ in 0..4 {
            let run = Arc::clone(&run);
            std::thread::spawn(move || (0..1000).for_each(|_| answer(&run)))
                .join()
                .unwrap();
        }
        (0..10).for_each(|_| answer(&run));
        assert_eq!(total(&run.read()), Some(4010));
    }

    #[test]
    fn a_thread_answering_for_two_runs_counts_each_in_its_own() {
        let (a, b) = (RunCounters::new(true), RunCounters::new(true));
        for _ in 0..3 {
            answer(&a);
            answer(&b);
            b.count_refused();
        }
        answer(&a);
        assert_eq!((total(&a.read()), a.read().refused), (Some(4), 0));
        assert_eq!((total(&b.read()), b.read().refused), (Some(3), 3));
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
    fn every_validation_failure_is_counted_and_its_line_goes_out_once_a_minute() {
        let run = RunCounters::new(false);
        let at = |secs| Duration::from_secs(secs);
        assert_eq!(run.validation_failed(at(1)), Some(0));
        assert_eq!(run.validation_failed(at(2)), None);
        assert_eq!(run.validation_failed(at(60)), None);
        assert_eq!(run.validation_failed(at(61)), Some(2));
        assert_eq!(run.read().validation_failures, 4);
    }
}
