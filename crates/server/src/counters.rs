//! Counts what the server does in one run: its answers and refusals, where its TPC threads could
//! not be placed, the drains that ran out of time, the stop watchers that did not end cleanly, the
//! blocking calls a stopping TPC thread left running, and the requests that failed validation.
//!
//! Each run holds its own [`ServerCounters`] and the paths that count are handed it, so a test
//! counts on a handle of its own and reads exact numbers. The run's workers are counted by
//! `runtime`, not here.

use std::cell::Cell;
use std::sync::{Arc, Weak};
use std::thread::ThreadId;
use std::time::Duration;

use crossbeam_utils::CachePadded;
use hyper::{Method, StatusCode};
use mipyre_http::plan::answer::{duration_bucket, MethodKind, StatusClass, DURATION_BUCKETS};
use mipyre_util::throttle::LogThrottle;

use crate::sync::{AtomicU64, Mutex, Ordering};

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
pub struct AnswerCounts {
    pub answers: [[u64; STATUS_CLASSES]; METHOD_KINDS],
    /// Each bucket holds the answers that fell in it alone; the bounds are
    /// [`mipyre_http::plan::answer::DURATION_BOUNDS_US`].
    pub duration_buckets: [u64; DURATION_BUCKETS],
    pub duration_sum_us: u64,
}

impl AnswerCounts {
    /// Returns how many answers there were, whatever their method and status.
    pub fn total(&self) -> u64 {
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
    slots: Mutex<Vec<(SlotOwner, Arc<CachePadded<RequestCounts>>)>>,
    /// A thread already tearing down its thread-locals counts here, atomically because several
    /// such threads may count at once.
    late: RequestCounts,
}

/// Who writes a slot: the thread it was made for on that thread's first count, found by its id,
/// or the TPC thread whose own copy of the site holds it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotOwner {
    Thread(ThreadId),
    SiteCopy,
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
        if let Some((_, slot)) = slots.iter().find(|(owner, _)| *owner == SlotOwner::Thread(thread)) {
            return Arc::clone(slot);
        }
        let slot = self.new_slot();
        slots.push((SlotOwner::Thread(thread), Arc::clone(&slot)));
        slot
    }

    /// Returns a slot for a site copy, which no thread looks up.
    fn site_slot(&self) -> Arc<CachePadded<RequestCounts>> {
        let slot = self.new_slot();
        self.slots.lock().push((SlotOwner::SiteCopy, Arc::clone(&slot)));
        slot
    }

    fn new_slot(&self) -> Arc<CachePadded<RequestCounts>> {
        Arc::new(CachePadded::new(RequestCounts::new(self.late.answers.is_some())))
    }

    /// Runs `record` on this thread's slot, or on `late` when the thread is tearing down.
    #[inline]
    fn count_on_this_thread(self: &Arc<Self>, record: impl Fn(&RequestCounts, Writer)) {
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

/// The server's counts of one run, made once per start and handed to every path of the run that
/// counts.
pub struct ServerCounters {
    /// This says whether the run counts its answers, as `MIPYRE_METRICS` said at its start;
    /// refusals are always counted. It is read on every request, so it is kept here rather than
    /// behind the slots, whose tables are there exactly when it is true.
    metrics: bool,
    answers: Arc<PerThreadCounts>,
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
    /// A WebSocket client the server pinged and then closed with 1011, because no frame came in
    /// time, counts here.
    ws_keepalive_timeouts: AtomicU64,
    /// Why the CPU counts the run was sized from are a fallback, as the read said it when the
    /// run started; `None` when the OS gave them.
    cpu_sizing_fallback: Option<String>,
    /// Why this run's TPC threads run unpinned, when the CPUs to pin to could not be read.
    unpinned_because: Mutex<Option<String>>,
}

/// The server's counts of one run, read at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerCounts {
    /// It is `None` when the run does not count its answers.
    pub answers: Option<AnswerCounts>,
    pub refused: u64,
    pub cpu_pin_failures: u64,
    pub cpu_qos_failures: u64,
    pub cpu_affinity_unreadable: u64,
    pub cpu_topology_unreadable: u64,
    pub cpu_threads_beyond_cpus: u64,
    pub drain_timeouts: u64,
    pub drain_closed_connections: u64,
    pub stop_watcher_failures: u64,
    pub blocking_calls_left: u64,
    pub validation_failures: u64,
    pub ws_keepalive_timeouts: u64,
    /// It is `None` when the OS gave the CPU counts the run is sized from.
    pub cpu_sizing_fallback: Option<String>,
    /// It is `None` unless the run's TPC threads run unpinned because the CPUs to pin to could
    /// not be read.
    pub unpinned_because: Option<String>,
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

impl ServerCounters {
    /// Makes a run's server counters at zero; `metrics` says whether the run counts its answers.
    pub fn new(metrics: bool) -> Self {
        ServerCounters {
            metrics,
            answers: Arc::new(PerThreadCounts::new(metrics)),
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
            ws_keepalive_timeouts: AtomicU64::new(0),
            cpu_sizing_fallback: None,
            unpinned_because: Mutex::new(None),
        }
    }

    /// Records why the CPU counts this run is sized from are a fallback (`ServerCpus::
    /// fallback_reason`), before the run starts.
    pub fn sized_from_fallback(mut self, reason: Option<String>) -> Self {
        self.cpu_sizing_fallback = reason;
        self
    }

    /// Counts a start whose TPC threads run unpinned because
    /// [`mipyre_cpu::HostCpus::pin_order`] could not say which CPUs to pin to.
    /// The run says why it runs unpinned (`ServerCounts::unpinned_because`).
    pub(crate) fn unpinned_start(&self, why: &mipyre_cpu::PinOrderError) {
        match why {
            mipyre_cpu::PinOrderError::Affinity(_)
            | mipyre_cpu::PinOrderError::AffinityNotListed
            | mipyre_cpu::PinOrderError::EmptyMask => &self.cpu_affinity_unreadable,
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

    pub(crate) fn ws_keepalive_timeout(&self) {
        self.ws_keepalive_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read(&self) -> ServerCounts {
        ServerCounts {
            answers: self.answers.answers(),
            refused: self.answers.refused(),
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
            ws_keepalive_timeouts: self.ws_keepalive_timeouts.load(Ordering::Relaxed),
            cpu_sizing_fallback: self.cpu_sizing_fallback.clone(),
            unpinned_because: self.unpinned_because.lock().clone(),
        }
    }
}

/// What one site counts its requests into: the run's counters, and on a TPC thread's own copy of
/// the site, that thread's slot in them. A site of the multi-thread server has no slot of its own,
/// even an IO thread's copy, since a task there moves between threads; each count finds the slot
/// of the thread it is on.
pub struct SiteCounters {
    pub run: Arc<ServerCounters>,
    /// Only the TPC thread the copy belongs to counts through it, so the slot has one writer.
    own: Option<Arc<CachePadded<RequestCounts>>>,
}

impl SiteCounters {
    /// Returns the counters of a site any thread may serve.
    pub fn new(run: Arc<ServerCounters>) -> Self {
        SiteCounters { run, own: None }
    }

    /// Returns the counters of a site copy that one TPC thread alone serves, with a slot of its
    /// own in the run's counters.
    pub(crate) fn for_thread(&self) -> Self {
        SiteCounters {
            run: Arc::clone(&self.run),
            own: Some(self.run.answers.site_slot()),
        }
    }

    /// Says whether the run counts its answers; a request it counts has its start read.
    #[inline]
    pub(crate) fn counts_answers(&self) -> bool {
        self.run.metrics
    }

    /// Counts one answer to a `method` request, with its `status` and the whole microseconds
    /// `elapsed_us` from the request's start to the response's head, when the run counts its
    /// answers.
    #[inline]
    pub(crate) fn count_answer(&self, method: &Method, status: StatusCode, elapsed_us: u64) {
        if self.run.metrics {
            let (kind, class) = (MethodKind::of(method), StatusClass::of(status));
            self.count(|counts, writer| {
                if let Some(answers) = &counts.answers {
                    answers.record(kind, class, elapsed_us, writer);
                }
            });
        }
    }

    /// Counts one request the server gave up on (`exchange::refuse`), whether or not the run
    /// counts its answers.
    #[cold]
    #[inline(never)]
    pub(crate) fn count_refused(&self) {
        self.count(|counts, writer| writer.add(&counts.refused, 1));
    }

    #[inline]
    fn count(&self, record: impl Fn(&RequestCounts, Writer)) {
        match &self.own {
            Some(slot) => record(slot, Writer::Sole),
            None => self.run.answers.count_on_this_thread(record),
        }
    }
}

#[cfg(test)]
impl SiteCounters {
    /// Returns the counters of a site any thread may serve, in a run of its own.
    pub(crate) fn of_own_run(metrics: bool) -> Self {
        SiteCounters::new(Arc::new(ServerCounters::new(metrics)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(site: &SiteCounters) {
        site.count_answer(&Method::GET, StatusCode::OK, 300);
    }

    fn total(counts: &ServerCounts) -> Option<u64> {
        counts.answers.as_ref().map(AnswerCounts::total)
    }

    #[test]
    fn an_answer_is_counted_in_its_cell_bucket_and_sum() {
        let site = SiteCounters::of_own_run(true);
        site.count_answer(&Method::GET, StatusCode::OK, 100);
        site.count_answer(&Method::POST, StatusCode::INTERNAL_SERVER_ERROR, 101);
        site.count_answer(&Method::POST, StatusCode::BAD_GATEWAY, 60_000_000);
        let answers = site.run.read().answers.unwrap();
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
        let site = SiteCounters::of_own_run(true);
        answer(&site);
        answer(&site);
        site.count_refused();
        answer(&site);
        let counts = site.run.read();
        assert_eq!((total(&counts), counts.refused), (Some(3), 1));
    }

    #[test]
    fn without_metrics_only_refusals_are_counted() {
        let site = SiteCounters::of_own_run(false);
        answer(&site);
        answer(&site);
        site.count_refused();
        let counts = site.run.read();
        assert_eq!((counts.answers, counts.refused), (None, 1));
    }

    #[test]
    fn the_answers_of_every_thread_add_up() {
        let site = Arc::new(SiteCounters::of_own_run(true));
        // Sequential threads: each exits before the next counts, so its slot outlives it.
        for _ in 0..4 {
            let site = Arc::clone(&site);
            std::thread::spawn(move || (0..1000).for_each(|_| answer(&site)))
                .join()
                .unwrap();
        }
        (0..10).for_each(|_| answer(&site));
        assert_eq!(total(&site.run.read()), Some(4010));
    }

    #[test]
    fn a_thread_answering_for_two_runs_counts_each_in_its_own() {
        let (a, b) = (SiteCounters::of_own_run(true), SiteCounters::of_own_run(true));
        for _ in 0..3 {
            answer(&a);
            answer(&b);
            b.count_refused();
        }
        answer(&a);
        assert_eq!((total(&a.run.read()), a.run.read().refused), (Some(4), 0));
        assert_eq!((total(&b.run.read()), b.run.read().refused), (Some(3), 3));
        // Switching runs keeps one slot per thread and run.
        assert_eq!(a.run.answers.slots.lock().len(), 1);
        assert_eq!(b.run.answers.slots.lock().len(), 1);
    }

    #[test]
    fn each_thread_copy_counts_in_its_own_slot_and_the_run_adds_them_up() {
        let site = SiteCounters::of_own_run(true);
        let copies = [site.for_thread(), site.for_thread()];
        std::thread::scope(|scope| {
            for copy in &copies {
                scope.spawn(move || {
                    (0..1000).for_each(|_| answer(copy));
                    copy.count_refused();
                });
            }
        });
        answer(&site);
        let counts = site.run.read();
        assert_eq!((total(&counts), counts.refused), (Some(2001), 2));
    }

    #[test]
    fn a_thread_copy_counts_without_a_slot_for_the_thread_it_is_on() {
        let site = SiteCounters::of_own_run(true);
        let copy = site.for_thread();
        answer(&copy);
        answer(&copy);
        assert_eq!(total(&site.run.read()), Some(2));
        assert_eq!(site.run.answers.slots.lock().len(), 1);
        answer(&site);
        assert_eq!(site.run.answers.slots.lock().len(), 2);
    }

    #[test]
    fn a_thread_copy_of_a_run_without_metrics_counts_only_refusals() {
        let copy = SiteCounters::of_own_run(false).for_thread();
        answer(&copy);
        copy.count_refused();
        let counts = copy.run.read();
        assert_eq!((counts.answers, counts.refused), (None, 1));
    }

    #[test]
    fn a_drain_that_ran_out_counts_once_with_its_open_connections() {
        let run = ServerCounters::new(false);
        run.drain_timeout(3);
        let counts = run.read();
        assert_eq!((counts.drain_timeouts, counts.drain_closed_connections), (1, 3));
    }

    #[test]
    fn every_validation_failure_is_counted_and_its_line_goes_out_once_a_minute() {
        let run = ServerCounters::new(false);
        let at = |secs| Duration::from_secs(secs);
        assert_eq!(run.validation_failed(at(1)), Some(0));
        assert_eq!(run.validation_failed(at(2)), None);
        assert_eq!(run.validation_failed(at(60)), None);
        assert_eq!(run.validation_failed(at(61)), Some(2));
        assert_eq!(run.read().validation_failures, 4);
    }
}
