//! Measures GIL contention and process RSS, and snapshots every counter the engine keeps.
//!
//! GIL waits are measured by the main-interpreter handler call as it acquires the GIL on the real
//! request path, so measuring adds no contention of its own. RSS comes from a sampler thread that
//! takes no GIL and runs only while a server with metrics serves.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use mipyre_http::io::events::{self as http_events, HttpEvent};
use mipyre_transport::stream::events::{self as stream_events, StreamEvent};
use mipyre_transport::tcp::events::{self as tcp_events, TcpEvent};
use mipyre_transport::tls::events::{self as tls_events, TlsEvent};
use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;
use mipyre_http::io::static_files::degraded::{self as static_files, StaticDegradation};
use pyo3::prelude::*;

use super::degraded;
use super::{clear_degraded, set_degraded};

pub(crate) static GIL_LATENCY_LAST_US: AtomicU64 = AtomicU64::new(0);
/// The peak lasts until `reset_peaks()`.
pub(crate) static GIL_LATENCY_MAX_US: AtomicU64 = AtomicU64::new(0);
/// Counts the handler calls that acquired the GIL.
pub(crate) static GIL_PROBE_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static GIL_TOTAL_WAIT_US: AtomicU64 = AtomicU64::new(0);

/// The sampler's latest RSS read in bytes. It is `None` until the sampler has read once, after a
/// read that failed, and once the last server with `MIPYRE_METRICS=1` stopped.
static MEMORY_RSS_BYTES: parking_lot::Mutex<Option<u64>> = parking_lot::Mutex::new(None);

pub(crate) static GIL_QUEUE_LENGTH: std::sync::atomic::AtomicIsize =
    std::sync::atomic::AtomicIsize::new(0);
/// The peak handler GIL hold lasts until `reset_peaks()`.
pub(crate) static GIL_HOLD_MAX_US: AtomicU64 = AtomicU64::new(0);

/// A GIL wait longer than this is congestion.
const GIL_CONGESTED_WARN_US: u64 = 50_000;

/// Congestion logs one line a minute, because under load every request past the threshold would.
static GIL_CONGESTED_THROTTLE: LogThrottle = LogThrottle::new(Duration::from_secs(60));

/// Records one GIL acquisition wait. A wait past [`GIL_CONGESTED_WARN_US`] is counted and logged
/// at most once a minute, with how many such waits were held back since the last line.
#[inline]
pub(crate) fn record_gil_wait(wait_us: u64) {
    GIL_LATENCY_LAST_US.store(wait_us, Ordering::Relaxed);
    GIL_LATENCY_MAX_US.fetch_max(wait_us, Ordering::Relaxed);
    GIL_TOTAL_WAIT_US.fetch_add(wait_us, Ordering::Relaxed);
    GIL_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);

    if wait_us > GIL_CONGESTED_WARN_US {
        degraded::count_gil_congested_wait();
        if let Some(held) = GIL_CONGESTED_THROTTLE.admit(clock::now()) {
            tracing::warn!(
                target: "mipyre::server",
                latency_ms = wait_us / 1000,
                held,
                "GIL congested (measured on real request)"
            );
        }
    }
}

/// Counts the holders of a thread shared by every server serving with metrics, so the thread runs
/// exactly while someone holds it. The first holder spawns it and the last one to go takes it to
/// stop; the spawn is handed in and the stop is the caller's.
struct Holders<T>(Option<(NonZeroUsize, T)>);

#[derive(Debug, PartialEq)]
enum ReleaseOutcome<T> {
    Held,
    /// The last holder went, and the caller stops the thread.
    Last(T),
    /// A release with no acquire before it is the caller's bug.
    NotHeld,
}

impl<T> Holders<T> {
    const fn new() -> Self {
        Holders(None)
    }

    /// Adds a holder, the first of which spawns the thread with `spawn`. A failed spawn is
    /// returned and holds nothing, so the next holder tries again.
    fn acquire<E>(&mut self, spawn: impl FnOnce() -> Result<T, E>) -> Result<(), E> {
        match &mut self.0 {
            Some((users, _)) => *users = users.saturating_add(1),
            None => self.0 = Some((NonZeroUsize::MIN, spawn()?)),
        }
        Ok(())
    }

    fn release(&mut self) -> ReleaseOutcome<T> {
        let Some((users, thread)) = self.0.take() else {
            return ReleaseOutcome::NotHeld;
        };
        match NonZeroUsize::new(users.get() - 1) {
            Some(left) => {
                self.0 = Some((left, thread));
                ReleaseOutcome::Held
            }
            None => ReleaseOutcome::Last(thread),
        }
    }
}

/// Dropping the sender stops the sampler thread at once, because it waits on the receiver between
/// samples.
type SamplerThread = (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>);

static SAMPLER: parking_lot::Mutex<Holders<SamplerThread>> =
    parking_lot::Mutex::new(Holders::new());

/// Keeps RSS sampling on while held, one per serving run with `MIPYRE_METRICS=1`. When the last
/// holder drops it the thread is joined, because it must not outlive `Py_Finalize`, which may
/// unload this library under it, and `rss_bytes` reads `None` again rather than a stale value.
pub(crate) struct RssSamplingGuard(());

const RSS_DEGRADED: super::Component = super::Component::RssSampling;

impl RssSamplingGuard {
    /// Starts sampling for this holder.
    ///
    /// # Errors
    ///
    /// Returns the spawn error when the thread cannot be started, with the sampler not held; the
    /// caller decides to serve without RSS and say so.
    pub(crate) fn start() -> std::io::Result<Self> {
        SAMPLER.lock().acquire(|| -> std::io::Result<SamplerThread> {
            let (stop_tx, stop_rx) = std::sync::mpsc::channel();
            let handle = std::thread::Builder::new()
                .name("mipyre-rss-sampler".to_string())
                .spawn(move || sample_rss_until_stopped(stop_rx))?;
            Ok((stop_tx, handle))
        })?;
        Ok(RssSamplingGuard(()))
    }
}

impl Drop for RssSamplingGuard {
    fn drop(&mut self) {
        // The lock is held through the join, which is immediate because the thread wakes on the
        // disconnect, so a server starting meanwhile spawns its thread after this one's last write.
        let mut sampler = SAMPLER.lock();
        let (stop_tx, handle) = match sampler.release() {
            ReleaseOutcome::Last(thread) => thread,
            ReleaseOutcome::Held => return,
            ReleaseOutcome::NotHeld => {
                // Only a `start` that acquired makes a guard, so this is a bug here, and no thread
                // runs that needs stopping.
                tracing::error!(target: "mipyre::server",
                    "RSS sampler released with no holder counted; no sampler thread runs");
                return;
            }
        };
        drop(stop_tx);
        if let Err(panic) = handle.join() {
            tracing::error!(target: "mipyre::server", ?panic, "RSS sampler thread panicked");
        }
        *MEMORY_RSS_BYTES.lock() = None;
        clear_degraded(&RSS_DEGRADED);
    }
}

/// RSS does not change fast enough to warrant more frequent sampling.
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

fn sample_rss_until_stopped(stop: std::sync::mpsc::Receiver<()>) {
    // Only the first failed read after a good one, or at start, warns.
    let mut warn_on_failure = true;
    loop {
        let rss = match get_rss_bytes() {
            Ok(bytes) => {
                warn_on_failure = true;
                clear_degraded(&RSS_DEGRADED);
                Some(bytes)
            }
            Err(e) => {
                if warn_on_failure {
                    tracing::warn!(target: "mipyre::server", error = %e,
                        "RSS read failed; metrics report rss_bytes=None until a read succeeds");
                }
                set_degraded(RSS_DEGRADED, format!("RSS read failed: {e}"));
                warn_on_failure = false;
                None
            }
        };
        *MEMORY_RSS_BYTES.lock() = rss;
        // Only a disconnect, when the last holder dropped, ends the wait early.
        if stop.recv_timeout(RSS_SAMPLE_INTERVAL) != Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            break;
        }
    }
    tracing::debug!(target: "mipyre::server", "RSS sampler stopped");
}

fn get_rss_bytes() -> std::io::Result<u64> {
    #[cfg(target_os = "macos")]
    {
        let info = mach_task_self_info().map_err(|kr| {
            std::io::Error::other(format!(
                "task_info(MACH_TASK_BASIC_INFO) returned kern_return_t {kr}"
            ))
        })?;
        Ok(info.resident_size)
    }
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm")?;
        let pages = statm
            .split_whitespace()
            .nth(1)
            .and_then(|field| field.parse::<u64>().ok())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected /proc/self/statm contents {statm:?}"),
                )
            })?;
        // `/proc/self/statm` counts pages, which are 4 KiB on x86_64 but 16 or 64 KiB on aarch64
        // Linux, so the size is the one the kernel reports.
        Ok(pages * rustix::param::page_size() as u64)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "RSS sampling is implemented for Linux and macOS only",
        ))
    }
}

/// This is the C layout of the macOS `task_basic_info` record that `task_info` writes.
#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Default)]
struct libc_mach_task_basic_info {
    virtual_size: u64,
    resident_size: u64,
    resident_size_max: u64,
    user_time: [u32; 2],
    system_time: [u32; 2],
    policy: i32,
    suspend_count: i32,
}

/// This is the `<mach/task_info.h>` flavor for `mach_task_basic_info`.
#[cfg(target_os = "macos")]
const MACH_TASK_BASIC_INFO: u32 = 20;
/// The buffer length is in `natural_t` (u32) words, as `task_info` counts it.
#[cfg(target_os = "macos")]
const MACH_TASK_BASIC_INFO_COUNT: u32 =
    (std::mem::size_of::<libc_mach_task_basic_info>() / std::mem::size_of::<u32>()) as u32;
#[cfg(target_os = "macos")]
const KERN_SUCCESS: i32 = 0;

/// Reads this task's `MACH_TASK_BASIC_INFO`, or the `kern_return_t` the read failed with.
#[cfg(target_os = "macos")]
fn mach_task_self_info() -> Result<libc_mach_task_basic_info, i32> {
    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(task: u32, flavor: u32, info: *mut u8, count: *mut u32) -> i32;
    }
    let mut info = libc_mach_task_basic_info::default();
    let mut count = MACH_TASK_BASIC_INFO_COUNT;
    // SAFETY: the declarations match `<mach/task.h>` (`mach_port_t`, `task_flavor_t` and
    // `mach_msg_type_number_t` are u32, `kern_return_t` is i32); `mach_task_self` returns this
    // task's own port, which needs no release; `info` is writable for `count` natural_t words,
    // the size of the flavor's record, and `task_info` writes at most `count` words.
    let kr = unsafe {
        task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            (&mut info as *mut libc_mach_task_basic_info).cast::<u8>(),
            &mut count,
        )
    };
    if kr != KERN_SUCCESS {
        return Err(kr);
    }
    Ok(info)
}

/// A snapshot of the engine's counters, shown to Python as `pyapi`'s `Metrics`. Reading one has
/// no side effect; only `reset_peaks()` clears the peaks.
#[derive(Debug, Clone)]
pub(crate) struct Counters {
    pub(crate) gil_wait_last_us: u64,
    /// The longest GIL wait since the last `reset_peaks()`.
    pub(crate) gil_wait_peak_us: u64,
    /// The number of handler calls whose GIL wait was measured.
    pub(crate) gil_wait_count: u64,
    pub(crate) gil_wait_total_us: u64,
    pub(crate) gil_queue_length: isize,
    /// The longest handler GIL hold since the last `reset_peaks()`.
    pub(crate) gil_hold_peak_us: u64,
    /// It is `None` while the sampler has no successful read.
    pub(crate) rss_bytes: Option<u64>,
    /// Every refusal counts here, a request the server answered without Python: 503 overloaded
    /// or shutting down, 504 past the request budget, 502 a process-lane worker died, 500 a worker
    /// dropped the request.
    pub(crate) dropped_requests: u64,
    /// It is `None` while hot-path metrics are off (`MIPYRE_METRICS` unset).
    pub(crate) total_requests: Option<u64>,
    pub(crate) workers_serving: i64,
    /// Workers count here when they stop on their own (thread state not bound, engine failed,
    /// retired) while the server runs.
    pub(crate) workers_died: u64,
    /// Each `gc.collect()` that raised or panicked counts here and retired its worker.
    pub(crate) gc_failures: u64,
    /// A retired worker takes no more requests.
    pub(crate) workers_retired: u64,
    pub(crate) last_worker_retirement: Option<String>,
    /// A pre-compressed static file that could not be read counts here, and the original was
    /// served.
    pub(crate) precompressed_fallbacks: u64,
    /// A static mount counts here when its file watch failed or was lost, so it is served uncached.
    pub(crate) static_unwatched_mounts: u64,
    /// An async database result counts here when its asyncio loop was closed.
    pub(crate) db_results_undeliverable: u64,
    /// A GIL wait counts here when it is past the congestion threshold of 50 ms.
    pub(crate) gil_congested_waits: u64,
    /// It is `None` when the OS gave the CPU counts the server is sized from.
    pub(crate) cpu_sizing_fallback: Option<String>,
    /// A peer going away (reset, aborted, closed mid-IO) counts here; one abandoned connection
    /// can count twice, for its body read and its response write.
    pub(crate) client_disconnects: u64,
    /// It counts accept() failures other than resource exhaustion or a peer going away.
    pub(crate) accept_failures: u64,
    /// It counts accept() failures for lack of file descriptors, sockets or kernel buffers.
    pub(crate) accept_exhausted: u64,
    /// It counts failed TLS handshakes: bad ClientHellos, probes, slow clients timing out.
    pub(crate) tls_handshake_failures: u64,
    /// It counts connections closed with a 400 because the client sent no HTTP/1 request
    /// (garbage, a bad request line or header).
    pub(crate) malformed_requests: u64,
    /// It counts connections closed with a 414 or 431 because the request head was past the
    /// limits.
    pub(crate) oversized_request_heads: u64,
    /// It counts connections closed because the client sent no first request within the
    /// request-head timeout (nothing, part or all of the HTTP/2 preface, part of a head).
    pub(crate) first_request_timeouts: u64,
    /// It counts kept-alive HTTP/1 connections closed because no next request head came within
    /// the request-head timeout.
    pub(crate) request_head_timeouts: u64,
    /// It counts connections without TCP_QUICKACK, where the delayed-ACK optimization is off.
    pub(crate) quickack_failures: u64,
    /// It counts listeners without TCP_DEFER_ACCEPT, where the cold-connect mitigation is off.
    pub(crate) defer_accept_failures: u64,
    /// It counts connections without TCP_NODELAY, which keep Nagle's algorithm.
    pub(crate) nodelay_failures: u64,
    pub(crate) cpu_pin_failures: u64,
    /// It counts macOS threads whose QoS class could not be raised, so they may run on efficiency
    /// cores.
    pub(crate) cpu_qos_failures: u64,
    /// It counts failed reads of the process's CPU affinity, after which no thread was pinned.
    pub(crate) cpu_affinity_unreadable: u64,
    /// It counts failed reads of the physical core topology, after which no thread was pinned.
    pub(crate) cpu_topology_unreadable: u64,
    /// It counts starts with more TPC threads than CPUs to pin them to; the extra threads run
    /// unpinned.
    pub(crate) cpu_threads_beyond_cpus: u64,
    /// It counts drains of stopped accept loops that ran out of time.
    pub(crate) drain_timeouts: u64,
    /// It counts the connections such a drain closed when it gave up.
    pub(crate) drain_closed_connections: u64,
    /// It counts stop watchers, the threads that stop a TPC run on SIGINT or SIGTERM, that
    /// panicked or were still running when their run ended.
    pub(crate) stop_watcher_failures: u64,
    /// It counts blocking calls, the main-interpreter handlers, still running when their TPC
    /// thread's runtime shut down; they were left running.
    pub(crate) blocking_calls_left: u64,
    /// Each component running degraded is one `key: reason` line.
    pub(crate) degraded: Vec<String>,
}

/// Reads every counter without resetting anything.
pub(crate) fn read_counters() -> Counters {
    let degradations = degraded::read();
    let runs = super::run_counters::process_counts();
    // This is `None` when the latest run does not count requests, or none ran.
    let total_requests = runs.as_ref().and_then(|runs| runs.requests);
    let runs = runs.unwrap_or_default();
    Counters {
        gil_wait_last_us: GIL_LATENCY_LAST_US.load(Ordering::Relaxed),
        gil_wait_peak_us: GIL_LATENCY_MAX_US.load(Ordering::Relaxed),
        gil_wait_count: GIL_PROBE_COUNT.load(Ordering::Relaxed),
        gil_wait_total_us: GIL_TOTAL_WAIT_US.load(Ordering::Relaxed),
        gil_queue_length: GIL_QUEUE_LENGTH.load(Ordering::Relaxed),
        gil_hold_peak_us: GIL_HOLD_MAX_US.load(Ordering::Relaxed),
        rss_bytes: *MEMORY_RSS_BYTES.lock(),
        dropped_requests: runs.refused,
        total_requests,
        workers_serving: runs.workers_serving,
        workers_died: runs.workers_died,
        gc_failures: runs.gc_failures,
        workers_retired: runs.workers_retired,
        last_worker_retirement: runs.last_worker_retirement,
        precompressed_fallbacks: static_files::total(StaticDegradation::PrecompressedFallback),
        static_unwatched_mounts: static_files::total(StaticDegradation::UnwatchedMount),
        db_results_undeliverable: degradations.db_results_undeliverable,
        gil_congested_waits: degradations.gil_congested_waits,
        cpu_sizing_fallback: degradations.cpu_sizing_fallback,
        client_disconnects: tcp_events::total(TcpEvent::PeerGoneInQueue) + stream_events::total(StreamEvent::PeerGone),
        accept_failures: tcp_events::total(TcpEvent::AcceptFailed) + stream_events::total(StreamEvent::RegisterFailed),
        accept_exhausted: tcp_events::total(TcpEvent::AcceptExhausted),
        tls_handshake_failures: tls_events::total(TlsEvent::HandshakeFailed),
        malformed_requests: http_events::total(HttpEvent::MalformedRequest),
        oversized_request_heads: http_events::total(HttpEvent::OversizedRequestHead),
        first_request_timeouts: http_events::total(HttpEvent::FirstRequestTimedOut),
        request_head_timeouts: http_events::total(HttpEvent::RequestHeadTimedOut),
        quickack_failures: tcp_events::total(TcpEvent::QuickackFailed),
        defer_accept_failures: tcp_events::total(TcpEvent::DeferAcceptFailed),
        nodelay_failures: tcp_events::total(TcpEvent::NodelayFailed),
        cpu_pin_failures: runs.cpu_pin_failures,
        cpu_qos_failures: runs.cpu_qos_failures,
        cpu_affinity_unreadable: runs.cpu_affinity_unreadable,
        cpu_topology_unreadable: runs.cpu_topology_unreadable,
        cpu_threads_beyond_cpus: runs.cpu_threads_beyond_cpus,
        drain_timeouts: runs.drain_timeouts,
        drain_closed_connections: runs.drain_closed_connections,
        stop_watcher_failures: runs.stop_watcher_failures,
        blocking_calls_left: runs.blocking_calls_left,
        degraded: super::degraded_list(),
    }
}

/// Clear the GIL wait and hold peaks, starting a new peak window.
#[pyfunction]
pub(crate) fn reset_peaks() {
    GIL_LATENCY_MAX_US.store(0, Ordering::Relaxed);
    GIL_HOLD_MAX_US.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_holders_spawn_one_thread_and_only_the_last_release_stops_it() {
        let mut holders = Holders::new();
        let mut spawns = 0;
        let mut spawn = || -> Result<&str, ()> {
            spawns += 1;
            Ok("the sampler")
        };
        holders.acquire(&mut spawn).unwrap();
        holders.acquire(&mut spawn).unwrap();
        assert_eq!(spawns, 1);
        assert_eq!(holders.release(), ReleaseOutcome::Held);
        assert_eq!(holders.release(), ReleaseOutcome::Last("the sampler"));
        assert_eq!(holders.release(), ReleaseOutcome::NotHeld);
    }

    #[test]
    fn a_failed_spawn_holds_nothing_and_the_next_holder_spawns_again() {
        let mut holders = Holders::new();
        let refused = holders.acquire(|| Err::<u8, _>(std::io::Error::other("EAGAIN: threads")));
        assert_eq!(refused.unwrap_err().to_string(), "EAGAIN: threads");
        assert_eq!(holders.release(), ReleaseOutcome::NotHeld);
        holders.acquire(|| Ok::<_, std::io::Error>(7)).unwrap();
        assert_eq!(holders.release(), ReleaseOutcome::Last(7));
    }
}
