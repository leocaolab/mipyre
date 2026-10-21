//! Says how a TPC start went: the banner, and what the pin order means for the threads.

use std::num::NonZeroUsize;

use mipyre_cpu::{CoreId, PinOrder, PinOrderError};
use mipyre_http::plan::route::PlaceCounts;
use mipyre_tpc::Placement;

use crate::telemetry::RunCounters;

/// Renders the banner before the server is handed to its threads; [`log_startup`] prints it once
/// they have all started. The GIL server passes `has_workers` so the banner does not say it has
/// no sub-interpreters.
pub(crate) fn tpc_banner(
    threads: usize,
    logical_cpus: NonZeroUsize,
    pin_order: &Result<PinOrder, PinOrderError>,
    has_workers: bool,
    async_workers: usize,
    counts: PlaceCounts,
) -> String {
    let mut banner = format!("  {}\n", tpc_threads_line(threads, logical_cpus.get(), pin_order));
    // An async route gives every thread a worker too, so without workers there is no async pool.
    if !has_workers {
        banner.push_str("  Sub-interpreters: none (no route runs on one)\n");
    }
    if async_workers > 0 {
        banner.push_str(&format!("  Async workers: {async_workers} (async def routes)\n"));
    }
    let stream_count = counts.main_streamed;
    banner.push_str(&format!(
        "  Routes: {} sub-interp + {} GIL + {} process + {} async{}\n",
        counts.worker_sync,
        counts.main(),
        counts.lane,
        counts.worker_async,
        if stream_count > 0 {
            format!(" ({stream_count} stream)")
        } else {
            String::new()
        }
    ));
    banner
}

/// Logs the start and also prints it to stdout.
pub(super) fn log_startup(mode: &str, listening: &[String], banner: &str) {
    tracing::info!(
        target: "mipyre::server",
        version = env!("CARGO_PKG_VERSION"),
        mode,
        tpc = true,
        listening = %listening.join(", "),
        banner = banner.trim_end(),
        "Mipyre started"
    );
    println!(
        "\n  Mipyre v{} [TPC mode, {mode}]",
        env!("CARGO_PKG_VERSION")
    );
    crate::serve::print_listening(listening);
    println!("{banner}");
}

/// Returns the CPUs the TPC threads are pinned to, in thread order. When the OS does not say which
/// CPUs (a container with `/sys` masked, an unreadable affinity mask), the threads run unpinned
/// rather than fail the start; [`report_pinning`] says so.
pub(crate) fn thread_cpus(pin_order: &Result<PinOrder, PinOrderError>) -> Vec<CoreId> {
    match pin_order {
        Ok(PinOrder::Pinned { cpus, .. }) => cpus.cpus().to_vec(),
        Ok(PinOrder::Unsupported) | Err(_) => Vec::new(),
    }
}

/// Logs and counts, once per start, what the pin order means for `threads` TPC threads. CPUs the OS
/// would not name also mark the server degraded; a platform without pinning says nothing.
pub(crate) fn report_pinning(
    pin_order: &Result<PinOrder, PinOrderError>,
    threads: usize,
    counters: &RunCounters,
) {
    match pin_order {
        Ok(PinOrder::Pinned {
            cpus,
            cache_unread,
            perf_unread,
        }) => {
            if let Some(e) = cache_unread {
                tracing::warn!(target: "mipyre::server", error = %e,
                    "could not read the L3 domains; pinning as one domain");
            }
            if let Some(e) = perf_unread {
                tracing::warn!(target: "mipyre::server", error = %e,
                    "could not read the performance tiers; pinning as one tier");
            }
            let cpus = cpus.cpus().len();
            if threads > cpus {
                counters.threads_beyond_cpus();
                tracing::warn!(
                    target: "mipyre::server",
                    threads,
                    cpus,
                    unpinned = threads - cpus,
                    "more TPC threads than CPUs to pin them to; the threads past the CPU list run unpinned"
                );
            }
        }
        Ok(PinOrder::Unsupported) => {}
        Err(error) => {
            counters.unpinned_start(error);
            let os_error = error.os_error();
            tracing::warn!(
                target: "mipyre::server",
                error = %error,
                error_kind = ?os_error.map(std::io::Error::kind),
                os_errno = ?os_error.and_then(std::io::Error::raw_os_error),
                "could not read which CPUs to pin the TPC threads to; they run unpinned"
            );
            crate::telemetry::set_degraded(crate::telemetry::Component::CpuPinning, format!("threads not pinned: {error}"));
        }
    }
}

fn tpc_threads_line(
    threads: usize,
    logical_cpus: usize,
    pin_order: &Result<PinOrder, PinOrderError>,
) -> String {
    let pinned = match pin_order {
        Ok(PinOrder::Pinned { cpus, .. }) if cpus.cpus().len() >= threads => "pinned".to_owned(),
        Ok(PinOrder::Pinned { cpus, .. }) => {
            format!("pinned {} of {threads}, the rest run unpinned", cpus.cpus().len())
        }
        Ok(PinOrder::Unsupported) => "not pinned: this platform has no CPU pinning".to_owned(),
        Err(error) => format!("not pinned: {error}"),
    };
    format!("TPC threads: {threads} (CPUs: {logical_cpus}, {pinned})")
}

/// Pairs thread `i` with the `i`-th of `cpus`. Threads past the end of the list run unpinned and
/// the OS places them, rather than two threads sharing one CPU's caches.
pub(crate) fn with_cores<T>(threads: Vec<T>, cpus: &[CoreId]) -> Vec<(T, Option<CoreId>)> {
    let mut cpus = cpus.iter().copied();
    threads.into_iter().map(|t| (t, cpus.next())).collect()
}

/// Counts and logs what pinning this thread and raising its QoS class came to; a thread that
/// could not be placed keeps serving where it is.
pub(super) fn report_placement(placement: &Placement, counters: &RunCounters) {
    if let Err(failure) = &placement.pin {
        report_pin_failure(failure, counters);
    }
    if let Err(failure) = &placement.qos {
        report_qos_failure(failure, counters);
    }
}

/// Counts and logs a thread that could not be pinned; the thread keeps serving unpinned.
fn report_pin_failure(failure: &mipyre_cpu::PinError, counters: &RunCounters) {
    counters.pin_failure();
    tracing::warn!(
        target: "mipyre::server",
        core = failure.core.id,
        error = %failure,
        os_errno = ?failure.os_error().and_then(std::io::Error::raw_os_error),
        "a thread could not be pinned to its CPU; it runs unpinned"
    );
}

/// Counts and logs a thread whose QoS class could not be raised on macOS, which may then put it
/// on the efficiency cores.
fn report_qos_failure(failure: &std::io::Error, counters: &RunCounters) {
    counters.qos_failure();
    tracing::warn!(
        target: "mipyre::server",
        error = %failure,
        os_errno = ?failure.raw_os_error(),
        "a thread's QoS class could not be raised; it may run on the efficiency cores"
    );
}

#[cfg(test)]
mod pin_tests {
    use super::*;
    use mipyre_cpu::CpuOrder;
    use std::io;
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

    /// Serializes the tests that touch the process's degraded map, so one test's entry is not
    /// another's.
    static PIN_TESTS: Mutex<()> = Mutex::new(());

    fn pin_lock() -> MutexGuard<'static, ()> {
        PIN_TESTS.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn ids(n: usize) -> Vec<CoreId> {
        (0..n).map(|id| CoreId { id }).collect()
    }

    fn pin_counters(run: &RunCounters) -> [u64; 4] {
        let c = run.read();
        [
            c.cpu_pin_failures,
            c.cpu_threads_beyond_cpus,
            c.cpu_affinity_unreadable,
            c.cpu_topology_unreadable,
        ]
    }

    /// Reports and then decides a start's pinning, as the server does.
    fn started(pin_order: &Result<PinOrder, PinOrderError>, threads: usize, run: &RunCounters) -> Vec<CoreId> {
        report_pinning(pin_order, threads, run);
        thread_cpus(pin_order)
    }

    fn pinned_to(cpus: &[CoreId]) -> Result<PinOrder, PinOrderError> {
        Ok(PinOrder::Pinned {
            cpus: CpuOrder::new(cpus.to_vec()).unwrap(),
            cache_unread: None,
            perf_unread: None,
        })
    }

    #[test]
    fn the_pin_order_decides_the_cpus_alone_and_unreadable_cpus_pin_nothing() {
        let pinned = Ok(PinOrder::Pinned {
            cpus: CpuOrder::new(ids(3)).unwrap(),
            cache_unread: None,
            perf_unread: None,
        });
        assert_eq!(thread_cpus(&pinned), ids(3));
        assert!(thread_cpus(&Ok(PinOrder::Unsupported)).is_empty());
        let affinity = PinOrderError::Affinity(io::Error::from_raw_os_error(libc::EPERM));
        assert!(thread_cpus(&Err(affinity)).is_empty());
    }

    #[test]
    fn as_many_threads_as_cpus_is_no_shortfall_and_one_more_is() {
        let cpus = ids(2);
        assert_eq!(
            with_cores(vec!["a", "b"], &cpus),
            vec![("a", Some(cpus[0])), ("b", Some(cpus[1]))]
        );
        let run = RunCounters::new(false);
        report_pinning(&pinned_to(&cpus), 2, &run);
        assert_eq!(pin_counters(&run), [0; 4]);
        assert_eq!(with_cores(vec!["a", "b", "c"], &cpus)[2], ("c", None));
        report_pinning(&pinned_to(&cpus), 3, &run);
        assert_eq!(run.read().cpu_threads_beyond_cpus, 1);
        // No CPU list at all is not a shortfall; that start is reported as unpinned instead.
        let run = RunCounters::new(false);
        report_pinning(&Ok(PinOrder::Unsupported), 2, &run);
        assert_eq!(run.read().cpu_threads_beyond_cpus, 0);
    }

    fn cpu_pinning_degraded() -> bool {
        crate::telemetry::degraded_list()
            .iter()
            .any(|line| line.starts_with("cpu_pinning:"))
    }

    #[test]
    fn a_platform_without_pinning_is_normal_not_degraded_and_counts_nothing() {
        let _lock = pin_lock();
        crate::telemetry::clear_degraded(&crate::telemetry::Component::CpuPinning);
        let run = RunCounters::new(false);
        let cpus = started(&Ok(PinOrder::Unsupported), 3, &run);
        assert!(cpus.is_empty());
        assert_eq!(
            with_cores(vec![1, 2, 3], &cpus),
            vec![(1, None), (2, None), (3, None)]
        );
        assert_eq!(pin_counters(&run), [0; 4]);
        assert!(!cpu_pinning_degraded());
    }

    #[test]
    fn an_unreadable_affinity_mask_is_degraded_counted_and_leaves_every_thread_unpinned() {
        let _lock = pin_lock();
        crate::telemetry::clear_degraded(&crate::telemetry::Component::CpuPinning);
        let run = RunCounters::new(false);
        let affinity = PinOrderError::Affinity(io::Error::from_raw_os_error(libc::EPERM));
        let cpus = started(&Err(affinity), 3, &run);
        assert!(cpus.is_empty());
        assert_eq!(
            with_cores(vec![1, 2, 3], &cpus),
            vec![(1, None), (2, None), (3, None)]
        );
        // Only `cpu_affinity_unreadable` moves.
        assert_eq!(pin_counters(&run), [0, 0, 1, 0]);
        assert!(cpu_pinning_degraded());
        crate::telemetry::clear_degraded(&crate::telemetry::Component::CpuPinning);
    }

    #[test]
    fn the_banner_line_says_all_partly_or_not_pinned_and_why() {
        let pinned = Ok(PinOrder::Pinned {
            cpus: CpuOrder::new(ids(4)).unwrap(),
            cache_unread: None,
            perf_unread: None,
        });
        assert_eq!(
            tpc_threads_line(4, 18, &pinned),
            "TPC threads: 4 (CPUs: 18, pinned)"
        );
        let partly = Ok(PinOrder::Pinned {
            cpus: CpuOrder::new(ids(2)).unwrap(),
            cache_unread: None,
            perf_unread: None,
        });
        assert_eq!(
            tpc_threads_line(4, 18, &partly),
            "TPC threads: 4 (CPUs: 18, pinned 2 of 4, the rest run unpinned)"
        );
        assert_eq!(
            tpc_threads_line(4, 18, &Ok(PinOrder::Unsupported)),
            "TPC threads: 4 (CPUs: 18, not pinned: this platform has no CPU pinning)"
        );
        let unreadable = Err(PinOrderError::Affinity(
            io::Error::from_raw_os_error(libc::EPERM),
        ));
        let line = tpc_threads_line(4, 18, &unreadable);
        assert!(
            line.starts_with("TPC threads: 4 (CPUs: 18, not pinned: could not read which CPUs"),
            "{line}"
        );
    }

    #[test]
    fn a_thread_with_no_pin_to_make_never_reports_a_pin_failure() {
        // A pin is refused on macOS, so only the count proves that no pin was attempted for the
        // `None` that `with_cores` gives when nothing is pinned.
        let _lock = pin_lock();
        let run = RunCounters::new(false);
        let cpus = started(&Ok(PinOrder::Unsupported), 1, &run);
        let (_, core) = with_cores(vec![()], &cpus).remove(0);
        assert_eq!(core, None);
        let placed = Arc::new(RunCounters::new(false));
        let counted = Arc::clone(&placed);
        std::thread::spawn(move || report_placement(&mipyre_tpc::pin_and_raise_qos(core), &counted))
            .join()
            .expect("the placing thread");
        assert_eq!(placed.read().cpu_pin_failures, 0, "a pin failure was counted");
    }

    #[test]
    fn threads_past_the_cpu_list_run_unpinned_and_the_start_is_counted_and_said() {
        // Three threads and two CPUs, as when `workers=` exceeds the CPUs this process may run on.
        let cpus = [CoreId { id: 4 }, CoreId { id: 5 }];
        let run = RunCounters::new(false);
        let log = crate::exchange::captured_log(|| {
            report_pinning(&pinned_to(&cpus), 3, &run);
            let paired = with_cores(vec!["a", "b", "c"], &cpus);
            assert_eq!(
                paired,
                vec![("a", Some(cpus[0])), ("b", Some(cpus[1])), ("c", None)]
            );
        });
        assert_eq!(run.read().cpu_threads_beyond_cpus, 1);
        let line = log
            .lines()
            .find(|l| l.contains("more TPC threads than CPUs"))
            .unwrap_or_else(|| panic!("no warning for the unpinned thread:\n{log}"));
        assert!(
            line.contains("\"threads\":3") && line.contains("\"cpus\":2"),
            "{line}"
        );
        assert!(line.contains("\"level\":\"WARN\""), "{line}");
    }

    #[test]
    fn as_many_cpus_as_threads_pins_every_thread_and_counts_nothing() {
        let cpus = [CoreId { id: 0 }, CoreId { id: 1 }];
        let run = RunCounters::new(false);
        let log = crate::exchange::captured_log(|| {
            report_pinning(&pinned_to(&cpus), 2, &run);
            assert_eq!(
                with_cores(vec![1, 2], &cpus),
                vec![(1, Some(cpus[0])), (2, Some(cpus[1]))]
            );
            // An empty list is reported as unpinned elsewhere, not as a shortfall.
            assert_eq!(with_cores(vec![1], &[]), vec![(1, None)]);
        });
        assert!(!log.contains("more TPC threads than CPUs"), "{log}");
        assert_eq!(pin_counters(&run), [0; 4]);
    }

    #[test]
    fn a_platform_without_pinning_pins_nothing_and_says_nothing() {
        let _lock = pin_lock();
        let log = crate::exchange::captured_log(|| {
            let cpus = started(&Ok(PinOrder::Unsupported), 1, &RunCounters::new(false));
            assert!(cpus.is_empty());
        });
        assert!(!log.contains("could not"), "{log}");
        assert!(!log.contains("unpinned"), "{log}");
    }

    #[test]
    fn cpus_that_cannot_be_read_leave_the_threads_unpinned_and_are_counted_by_cause() {
        let _lock = pin_lock();
        let run = RunCounters::new(false);
        let affinity = PinOrderError::Affinity(io::Error::from_raw_os_error(libc::EPERM));
        assert!(started(&Err(affinity), 1, &run).is_empty());
        assert_eq!(pin_counters(&run), [0, 0, 1, 0]);

        let topology = PinOrderError::Topology(mipyre_cpu::CpuListFileError::Os(mipyre_cpu::FileError {
            path: "/sys/devices/system/cpu/cpu0/topology/thread_siblings_list".into(),
            source: io::Error::from_raw_os_error(libc::ENOENT),
        }));
        let log = crate::exchange::captured_log(|| {
            assert!(started(&Err(topology), 1, &run).is_empty());
        });
        assert_eq!(pin_counters(&run), [0, 0, 1, 1]);
        let line = log.lines().find(|l| l.contains("thread_siblings_list")).expect(&log);
        assert!(line.contains(&format!("Some({})", libc::ENOENT)), "the errno is logged: {line}");
    }
}
