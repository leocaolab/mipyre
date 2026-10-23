//! Counts what a bench's servers did over a measured window: requests, and on a profile also
//! instructions and allocations.

use std::time::{Duration, Instant};

use crate::sync::{AtomicU64, Ordering};

/// Is served but not measured, so every connection is past its probe when counting starts.
const WARMUP: Duration = Duration::from_secs(1);

/// Counts the requests completed within the measured window.
pub(crate) struct Measurement {
    pub(crate) requests: u64,
    pub(crate) elapsed: Duration,
}

/// Holds what each profiling window counted; each measure runs in its own window because
/// counting allocations costs instructions.
pub(crate) struct ProfileReport {
    /// Counts instructions retired; it is `None` off macOS.
    pub(crate) instructions: Option<Window<u64>>,
    pub(crate) allocs: Window<crate::bench::alloc::Allocations>,
    /// Holds the allocations with their stacks; it is `None` unless stacks were asked for.
    pub(crate) stacks: Option<Window<crate::bench::alloc::Allocations>>,
}

/// Holds the requests served in one measurement window and what it counted.
pub(crate) struct Window<T> {
    pub(crate) requests: u64,
    pub(crate) counted: T,
}

/// Says whether a profile also records the call stacks that allocated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProfileDepth {
    Counts,
    WithStacks,
}

pub(crate) fn measure(counter: &AtomicU64, duration: Duration) -> Measurement {
    std::thread::sleep(WARMUP);
    let start = counter.load(Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(duration);
    Measurement {
        requests: counter.load(Ordering::Relaxed) - start,
        elapsed: t0.elapsed(),
    }
}

/// Runs [`measure`]'s window, then an allocation window and optionally a stack window, each
/// `duration` long.
pub(crate) fn measure_profiled(
    counter: &AtomicU64,
    duration: Duration,
    depth: ProfileDepth,
) -> (Measurement, ProfileReport) {
    std::thread::sleep(WARMUP);
    let window = |f: &mut dyn FnMut()| {
        let start = counter.load(Ordering::Relaxed);
        f();
        counter.load(Ordering::Relaxed) - start
    };

    let t0 = Instant::now();
    let i0 = process_instructions();
    let requests = window(&mut || std::thread::sleep(duration));
    let instructions = i0.zip(process_instructions()).map(|(a, b)| Window {
        requests,
        counted: b - a,
    });
    let elapsed = t0.elapsed();

    crate::bench::alloc::start_counting();
    let alloc_requests = window(&mut || std::thread::sleep(duration));
    let allocs = Window {
        requests: alloc_requests,
        counted: crate::bench::alloc::stop(),
    };

    let stacks = matches!(depth, ProfileDepth::WithStacks).then(|| {
        crate::bench::alloc::start_recording_stacks();
        let n = window(&mut || std::thread::sleep(duration));
        Window {
            requests: n,
            counted: crate::bench::alloc::stop(),
        }
    });
    (
        Measurement { requests, elapsed },
        ProfileReport {
            instructions,
            allocs,
            stacks,
        },
    )
}

/// Returns the instructions this process has retired.
#[cfg(target_os = "macos")]
fn process_instructions() -> Option<u64> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::zeroed();
    // SAFETY: `info` is a writable `rusage_info_v4`, the struct `RUSAGE_INFO_V4` fills.
    let rc = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            info.as_mut_ptr() as *mut libc::rusage_info_t,
        )
    };
    // SAFETY: `proc_pid_rusage` filled `info` because it returned 0.
    (rc == 0).then(|| unsafe { info.assume_init() }.ri_instructions)
}

#[cfg(not(target_os = "macos"))]
fn process_instructions() -> Option<u64> {
    None
}
