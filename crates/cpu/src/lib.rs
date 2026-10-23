//! Finds the CPUs this process may use and pins threads to them.

mod os;
// The topology and cgroup decisions have one caller, the Linux read path in os.rs; they compile
// everywhere so their tests run on every machine. Registered in dev/arch_waivers.txt (E-S3, P3).
#[cfg_attr(
    not(target_os = "linux"),
    expect(dead_code, reason = "their one caller reads Linux's sysfs and cgroups")
)]
mod plan;

pub use core_affinity::CoreId;
pub use os::{
    elevate_thread_qos_macos, read_cpu_quota, try_pin_current, CapacityFileError, CpuListFileError, CpuReadError,
    FileError, HostCpus, PinError, PinFault, PinOrder, PinOrderError, QuotaReadError, TopologyReadError,
};
pub use plan::{CpuMaxError, CpuMaxFault, CpuOrder, Cpus, PerfValueError, Quota, MAX_CPUS};

/// Holds a value on its own cache line, so one core's writes do not invalidate another's copy.
/// 128 bytes covers Apple silicon's 128-byte lines and x86's 64-byte lines, which it fetches in
/// pairs. `repr(C)` keeps the layout fixed in memory shared between processes.
#[repr(C, align(128))]
#[derive(Debug, Default)]
pub struct CacheLine<T>(pub T);

const _: () = assert!(std::mem::align_of::<CacheLine<std::sync::atomic::AtomicU64>>() == 128);

impl<T> std::ops::Deref for CacheLine<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
