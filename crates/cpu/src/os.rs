//! Reads and sets CPU state through each platform's APIs; this is the only file with `#[cfg]`.

#[cfg(any(target_os = "linux", test))]
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::num::{NonZeroUsize, ParseIntError};
#[cfg(target_os = "linux")]
use std::path::Component;
#[cfg(any(target_os = "linux", test))]
use std::path::Path;
use std::path::PathBuf;

use core_affinity::CoreId;

#[cfg(any(target_os = "linux", test))]
use crate::plan::{decide, parse_cpu_max, perf_gap, perf_value, CoreGroups, CpuFacts, CpuSet, Facts};
#[cfg(target_os = "linux")]
use crate::plan::{parse_cpu_list, tightest, CpuMask, MAX_CPUS};
use crate::plan::{threads_for, CpuListError, CpuMaskError, CpuMaxError, CpuOrder, Cpus, PerfValueError, Quota};

/// Holds the counts the threads are sized from and the CPUs they are pinned to. On Linux the
/// physical cores and the pin order come from one read, so they cannot disagree; the logical
/// count is the OS's `available_parallelism`, read on its own.
#[derive(Debug)]
pub struct HostCpus {
    pub cpus: Result<Cpus, CpuReadError>,
    pub pin_order: Result<PinOrder, PinOrderError>,
}

/// macOS is [`Unsupported`](Self::Unsupported): its affinity tags are cache hints, not CPUs.
#[derive(Debug)]
pub enum PinOrder {
    /// Thread `i` goes to the `i`-th CPU; threads past the end are not pinned.
    Pinned {
        cpus: CpuOrder,
        /// Set when the cache domains were not read: one domain was assumed.
        cache_unread: Option<TopologyReadError>,
        /// Set when the performance tiers were not read: one tier was assumed.
        perf_unread: Option<TopologyReadError>,
    },
    Unsupported,
}

#[derive(Debug)]
pub enum TopologyReadError {
    /// The L3 lists, or a hybrid host's list of P-cores.
    CpuList(CpuListFileError),
    Capacity(CapacityFileError),
    NotReadHere,
    Partial(PerfValueError),
}

impl fmt::Display for TopologyReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopologyReadError::CpuList(e) => write!(f, "{e}"),
            TopologyReadError::Capacity(e) => write!(f, "{e}"),
            TopologyReadError::NotReadHere => {
                write!(f, "this platform does not read the CPU topology")
            }
            TopologyReadError::Partial(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Debug)]
pub enum CpuReadError {
    Logical(io::Error),
    /// The physical cores come from the pin-order read, which failed; its error is in
    /// [`HostCpus::pin_order`].
    Cores {
        logical: NonZeroUsize,
    },
    /// The physical cores were counted on their own, and that failed.
    CoreCount {
        logical: NonZeroUsize,
        source: io::Error,
    },
    /// The physical cores are not counted on this platform, so no read failed.
    CoresNotCountedHere {
        logical: NonZeroUsize,
    },
}

impl fmt::Display for CpuReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CpuReadError::Logical(e) => {
                write!(f, "the OS did not say how many CPUs this process may use: {e}")
            }
            CpuReadError::Cores { logical } => write!(
                f,
                "could not read this process's physical cores ({logical} logical CPUs): they come \
                 from the read of the CPUs to pin to, which failed"
            ),
            CpuReadError::CoreCount { logical, source } => write!(
                f,
                "could not read this process's physical cores ({logical} logical CPUs): {source}"
            ),
            CpuReadError::CoresNotCountedHere { logical } => write!(
                f,
                "this platform does not count physical cores ({logical} logical CPUs)"
            ),
        }
    }
}

impl std::error::Error for CpuReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CpuReadError::Logical(e) | CpuReadError::CoreCount { source: e, .. } => Some(e),
            CpuReadError::Cores { .. } | CpuReadError::CoresNotCountedHere { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum PinOrderError {
    Affinity(io::Error),
    /// core_affinity listed no CPUs for this process and gives no reason.
    AffinityNotListed,
    EmptyMask,
    /// A CPU's SMT siblings could not be read.
    Topology(CpuListFileError),
}

impl PinOrderError {
    pub fn os_error(&self) -> Option<&io::Error> {
        match self {
            PinOrderError::Affinity(e) => Some(e),
            PinOrderError::Topology(e) => e.os_error(),
            PinOrderError::AffinityNotListed | PinOrderError::EmptyMask => None,
        }
    }
}

impl fmt::Display for PinOrderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinOrderError::Affinity(e) => {
                write!(f, "could not read which CPUs this process may run on: {e}")
            }
            PinOrderError::AffinityNotListed => {
                write!(
                    f,
                    "could not read which CPUs this process may run on; core_affinity gives no reason"
                )
            }
            PinOrderError::EmptyMask => {
                write!(f, "the CPU affinity mask of this process lists no CPU")
            }
            PinOrderError::Topology(e) => {
                write!(f, "could not read which CPUs are one physical core: {e}")
            }
        }
    }
}

impl std::error::Error for PinOrderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PinOrderError::Affinity(e) => Some(e),
            PinOrderError::Topology(e) => Some(e),
            PinOrderError::AffinityNotListed | PinOrderError::EmptyMask => None,
        }
    }
}

/// A file that could not be read; the OS error is kept whole.
#[derive(Debug)]
pub struct FileError {
    pub path: PathBuf,
    pub source: io::Error,
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "could not read {}: {}", self.path.display(), self.source)
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// A CPU list in sysfs (a core's SMT siblings, an L3 domain, a hybrid host's P-cores) that
/// could not be used.
#[derive(Debug)]
pub enum CpuListFileError {
    Os(FileError),
    Malformed { path: PathBuf, source: CpuListError },
}

impl CpuListFileError {
    pub fn os_error(&self) -> Option<&io::Error> {
        match self {
            CpuListFileError::Os(e) => Some(&e.source),
            CpuListFileError::Malformed { .. } => None,
        }
    }
}

impl fmt::Display for CpuListFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CpuListFileError::Os(e) => write!(f, "{e}"),
            CpuListFileError::Malformed { path, source } => {
                write!(f, "{}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for CpuListFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CpuListFileError::Os(e) => Some(e),
            CpuListFileError::Malformed { source, .. } => Some(source),
        }
    }
}

/// A CPU's `cpu_capacity` that could not be used.
#[derive(Debug)]
pub enum CapacityFileError {
    Os(FileError),
    Malformed {
        path: PathBuf,
        text: String,
        source: ParseIntError,
    },
}

impl fmt::Display for CapacityFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapacityFileError::Os(e) => write!(f, "{e}"),
            CapacityFileError::Malformed { path, text, source } => {
                write!(f, "{}: {text:?} is not a capacity: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for CapacityFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CapacityFileError::Os(e) => Some(e),
            CapacityFileError::Malformed { source, .. } => Some(source),
        }
    }
}

#[derive(Debug)]
pub enum QuotaReadError {
    ProcCgroup(io::Error),
    NotCgroupV2 {
        text: String,
    },
    /// A missing `cpu.max` is not this error: that level has no limit.
    CpuMax {
        path: String,
        source: io::Error,
    },
    Malformed {
        path: String,
        source: CpuMaxError,
    },
    /// The cgroup path climbs out of the cgroup root with `..`, so no `cpu.max` above it is read.
    OutsideNamespace {
        group: String,
    },
    /// The group has no directory under the cgroup root, as on a host that mounts the v2 tree
    /// elsewhere (v1 and v2 side by side), so the quota is unknown rather than none.
    NoGroupDir {
        group: String,
        dir: String,
    },
}

impl fmt::Display for QuotaReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuotaReadError::ProcCgroup(e) => write!(f, "could not read /proc/self/cgroup: {e}"),
            QuotaReadError::NotCgroupV2 { text } => write!(
                f,
                "/proc/self/cgroup has no cgroup v2 line (cpu.max is v2-only): {text:?}"
            ),
            QuotaReadError::CpuMax { path, source } => {
                write!(f, "could not read {path}: {source}")
            }
            QuotaReadError::Malformed { path, source } => write!(f, "{path}: {source}"),
            QuotaReadError::OutsideNamespace { group } => write!(
                f,
                "/proc/self/cgroup names the group {group:?}, which climbs out of the cgroup root \
                 with `..`; no cpu.max is read for it"
            ),
            QuotaReadError::NoGroupDir { group, dir } => write!(
                f,
                "/proc/self/cgroup names the group {group:?}, but {dir} does not exist: the \
                 cgroup v2 tree is not mounted there (v1 and v2 mounted side by side?), so its \
                 cpu.max is not read"
            ),
        }
    }
}

impl std::error::Error for QuotaReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            QuotaReadError::ProcCgroup(e) | QuotaReadError::CpuMax { source: e, .. } => Some(e),
            QuotaReadError::Malformed { source, .. } => Some(source),
            QuotaReadError::NotCgroupV2 { .. }
            | QuotaReadError::OutsideNamespace { .. }
            | QuotaReadError::NoGroupDir { .. } => None,
        }
    }
}

/// An error from pinning a thread; the thread keeps running where it was.
#[derive(Debug)]
pub struct PinError {
    pub core: CoreId,
    pub fault: PinFault,
}

impl PinError {
    pub fn os_error(&self) -> Option<&io::Error> {
        match &self.fault {
            PinFault::Os(e) => Some(e),
            PinFault::NoRoom(_) | PinFault::Unsupported | PinFault::Refused => None,
        }
    }
}

impl fmt::Display for PinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "could not pin the thread to CPU {}: {}", self.core.id, self.fault)
    }
}

impl std::error::Error for PinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.fault {
            PinFault::NoRoom(e) => Some(e),
            PinFault::Os(e) => Some(e),
            PinFault::Unsupported | PinFault::Refused => None,
        }
    }
}

#[derive(Debug)]
pub enum PinFault {
    /// The CPU is at or past [`MAX_CPUS`](crate::MAX_CPUS), so no affinity mask holds it.
    NoRoom(CpuMaskError),
    Os(io::Error),
    /// The platform has no CPU pinning.
    Unsupported,
    /// `core_affinity` refused, and it gives no OS error code.
    Refused,
}

impl fmt::Display for PinFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinFault::NoRoom(e) => write!(f, "{e}"),
            PinFault::Os(e) => write!(f, "{e}"),
            PinFault::Unsupported => write!(f, "this platform has no CPU pinning (macOS keeps no thread affinity)"),
            PinFault::Refused => write!(f, "core_affinity::set_for_current refused (it gives no OS error code)"),
        }
    }
}

pub fn try_pin_current(core_id: Option<CoreId>) -> Result<(), PinError> {
    let Some(core) = core_id else {
        return Ok(());
    };
    pin_current(core).map_err(|fault| PinError { core, fault })
}

/// Threads started afterwards inherit the mask, and so do processes they fork.
#[cfg(target_os = "linux")]
fn pin_current(core: CoreId) -> Result<(), PinFault> {
    let mask = CpuMask::of_one(core.id).map_err(PinFault::NoRoom)?;
    // SAFETY: pid 0 is the calling thread; the kernel reads `mask.bytes()` from the mask's
    // words, which outlive the call.
    let rc = unsafe { libc::sched_setaffinity(0, mask.bytes(), mask.words().as_ptr().cast::<libc::cpu_set_t>()) };
    if rc != 0 {
        return Err(PinFault::Os(io::Error::last_os_error()));
    }
    Ok(())
}

/// macOS has no CPU pinning: the affinity tag is only an L2-sharing hint, arm64 refuses it,
/// and tag 0 means "no tag", so CPU 0 could never be tagged. Use the QoS class instead.
#[cfg(target_os = "macos")]
fn pin_current(_core: CoreId) -> Result<(), PinFault> {
    Err(PinFault::Unsupported)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn pin_current(core: CoreId) -> Result<(), PinFault> {
    if core_affinity::set_for_current(core) {
        Ok(())
    } else {
        Err(PinFault::Refused)
    }
}

/// Keeps the thread on performance cores: an efficiency core runs about a third as fast.
#[cfg(target_os = "macos")]
pub fn elevate_thread_qos_macos() -> io::Result<()> {
    // SAFETY: sets the calling thread's own QoS class; no pointers.
    let rc = unsafe { libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0) };
    if rc == 0 {
        return Ok(());
    }
    // pthread returns the errno instead of setting it.
    let errno = io::Error::from_raw_os_error(rc);
    Err(io::Error::new(
        errno.kind(),
        format!("pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE): {errno}"),
    ))
}

#[cfg(not(target_os = "macos"))]
#[inline(always)]
pub fn elevate_thread_qos_macos() -> io::Result<()> {
    Ok(())
}

/// Counts cores within the affinity mask, not across sysfs: sysfs lists every host CPU, so a
/// container on 32 of 64 cores got 64 threads, half of them on the other half's SMT siblings.
#[cfg(target_os = "linux")]
impl HostCpus {
    pub fn read(quota: Quota) -> Self {
        let logical = std::thread::available_parallelism();
        let read = allowed_cpus()
            .map_err(PinOrderError::Affinity)
            .and_then(|allowed| read_order(&allowed));
        let cores = match &read {
            Ok((cores, _)) => Some(threads_for(cores.top_count(), quota)),
            Err(_) => None,
        };
        HostCpus {
            cpus: counted(logical, cores),
            pin_order: read.map(|(_, pin_order)| pin_order),
        }
    }
}

/// Takes `cores` as `None` when the pin-order read failed; that read keeps its own error.
#[cfg(any(target_os = "linux", test))]
fn counted(logical: io::Result<NonZeroUsize>, cores: Option<NonZeroUsize>) -> Result<Cpus, CpuReadError> {
    match (logical, cores) {
        (Err(e), _) => Err(CpuReadError::Logical(e)),
        (Ok(logical), Some(cores)) => Ok(Cpus::new(logical, cores)),
        (Ok(logical), None) => Err(CpuReadError::Cores { logical }),
    }
}

#[cfg(not(target_os = "linux"))]
impl HostCpus {
    pub fn read(quota: Quota) -> Self {
        let cpus = std::thread::available_parallelism()
            .map_err(CpuReadError::Logical)
            .and_then(|logical| cores_counted(logical).map(|cores| Cpus::new(logical, threads_for(cores, quota))));
        HostCpus {
            cpus,
            pin_order: read_pinning(),
        }
    }
}

#[cfg(target_os = "macos")]
fn cores_counted(logical: NonZeroUsize) -> Result<NonZeroUsize, CpuReadError> {
    physical_cores().map_err(|source| CpuReadError::CoreCount { logical, source })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn cores_counted(logical: NonZeroUsize) -> Result<NonZeroUsize, CpuReadError> {
    Err(CpuReadError::CoresNotCountedHere { logical })
}

#[cfg(target_os = "macos")]
fn read_pinning() -> Result<PinOrder, PinOrderError> {
    Ok(PinOrder::Unsupported)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_pinning() -> Result<PinOrder, PinOrderError> {
    let ids = core_affinity::get_core_ids().ok_or(PinOrderError::AffinityNotListed)?;
    let cpus = CpuOrder::new(ids).ok_or(PinOrderError::EmptyMask)?;
    Ok(PinOrder::Pinned {
        cpus,
        cache_unread: Some(TopologyReadError::NotReadHere),
        perf_unread: Some(TopologyReadError::NotReadHere),
    })
}

#[cfg(target_os = "linux")]
fn read_cpu_list_file(path: PathBuf) -> Result<CpuSet, CpuListFileError> {
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(source) => return Err(CpuListFileError::Os(FileError { path, source })),
    };
    parse_cpu_list(&text).map_err(|source| CpuListFileError::Malformed { path, source })
}

#[cfg(target_os = "linux")]
fn cpu_file(cpu: CoreId, file: &str) -> PathBuf {
    PathBuf::from(format!("/sys/devices/system/cpu/cpu{}/{file}", cpu.id))
}

/// Returns `None` when there is no list: only a hybrid Intel host has `/sys/devices/cpu_core/cpus`.
#[cfg(target_os = "linux")]
fn read_p_cores() -> Result<Option<CpuSet>, CpuListFileError> {
    match read_cpu_list_file(PathBuf::from("/sys/devices/cpu_core/cpus")) {
        Ok(p_cores) => Ok(Some(p_cores)),
        Err(CpuListFileError::Os(e)) if e.source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Returns `None` when the CPU has no `cpu_capacity` file.
#[cfg(target_os = "linux")]
fn read_capacity(cpu: CoreId) -> Result<Option<u32>, CapacityFileError> {
    let path = cpu_file(cpu, "cpu_capacity");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(CapacityFileError::Os(FileError { path, source })),
    };
    match text.trim().parse::<u32>() {
        Ok(capacity) => Ok(Some(capacity)),
        Err(source) => Err(CapacityFileError::Malformed { path, text, source }),
    }
}

/// What one allowed CPU's sysfs files said. Its siblings must be read; the other readings are
/// used only when every CPU's could be.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug)]
struct CpuFiles {
    siblings: Vec<CoreId>,
    cache: Result<Vec<CoreId>, CpuListFileError>,
    /// `Ok(None)` when the CPU has no `cpu_capacity`, and on a host whose P-core list gives the
    /// tiers, where it is not read.
    capacity: Result<Option<u32>, CapacityFileError>,
}

/// Reads all of one CPU's files before the next CPU's, so its readings stay together. An
/// unreadable siblings file fails the read instead of making the CPU its own core: with `/sys`
/// masked in a container, that made every logical CPU a physical one.
#[cfg(target_os = "linux")]
fn read_order(allowed: &[CoreId]) -> Result<(CoreGroups, PinOrder), PinOrderError> {
    let p_cores = read_p_cores();
    let capacities_give_tiers = matches!(p_cores, Ok(None));
    let files = allowed
        .iter()
        .map(|&cpu| {
            let siblings = read_cpu_list_file(cpu_file(cpu, "topology/thread_siblings_list"))?;
            let files = CpuFiles {
                siblings: siblings.iter().collect(),
                cache: read_cpu_list_file(cpu_file(cpu, "cache/index3/shared_cpu_list"))
                    .map(|cpus| cpus.iter().collect()),
                capacity: if capacities_give_tiers {
                    read_capacity(cpu)
                } else {
                    Ok(None)
                },
            };
            Ok((cpu, files))
        })
        .collect::<Result<BTreeMap<_, _>, CpuListFileError>>()
        .map_err(PinOrderError::Topology)?;
    decided(files, p_cores)
}

/// Says why a reading is not used with the first error it met; later CPUs' errors are dropped.
#[cfg(any(target_os = "linux", test))]
fn decided(
    files: BTreeMap<CoreId, CpuFiles>,
    p_cores: Result<Option<CpuSet>, CpuListFileError>,
) -> Result<(CoreGroups, PinOrder), PinOrderError> {
    let (p_cores, mut perf_unread) = match p_cores {
        Ok(p_cores) => (p_cores, None),
        Err(e) => (None, Some(TopologyReadError::CpuList(e))),
    };
    let mut cache_unread = None;
    let mut cpus = BTreeMap::new();
    for (cpu, files) in files {
        let cache = match files.cache {
            Ok(list) => Some(list),
            Err(e) => {
                cache_unread.get_or_insert(TopologyReadError::CpuList(e));
                None
            }
        };
        let capacity = match files.capacity {
            Ok(capacity) => capacity,
            Err(e) => {
                perf_unread.get_or_insert(TopologyReadError::Capacity(e));
                None
            }
        };
        let facts = CpuFacts {
            siblings: files.siblings,
            cache,
            perf: perf_value(p_cores.as_ref(), cpu, capacity),
        };
        cpus.insert(cpu, facts);
    }
    let perf_unread = perf_unread.or_else(|| perf_gap(&cpus).map(TopologyReadError::Partial));
    let facts = Facts::new(cpus).ok_or(PinOrderError::EmptyMask)?;
    let cores = decide(&facts);
    let cpus = cores.pin_order();
    Ok((
        cores,
        PinOrder::Pinned {
            cpus,
            cache_unread,
            perf_unread,
        },
    ))
}

/// Walks the cgroups the same way std's `available_parallelism` does.
#[cfg(target_os = "linux")]
pub fn read_cpu_quota() -> Result<Quota, QuotaReadError> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").map_err(QuotaReadError::ProcCgroup)?;
    let Some(path) = cgroup.lines().find_map(|line| line.strip_prefix("0::")) else {
        return Err(QuotaReadError::NotCgroupV2 { text: cgroup });
    };
    quota_under(Path::new("/sys/fs/cgroup"), path)
}

/// Refuses a group with `..` in it: joined to `root`, it would read files outside the cgroup tree.
#[cfg(target_os = "linux")]
fn quota_under(root: &Path, group: &str) -> Result<Quota, QuotaReadError> {
    if Path::new(group).components().any(|c| c == Component::ParentDir) {
        return Err(QuotaReadError::OutsideNamespace {
            group: group.to_owned(),
        });
    }
    let mut dir = root.join(group.trim_start_matches('/'));
    let mut quota = Quota::Unlimited;
    loop {
        let entry = read_cpu_max_entry(&dir)?;
        quota = tightest(quota, level_quota(group, &dir, entry)?);
        if dir == root || !dir.pop() {
            return Ok(quota);
        }
    }
}

/// What one cgroup level holds where its `cpu.max` would be.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug)]
enum CpuMaxEntry {
    Text(String),
    /// The group's directory exists without a `cpu.max`; the root group never has one.
    NoFile,
    NoGroup,
}

/// Looks at the directory only when `cpu.max` is not found, so a level with the file costs one
/// read.
#[cfg(target_os = "linux")]
fn read_cpu_max_entry(dir: &Path) -> Result<CpuMaxEntry, QuotaReadError> {
    let file = dir.join("cpu.max");
    match std::fs::read_to_string(&file) {
        Ok(text) => Ok(CpuMaxEntry::Text(text)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => match dir.try_exists() {
            Ok(true) => Ok(CpuMaxEntry::NoFile),
            Ok(false) => Ok(CpuMaxEntry::NoGroup),
            Err(source) => Err(QuotaReadError::CpuMax {
                path: dir.display().to_string(),
                source,
            }),
        },
        Err(source) => Err(QuotaReadError::CpuMax {
            path: file.display().to_string(),
            source,
        }),
    }
}

/// A level without the file sets no limit; a level without the directory is not a cgroup at
/// all, so its limit is unknown and the caller hears why.
#[cfg(any(target_os = "linux", test))]
fn level_quota(group: &str, dir: &Path, entry: CpuMaxEntry) -> Result<Quota, QuotaReadError> {
    match entry {
        CpuMaxEntry::Text(text) => parse_cpu_max(&text).map_err(|source| QuotaReadError::Malformed {
            path: dir.join("cpu.max").display().to_string(),
            source,
        }),
        CpuMaxEntry::NoFile => Ok(Quota::Unlimited),
        CpuMaxEntry::NoGroup => Err(QuotaReadError::NoGroupDir {
            group: group.to_owned(),
            dir: dir.display().to_string(),
        }),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn read_cpu_quota() -> Result<Quota, QuotaReadError> {
    Ok(Quota::Unlimited)
}

/// Counts only performance cores, because an efficiency core serves about a third as fast.
/// Intel Macs have no performance levels (`ENOENT`), so all their cores count.
#[cfg(target_os = "macos")]
fn physical_cores() -> io::Result<NonZeroUsize> {
    let (name, count) = match sysctl_int(c"hw.perflevel0.physicalcpu") {
        Ok(count) => ("hw.perflevel0.physicalcpu", count),
        Err(e) if e.kind() == io::ErrorKind::NotFound => ("hw.physicalcpu", sysctl_int(c"hw.physicalcpu")?),
        Err(e) => return Err(e),
    };
    usize::try_from(count).ok().and_then(NonZeroUsize::new).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("sysctl {name} answered {count} cores"),
        )
    })
}

#[cfg(target_os = "macos")]
fn sysctl_int(name: &std::ffi::CStr) -> io::Result<i32> {
    let mut value: i32 = 0;
    let mut size = std::mem::size_of::<i32>();
    // SAFETY: `name` is NUL-terminated; `value`/`size` describe a writable i32.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            (&mut value as *mut i32).cast::<libc::c_void>(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(e.kind(), format!("sysctl {name:?}: {e}")));
    }
    if size != std::mem::size_of::<i32>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "sysctl {name:?} answered {size} bytes, not the {} of an int",
                std::mem::size_of::<i32>()
            ),
        ));
    }
    Ok(value)
}

// `CpuMask`'s words are 64-bit and the kernel reads `unsigned long`s. The bits land in the same
// place only when those are 64-bit or the target is little-endian; a 32-bit big-endian target
// fails to compile rather than pinning the wrong CPUs.
#[cfg(target_os = "linux")]
const _: () = assert!(
    libc::c_ulong::BITS == 64 || cfg!(target_endian = "little"),
    "CpuMask's 64-bit words are not this target's cpu_set_t layout"
);
#[cfg(target_os = "linux")]
const _: () = assert!(CpuMask::SETSIZE == libc::CPU_SETSIZE as usize);

#[cfg(target_os = "linux")]
fn allowed_cpus() -> io::Result<Vec<CoreId>> {
    let mut mask = CpuMask::empty(CpuMask::SETSIZE);
    loop {
        // SAFETY: pid 0 is the calling thread; the kernel writes at most `mask.bytes()` into
        // the mask's words, which outlive the call.
        let rc = unsafe {
            libc::sched_getaffinity(0, mask.bytes(), mask.words_mut().as_mut_ptr().cast::<libc::cpu_set_t>())
        };
        if rc == 0 {
            break;
        }
        let e = io::Error::last_os_error();
        // EINVAL: the mask is smaller than the kernel's possible CPUs.
        if e.raw_os_error() != Some(libc::EINVAL) {
            return Err(e);
        }
        if mask.cpus() >= MAX_CPUS {
            return Err(io::Error::new(
                e.kind(),
                format!("sched_getaffinity refused a {}-CPU mask: {e}", mask.cpus()),
            ));
        }
        mask = CpuMask::empty(mask.cpus() * 2);
    }
    Ok(mask.members().map(|id| CoreId { id }).collect())
}

#[cfg(test)]
mod reading_tests {
    use super::*;
    use crate::plan::tests::domain_count;

    fn ids(ids: &[usize]) -> Vec<CoreId> {
        ids.iter().map(|&id| CoreId { id }).collect()
    }

    fn n(value: usize) -> NonZeroUsize {
        match NonZeroUsize::new(value) {
            Some(n) => n,
            None => panic!("a test count is not zero"),
        }
    }

    fn os_error(path: &str, errno: i32) -> FileError {
        FileError {
            path: PathBuf::from(path),
            source: io::Error::from_raw_os_error(errno),
        }
    }

    /// Builds CPUs 0-7, where CPU n and CPU n+4 are one core, all in one L3 domain and with
    /// one capacity; `edit` changes one CPU's files.
    fn eight(edit: impl Fn(usize, &mut CpuFiles)) -> BTreeMap<CoreId, CpuFiles> {
        (0..8)
            .map(|c| {
                let mut files = CpuFiles {
                    siblings: ids(&[c % 4, c % 4 + 4]),
                    cache: Ok(ids(&(0..8).collect::<Vec<_>>())),
                    capacity: Ok(Some(1024)),
                };
                edit(c, &mut files);
                (CoreId { id: c }, files)
            })
            .collect()
    }

    fn pinned(
        read: Result<(CoreGroups, PinOrder), PinOrderError>,
    ) -> (CoreGroups, Option<TopologyReadError>, Option<TopologyReadError>) {
        let (cores, pin_order) = read.unwrap();
        let PinOrder::Pinned {
            cache_unread,
            perf_unread,
            ..
        } = pin_order
        else {
            panic!("{pin_order:?}")
        };
        (cores, cache_unread, perf_unread)
    }

    #[test]
    fn every_reading_made_is_used_and_nothing_is_said() {
        let (cores, cache_unread, perf_unread) = pinned(decided(eight(|_, _| {}), Ok(None)));
        assert_eq!(cores.top_count().get(), 4);
        assert!(cache_unread.is_none(), "{cache_unread:?}");
        assert!(perf_unread.is_none(), "{perf_unread:?}");
    }

    /// The last CPU's list is the one unread: the lists of the CPUs before it are not used either.
    #[test]
    fn an_unread_cache_is_one_domain_and_says_why_with_the_os_error() {
        let path = "/sys/devices/system/cpu/cpu7/cache/index3/shared_cpu_list";
        let files = eight(|c, files| {
            if c == 7 {
                files.cache = Err(CpuListFileError::Os(os_error(path, libc::ENOENT)));
            } else {
                files.cache = Ok(ids(&[c % 2]));
            }
        });
        let (cores, cache_unread, perf_unread) = pinned(decided(files, Ok(None)));
        assert_eq!(domain_count(&cores), 1);
        let Some(TopologyReadError::CpuList(e)) = &cache_unread else {
            panic!("{cache_unread:?}")
        };
        assert_eq!(e.os_error().and_then(io::Error::raw_os_error), Some(libc::ENOENT));
        assert!(e.to_string().contains(path), "{e}");
        assert!(perf_unread.is_none());
    }

    #[test]
    fn an_unread_capacity_is_one_tier_and_says_why_with_the_os_error() {
        let files = eight(|c, files| {
            if c == 0 {
                files.capacity = Ok(Some(2048));
            }
            if c == 3 {
                let path = "/sys/devices/system/cpu/cpu3/cpu_capacity";
                files.capacity = Err(CapacityFileError::Os(os_error(path, libc::EIO)));
            }
        });
        let (cores, cache_unread, perf_unread) = pinned(decided(files, Ok(None)));
        assert_eq!(cores.top_count().get(), 4, "CPU 0 alone would be the fast tier");
        assert!(cache_unread.is_none());
        let Some(TopologyReadError::Capacity(CapacityFileError::Os(e))) = &perf_unread else {
            panic!("{perf_unread:?}")
        };
        assert_eq!(e.source.raw_os_error(), Some(libc::EIO));
        assert!(e.to_string().contains("cpu_capacity"), "{e}");
    }

    #[test]
    fn an_unreadable_p_core_list_is_one_tier_and_says_why() {
        let path = "/sys/devices/cpu_core/cpus";
        let p_cores = Err(CpuListFileError::Os(os_error(path, libc::EACCES)));
        let (cores, _, perf_unread) = pinned(decided(eight(|_, _| {}), p_cores));
        assert_eq!(cores.top_count().get(), 4);
        let Some(TopologyReadError::CpuList(e)) = &perf_unread else {
            panic!("{perf_unread:?}")
        };
        assert_eq!(e.os_error().and_then(io::Error::raw_os_error), Some(libc::EACCES));
    }

    #[test]
    fn a_p_core_list_is_two_tiers_whatever_the_capacities() {
        let p_cores = crate::plan::parse_cpu_list("0").unwrap();
        let (cores, _, perf_unread) = pinned(decided(eight(|_, _| {}), Ok(Some(p_cores))));
        assert_eq!(cores.top_count().get(), 1);
        assert!(perf_unread.is_none(), "{perf_unread:?}");
    }

    #[test]
    fn a_capacity_missing_for_one_cpu_is_one_tier_and_names_the_cpu() {
        let files = eight(|c, files| {
            if c == 7 {
                files.capacity = Ok(None);
            }
        });
        let (cores, _, perf_unread) = pinned(decided(files, Ok(None)));
        assert_eq!(cores.top_count().get(), 4);
        assert!(
            matches!(perf_unread, Some(TopologyReadError::Partial(PerfValueError::CapacityMissing { cpu })) if cpu.id == 7),
            "{perf_unread:?}"
        );
    }

    /// The most common way to lose the pin order is `/sys` masked in a container; the caller
    /// logs the errno it gets here.
    #[test]
    fn an_unreadable_siblings_file_keeps_its_errno_for_the_caller() {
        let path = "/sys/devices/system/cpu/cpu0/topology/thread_siblings_list";
        let err = PinOrderError::Topology(CpuListFileError::Os(os_error(path, libc::EACCES)));
        assert_eq!(err.os_error().and_then(io::Error::raw_os_error), Some(libc::EACCES));
        assert!(err.to_string().contains(path), "{err}");
        let source = std::error::Error::source(&err).expect("the file error");
        assert!(source.is::<CpuListFileError>(), "{source}");
    }

    #[test]
    fn an_empty_mask_is_its_own_error_not_an_os_error() {
        let err = decided(BTreeMap::new(), Ok(None)).unwrap_err();
        assert!(matches!(err, PinOrderError::EmptyMask), "{err}");
        assert!(err.os_error().is_none());
    }

    #[test]
    fn counts_read_are_counts_and_each_failure_is_its_own_error() {
        assert_eq!(counted(Ok(n(16)), Some(n(8))).unwrap(), Cpus::new(n(16), n(8)));
        let Err(CpuReadError::Cores { logical }) = counted(Ok(n(16)), None) else {
            panic!("no cores read")
        };
        assert_eq!(logical, n(16));
        let Err(CpuReadError::Logical(e)) = counted(Err(io::Error::other("no parallelism")), Some(n(8))) else {
            panic!("no logical count")
        };
        assert_eq!(e.to_string(), "no parallelism");
    }
}

#[cfg(test)]
mod cgroup_level_tests {
    use super::*;

    fn cores(value: usize) -> Quota {
        match NonZeroUsize::new(value) {
            Some(n) => Quota::Cores(n),
            None => panic!("a test count is not zero"),
        }
    }

    #[test]
    fn a_group_directory_without_cpu_max_sets_no_limit() {
        let dir = Path::new("/sys/fs/cgroup/a");
        assert_eq!(level_quota("/a", dir, CpuMaxEntry::NoFile).unwrap(), Quota::Unlimited);
    }

    #[test]
    fn a_group_without_its_directory_is_an_error_naming_the_group_and_the_directory() {
        let dir = Path::new("/sys/fs/cgroup/user.slice/a.scope");
        let e = level_quota("/user.slice/a.scope", dir, CpuMaxEntry::NoGroup).unwrap_err();
        let QuotaReadError::NoGroupDir { group, dir } = &e else {
            panic!("{e}")
        };
        assert_eq!(group, "/user.slice/a.scope");
        assert_eq!(dir, "/sys/fs/cgroup/user.slice/a.scope");
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn a_level_with_cpu_max_is_its_parsed_limit_and_a_bad_one_names_the_file() {
        let dir = Path::new("/sys/fs/cgroup/a");
        let text = |t: &str| CpuMaxEntry::Text(t.to_owned());
        assert_eq!(level_quota("/a", dir, text("200000 100000\n")).unwrap(), cores(2));
        assert_eq!(level_quota("/a", dir, text("max 100000\n")).unwrap(), Quota::Unlimited);
        let e = level_quota("/a", dir, text("oops 100000")).unwrap_err();
        let QuotaReadError::Malformed { path, .. } = &e else {
            panic!("{e}")
        };
        assert_eq!(path, "/sys/fs/cgroup/a/cpu.max");
    }
}

#[cfg(all(test, target_os = "macos"))]
mod macos_tests {
    use super::*;

    #[test]
    fn this_mac_reads_no_pinning_and_refuses_a_pin_as_unsupported() {
        let host = HostCpus::read(Quota::Unlimited);
        assert!(
            matches!(host.pin_order, Ok(PinOrder::Unsupported)),
            "{:?}",
            host.pin_order
        );
        let e = try_pin_current(Some(CoreId { id: 0 })).unwrap_err();
        assert!(matches!(e.fault, PinFault::Unsupported), "{e}");
    }
}

#[cfg(test)]
mod host_tests {
    use super::*;

    #[test]
    fn this_host_reads_its_counts_and_pin_order() {
        let host = HostCpus::read(Quota::Unlimited);
        let cpus = host.cpus.expect("this host's CPU counts");
        let pinning = host.pin_order;
        eprintln!(
            "logical {} physical {} pinning {pinning:?}",
            cpus.logical(),
            cpus.physical(),
        );
        assert!(pinning.is_ok(), "this host's pinning: {pinning:?}");
        assert!(cpus.physical() <= cpus.logical());
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::plan::tests::{core_count, first_cpus};

    /// Pins a spawned thread, so the test runner's threads keep their mask.
    #[test]
    fn a_thread_pinned_to_one_allowed_cpu_reads_back_that_cpu() {
        let allowed = allowed_cpus().expect("sched_getaffinity");
        assert!(!allowed.is_empty());
        if allowed.len() == 1 {
            eprintln!("one allowed CPU ({:?}): narrowing cannot be observed", allowed[0]);
        }
        let one = allowed[0];
        let (seen, still_allowed) = std::thread::spawn(move || {
            pin_current(one).expect("pin to an allowed CPU");
            (allowed_cpus().expect("sched_getaffinity after pinning"), vec![one])
        })
        .join()
        .expect("the pinning thread");
        assert_eq!(seen, still_allowed);
        assert_eq!(allowed_cpus().expect("sched_getaffinity again"), allowed);
    }

    #[test]
    fn the_smallest_mask_is_the_size_of_cpu_set_t() {
        assert_eq!(CpuMask::empty(1).bytes(), std::mem::size_of::<libc::cpu_set_t>());
    }

    #[test]
    fn a_cpu_past_max_cpus_is_refused_before_the_kernel_sees_it() {
        let e = pin_current(CoreId { id: MAX_CPUS }).unwrap_err();
        assert!(matches!(e, PinFault::NoRoom(_)), "{e}");
    }

    /// The kernel's own refusal cannot be made on demand, so this hits the CPU-number check.
    #[test]
    fn a_refused_pin_comes_back_naming_the_cpu_and_the_reason() {
        let core = CoreId { id: MAX_CPUS };
        let e = std::thread::spawn(move || try_pin_current(Some(core)))
            .join()
            .expect("the pinning thread")
            .unwrap_err();
        assert_eq!(e.core, core);
        assert!(matches!(e.fault, PinFault::NoRoom(_)), "{e}");
        assert!(e.os_error().is_none());
        assert!(e.to_string().contains("CPU 8192"), "{e}");
    }

    struct Tree(std::path::PathBuf);

    static TREE_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    impl Tree {
        fn new(tag: &str) -> io::Result<Self> {
            let seq = TREE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("mipyre_cpu_quota_{tag}_{}_{seq}", std::process::id()));
            std::fs::create_dir_all(&dir)?;
            Ok(Tree(dir))
        }

        fn level(&self, group: &str, cpu_max: Option<&str>) -> io::Result<&Self> {
            let dir = self.0.join(group);
            std::fs::create_dir_all(&dir)?;
            if let Some(text) = cpu_max {
                std::fs::write(dir.join("cpu.max"), text)?;
            }
            Ok(self)
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(&self.0) {
                eprintln!("could not remove the test tree {}: {e}", self.0.display());
            }
        }
    }

    fn n(value: usize) -> NonZeroUsize {
        match NonZeroUsize::new(value) {
            Some(n) => n,
            None => panic!("a test count is not zero"),
        }
    }

    #[test]
    fn a_parent_quota_of_two_cpus_limits_an_unlimited_child_to_two() {
        let tree = Tree::new("parent").unwrap();
        tree.level("a", Some("200000 100000\n")).unwrap();
        tree.level("a/b", Some("max 100000\n")).unwrap();
        assert_eq!(quota_under(&tree.0, "/a/b").unwrap(), Quota::Cores(n(2)));
    }

    #[test]
    fn the_tightest_level_wins_and_a_level_without_a_file_is_no_limit() {
        let tree = Tree::new("tight").unwrap();
        tree.level("a", Some("400000 100000")).unwrap();
        tree.level("a/b", Some("600000 100000")).unwrap();
        assert_eq!(quota_under(&tree.0, "/a/b").unwrap(), Quota::Cores(n(4)));
        tree.level("a/b", Some("150000 100000")).unwrap();
        assert_eq!(quota_under(&tree.0, "/a/b").unwrap(), Quota::Cores(n(1)));
        let bare = Tree::new("bare").unwrap();
        bare.level("a", Some("300000 100000")).unwrap();
        bare.level("a/b", None).unwrap();
        assert_eq!(quota_under(&bare.0, "/a/b").unwrap(), Quota::Cores(n(3)));
        let none = Tree::new("none").unwrap();
        none.level("a/b", None).unwrap();
        assert_eq!(quota_under(&none.0, "/a/b").unwrap(), Quota::Unlimited);
    }

    #[test]
    fn a_malformed_level_is_an_error_naming_the_file_not_a_silent_skip() {
        let tree = Tree::new("bad").unwrap();
        tree.level("a", Some("oops 100000")).unwrap();
        tree.level("a/b", None).unwrap();
        let e = quota_under(&tree.0, "/a/b").unwrap_err();
        assert!(matches!(e, QuotaReadError::Malformed { .. }), "{e}");
    }

    /// The shape of a host with v1 and v2 side by side: `/proc/self/cgroup` names a v2 group, but
    /// the tree read holds no directory for it.
    #[test]
    fn a_group_with_no_directory_under_the_root_is_an_error_not_no_limit() {
        let tree = Tree::new("nogroup").unwrap();
        tree.level("other", Some("100000 100000")).unwrap();
        let e = quota_under(&tree.0, "/user.slice/a.scope").unwrap_err();
        let QuotaReadError::NoGroupDir { group, dir } = &e else {
            panic!("{e}")
        };
        assert_eq!(group, "/user.slice/a.scope");
        assert_eq!(*dir, tree.0.join("user.slice/a.scope").display().to_string());
    }

    #[test]
    fn a_group_that_climbs_out_with_dot_dot_is_refused_and_dots_in_a_name_are_not() {
        // The tree's root is `cg`; a quota planted beside it is what `..` would reach.
        let tree = Tree::new("dotdot").unwrap();
        tree.level("outside", Some("100000 100000")).unwrap();
        tree.level("cg/a..b", Some("300000 100000")).unwrap();
        let root = tree.0.join("cg");
        let e = quota_under(&root, "/../outside").unwrap_err();
        let QuotaReadError::OutsideNamespace { group } = &e else {
            panic!("{e}")
        };
        assert_eq!(group, "/../outside");
        assert!(matches!(
            quota_under(&root, "/a/../../outside"),
            Err(QuotaReadError::OutsideNamespace { .. })
        ));
        assert_eq!(quota_under(&root, "/a..b").unwrap(), Quota::Cores(n(3)));
    }

    #[test]
    fn the_host_sysfs_read_holds_the_pin_order_invariants() {
        let allowed = allowed_cpus().unwrap();
        let (cores, pin_order) = read_order(&allowed).unwrap();
        let PinOrder::Pinned { cpus, .. } = &pin_order else {
            panic!("{pin_order:?}")
        };
        let pinned: Vec<CoreId> = cpus.cpus().to_vec();
        let mut sorted: Vec<usize> = pinned.iter().map(|c| c.id).collect();
        sorted.sort_unstable();
        let mut want: Vec<usize> = allowed.iter().map(|c| c.id).collect();
        want.sort_unstable();
        assert_eq!(sorted, want, "every allowed CPU is pinned exactly once");
        let firsts: std::collections::BTreeSet<CoreId> = first_cpus(&cores).into_iter().collect();
        let leading: std::collections::BTreeSet<CoreId> = pinned[..firsts.len()].iter().copied().collect();
        assert_eq!(leading, firsts, "first CPUs of the cores come first");
        assert!(cores.top_count().get() <= core_count(&cores));
        assert_eq!(threads_for(cores.top_count(), Quota::Unlimited), cores.top_count());
    }

    #[test]
    fn an_unreadable_topology_is_the_callers_error_not_an_empty_pin_order() {
        // No host has this CPU, so its siblings file cannot be read.
        let err = read_order(&[CoreId { id: 1 << 20 }]).unwrap_err();
        assert!(matches!(err, PinOrderError::Topology(_)), "{err}");
        assert_eq!(
            err.os_error().and_then(io::Error::raw_os_error),
            Some(libc::ENOENT),
            "{err}"
        );
    }

    #[test]
    fn a_cpu_list_file_that_does_not_parse_keeps_the_parse_error_and_the_path() {
        let tree = Tree::new("list").unwrap();
        let path = tree.0.join("cpus");
        std::fs::write(&path, "3-1\n").unwrap();
        let e = read_cpu_list_file(path.clone()).unwrap_err();
        let CpuListFileError::Malformed { path: named, .. } = &e else {
            panic!("{e}")
        };
        assert_eq!(*named, path);
        let source = std::error::Error::source(&e).expect("the CPU list error");
        assert!(source.is::<CpuListError>(), "{source}");
        assert!(e.to_string().contains("runs backwards"), "{e}");
        assert!(e.os_error().is_none());
    }

    #[test]
    fn a_cpu_list_file_that_is_not_there_keeps_the_errno_and_the_path() {
        let tree = Tree::new("absent").unwrap();
        let path = tree.0.join("cpus");
        let e = read_cpu_list_file(path.clone()).unwrap_err();
        let CpuListFileError::Os(FileError { path: named, source }) = &e else {
            panic!("{e}")
        };
        assert_eq!(*named, path);
        assert_eq!(source.raw_os_error(), Some(libc::ENOENT));
    }
}
