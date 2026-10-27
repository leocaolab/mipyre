//! Decides the thread count and pin order from data the caller hands in, so any topology can be
//! tested on any machine.
//!
//! - A reading missing for any CPU is not used at all: without cache readings every core is in
//!   one domain, and without performance readings every core is in one tier.
//! - The default thread count is the fastest tier's cores, capped by the quota. Slow cores are
//!   left out because the scheduler cannot move work off them to a fast core busy with a pinned
//!   thread.
//! - The pin order is the fast cores' first CPUs, filling one cache domain before the next; then
//!   the slow cores; then the fast cores' SMT siblings; then the slow cores' siblings.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use core_affinity::CoreId;

/// `physical` is capped by `logical`: a 2-CPU quota on a 32-core mask runs two threads at
/// once, so `physical` is 2, not 32.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cpus {
    logical: NonZeroUsize,
    physical: NonZeroUsize,
}

impl Cpus {
    pub const fn new(logical: NonZeroUsize, cores: NonZeroUsize) -> Self {
        let physical = if cores.get() < logical.get() { cores } else { logical };
        Cpus { logical, physical }
    }

    /// Returns the logical CPUs, SMT siblings included, capped by the CPU quota.
    pub const fn logical(&self) -> NonZeroUsize {
        self.logical
    }

    /// Returns how many physical cores can be busy at once; on macOS only performance cores count.
    pub const fn physical(&self) -> NonZeroUsize {
        self.physical
    }
}

/// The allowed CPUs of one physical core. It is never empty, because only [`decide`] makes one,
/// and it puts the core's own CPU first.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CoreCpus {
    cpus: Vec<CoreId>,
}

impl CoreCpus {
    fn first(&self) -> CoreId {
        self.cpus[0]
    }

    fn rest(&self) -> &[CoreId] {
        &self.cpus[1..]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Core {
    cpus: CoreCpus,
    /// Domain 0 holds the lowest CPU.
    domain: usize,
    perf: u32,
}

/// This process's physical cores in order of their first CPU. It is never empty, because only
/// [`decide`] makes one, from [`Facts`] that hold at least one CPU.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CoreGroups {
    cores: Vec<Core>,
}

impl CoreGroups {
    fn fastest(&self) -> u32 {
        self.cores.iter().map(|c| c.perf).fold(u32::MIN, u32::max)
    }

    /// Returns how many cores are in the fastest tier.
    pub(crate) fn top_count(&self) -> NonZeroUsize {
        let fastest = self.fastest();
        // The count starts at one for the core that set `fastest`; the others add to it.
        self.cores
            .iter()
            .filter(|c| c.perf == fastest)
            .skip(1)
            .fold(NonZeroUsize::MIN, |n, _| n.saturating_add(1))
    }

    pub(crate) fn pin_order(&self) -> CpuOrder {
        let fastest = self.fastest();
        let mut ordered: Vec<&Core> = self.cores.iter().collect();
        ordered.sort_by_key(|c| (c.domain, c.cpus.first()));
        let (fast, slow): (Vec<&Core>, Vec<&Core>) = ordered.into_iter().partition(|c| c.perf == fastest);
        let cpus = fast
            .iter()
            .map(|c| c.cpus.first())
            .chain(slow.iter().map(|c| c.cpus.first()))
            .chain(fast.iter().flat_map(|c| c.cpus.rest().iter().copied()))
            .chain(slow.iter().flat_map(|c| c.cpus.rest().iter().copied()))
            .collect();
        // Not empty: every core gives its first CPU, and there is at least one core.
        CpuOrder { cpus }
    }
}

/// One allowed CPU's readings; a reading that was not made is `None`.
#[derive(Clone, Debug)]
pub(crate) struct CpuFacts {
    /// The CPU's SMT siblings; the list may name the CPU itself and CPUs outside the mask.
    pub(crate) siblings: Vec<CoreId>,
    pub(crate) cache: Option<Vec<CoreId>>,
    /// Higher is faster.
    pub(crate) perf: Option<u32>,
}

/// The readings of the allowed CPUs, each reading whole or not at all.
#[derive(Clone, Debug)]
pub(crate) struct Facts {
    /// Each allowed CPU's SMT siblings; a list may name the CPU itself and CPUs outside the mask.
    siblings: BTreeMap<CoreId, Vec<CoreId>>,
    /// Each allowed CPU's L3 list, in the order of `siblings`.
    cache: Option<Vec<Vec<CoreId>>>,
    /// Each allowed CPU's performance tier, in the order of `siblings`; higher is faster.
    perf: Option<Vec<u32>>,
}

impl Facts {
    /// Uses a reading only when every CPU has it. Returns `None` when there is no CPU.
    pub(crate) fn new(cpus: BTreeMap<CoreId, CpuFacts>) -> Option<Self> {
        if cpus.is_empty() {
            return None;
        }
        let mut siblings = BTreeMap::new();
        let mut cache = Some(Vec::with_capacity(cpus.len()));
        let mut perf = Some(Vec::with_capacity(cpus.len()));
        for (cpu, facts) in cpus {
            siblings.insert(cpu, facts.siblings);
            cache = cache.zip(facts.cache).map(|(mut lists, list)| {
                lists.push(list);
                lists
            });
            perf = perf.zip(facts.perf).map(|(mut values, value)| {
                values.push(value);
                values
            });
        }
        Some(Facts { siblings, cache, perf })
    }

    fn allows(&self, cpu: CoreId) -> bool {
        self.siblings.contains_key(&cpu)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Quota {
    Unlimited,
    Cores(NonZeroUsize),
}

pub(crate) fn threads_for(fast_cores: NonZeroUsize, quota: Quota) -> NonZeroUsize {
    match quota {
        Quota::Unlimited => fast_cores,
        Quota::Cores(q) => std::cmp::min(fast_cores, q),
    }
}

/// CPUs in pin order, never empty. Thread `i` goes to the `i`-th CPU; threads past the end are
/// not pinned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuOrder {
    cpus: Vec<CoreId>,
}

impl CpuOrder {
    pub fn new(cpus: Vec<CoreId>) -> Option<Self> {
        (!cpus.is_empty()).then_some(CpuOrder { cpus })
    }

    pub fn cpus(&self) -> &[CoreId] {
        &self.cpus
    }
}

/// A CPU already grouped stays put even when a later CPU's siblings list it (the lists disagree).
/// Cores come in CPU order, so numbering cache keys by first appearance numbers the domains by
/// their lowest CPU.
pub(crate) fn decide(facts: &Facts) -> CoreGroups {
    let mut placed: BTreeSet<CoreId> = BTreeSet::new();
    let mut domains: BTreeMap<CoreId, usize> = BTreeMap::new();
    let mut cores = Vec::new();
    for (row, (&cpu, siblings)) in facts.siblings.iter().enumerate() {
        if placed.contains(&cpu) {
            continue;
        }
        let others: BTreeSet<CoreId> = siblings
            .iter()
            .copied()
            .filter(|&c| c != cpu && facts.allows(c) && !placed.contains(&c))
            .collect();
        placed.insert(cpu);
        placed.extend(others.iter().copied());
        let domain = match &facts.cache {
            Some(lists) => {
                let next = domains.len();
                *domains.entry(cache_key(facts, &lists[row], cpu)).or_insert(next)
            }
            None => 0,
        };
        cores.push(Core {
            cpus: CoreCpus {
                cpus: std::iter::once(cpu).chain(others).collect(),
            },
            domain,
            perf: facts.perf.as_ref().map_or(0, |values| values[row]),
        });
    }
    CoreGroups { cores }
}

/// Names an L3 domain by its lowest allowed CPU. The core's own CPU counts, so a list that names
/// no allowed CPU still gives the core a domain of its own.
fn cache_key(facts: &Facts, list: &[CoreId], cpu: CoreId) -> CoreId {
    list.iter()
        .copied()
        .filter(|&c| facts.allows(c))
        .fold(cpu, std::cmp::min)
}

const P_CORE: u32 = 2;
const E_CORE: u32 = 1;
// `CoreGroups` takes the highest value as the fastest tier, so a P-core must be above an E-core.
const _: () = assert!(P_CORE > E_CORE);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PerfValueError {
    /// `cpu` is the first CPU without a `cpu_capacity`.
    CapacityMissing { cpu: CoreId },
}

impl std::fmt::Display for PerfValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PerfValueError::CapacityMissing { cpu } => {
                write!(f, "CPU {} has no cpu_capacity while other CPUs have one", cpu.id)
            }
        }
    }
}

impl std::error::Error for PerfValueError {}

/// Returns a CPU's performance value: on a hybrid host, which lists its P-cores, whether the CPU
/// is one; elsewhere its `cpu_capacity`.
pub(crate) fn perf_value(p_cores: Option<&CpuSet>, cpu: CoreId, capacity: Option<u32>) -> Option<u32> {
    match p_cores {
        Some(p_cores) => Some(if p_cores.contains(cpu) { P_CORE } else { E_CORE }),
        None => capacity,
    }
}

/// Returns the first CPU without a performance value when another CPU has one, which leaves
/// the values unused. A host where no CPU has one has one tier, which is not a gap.
pub(crate) fn perf_gap(cpus: &BTreeMap<CoreId, CpuFacts>) -> Option<PerfValueError> {
    if cpus.values().all(|facts| facts.perf.is_none()) {
        return None;
    }
    cpus.iter()
        .find(|(_, facts)| facts.perf.is_none())
        .map(|(&cpu, _)| PerfValueError::CapacityMissing { cpu })
}

/// A CPU set in the layout of the kernel's `cpu_set_t`, sized at run time like glibc's
/// `CPU_ALLOC`: on a host with more than 1024 possible CPUs, `sched_getaffinity` refuses the fixed
/// `cpu_set_t` with `EINVAL`. A CPU it has no room for is refused rather than a panic, because the
/// numbers come from the OS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CpuMask(Vec<u64>);

impl CpuMask {
    const WORD_BITS: usize = u64::BITS as usize;
    /// Matches glibc's fixed `cpu_set_t`, and no mask is made smaller.
    pub(crate) const SETSIZE: usize = 1024;

    pub(crate) fn empty(cpus: usize) -> Self {
        CpuMask(vec![0; cpus.max(Self::SETSIZE).div_ceil(Self::WORD_BITS)])
    }

    /// Refuses a CPU at or past [`MAX_CPUS`], which Linux cannot have.
    pub(crate) fn of_one(cpu: usize) -> Result<Self, CpuMaskError> {
        if cpu >= MAX_CPUS {
            return Err(CpuMaskError::PastRoom { cpu, room: MAX_CPUS });
        }
        let mut mask = CpuMask::empty(cpu + 1);
        mask.insert(cpu)?;
        Ok(mask)
    }

    /// Returns how many CPUs the mask has room for.
    pub(crate) fn cpus(&self) -> usize {
        self.0.len() * Self::WORD_BITS
    }

    pub(crate) fn bytes(&self) -> usize {
        std::mem::size_of_val(self.0.as_slice())
    }

    pub(crate) fn words(&self) -> &[u64] {
        &self.0
    }

    pub(crate) fn words_mut(&mut self) -> &mut [u64] {
        &mut self.0
    }

    /// Refuses a CPU past the mask's room.
    pub(crate) fn insert(&mut self, cpu: usize) -> Result<(), CpuMaskError> {
        let room = self.cpus();
        let Some(word) = self.0.get_mut(cpu / Self::WORD_BITS) else {
            return Err(CpuMaskError::PastRoom { cpu, room });
        };
        *word |= 1 << (cpu % Self::WORD_BITS);
        Ok(())
    }

    /// Yields the CPUs in the mask, lowest first.
    pub(crate) fn members(&self) -> impl Iterator<Item = usize> + '_ {
        self.0.iter().enumerate().flat_map(|(w, &word)| {
            (0..Self::WORD_BITS)
                .filter(move |bit| word & (1 << bit) != 0)
                .map(move |bit| w * Self::WORD_BITS + bit)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CpuMaskError {
    PastRoom { cpu: usize, room: usize },
}

impl std::fmt::Display for CpuMaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CpuMaskError::PastRoom { cpu, room } => {
                write!(f, "CPU {cpu} is past the {room} CPUs the mask holds")
            }
        }
    }
}

impl std::error::Error for CpuMaskError {}

/// The largest CPU count Linux supports (`CONFIG_NR_CPUS`). A range that reaches it is not a CPU
/// list, and expanding `0-18446744073709551615` would not fit in memory.
pub const MAX_CPUS: usize = 8192;

/// A set of CPU numbers below [`MAX_CPUS`]. Its size is fixed, so a CPU list read from outside
/// cannot make it grow, and a CPU listed twice is in it once.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CpuSet {
    /// Boxed, so passing a set along moves a pointer and not a kilobyte.
    words: Box<[u64; MAX_CPUS / 64]>,
    /// One past the last word with a CPU in it, so listing a small host's set reads one word.
    end: usize,
}

impl CpuSet {
    fn empty() -> Self {
        CpuSet {
            words: Box::new([0; MAX_CPUS / 64]),
            end: 0,
        }
    }

    /// Inserts `lo..=hi`; both are below [`MAX_CPUS`] and `lo <= hi`.
    fn insert_range(&mut self, lo: usize, hi: usize) {
        for word in lo / 64..=hi / 64 {
            let from = if word == lo / 64 { lo % 64 } else { 0 };
            let to = if word == hi / 64 { hi % 64 } else { 63 };
            self.words[word] |= (u64::MAX >> (63 - (to - from))) << from;
        }
        self.end = self.end.max(hi / 64 + 1);
    }

    pub(crate) fn contains(&self, cpu: CoreId) -> bool {
        cpu.id < MAX_CPUS && self.words[cpu.id / 64] & (1 << (cpu.id % 64)) != 0
    }

    /// Lists the CPUs in ascending order, visiting only set bits.
    pub(crate) fn iter(&self) -> impl Iterator<Item = CoreId> + '_ {
        self.words[..self.end].iter().enumerate().flat_map(|(w, &word)| {
            let mut bits = word;
            std::iter::from_fn(move || {
                if bits == 0 {
                    return None;
                }
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                Some(CoreId { id: w * 64 + bit })
            })
        })
    }
}

impl std::fmt::Debug for CpuSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.iter().map(|c| c.id)).finish()
    }
}

pub(crate) fn parse_cpu_list(text: &str) -> Result<CpuSet, CpuListError> {
    let fault = |fault| CpuListError {
        text: text.to_owned(),
        fault,
    };
    let number = |part: &str| {
        let cpu: usize = part
            .parse()
            .map_err(|_| fault(CpuListFault::NotACpu { part: part.to_owned() }))?;
        if cpu >= MAX_CPUS {
            return Err(fault(CpuListFault::PastMaxCpus { cpu }));
        }
        Ok(cpu)
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(fault(CpuListFault::Empty));
    }
    let mut cpus = CpuSet::empty();
    for part in trimmed.split(',') {
        if part.is_empty() {
            return Err(fault(CpuListFault::EmptyPart));
        }
        let (lo, hi) = match part.split_once('-') {
            Some((lo, hi)) => (number(lo)?, number(hi)?),
            None => {
                let cpu = number(part)?;
                (cpu, cpu)
            }
        };
        if lo > hi {
            return Err(fault(CpuListFault::Reversed { lo, hi }));
        }
        cpus.insert_range(lo, hi);
    }
    Ok(cpus)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuListError {
    text: String,
    fault: CpuListFault,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CpuListFault {
    Empty,
    EmptyPart,
    NotACpu { part: String },
    Reversed { lo: usize, hi: usize },
    PastMaxCpus { cpu: usize },
}

impl std::fmt::Display for CpuListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = &self.text;
        match &self.fault {
            CpuListFault::Empty => write!(f, "{text:?} is not a CPU list: it is empty"),
            CpuListFault::EmptyPart => {
                write!(f, "{text:?} is not a CPU list: it has an empty part")
            }
            CpuListFault::NotACpu { part } => {
                write!(f, "{text:?} is not a CPU list: {part:?} is not a CPU number")
            }
            CpuListFault::Reversed { lo, hi } => {
                write!(f, "{text:?} is not a CPU list: the range {lo}-{hi} runs backwards")
            }
            CpuListFault::PastMaxCpus { cpu } => write!(
                f,
                "{text:?} is not a CPU list: CPU {cpu} is past the {MAX_CPUS} CPUs Linux can have"
            ),
        }
    }
}

impl std::error::Error for CpuListError {}

pub(crate) fn tightest(a: Quota, b: Quota) -> Quota {
    match (a, b) {
        (Quota::Unlimited, other) | (other, Quota::Unlimited) => other,
        (Quota::Cores(x), Quota::Cores(y)) => Quota::Cores(std::cmp::min(x, y)),
    }
}

/// Rounds down but never below one: a 2.5-CPU quota gives 2, because a third pinned thread
/// would stall on the quota.
pub(crate) fn parse_cpu_max(text: &str) -> Result<Quota, CpuMaxError> {
    let fault = |fault| CpuMaxError {
        text: text.to_owned(),
        fault,
    };
    let mut parts = text.split_whitespace();
    let (Some(quota), Some(period), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(fault(CpuMaxFault::NotTwoFields));
    };
    let period: u64 = period.parse().map_err(|_| fault(CpuMaxFault::PeriodNotANumber))?;
    if period == 0 {
        return Err(fault(CpuMaxFault::PeriodZero));
    }
    if quota == "max" {
        return Ok(Quota::Unlimited);
    }
    let quota: u64 = quota.parse().map_err(|_| fault(CpuMaxFault::QuotaNotANumber))?;
    if quota == 0 {
        return Err(fault(CpuMaxFault::QuotaZero));
    }
    let cores = usize::try_from(quota / period).map_err(|_| fault(CpuMaxFault::PastUsize))?;
    Ok(Quota::Cores(NonZeroUsize::new(cores).unwrap_or(NonZeroUsize::MIN)))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpuMaxError {
    pub text: String,
    pub fault: CpuMaxFault,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CpuMaxFault {
    NotTwoFields,
    PeriodNotANumber,
    PeriodZero,
    QuotaNotANumber,
    QuotaZero,
    PastUsize,
}

impl std::fmt::Display for CpuMaxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let why = match self.fault {
            CpuMaxFault::NotTwoFields => "expected two fields: quota and period",
            CpuMaxFault::PeriodNotANumber => "the period is not a number",
            CpuMaxFault::PeriodZero => "the period is zero",
            CpuMaxFault::QuotaNotANumber => "the quota is neither `max` nor a number",
            CpuMaxFault::QuotaZero => "the quota is zero",
            CpuMaxFault::PastUsize => "the quota is more CPUs than this platform counts",
        };
        write!(f, "{:?} is not a cpu.max line: {why}", self.text)
    }
}

impl std::error::Error for CpuMaxError {}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Returns each core's first CPU, for the host test in `os.rs`.
    pub(crate) fn first_cpus(cores: &CoreGroups) -> Vec<CoreId> {
        cores.cores.iter().map(|c| c.cpus.first()).collect()
    }

    /// Returns how many cores there are, for the host test in `os.rs`.
    pub(crate) fn core_count(cores: &CoreGroups) -> usize {
        cores.cores.len()
    }

    fn ids(ids: &[usize]) -> Vec<CoreId> {
        ids.iter().map(|&id| CoreId { id }).collect()
    }

    fn n(value: usize) -> NonZeroUsize {
        match NonZeroUsize::new(value) {
            Some(n) => n,
            None => panic!("a test count is not zero"),
        }
    }

    fn host(
        allowed: &[usize],
        siblings: impl Fn(usize) -> Vec<usize>,
        cache: impl Fn(usize) -> Vec<usize>,
        perf: impl Fn(usize) -> u32,
    ) -> Facts {
        rows(allowed, |c| CpuFacts {
            siblings: ids(&siblings(c)),
            cache: Some(ids(&cache(c))),
            perf: Some(perf(c)),
        })
    }

    fn rows(allowed: &[usize], facts: impl Fn(usize) -> CpuFacts) -> Facts {
        Facts::new(allowed.iter().map(|&c| (CoreId { id: c }, facts(c))).collect()).unwrap()
    }

    fn order(facts: &Facts, k: Option<usize>) -> Vec<usize> {
        let cpus: Vec<usize> = decide(facts).pin_order().cpus().iter().map(|c| c.id).collect();
        match k {
            Some(k) => cpus[..k.min(cpus.len())].to_vec(),
            None => cpus,
        }
    }

    fn threads(facts: &Facts, quota: Quota) -> usize {
        threads_for(decide(facts).top_count(), quota).get()
    }

    fn cores_of(facts: &Facts) -> CoreGroups {
        decide(facts)
    }

    /// Returns how many cache domains the cores are in.
    pub(crate) fn domain_count(cores: &CoreGroups) -> usize {
        cores.cores.iter().map(|c| c.domain).collect::<BTreeSet<_>>().len()
    }

    /// Builds a host where CPU n and CPU n+8 are one core, with one L3 and one tier.
    fn smt_pairs_one_l3(allowed: &[usize]) -> Facts {
        host(allowed, |c| vec![c % 8, c % 8 + 8], |_| (0..16).collect(), |_| 1024)
    }

    /// Builds a cpuset of CPUs 0-31 and 64-95, where CPU n and CPU n+64 are one core, over 16
    /// four-core L3 domains.
    fn four_core_l3_domains_under_a_cpuset() -> Facts {
        let allowed: Vec<usize> = (0..32).chain(64..96).collect();
        host(
            &allowed,
            |c| vec![c % 64, c % 64 + 64],
            |c| {
                let domain = c % 64 / 4;
                (0..4).flat_map(|i| [domain * 4 + i, domain * 4 + i + 64]).collect()
            },
            |_| 1024,
        )
    }

    /// Builds six fast cores (0-5) and twelve slow ones (6-17), one CPU each, in one L3.
    fn two_tiers_no_smt() -> Facts {
        host(
            &(0..18).collect::<Vec<_>>(),
            |c| vec![c],
            |_| (0..18).collect(),
            |c| if c < 6 { 2 } else { 1 },
        )
    }

    /// Builds fast cores 0-15 as SMT pairs (2n, 2n+1) and slow cores 16-31, in one L3.
    fn hybrid_fast_and_slow_cores() -> Facts {
        host(
            &(0..32).collect::<Vec<_>>(),
            |c| {
                if c < 16 {
                    vec![c / 2 * 2, c / 2 * 2 + 1]
                } else {
                    vec![c]
                }
            },
            |_| (0..32).collect(),
            |c| if c < 16 { 2 } else { 1 },
        )
    }

    #[test]
    fn a_cpu_quota_below_the_cores_in_the_mask_bounds_the_physical_count() {
        let cpus = Cpus::new(n(2), n(32));
        assert_eq!((cpus.logical().get(), cpus.physical().get()), (2, 2));
    }

    #[test]
    fn without_a_quota_the_physical_count_is_the_cores_in_the_mask() {
        let cpus = Cpus::new(n(64), n(32));
        assert_eq!((cpus.logical().get(), cpus.physical().get()), (64, 32));
        let cpus = Cpus::new(n(20), n(32));
        assert_eq!((cpus.logical().get(), cpus.physical().get()), (20, 20));
    }

    #[test]
    fn smt_pairs_one_l3_full_mask_keeps_eight_threads_and_firsts_before_siblings() {
        let f = smt_pairs_one_l3(&(0..16).collect::<Vec<_>>());
        assert_eq!(threads(&f, Quota::Unlimited), 8);
        assert_eq!(order(&f, None), (0..16).collect::<Vec<_>>());
        assert_eq!(order(&f, Some(4)), vec![0, 1, 2, 3]);
        assert_eq!(order(&f, Some(8)), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(domain_count(&cores_of(&f)), 1);
        assert_eq!(first_cpus(&cores_of(&f)), ids(&[0, 1, 2, 3, 4, 5, 6, 7]));
    }

    #[test]
    fn four_core_l3_domains_fill_one_domain_before_the_next() {
        let f = four_core_l3_domains_under_a_cpuset();
        assert_eq!(threads(&f, Quota::Unlimited), 32);
        let full: Vec<usize> = (0..32).chain(64..96).collect();
        assert_eq!(order(&f, None), full);
        // Four threads share one L3: the first four cores are one domain.
        assert_eq!(order(&f, Some(4)), vec![0, 1, 2, 3]);
        assert_eq!(order(&f, Some(8)), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(domain_count(&cores_of(&f)), 8);
    }

    #[test]
    fn two_tiers_without_smt_count_the_fast_tier_only() {
        let f = two_tiers_no_smt();
        assert_eq!(threads(&f, Quota::Unlimited), 6);
        assert_eq!(order(&f, Some(6)), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn hybrid_fast_and_slow_cores_count_fast_then_slow_then_siblings() {
        let f = hybrid_fast_and_slow_cores();
        assert_eq!(threads(&f, Quota::Unlimited), 8);
        assert_eq!(order(&f, Some(4)), vec![0, 2, 4, 6]);
        assert_eq!(order(&f, Some(8)), vec![0, 2, 4, 6, 8, 10, 12, 14]);
        // With more threads than fast cores, the slow cores come before any sibling.
        assert_eq!(order(&f, Some(12)), vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 17, 18, 19]);
        let full: Vec<usize> = [0, 2, 4, 6, 8, 10, 12, 14]
            .into_iter()
            .chain(16..32)
            .chain([1, 3, 5, 7, 9, 11, 13, 15])
            .collect();
        assert_eq!(order(&f, None), full);
    }

    #[test]
    fn a_narrowed_mask_with_a_quota_caps_threads_and_pins_only_the_mask() {
        // The mask holds eight cores; their siblings 8-15 are not allowed, though the L3 list
        // names them.
        let f = smt_pairs_one_l3(&(0..8).collect::<Vec<_>>());
        assert_eq!(threads(&f, Quota::Unlimited), 8);
        assert_eq!(threads(&f, Quota::Cores(n(4))), 4);
        assert_eq!(order(&f, None), (0..8).collect::<Vec<_>>());
        assert_eq!(domain_count(&cores_of(&f)), 1);
    }

    #[test]
    fn quota_caps_threads_and_not_the_pin_order() {
        let f = four_core_l3_domains_under_a_cpuset();
        assert_eq!(
            (threads(&f, Quota::Cores(n(2))), order(&f, Some(4))),
            (2, vec![0, 1, 2, 3])
        );
        let f = hybrid_fast_and_slow_cores();
        assert_eq!(
            threads(&f, Quota::Cores(n(12))),
            8,
            "a quota above the fast cores: still 8"
        );
    }

    #[test]
    fn a_cpu_max_line_rounds_down_and_is_never_below_one() {
        assert_eq!(parse_cpu_max("max 100000\n").unwrap(), Quota::Unlimited);
        assert_eq!(parse_cpu_max("400000 100000").unwrap(), Quota::Cores(n(4)));
        assert_eq!(parse_cpu_max("250000 100000").unwrap(), Quota::Cores(n(2)));
        assert_eq!(parse_cpu_max("50000 100000").unwrap(), Quota::Cores(n(1)));
        for bad in ["", "max", "0 100000", "400000 0", "x 100000", "1 2 3", "max x"] {
            assert!(matches!(parse_cpu_max(bad), Err(CpuMaxError { .. })), "{bad:?}");
        }
    }

    #[test]
    fn tightest_takes_the_lower_limit_and_no_limit_defers() {
        assert_eq!(tightest(Quota::Cores(n(2)), Quota::Unlimited), Quota::Cores(n(2)));
        assert_eq!(tightest(Quota::Unlimited, Quota::Cores(n(3))), Quota::Cores(n(3)));
        assert_eq!(tightest(Quota::Cores(n(4)), Quota::Cores(n(2))), Quota::Cores(n(2)));
        assert_eq!(tightest(Quota::Unlimited, Quota::Unlimited), Quota::Unlimited);
    }

    #[test]
    fn an_unread_cache_is_one_domain() {
        // CPU n and CPU n+4 are one core, and each pair would have its own L3 if it were read.
        let f = rows(&(0..8).collect::<Vec<_>>(), |c| CpuFacts {
            siblings: ids(&[c % 4, c % 4 + 4]),
            cache: None,
            perf: Some(1),
        });
        assert_eq!(domain_count(&cores_of(&f)), 1);
    }

    #[test]
    fn a_cache_read_for_some_cores_only_is_not_used() {
        // There would be two L3s, but core 1's is not read, so there is one domain.
        let f = rows(&[0, 1], |c| CpuFacts {
            siblings: ids(&[c]),
            cache: (c == 0).then(|| ids(&[0])),
            perf: Some(1),
        });
        assert_eq!(domain_count(&cores_of(&f)), 1);
        let read = rows(&[0, 1], |c| CpuFacts {
            siblings: ids(&[c]),
            cache: Some(ids(&[c])),
            perf: Some(1),
        });
        assert_eq!(domain_count(&cores_of(&read)), 2);
    }

    #[test]
    fn an_l3_list_without_an_allowed_cpu_is_the_cores_own_domain() {
        // CPU 1's L3 list names only CPUs outside the mask; the key falls back to CPU 1 itself.
        let f = rows(&[0, 1], |c| CpuFacts {
            siblings: ids(&[c]),
            cache: Some(if c == 0 { ids(&[0]) } else { ids(&[9]) }),
            perf: Some(1),
        });
        assert_eq!(domain_count(&cores_of(&f)), 2);
    }

    #[test]
    fn an_unread_perf_is_one_tier_so_every_core_counts() {
        let f = rows(&[0, 1, 2, 3], |c| CpuFacts {
            siblings: ids(&[c]),
            cache: Some(ids(&[0, 1, 2, 3])),
            perf: None,
        });
        assert_eq!(threads(&f, Quota::Unlimited), 4);
    }

    #[test]
    fn a_perf_read_for_some_cores_only_is_not_used() {
        // Core 0 would be the one fast core, but core 3's value is not read: one tier.
        let f = rows(&[0, 1, 2, 3], |c| CpuFacts {
            siblings: ids(&[c]),
            cache: Some(ids(&[0, 1, 2, 3])),
            perf: (c != 3).then_some(if c == 0 { 2 } else { 1 }),
        });
        assert_eq!(threads(&f, Quota::Unlimited), 4);
    }

    #[test]
    fn facts_with_no_cpu_are_refused() {
        assert!(Facts::new(BTreeMap::new()).is_none());
    }

    /// Both edges of "every CPU has the reading": the first CPU or the last one without it.
    #[test]
    fn a_reading_missing_for_the_first_or_the_last_cpu_is_used_for_none() {
        for missing in [0, 2] {
            let f = rows(&[0, 1, 2], |c| CpuFacts {
                siblings: ids(&[c]),
                cache: (c != missing).then(|| ids(&[0, 1, 2])),
                perf: (c != missing).then_some(1),
            });
            assert_eq!(f.cache, None, "missing for CPU {missing}");
            assert_eq!(f.perf, None, "missing for CPU {missing}");
        }
        let whole = rows(&[0, 1, 2], |c| CpuFacts {
            siblings: ids(&[c]),
            cache: Some(ids(&[0, 1, 2])),
            perf: Some(1),
        });
        assert_eq!(whole.cache.map(|lists| lists.len()), Some(3));
        assert_eq!(whole.perf, Some(vec![1; 3]));
    }

    fn set(cpus: &[usize]) -> CpuSet {
        let mut set = CpuSet::empty();
        for &c in cpus {
            set.insert_range(c, c);
        }
        set
    }

    #[test]
    fn a_hybrid_p_core_list_is_two_tiers_p_above_e_whatever_the_capacity() {
        let p_cores = set(&[0, 1]);
        let value = |cpu| perf_value(Some(&p_cores), CoreId { id: cpu }, Some(512));
        assert_eq!(
            [0, 1, 2, 3].map(value),
            [Some(P_CORE), Some(P_CORE), Some(E_CORE), Some(E_CORE)]
        );
    }

    #[test]
    fn without_a_p_core_list_the_value_is_the_capacity_as_read() {
        assert_eq!(perf_value(None, CoreId { id: 0 }, Some(1024)), Some(1024));
        assert_eq!(perf_value(None, CoreId { id: 0 }, None), None);
    }

    fn perfs(values: &[(usize, Option<u32>)]) -> BTreeMap<CoreId, CpuFacts> {
        values
            .iter()
            .map(|&(c, perf)| {
                let facts = CpuFacts {
                    siblings: ids(&[c]),
                    cache: None,
                    perf,
                };
                (CoreId { id: c }, facts)
            })
            .collect()
    }

    #[test]
    fn values_for_every_cpu_or_for_none_leave_no_gap() {
        let every = perfs(&[(0, Some(1024)), (1, Some(1024)), (2, Some(512))]);
        assert_eq!(perf_gap(&every), None);
        let none = perfs(&[(0, None), (1, None), (2, None)]);
        assert_eq!(perf_gap(&none), None);
    }

    #[test]
    fn a_capacity_missing_for_one_cpu_is_a_gap_naming_it() {
        // The CPU without a capacity is the last, then the first: both edges of "every CPU has one".
        let last = perfs(&[(4, Some(1024)), (5, Some(1024)), (6, None)]);
        assert_eq!(
            perf_gap(&last),
            Some(PerfValueError::CapacityMissing { cpu: CoreId { id: 6 } })
        );
        let first = perfs(&[(4, None), (5, Some(1024)), (6, Some(1024))]);
        assert_eq!(
            perf_gap(&first),
            Some(PerfValueError::CapacityMissing { cpu: CoreId { id: 4 } })
        );
    }

    #[test]
    fn readings_become_one_row_per_allowed_cpu_and_an_unmade_one_is_unused() {
        let facts = Facts::new(
            [
                (
                    CoreId { id: 0 },
                    CpuFacts {
                        siblings: ids(&[0]),
                        cache: Some(ids(&[0, 1])),
                        perf: None,
                    },
                ),
                (
                    CoreId { id: 1 },
                    CpuFacts {
                        siblings: ids(&[1]),
                        cache: Some(ids(&[0, 1])),
                        perf: None,
                    },
                ),
            ]
            .into(),
        )
        .unwrap();
        assert_eq!(facts.cache.as_ref().map(|lists| &lists[0]), Some(&ids(&[0, 1])));
        assert_eq!(facts.perf, None);
        assert_eq!(facts.siblings[&CoreId { id: 1 }], ids(&[1]));
    }

    #[test]
    fn a_mask_holds_cpus_up_to_its_room_and_refuses_the_next() {
        let mut mask = CpuMask::empty(2049);
        assert!(mask.cpus() >= 2049);
        let room = mask.cpus();
        for cpu in [0, 63, 64, 1023, 1024, 2048, room - 1] {
            mask.insert(cpu).unwrap();
        }
        assert_eq!(
            mask.members().collect::<Vec<_>>(),
            vec![0, 63, 64, 1023, 1024, 2048, room - 1]
        );
        assert_eq!(mask.insert(room), Err(CpuMaskError::PastRoom { cpu: room, room }));
        assert_eq!(CpuMask::empty(1).cpus(), CpuMask::SETSIZE);
        assert_eq!(CpuMask::empty(1).bytes(), CpuMask::SETSIZE / 8);
    }

    #[test]
    fn a_mask_of_one_cpu_takes_the_last_linux_cpu_and_refuses_past_it() {
        let last = CpuMask::of_one(MAX_CPUS - 1).unwrap();
        assert_eq!(last.members().collect::<Vec<_>>(), vec![MAX_CPUS - 1]);
        assert_eq!(
            CpuMask::of_one(MAX_CPUS),
            Err(CpuMaskError::PastRoom {
                cpu: MAX_CPUS,
                room: MAX_CPUS
            })
        );
    }

    #[test]
    fn the_quota_caps_the_fast_cores_at_its_edge() {
        assert_eq!(threads_for(n(4), Quota::Unlimited), n(4));
        assert_eq!(threads_for(n(4), Quota::Cores(n(4))), n(4));
        assert_eq!(threads_for(n(4), Quota::Cores(n(3))), n(3));
        assert_eq!(threads_for(n(4), Quota::Cores(n(5))), n(4));
    }

    #[test]
    fn a_cpu_list_parses_ranges_and_singletons() {
        assert_eq!(parse_cpu_list("0-3,8"), Ok(set(&[0, 1, 2, 3, 8])));
        assert_eq!(parse_cpu_list("5\n"), Ok(set(&[5])));
        assert!(parse_cpu_list(" ").is_err());
    }

    #[test]
    fn a_cpu_set_holds_ranges_across_words_and_lists_them_in_order() {
        let mut cpus = CpuSet::empty();
        cpus.insert_range(62, 129);
        cpus.insert_range(MAX_CPUS - 1, MAX_CPUS - 1);
        let listed: Vec<usize> = cpus.iter().map(|c| c.id).collect();
        let want: Vec<usize> = (62..=129).chain([MAX_CPUS - 1]).collect();
        assert_eq!(listed, want);
        assert!(cpus.contains(CoreId { id: 62 }) && !cpus.contains(CoreId { id: 61 }));
        assert!(!cpus.contains(CoreId { id: MAX_CPUS }));
    }

    #[test]
    fn a_cpu_list_repeating_the_whole_range_is_one_set_of_max_cpus() {
        // A 4 KB forged list that names every CPU hundreds of times is still 8192 CPUs.
        let text = vec!["0-8191"; 4096 / 7].join(",");
        assert!(text.len() >= 4000);
        let cpus = parse_cpu_list(&text).unwrap();
        assert_eq!(cpus.iter().count(), MAX_CPUS);
        assert_eq!(parse_cpu_list("3,3,1-3"), Ok(set(&[1, 2, 3])));
    }

    #[test]
    fn groups_come_in_cpu_order_without_repeats_whatever_order_they_are_given_in() {
        let shown = grouped(&[9, 1, 8, 0, 1], |cpu| {
            let core = cpu % 8;
            ids(&[core + 8, core, core + 8])
        });
        assert_eq!(shown, vec![ids(&[0, 8]), ids(&[1, 9])]);
    }

    #[test]
    fn disagreeing_sibling_lists_still_put_each_cpu_in_one_group() {
        let shown = grouped(&[0, 1, 2], |cpu| match cpu {
            0 => ids(&[0, 1]),
            1 => ids(&[1, 2]),
            _ => ids(&[2]),
        });
        assert_eq!(shown, vec![ids(&[0, 1]), ids(&[2])]);
    }

    fn grouped(allowed: &[usize], siblings: impl Fn(usize) -> Vec<CoreId>) -> Vec<Vec<CoreId>> {
        let f = rows(allowed, |c| CpuFacts {
            siblings: siblings(c),
            cache: None,
            perf: None,
        });
        cores_of(&f).cores.iter().map(|c| c.cpus.cpus.clone()).collect()
    }

    #[test]
    fn a_core_lists_its_first_cpu_then_its_siblings() {
        let alone = cores_of(&rows(&[5], |_| CpuFacts {
            siblings: Vec::new(),
            cache: None,
            perf: None,
        }));
        let alone = &alone.cores[0].cpus;
        let cpus: &[CoreId] = &alone.cpus;
        assert_eq!(cpus, &ids(&[5])[..]);
        assert_eq!(alone.first(), CoreId { id: 5 });
        assert!(alone.rest().is_empty());
        let smt = cores_of(&rows(&[0, 8], |c| CpuFacts {
            siblings: ids(&[c % 8, c % 8 + 8]),
            cache: None,
            perf: None,
        }));
        let smt = &smt.cores[0].cpus;
        let cpus: &[CoreId] = &smt.cpus;
        assert_eq!(cpus, &ids(&[0, 8])[..]);
        assert_eq!(smt.first(), CoreId { id: 0 });
        assert_eq!(smt.rest(), &ids(&[8])[..]);
    }

    #[test]
    fn a_pin_order_with_no_cpu_is_not_made() {
        assert!(CpuOrder::new(Vec::new()).is_none());
        let order = CpuOrder::new(ids(&[3, 1])).unwrap();
        assert_eq!(order.cpus(), &ids(&[3, 1])[..]);
    }

    #[test]
    fn a_core_group_order_counts_its_physical_cores() {
        let one = host(&[5], |c| vec![c], |c| vec![c], |_| 1);
        let d = decide(&one);
        assert_eq!(d.pin_order().cpus(), &ids(&[5])[..]);
        assert_eq!(core_count(&d), 1);
        let eight = smt_pairs_one_l3(&(0..16).collect::<Vec<_>>());
        let d = decide(&eight);
        assert_eq!(core_count(&d), 8);
        assert_eq!(d.pin_order().cpus().len(), 16);
    }

    /// Builds slow cores 0-11 and fast cores 12-17, one CPU each, in one L3.
    fn slow_cores_numbered_first() -> Facts {
        host(
            &(0..18).collect::<Vec<_>>(),
            |c| vec![c],
            |_| (0..18).collect(),
            |c| if c < 12 { 1 } else { 2 },
        )
    }

    /// Builds two sockets with interleaved CPUs: even CPUs share socket 0's L3 and odd ones socket
    /// 1's; eight cores, no SMT.
    fn two_sockets_interleaved_even_odd() -> Facts {
        host(
            &(0..8).collect::<Vec<_>>(),
            |c| vec![c],
            |c| (0..8).filter(|x| x % 2 == c % 2).collect(),
            |_| 1024,
        )
    }

    #[test]
    fn slow_cores_numbered_first_are_pinned_after_the_fast_ones() {
        // Sorting by CPU number would give 0, 1, ...: the order goes by tier first.
        let f = slow_cores_numbered_first();
        assert_eq!(threads(&f, Quota::Unlimited), 6);
        assert_eq!(order(&f, Some(6)), vec![12, 13, 14, 15, 16, 17]);
        assert_eq!(order(&f, Some(8)), vec![12, 13, 14, 15, 16, 17, 0, 1]);
    }

    #[test]
    fn interleaved_sockets_fill_one_socket_before_the_other() {
        // Sorting by CPU number would alternate sockets: the order goes by cache domain first.
        let f = two_sockets_interleaved_even_odd();
        assert_eq!(domain_count(&cores_of(&f)), 2);
        assert_eq!(order(&f, None), vec![0, 2, 4, 6, 1, 3, 5, 7]);
    }

    fn fault(text: &str) -> CpuListFault {
        let e = parse_cpu_list(text).unwrap_err();
        assert_eq!(e.text, text, "the error carries the text as given");
        e.fault
    }

    #[test]
    fn a_cpu_list_that_is_not_one_says_what_is_wrong_with_the_text() {
        assert_eq!(fault(" "), CpuListFault::Empty);
        assert_eq!(fault("3-1"), CpuListFault::Reversed { lo: 3, hi: 1 });
        assert_eq!(fault(",0,,1,"), CpuListFault::EmptyPart);
        assert_eq!(fault("8192"), CpuListFault::PastMaxCpus { cpu: 8192 });
        assert_eq!(parse_cpu_list("8191"), Ok(set(&[8191])));
    }

    /// Writes a set the way the kernel writes a CPU list: runs as `lo-hi`, a lone CPU as itself.
    fn kernel_cpu_list(cpus: &CpuSet) -> String {
        let mut runs: Vec<(usize, usize)> = Vec::new();
        for cpu in cpus.iter().map(|c| c.id) {
            match runs.last_mut() {
                Some((_, hi)) if *hi + 1 == cpu => *hi = cpu,
                _ => runs.push((cpu, cpu)),
            }
        }
        let parts: Vec<String> = runs
            .into_iter()
            .map(|(lo, hi)| if lo == hi { lo.to_string() } else { format!("{lo}-{hi}") })
            .collect();
        format!("{}\n", parts.join(","))
    }

    /// Text near a CPU list (its own alphabet plus a sign) and any text at all.
    fn cpu_list_like_text() -> impl proptest::strategy::Strategy<Value = String> {
        proptest::prop_oneof!["[0-9,+ \n-]{0,48}", "\\PC{0,48}"]
    }

    /// Mostly the edges a division turns on, sometimes any value.
    fn cpu_max_field() -> impl proptest::strategy::Strategy<Value = u64> {
        proptest::prop_oneof![0..4u64, 99_999..100_002u64, proptest::num::u64::ANY]
    }

    proptest::proptest! {
        #[test]
        fn any_text_parses_without_panicking_into_cpus_below_max_cpus(text in cpu_list_like_text()) {
            if let Ok(cpus) = parse_cpu_list(&text) {
                proptest::prop_assert!(cpus.iter().all(|c| c.id < MAX_CPUS));
                proptest::prop_assert!(cpus.iter().next().is_some(), "a parsed list names a CPU");
            }
        }

        #[test]
        fn a_cpu_set_written_as_the_kernel_writes_it_parses_back_to_itself(
            ranges in proptest::collection::vec((0..MAX_CPUS, 0usize..200), 1..8),
        ) {
            let mut cpus = CpuSet::empty();
            let mut want = BTreeSet::new();
            for (lo, len) in ranges {
                let hi = (lo + len).min(MAX_CPUS - 1);
                cpus.insert_range(lo, hi);
                want.extend(lo..=hi);
            }
            proptest::prop_assert_eq!(cpus.iter().map(|c| c.id).collect::<BTreeSet<_>>(), want);
            let text = kernel_cpu_list(&cpus);
            proptest::prop_assert_eq!(parse_cpu_list(&text), Ok(cpus), "{:?}", text);
        }

        #[test]
        fn a_cpu_max_line_is_the_quota_over_the_period_but_at_least_one(
            quota in cpu_max_field(),
            period in cpu_max_field(),
        ) {
            let want = if period == 0 {
                Err(CpuMaxFault::PeriodZero)
            } else if quota == 0 {
                Err(CpuMaxFault::QuotaZero)
            } else {
                usize::try_from(quota / period)
                    .map(|cores| Quota::Cores(n(cores.max(1))))
                    .map_err(|_| CpuMaxFault::PastUsize)
            };
            let got = parse_cpu_max(&format!("{quota} {period}\n")).map_err(|e| e.fault);
            proptest::prop_assert_eq!(got, want);
            let unlimited = parse_cpu_max(&format!("max {period}\n")).map_err(|e| e.fault);
            let want = if period == 0 { Err(CpuMaxFault::PeriodZero) } else { Ok(Quota::Unlimited) };
            proptest::prop_assert_eq!(unlimited, want);
        }

        #[test]
        fn any_text_as_a_cpu_max_line_parses_without_panicking(text in cpu_list_like_text()) {
            if let Err(e) = parse_cpu_max(&text) {
                proptest::prop_assert_eq!(e.text, text);
            }
        }
    }
}

/// Proves `CpuSet::insert_range`'s bit arithmetic with Kani for every range of Linux CPUs spanning up to three
/// words: a first, a middle and a last word cover every case of the loop, and a longer range repeats the middle one.
#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    #[kani::unwind(4)]
    fn a_range_sets_exactly_its_cpus() {
        let (lo, hi, cpu): (usize, usize, usize) = (kani::any(), kani::any(), kani::any());
        kani::assume(lo <= hi && hi < MAX_CPUS && hi / 64 <= lo / 64 + 2 && cpu < MAX_CPUS);
        let mut cpus = CpuSet::empty();
        cpus.insert_range(lo, hi);
        assert_eq!(cpus.contains(CoreId { id: cpu }), (lo..=hi).contains(&cpu));
        assert_eq!(cpus.end, hi / 64 + 1);
    }
}
