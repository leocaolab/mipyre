//! Lays out the process lanes' shared memory, one lane per worker, and waits on it across
//! processes.
//!
//! A lane is two single-producer single-consumer rings sharing a slot index: request `k` goes into
//! request slot `k % depth` and its answer into response slot `k % depth`. Each counter has one
//! writer: the server writes `req_head`, the worker writes `taken` (`k + 1` while it runs `k`) and
//! `resp_head`. A slot is free again once the server has read its answer.
//!
//! A writer publishes with `SeqCst` and wakes the other side only when it flagged that it sleeps;
//! the sleeper sets its flag and re-reads the counter before it parks (Dekker), so a busy lane
//! never enters the kernel.

use std::num::NonZeroUsize;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::*};

use super::os::WaitError;
use super::spin::{spin, SpinEnd};
use super::wire::MessageWriter;
use crate::exchange::LaneDecodeError;
use mipyre_shm::plan::ring::{RingDepth, Seq};
use mipyre_shm::plan::slot_len::{decode_len, SlotLengthError};
use mipyre_shm::plan::spin_window::SpinWindow;

pub(crate) use mipyre_cpu::CacheLine;

/// Holds a lane's counters and flags; the lane's slots follow it.
#[repr(C)]
pub(crate) struct LaneHeader {
    pub(crate) req_head: CacheLine<AtomicU32>,
    pub(crate) taken: CacheLine<AtomicU32>,
    pub(crate) resp_head: CacheLine<AtomicU32>,
    /// It is 1 while the worker is parked, or about to park, on `req_head`.
    pub(crate) worker_sleeping: CacheLine<AtomicU32>,
    /// It is 1 while the server's reaper is parked, or about to park, on `resp_head`.
    pub(crate) server_sleeping: CacheLine<AtomicU32>,
    /// The worker writes its pid here when it is ready.
    pub(crate) ready_pid: CacheLine<AtomicU32>,
    /// It holds when the worker started its current request, in `monotonic_ns`.
    pub(crate) started_ns: CacheLine<AtomicU64>,
    /// A worker that could not start sets this after writing its traceback into response
    /// slot 0.
    pub(crate) start_failed: CacheLine<AtomicU32>,
    /// Only the worker serving the lane writes these, and they accumulate over the server's
    /// life.
    pub(crate) worker_waits: CacheLine<WaitCounts>,
}

/// Counts how one side of a lane waited for the other; it has one writer, and others read it
/// only for stats.
#[derive(Default)]
pub(crate) struct WaitCounts {
    /// A worker counts a wait for each request it did not find at once; a host counts each spin
    /// before its thread parks.
    pub(crate) waits: OwnCounter,
    pub(crate) spin_rounds: OwnCounter,
    pub(crate) parks: OwnCounter,
    pub(crate) wakes: OwnCounter,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WaitTotals {
    pub(crate) waits: u64,
    pub(crate) spin_rounds: u64,
    pub(crate) parks: u64,
    pub(crate) wakes: u64,
}

/// Holds a counter whose one writer adds with a plain load and store instead of a
/// read-modify-write; others only read it. It is one word, as the lane header lays it out.
#[derive(Default)]
#[repr(transparent)]
pub(crate) struct OwnCounter(AtomicU64);

impl OwnCounter {
    /// Adds `n`; only the counter's one writer may call it.
    #[inline]
    pub(crate) fn add(&self, n: u64) {
        self.0.store(self.0.load(Relaxed).wrapping_add(n), Relaxed);
    }

    pub(crate) fn read(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

impl WaitCounts {
    /// Adds `t` to the counts; only their one writer may call it.
    #[inline]
    pub(crate) fn add(&self, t: WaitTotals) {
        self.waits.add(t.waits);
        self.spin_rounds.add(t.spin_rounds);
        self.parks.add(t.parks);
        self.wakes.add(t.wakes);
    }

    pub(crate) fn read(&self) -> WaitTotals {
        WaitTotals {
            waits: self.waits.read(),
            spin_rounds: self.spin_rounds.read(),
            parks: self.parks.read(),
            wakes: self.wakes.read(),
        }
    }
}

/// Describes the lanes' layout; the field types carry the lower bounds and [`Geometry::new`]
/// checks the upper ones.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Geometry {
    lanes: NonZeroUsize,
    depth: RingDepth,
    /// It is at most [`MAX_SLOT_BYTES`].
    slot_bytes: SlotBytes,
    /// It is `slot_bytes` rounded up to a power of two.
    slot_stride: usize,
    /// It is a power of two that holds the lane's two rings.
    lane_stride: usize,
    /// It is where the first lane's slots start, after every lane's header.
    slots_at: usize,
    total_bytes: usize,
}

/// A lane's header takes one 16 KiB page, the largest page size we run on, so each header starts
/// page-aligned.
const HEADER_SHIFT: u32 = 14;
const _: () = assert!(std::mem::size_of::<LaneHeader>() <= 1 << HEADER_SHIFT);

/// Returns `x · 2^k`, or `None` when it does not fit a `usize`.
fn shifted(x: usize, k: u32) -> Option<usize> {
    (k < usize::BITS && x.leading_zeros() >= k).then(|| x << k)
}

/// Any failure message fits a slot this large once `wire::encode_failure` cuts it.
pub(crate) const MIN_SLOT_BYTES: usize = 4096;

/// A slot's message length is a 4-byte prefix, so a larger slot would record a wrapped length.
pub(crate) const MAX_SLOT_BYTES: usize = u32::MAX as usize + 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ring {
    Request,
    Response,
}

/// Counts a slot's bytes, the 4-byte length prefix included; it is never below
/// [`MIN_SLOT_BYTES`], because only [`SlotBytes::new`] makes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SlotBytes(usize);

impl SlotBytes {
    /// Returns `None` below [`MIN_SLOT_BYTES`].
    pub(crate) fn new(bytes: usize) -> Option<Self> {
        (bytes >= MIN_SLOT_BYTES).then_some(SlotBytes(bytes))
    }

    pub(crate) fn get(self) -> usize {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GeometryError {
    #[error("process lane slot_bytes must be ≤ {MAX_SLOT_BYTES} (a slot's message length is a u32), got {slot_bytes}")]
    SlotPastLengthPrefix { slot_bytes: usize },
    /// The sizes overflow `usize` or pass `off_t::MAX`; a wrapped product would map less than
    /// the slots written into it.
    #[error("process lanes: the shared memory for workers={lanes}, depth={depth}, slot_bytes={slot_bytes} is too large")]
    TooLarge {
        lanes: usize,
        depth: u32,
        slot_bytes: usize,
    },
}

impl Geometry {
    pub(crate) fn new(
        lanes: NonZeroUsize,
        depth: RingDepth,
        slot_bytes: SlotBytes,
    ) -> Result<Self, GeometryError> {
        let too_large = || GeometryError::TooLarge {
            lanes: lanes.get(),
            depth: depth.get(),
            slot_bytes: slot_bytes.get(),
        };
        let slot_shift = slot_bytes
            .get()
            .checked_next_power_of_two()
            .ok_or_else(too_large)?
            .trailing_zeros();
        let lane_shift = slot_shift + depth.get().trailing_zeros() + 1;
        let lane_stride = shifted(1, lane_shift).ok_or_else(too_large)?;
        let slots_at = shifted(lanes.get(), HEADER_SHIFT).ok_or_else(too_large)?;
        let total_bytes = shifted(lanes.get(), lane_shift)
            .and_then(|slots| slots.checked_add(slots_at))
            .ok_or_else(too_large)?;
        if total_bytes > libc::off_t::MAX as usize {
            return Err(too_large());
        }
        // A layout too large to map is reported before a slot past its prefix.
        if slot_bytes.get() > MAX_SLOT_BYTES {
            return Err(GeometryError::SlotPastLengthPrefix {
                slot_bytes: slot_bytes.get(),
            });
        }
        Ok(Geometry {
            lanes,
            depth,
            slot_bytes,
            slot_stride: 1 << slot_shift,
            lane_stride,
            slots_at,
            total_bytes,
        })
    }

    pub(crate) fn lanes(&self) -> usize {
        self.lanes.get()
    }

    pub(crate) fn depth(&self) -> u32 {
        self.depth.get()
    }

    pub(crate) fn ring_depth(&self) -> RingDepth {
        self.depth
    }

    #[inline]
    fn header_offset(lane: usize) -> usize {
        lane << HEADER_SHIFT
    }

    #[inline]
    fn slot_offset(&self, lane: usize, ring: Ring, seq: u32) -> usize {
        self.lane_slots_offset(lane) + self.slot_in_lane(ring, seq)
    }

    #[inline]
    fn lane_slots_offset(&self, lane: usize) -> usize {
        self.slots_at + lane * self.lane_stride
    }

    #[inline]
    fn slot_in_lane(&self, ring: Ring, seq: u32) -> usize {
        let ring_at = match ring {
            Ring::Request => 0,
            Ring::Response => self.depth.get() as usize,
        };
        (ring_at + Seq::new(seq).slot_index(self.depth)) * self.slot_stride
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(crate) fn max_message(&self) -> usize {
        self.slot_bytes.get() - 4
    }
}

/// Stands for "no worker" in a lane's atomic pid fields, which cannot hold an `Option`; no
/// process has pid 0.
pub(crate) const NO_WORKER: i32 = 0;

/// Points at the lanes' mapping, which is never unmapped because every process uses it for its
/// whole life; a copy is the same mapping.
#[derive(Clone, Copy)]
pub(crate) struct LaneShm {
    base: *mut u8,
    /// It stays private because the slot offsets are computed from it.
    geometry: Geometry,
}

// SAFETY: The region is used only through atomics and through slots that the counters give to
// one side at a time.
unsafe impl Send for LaneShm {}
unsafe impl Sync for LaneShm {}

impl LaneShm {
    /// Creates a zeroed shared memory object and maps it, returning the fd so the template can map
    /// it too; the template was forked before this runs, so it cannot inherit the mapping.
    pub(crate) fn create(geometry: Geometry) -> std::io::Result<(Self, OwnedFd)> {
        let fd = shared_memory_fd()?;
        // `Geometry` keeps the total within `off_t`, so it fits `u64` too.
        fix_size(fd.as_fd(), geometry.total_bytes() as u64)?;
        let region = Self::attach(geometry, fd.as_fd())?;
        Ok((region, fd))
    }

    /// Maps the shared memory object `fd` that [`LaneShm::create`] made.
    ///
    /// # Errors
    ///
    /// An `InvalidData` error carrying an `AttachError` when `fd` is smaller than `geometry`
    /// lays out, so that no page of the mapping lies past the end of the object.
    pub(crate) fn attach(geometry: Geometry, fd: BorrowedFd<'_>) -> std::io::Result<Self> {
        check_covers(rustix::fs::fstat(fd)?.st_size, geometry.total_bytes())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(LaneShm {
            base: super::os::map_shared(fd, geometry.total_bytes())?,
            geometry,
        })
    }

    pub(crate) fn geometry(&self) -> Geometry {
        self.geometry
    }

    pub(crate) fn header(&self, lane: usize) -> &LaneHeader {
        assert!(lane < self.geometry.lanes(), "lane {lane} out of range");
        // SAFETY: For a lane below `lanes` the header lies inside the mapping, as the Kani proof
        // `every_header_lies_before_the_slots` shows; it is page-aligned because the mapping is
        // and headers are a page apart, and all its fields are atomics.
        unsafe { &*(self.base.add(Geometry::header_offset(lane)) as *const LaneHeader) }
    }

    pub(crate) fn req_slot(&self, lane: usize, seq: u32) -> *mut u8 {
        self.slot(lane, Ring::Request, seq)
    }

    pub(crate) fn resp_slot(&self, lane: usize, seq: u32) -> *mut u8 {
        self.slot(lane, Ring::Response, seq)
    }

    fn slot(&self, lane: usize, ring: Ring, seq: u32) -> *mut u8 {
        assert!(lane < self.geometry.lanes(), "lane {lane} out of range");
        // SAFETY: For a lane below `lanes` the slot lies inside the mapping, as the Kani proofs
        // `every_lanes_slots_lie_inside_the_mapping` and `every_slot_lies_inside_its_lane` show.
        unsafe { self.base.add(self.geometry.slot_offset(lane, ring, seq)) }
    }

    /// Lets `fill` write a message into `slot` and records the written length, which never
    /// passes the slot's room.
    ///
    /// # Safety
    ///
    /// The caller owns the slot.
    pub(crate) unsafe fn write_slot<R>(
        &self,
        slot: *mut u8,
        fill: impl FnOnce(&mut MessageWriter<'_>) -> R,
    ) -> R {
        let buf = std::slice::from_raw_parts_mut(slot.add(4), self.geometry.max_message());
        let mut w = MessageWriter::new(buf);
        let out = fill(&mut w);
        (slot as *mut u32).write_unaligned(w.len());
        out
    }

    /// Returns a slot's message.
    ///
    /// # Errors
    ///
    /// Returns [`LaneDecodeError::PastSlot`] when the recorded length runs past the slot.
    ///
    /// # Safety
    ///
    /// The caller owns the slot, and it holds a message.
    pub(crate) unsafe fn read_slot<'a>(
        &self,
        slot: *const u8,
    ) -> Result<&'a [u8], LaneDecodeError> {
        // `Geometry::new` refuses a slot past its 4-byte length prefix, so the room fits a `u32`.
        let max = self.geometry.max_message() as u32;
        // The lane's own error takes the values; carrying `SlotLengthError` inside `LaneDecodeError` changes the
        // layout of the decode result and lengthens the collect loop on every answer.
        let n = decode_len((slot as *const u32).read_unaligned(), max)
            .map_err(|SlotLengthError::PastSlot { len, max }| LaneDecodeError::PastSlot { len: len as usize, max: max as usize })?;
        Ok(std::slice::from_raw_parts(slot.add(4), n as usize))
    }

    /// Returns the traceback a worker of `lane` left when it could not start.
    pub(crate) fn start_failure(&self, lane: usize) -> Option<String> {
        if self.header(lane).start_failed.0.load(Acquire) == 0 {
            return None;
        }
        // SAFETY: The worker wrote the message before it set the flag, and then ended.
        let failure = match unsafe { self.read_slot(self.resp_slot(lane, 0)) } {
            Ok(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Err(malformed) => malformed.to_string(),
        };
        Some(failure)
    }

    /// Resets every lane's counters; it is for between two server runs, when no worker is alive.
    pub(crate) fn reset(&self) {
        for lane in 0..self.geometry.lanes() {
            let h = self.header(lane);
            for c in [
                &h.req_head.0,
                &h.taken.0,
                &h.resp_head.0,
                &h.worker_sleeping.0,
                &h.server_sleeping.0,
                &h.start_failed.0,
            ] {
                c.store(0, SeqCst);
            }
            h.ready_pid.0.store(NO_WORKER as u32, SeqCst);
            h.started_ns.0.store(0, SeqCst);
        }
    }
}

/// Reports a shared memory object smaller than the layout mapped over it: touching a page past
/// its end raises SIGBUS in whichever process touches it.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("the lanes' shared memory object is {file_bytes} bytes, but their layout needs {need}")]
struct AttachError {
    file_bytes: i64,
    need: usize,
}

fn check_covers(file_bytes: i64, need: usize) -> Result<(), AttachError> {
    if u64::try_from(file_bytes).is_ok_and(|bytes| bytes >= need as u64) {
        return Ok(());
    }
    Err(AttachError { file_bytes, need })
}

#[cfg(target_os = "linux")]
fn shared_memory_fd() -> std::io::Result<OwnedFd> {
    use rustix::fs::MemfdFlags;
    Ok(rustix::fs::memfd_create(c"mipyre-lanes", MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING)?)
}

/// Sizes the lanes' shared memory to `bytes` for good. Every worker holds the fd and runs user C
/// extensions; one that shrank the object would make the server's next touch of a cut page a
/// SIGBUS that ends the whole server, so on Linux the size is sealed before the fd leaves.
#[cfg(target_os = "linux")]
fn fix_size(fd: BorrowedFd<'_>, bytes: u64) -> std::io::Result<()> {
    use rustix::fs::SealFlags;
    rustix::fs::ftruncate(fd, bytes)?;
    rustix::fs::fcntl_add_seals(fd, SealFlags::SHRINK | SealFlags::GROW | SealFlags::SEAL)?;
    Ok(())
}

/// Sizes the lanes' shared memory to `bytes` for good: macOS sizes a POSIX shared memory object
/// once, and refuses every later `ftruncate` with EINVAL.
#[cfg(target_os = "macos")]
fn fix_size(fd: BorrowedFd<'_>, bytes: u64) -> std::io::Result<()> {
    Ok(rustix::fs::ftruncate(fd, bytes)?)
}

#[cfg(target_os = "macos")]
fn shared_memory_fd() -> std::io::Result<OwnedFd> {
    use std::sync::atomic::AtomicU64;
    static N: AtomicU64 = AtomicU64::new(0);
    // macOS limits the name to 31 bytes.
    let name = std::ffi::CString::new(format!(
        "/pyrl.{}.{}",
        std::process::id(),
        N.fetch_add(1, Relaxed)
    ))
    .map_err(std::io::Error::other)?;
    use rustix::shm::OFlags;
    let fd = rustix::shm::open(
        name.as_c_str(),
        OFlags::CREATE | OFlags::EXCL | OFlags::RDWR,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )?;
    // The object lives on through the fds, and nothing else should find it by name: a name left
    // behind would outlive the process, so failing to remove it fails the creation.
    rustix::shm::unlink(name.as_c_str())?;
    rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)?;
    Ok(fd)
}

/// Reads `CLOCK_MONOTONIC` in nanoseconds, which every process on the machine shares.
pub(crate) fn monotonic_ns() -> u64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Parks until `a` is no longer `expect`, a wake comes or `timeout_ns` passes; it may also
/// return spuriously.
///
/// # Errors
///
/// [`WaitError`] when the OS refuses the wait.
pub(super) fn park(a: &AtomicU32, expect: u32, timeout_ns: u64) -> Result<(), WaitError> {
    super::os::wait(a, expect, timeout_ns)
}

pub(crate) fn wake(a: &AtomicU32) {
    super::os::wake(a)
}

/// Spins until `counter != seen` or `window` is over; it is never inlined, so its rounds cost
/// the same instructions wherever they are counted.
#[inline(never)]
pub(crate) fn spin_for_change(counter: &AtomicU32, seen: u32, window: SpinWindow) -> SpinEnd {
    spin(window, monotonic_ns, || counter.load(Acquire) != seen)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CounterWait {
    pub(crate) value: u32,
    pub(crate) parks: u64,
}

/// Parks until `counter != seen` in slices of `slice_ns`, raising the `sleeping` flag and
/// re-reading the counter before each park so a publisher cannot miss it.
///
/// # Errors
///
/// Returns the error `park_with` returns, or the one `between` returns after a slice to give up.
pub(crate) fn park_for_change<E>(
    counter: &AtomicU32,
    sleeping: &AtomicU32,
    seen: u32,
    slice_ns: u64,
    mut park_with: impl FnMut(&AtomicU32, u32, u64) -> Result<(), E>,
    mut between: impl FnMut() -> Result<(), E>,
) -> Result<CounterWait, E> {
    let mut parks = 0;
    loop {
        sleeping.store(1, SeqCst);
        let v = counter.load(SeqCst);
        if v != seen {
            sleeping.store(0, Relaxed);
            return Ok(CounterWait { value: v, parks });
        }
        let parked = park_with(counter, seen, slice_ns);
        parks += 1;
        sleeping.store(0, Relaxed);
        parked?;
        let v = counter.load(Acquire);
        if v != seen {
            return Ok(CounterWait { value: v, parks });
        }
        between()?;
    }
}

/// Publishes `value` into `counter` and returns whether it woke the other side, which it does
/// only when the other side flagged that it sleeps.
#[inline]
pub(crate) fn publish(counter: &AtomicU32, sleeping: &AtomicU32, value: u32) -> bool {
    counter.store(value, SeqCst);
    let asleep = sleeping.load(SeqCst) == 1;
    if asleep {
        wake(counter);
    }
    asleep
}

#[cfg(test)]
mod tests {
    use super::*;

    fn geometry(lanes: usize, depth: u32, slot_bytes: usize) -> Result<Geometry, GeometryError> {
        Geometry::new(
            NonZeroUsize::new(lanes).expect("a test asks for at least one lane"),
            RingDepth::new(depth).expect("a test asks for a power-of-two depth"),
            SlotBytes::new(slot_bytes).expect("a test asks for at least MIN_SLOT_BYTES"),
        )
    }

    #[test]
    fn a_slot_whose_length_runs_past_the_slot_is_malformed() {
        let geometry = geometry(1, 1, MIN_SLOT_BYTES).unwrap();
        let (region, _fd) = LaneShm::create(geometry).unwrap();
        let slot = region.req_slot(0, 0);
        // SAFETY: the test owns the slot.
        unsafe {
            (slot as *mut u32).write_unaligned(geometry.max_message() as u32 + 1);
            assert!(region.read_slot(slot).is_err());
        }
    }

    #[test]
    fn a_message_of_the_slot_room_round_trips() {
        let geometry = geometry(1, 1, MIN_SLOT_BYTES).unwrap();
        let (region, _fd) = LaneShm::create(geometry).unwrap();
        let slot = region.req_slot(0, 0);
        let max = geometry.max_message();
        // SAFETY: the test owns the slot.
        unsafe {
            region.write_slot(slot, |w| w.put(&vec![7; max])).unwrap();
            let msg = region.read_slot(slot).unwrap();
            assert_eq!(msg.len(), max);
            assert!(msg.iter().all(|&b| b == 7));
        }
    }

    #[test]
    fn a_slot_below_the_minimum_is_refused() {
        assert!(SlotBytes::new(MIN_SLOT_BYTES - 1).is_none());
    }

    #[test]
    fn geometry_refuses_sizes_that_wrap_usize() {
        let err = geometry(1, 1 << 31, usize::MAX / 2).unwrap_err();
        assert!(matches!(err, GeometryError::TooLarge { .. }), "{err}");
    }

    /// With minimum slots a lane is 32 KiB, so `2^48` lanes make `2^63` bytes, one past
    /// `off_t::MAX`.
    #[test]
    fn geometry_refuses_a_mapping_past_the_file_offset() {
        let err = geometry(1 << 48, 2, MIN_SLOT_BYTES).unwrap_err();
        assert!(matches!(err, GeometryError::TooLarge { .. }), "{err}");
        let just_under = geometry((1 << 48) - 1, 2, MIN_SLOT_BYTES).unwrap();
        assert!(just_under.total_bytes() <= libc::off_t::MAX as usize);
    }

    #[test]
    fn geometry_totals_match_the_lane_layout() {
        let g = geometry(3, 4, MIN_SLOT_BYTES + 1).unwrap();
        let (header, slot) = (16 * 1024, 2 * MIN_SLOT_BYTES);
        assert_eq!(g.total_bytes(), 3 * header + 3 * 2 * 4 * slot);
        assert_eq!(g.slot_offset(0, Ring::Request, 0), 3 * header);
        assert_eq!(g.slot_offset(1, Ring::Response, 5), 3 * header + 2 * 4 * slot + 4 * slot + slot);
        assert_eq!(g.max_message(), MIN_SLOT_BYTES - 3);
    }

    #[test]
    fn a_slot_past_the_length_prefix_is_refused() {
        let err = geometry(1, 1, MAX_SLOT_BYTES + 1).unwrap_err();
        assert!(
            matches!(err, GeometryError::SlotPastLengthPrefix { .. }),
            "{err}"
        );
        let at_most = geometry(1, 1, MAX_SLOT_BYTES).unwrap();
        assert_eq!(at_most.max_message(), u32::MAX as usize);
    }

    #[test]
    fn a_fill_past_the_room_leaves_the_length_within_the_slot() {
        let geometry = geometry(1, 1, MIN_SLOT_BYTES).unwrap();
        let (region, _fd) = LaneShm::create(geometry).unwrap();
        let slot = region.req_slot(0, 0);
        let max = geometry.max_message();
        // SAFETY: the test owns the slot.
        unsafe {
            let refused = region.write_slot(slot, |w| {
                w.put(&[1; 10])?;
                w.put(&vec![2; max])
            });
            assert!(refused.is_err());
            assert_eq!(region.read_slot(slot).unwrap(), &[1; 10]);
        }
    }

    #[test]
    fn a_change_already_there_is_returned_without_parking() {
        let (counter, sleeping) = (AtomicU32::new(3), AtomicU32::new(0));
        let parked = park_for_change(
            &counter,
            &sleeping,
            2,
            1,
            |_, _, _| panic!("no park for a change already there"),
            || Ok::<(), ()>(()),
        );
        assert_eq!(parked, Ok(CounterWait { value: 3, parks: 0 }));
        assert_eq!(sleeping.load(SeqCst), 0);
    }

    #[test]
    fn each_slice_without_a_change_is_one_more_park() {
        let (counter, sleeping) = (AtomicU32::new(0), AtomicU32::new(0));
        let mut slices = 0;
        let parked = park_for_change(
            &counter,
            &sleeping,
            0,
            1,
            |c, _, _| {
                slices += 1;
                if slices == 3 {
                    c.store(1, SeqCst);
                }
                Ok(())
            },
            || Ok::<(), ()>(()),
        );
        assert_eq!(parked, Ok(CounterWait { value: 1, parks: 3 }));
        assert_eq!(sleeping.load(SeqCst), 0);
    }

    #[test]
    fn between_giving_up_ends_the_park_with_its_error() {
        let (counter, sleeping) = (AtomicU32::new(0), AtomicU32::new(0));
        let parked = park_for_change(&counter, &sleeping, 0, 1, |_, _, _| Ok(()), || Err("gone"));
        assert_eq!(parked, Err("gone"));
    }

    #[test]
    fn a_refused_park_ends_the_wait_with_its_error_and_lowers_the_flag() {
        let (counter, sleeping) = (AtomicU32::new(0), AtomicU32::new(0));
        let parked = park_for_change(
            &counter,
            &sleeping,
            0,
            1,
            |_, _, _| Err("refused"),
            || -> Result<(), &str> { panic!("no slice passes after a refused park") },
        );
        assert_eq!(parked, Err("refused"));
        assert_eq!(sleeping.load(SeqCst), 0);
    }

    #[test]
    fn memory_covers_a_layout_from_exactly_its_size() {
        let need = 1 << 20;
        assert_eq!(check_covers(need as i64, need), Ok(()));
        assert_eq!(check_covers(need as i64 + 1, need), Ok(()));
        assert_eq!(
            check_covers(need as i64 - 1, need),
            Err(AttachError { file_bytes: need as i64 - 1, need })
        );
        assert!(check_covers(-1, 0).is_err());
    }

    #[test]
    fn attach_refuses_memory_smaller_than_the_layout() {
        let geometry = geometry(2, 2, MIN_SLOT_BYTES).unwrap();
        let fd = shared_memory_fd().unwrap();
        // One header page short: macOS rounds a shared memory object up to whole pages.
        rustix::fs::ftruncate(&fd, (geometry.total_bytes() - (1 << HEADER_SHIFT)) as u64).unwrap();
        let refused = LaneShm::attach(geometry, fd.as_fd()).err().expect("a short object is refused");
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);
        assert!(refused.to_string().contains("needs"), "{refused}");
    }

    /// Runs the shrink a worker could attempt from a real second process holding the fd the
    /// server hands out, then touches the last byte of the mapping: a shrink that went through
    /// would end this test process with SIGBUS.
    #[test]
    fn a_peer_process_cannot_shrink_the_lanes_memory_under_the_server() {
        use std::os::fd::AsRawFd;
        let geometry = geometry(2, 2, MIN_SLOT_BYTES).unwrap();
        let (region, fd) = LaneShm::create(geometry).unwrap();
        let raw = fd.as_raw_fd();
        // SAFETY: the child calls only `ftruncate` and `_exit`, which are async-signal-safe, so
        // no lock another test thread held at the fork can block it.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
        if pid == 0 {
            // SAFETY: as above; `raw` is open in the child, which inherited it.
            unsafe {
                let shrunk = libc::ftruncate(raw, 4096) == 0;
                libc::_exit(i32::from(shrunk));
            }
        }
        let mut status = 0;
        // SAFETY: `status` is a writable int and `pid` is this process's child.
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        assert!(libc::WIFEXITED(status), "the peer ended with wait status {status}");
        assert_eq!(libc::WEXITSTATUS(status), 0, "the peer shrank the lanes' memory");
        assert_eq!(rustix::fs::fstat(&fd).unwrap().st_size, geometry.total_bytes() as i64);
        // SAFETY: the last byte lies inside the mapping, and no other thread uses this region.
        let last = unsafe { region.base.add(geometry.total_bytes() - 1).read_volatile() };
        assert_eq!(last, 0);
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    fn any_depth() -> RingDepth {
        let log2: u32 = kani::any();
        kani::assume(log2 < u32::BITS);
        RingDepth::new(1 << log2).expect("a power of two")
    }

    fn any_geometry() -> Geometry {
        let lanes: NonZeroUsize = kani::any();
        let slot_bytes = SlotBytes::new(kani::any());
        kani::assume(slot_bytes.is_some());
        let made = Geometry::new(lanes, any_depth(), slot_bytes.expect("assumed"));
        kani::assume(made.is_ok());
        made.expect("assumed")
    }

    /// `ptr::add` may not span more than `isize::MAX` bytes.
    #[kani::proof]
    fn the_mapping_is_one_pointer_offsets_span() {
        assert!(any_geometry().total_bytes() <= isize::MAX as usize);
    }

    #[kani::proof]
    fn every_header_lies_before_the_slots() {
        let g = any_geometry();
        let lane: usize = kani::any();
        kani::assume(lane < g.lanes());
        assert!(Geometry::header_offset(lane) + (1 << HEADER_SHIFT) <= g.slots_at);
        assert!(g.slots_at <= g.total_bytes());
    }

    #[kani::proof]
    fn every_lanes_slots_lie_inside_the_mapping() {
        let g = any_geometry();
        let lane: usize = kani::any();
        kani::assume(lane < g.lanes());
        let at = g.lane_slots_offset(lane);
        assert!(at >= g.slots_at);
        assert!(at + g.lane_stride <= g.total_bytes());
    }

    /// Backs the slices `write_slot` and `read_slot` make, too.
    #[kani::proof]
    fn every_slot_lies_inside_its_lane() {
        let g = any_geometry();
        let ring = if kani::any() { Ring::Request } else { Ring::Response };
        let at = g.slot_in_lane(ring, kani::any());
        assert_eq!(4 + g.max_message(), g.slot_bytes.get());
        assert!(at + g.slot_bytes.get() <= g.lane_stride);
    }

    /// A lane holds at most `depth` consecutive sequence numbers at once, across the wrap of `u32`.
    #[kani::proof]
    fn requests_in_flight_together_are_in_distinct_slots() {
        let depth = any_depth();
        let (seq, later): (u32, u32) = (kani::any(), kani::any());
        kani::assume(later > 0 && later < depth.get());
        let (first, then) = (Seq::new(seq), Seq::new(seq.wrapping_add(later)));
        assert_ne!(first.slot_index(depth), then.slot_index(depth));
        assert!(first.slot_index(depth) < depth.get() as usize);
    }
}
