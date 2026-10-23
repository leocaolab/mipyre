//! Says where a channel's header and slots are: the [`Memory`] port and the stores behind it.
//!
//! The port is sealed: outside this crate a memory can be named and made, never read, so no safe code there can
//! reach a slot through an offset from another geometry or borrow bytes the peer is writing. A store over plain
//! bytes ([`HeapMemory`] here, the shared mapping in the adapter) vouches only for its bytes, and the header and
//! slots are laid over them once, here. The model's memory keeps its slots in cells instead, because Loom checks
//! cells and not bytes; that is the only reason for the second trait.

use std::marker::PhantomData;
#[cfg(not(shm_model))]
use std::ptr::NonNull;

use bytes::BytesMut;

#[cfg(all(shm_model, shm_shuttle))]
use self::shuttle_cell::{SlotCell, SlotRead};
#[cfg(not(shm_model))]
use super::header::NOTE_AT;
use super::header::{ChannelHeader, NOTE_BYTES};
use super::slot::SlotRoomError;
#[cfg(not(shm_model))]
use crate::plan::geometry::SLOT_HEADER;
use crate::plan::geometry::{Geometry, SlotOffset};
use crate::plan::slot_len::{decode_len, SlotLengthError};
use crate::sync::{AtomicU32, Ordering::Relaxed};
#[cfg(all(shm_model, not(shm_shuttle)))]
use crate::sync::{SlotCell, SlotRead};

/// The memory a channel lives in, made for one [`Geometry`].
pub trait Memory: sealed::Access + Send + Sync {}

pub(crate) mod sealed {
    //! Holds what only the crate reaches behind [`Memory`]: the trait, and the slot it hands out.
    use super::*;

    /// Reaches a channel's header and slots.
    ///
    /// # Safety
    ///
    /// The implementer vouches that for every [`SlotOffset`] that `geometry()` makes, `slot()` gives a slot inside
    /// the memory, valid while `self` lives and apart from every other offset's slot; and that `header()` lies inside
    /// it apart from the slots. That the offsets lie inside the geometry's total is the Kani proofs of
    /// `plan::geometry`; the vow is that the memory was made for this geometry. `note()` gives the start note's
    /// [`NOTE_BYTES`](crate::channel::NOTE_BYTES) in the header page after the header, measured by the header's
    /// `note_len`.
    pub unsafe trait Access {
        fn geometry(&self) -> &Geometry;
        fn header(&self) -> &ChannelHeader;
        fn slot(&self, at: SlotOffset) -> SlotRef<'_>;
        fn note(&self) -> SlotRef<'_>;
    }
}

/// Bytes a channel can be laid over.
///
/// # Safety
///
/// `base()` is the start of `region_geometry().total_bytes()` bytes, readable and writable, aligned to
/// [`HEADER_PAGE`](crate::plan::geometry::HEADER_PAGE) and zeroed before the first read, that stay put until the
/// region is dropped.
#[cfg(not(shm_model))]
pub(crate) unsafe trait ByteRegion: Send + Sync {
    fn base(&self) -> NonNull<u8>;
    /// It is named apart from `Access::geometry` so that the two never meet as one method name in one scope.
    fn region_geometry(&self) -> &Geometry;
}

// SAFETY: the header sits at the region's start and the slots at their offsets, all inside `total_bytes()` (the
// geometry's proofs); a header of atomics is valid over any bytes, and the region's alignment covers its alignment.
#[cfg(not(shm_model))]
unsafe impl<R: ByteRegion> sealed::Access for R {
    fn geometry(&self) -> &Geometry {
        self.region_geometry()
    }

    fn header(&self) -> &ChannelHeader {
        // SAFETY: as above; the header page holds the header (a `const` assertion in `header.rs`).
        unsafe { self.base().cast::<ChannelHeader>().as_ref() }
    }

    fn slot(&self, at: SlotOffset) -> SlotRef<'_> {
        // SAFETY: `at` comes from this region's geometry, so the slot, its 8-aligned length word first and then
        // `max_message()` bytes, lies inside the region; an atomic is valid over any bytes.
        let (len, start) = unsafe {
            let at = self.base().add(at.get());
            (at.cast::<AtomicU32>().as_ref(), at.add(SLOT_HEADER))
        };
        SlotRef {
            len,
            start,
            room: self.region_geometry().max_message(),
            region: PhantomData,
        }
    }

    fn note(&self) -> SlotRef<'_> {
        // SAFETY: the note's bytes follow the header inside the header page (a `const` assertion in `header.rs`).
        let start = unsafe { self.base().add(NOTE_AT) };
        SlotRef {
            len: &self.header().start.note_len.0,
            start,
            room: NOTE_BYTES,
            region: PhantomData,
        }
    }
}

/// A length word and the bytes it measures: one slot of a region (its 8-byte header holds the word, its message
/// follows), or the start note in the header page. Every access stays inside the `room` bytes after `start`.
#[cfg(not(shm_model))]
pub struct SlotRef<'r> {
    len: &'r AtomicU32,
    start: NonNull<u8>,
    room: u32,
    region: PhantomData<&'r [u8]>,
}

#[cfg(not(shm_model))]
impl<'r> SlotRef<'r> {
    /// Returns the word that holds the message length.
    pub fn len_word(&self) -> &'r AtomicU32 {
        self.len
    }

    /// Returns how many bytes a message here may take.
    pub fn room(&self) -> u32 {
        self.room
    }

    /// Copies `src` into the message at byte `at` and returns where it ends; a message that would pass the room is
    /// refused and nothing is written. It does not touch the length word.
    ///
    /// The peer may write the same bytes meanwhile: that race is one of the two the channel registers, and it spoils
    /// only this message's bytes.
    pub fn copy_in(&self, at: u32, src: &[u8]) -> Result<u32, SlotRoomError> {
        let end = u64::from(at) + src.len() as u64;
        if end > u64::from(self.room) {
            return Err(SlotRoomError {
                room: self.room,
                needed: end,
            });
        }
        // SAFETY: `[at, end)` lies inside the room after `start`, and private `src` cannot overlap the region.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.start.as_ptr().add(at as usize), src.len()) };
        Ok(end as u32)
    }

    /// Copies as much of `src` as the room takes into the message's start and returns how much that was.
    pub fn copy_prefix_in(&self, src: &[u8]) -> u32 {
        let n = src.len().min(self.room as usize);
        // SAFETY: `n` bytes fit the room after `start`, and private `src` cannot overlap the region.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.start.as_ptr(), n) };
        n as u32
    }

    /// Reads the length word once and appends that many message bytes to `dst`; a length past the room is refused
    /// and `dst` is left as it was. Every decision after this reads only `dst`.
    ///
    /// The peer may write the bytes meanwhile (the other registered race): `dst` then holds spoiled bytes.
    pub fn take_message(&self, dst: &mut BytesMut) -> Result<u32, SlotLengthError> {
        let len = decode_len(self.len.load(Relaxed), self.room)?;
        self.append_within_room(len, dst);
        Ok(len)
    }

    /// Appends the first `len` message bytes to `dst`, when `len` fits the room.
    pub fn append_to(&self, len: u32, dst: &mut BytesMut) -> Result<(), SlotLengthError> {
        self.append_within_room(decode_len(len, self.room)?, dst);
        Ok(())
    }

    /// Takes a `len` already checked against the room.
    fn append_within_room(&self, len: u32, dst: &mut BytesMut) {
        let len = len as usize;
        dst.reserve(len);
        // SAFETY: `len` is inside the room after `start`, and `reserve` made `len` spare bytes in private `dst`, which
        // the copy initializes before `set_len` counts them.
        unsafe {
            std::ptr::copy_nonoverlapping(self.start.as_ptr(), dst.spare_capacity_mut().as_mut_ptr().cast(), len);
            dst.set_len(dst.len() + len);
        }
    }

    /// Reads the length word once and borrows that many message bytes. Only the side whose message the other side
    /// does not write while it reads may borrow: the peer reading the host's request.
    pub fn view_message(&self) -> Result<MessageView<'r>, SlotLengthError> {
        let len = decode_len(self.len.load(Relaxed), self.room)?;
        // SAFETY: `len` bytes inside the room, alive as long as the region (`'r`); by the protocol the writer does not
        // write them while the reader holds the slot.
        Ok(MessageView(unsafe {
            std::slice::from_raw_parts(self.start.as_ptr(), len as usize)
        }))
    }
}

/// A message read where it lies in its slot, for as long as the view is held.
#[cfg(not(shm_model))]
pub struct MessageView<'r>(&'r [u8]);

#[cfg(not(shm_model))]
impl std::ops::Deref for MessageView<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.0
    }
}

/// Bytes on the heap for one channel, zeroed and page-aligned: the same core runs on them in tests and under Miri.
#[cfg(not(shm_model))]
pub struct HeapMemory {
    base: NonNull<u8>,
    geometry: Geometry,
}

#[cfg(not(shm_model))]
impl HeapMemory {
    pub fn new(geometry: &Geometry) -> Self {
        let layout = heap_layout(geometry);
        // SAFETY: the layout is not zero-sized: a geometry is at least the header page.
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        let Some(base) = NonNull::new(base) else {
            std::alloc::handle_alloc_error(layout)
        };
        HeapMemory {
            base,
            geometry: *geometry,
        }
    }
}

#[cfg(not(shm_model))]
fn heap_layout(geometry: &Geometry) -> std::alloc::Layout {
    use crate::plan::geometry::HEADER_PAGE;
    // SAFETY: HEADER_PAGE is a power of two, and the total is a multiple of it no larger than `isize::MAX` (the
    // geometry's proofs), so rounding it up to the alignment changes nothing.
    unsafe { std::alloc::Layout::from_size_align_unchecked(geometry.total_bytes(), HEADER_PAGE) }
}

#[cfg(not(shm_model))]
impl Drop for HeapMemory {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this layout and not freed since.
        unsafe { std::alloc::dealloc(self.base.as_ptr(), heap_layout(&self.geometry)) }
    }
}

// SAFETY: the bytes are reached only through atomics and through slots the ring's counters give to one side at a
// time; the pointer itself is never handed out as a mutable borrow.
#[cfg(not(shm_model))]
unsafe impl Send for HeapMemory {}
// SAFETY: as for `Send`: a shared `HeapMemory` hands out only atomics and slots, never `&mut` to its bytes.
#[cfg(not(shm_model))]
unsafe impl Sync for HeapMemory {}

// SAFETY: `new` allocated `total_bytes()` zeroed bytes aligned to the header page, freed only on drop.
#[cfg(not(shm_model))]
unsafe impl ByteRegion for HeapMemory {
    fn base(&self) -> NonNull<u8> {
        self.base
    }

    fn region_geometry(&self) -> &Geometry {
        &self.geometry
    }
}

#[cfg(not(shm_model))]
impl Memory for HeapMemory {}

/// A slot's bytes under Shuttle, with Loom's cell's calls: Shuttle checks no cell (it explores schedules, not data
/// races; Loom's models check the slots), so this is a plain cell that hands out its pointer.
#[cfg(all(shm_model, shm_shuttle))]
mod shuttle_cell {
    pub(crate) struct SlotCell<T>(std::cell::UnsafeCell<T>);

    impl<T> SlotCell<T> {
        pub(crate) fn new(value: T) -> Self {
            SlotCell(std::cell::UnsafeCell::new(value))
        }

        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }

        pub(crate) fn get(&self) -> SlotRead<T> {
            SlotRead(self.0.get())
        }
    }

    /// A read of a [`SlotCell`], as Loom's `ConstPtr`.
    pub(crate) struct SlotRead<T>(*const T);

    impl<T> SlotRead<T> {
        /// # Safety
        ///
        /// The cell outlives the returned borrow, and nothing writes it meanwhile (the ring's counters order the
        /// two sides; Loom's models check that, not Shuttle's).
        pub(crate) unsafe fn deref(&self) -> &T {
            // SAFETY: the caller's contract above.
            unsafe { &*self.0 }
        }
    }
}

/// Bytes a slot of the model's memory holds at most: a model's messages are a few bytes.
#[cfg(shm_model)]
pub const MODEL_SLOT: usize = 64;

/// A channel's memory for the Loom models: the header made of Loom's atomics, each slot's length word an atomic and
/// its bytes a cell Loom checks.
#[cfg(shm_model)]
pub struct ModelMemory {
    geometry: Geometry,
    header: ChannelHeader,
    lens: Box<[AtomicU32]>,
    slots: Box<[SlotCell<Box<[u8]>>]>,
    note: SlotCell<Box<[u8]>>,
}

#[cfg(shm_model)]
impl ModelMemory {
    /// Returns `None` for a geometry whose slots are larger than [`MODEL_SLOT`].
    pub fn new(geometry: &Geometry) -> Option<Self> {
        if geometry.slot_bytes().get() > MODEL_SLOT {
            return None;
        }
        let slots = 2 * geometry.depth().get() as usize;
        let bytes = |n: usize| SlotCell::new(vec![0; n].into_boxed_slice());
        Some(ModelMemory {
            geometry: *geometry,
            header: ChannelHeader::new(),
            lens: (0..slots).map(|_| AtomicU32::new(0)).collect(),
            slots: (0..slots).map(|_| bytes(geometry.max_message() as usize)).collect(),
            note: bytes(NOTE_BYTES as usize),
        })
    }
}

/// A length word and the bytes it measures, in the model's memory: the bytes are a cell, so Loom sees each access.
#[cfg(shm_model)]
pub struct SlotRef<'r> {
    len: &'r AtomicU32,
    cell: &'r SlotCell<Box<[u8]>>,
    room: u32,
}

#[cfg(shm_model)]
impl<'r> SlotRef<'r> {
    pub fn len_word(&self) -> &'r AtomicU32 {
        self.len
    }

    pub fn room(&self) -> u32 {
        self.room
    }

    pub fn copy_in(&self, at: u32, src: &[u8]) -> Result<u32, SlotRoomError> {
        let end = u64::from(at) + src.len() as u64;
        if end > u64::from(self.room) {
            return Err(SlotRoomError {
                room: self.room,
                needed: end,
            });
        }
        // SAFETY: Loom checks that no other access to the cell overlaps this one.
        self.cell
            .with_mut(|bytes| unsafe { (&mut *bytes)[at as usize..end as usize].copy_from_slice(src) });
        Ok(end as u32)
    }

    pub fn copy_prefix_in(&self, src: &[u8]) -> u32 {
        let n = src.len().min(self.room as usize);
        // SAFETY: Loom checks that no other access to the cell overlaps this one.
        self.cell
            .with_mut(|bytes| unsafe { (&mut *bytes)[..n].copy_from_slice(&src[..n]) });
        n as u32
    }

    pub fn take_message(&self, dst: &mut BytesMut) -> Result<u32, SlotLengthError> {
        let len = decode_len(self.len.load(Relaxed), self.room)?;
        self.append_within_room(len, dst);
        Ok(len)
    }

    pub fn append_to(&self, len: u32, dst: &mut BytesMut) -> Result<(), SlotLengthError> {
        self.append_within_room(decode_len(len, self.room)?, dst);
        Ok(())
    }

    fn append_within_room(&self, len: u32, dst: &mut BytesMut) {
        // SAFETY: as in `copy_in`.
        self.cell
            .with(|bytes| dst.extend_from_slice(unsafe { &(&*bytes)[..len as usize] }));
    }

    /// Borrows the message: the view holds a read of the cell, so Loom sees the read for as long as the view lives.
    pub fn view_message(&self) -> Result<MessageView<'r>, SlotLengthError> {
        let len = decode_len(self.len.load(Relaxed), self.room)? as usize;
        Ok(MessageView {
            read: self.cell.get(),
            len,
            cell: PhantomData,
        })
    }
}

/// A message read where it lies in its slot: the read of the cell lasts as long as the view.
#[cfg(shm_model)]
pub struct MessageView<'r> {
    read: SlotRead<Box<[u8]>>,
    len: usize,
    cell: PhantomData<&'r [u8]>,
}

#[cfg(shm_model)]
impl std::ops::Deref for MessageView<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the cell lives as long as the memory (`'r`) and `len` fits it (`view_message` checked it against
        // the room); Loom reports any write to it while `read` is alive.
        unsafe { &self.read.deref()[..self.len] }
    }
}

// SAFETY: the model's memory reaches no raw byte: its header and slots are its own fields, one cell per slot.
#[cfg(shm_model)]
unsafe impl sealed::Access for ModelMemory {
    fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    fn header(&self) -> &ChannelHeader {
        &self.header
    }

    fn slot(&self, at: SlotOffset) -> SlotRef<'_> {
        use crate::plan::geometry::HEADER_PAGE;
        let stride = self.geometry.slot_bytes().get().next_power_of_two();
        let i = (at.get() - HEADER_PAGE) / stride;
        SlotRef {
            len: &self.lens[i],
            cell: &self.slots[i],
            room: self.geometry.max_message(),
        }
    }

    fn note(&self) -> SlotRef<'_> {
        SlotRef {
            len: &self.header.start.note_len.0,
            cell: &self.note,
            room: NOTE_BYTES,
        }
    }
}

// SAFETY: the slots' cells are reached by one side at a time, as the ring's counters hand them over; checking that
// is what the cells are for, and Loom reports any access that breaks it.
#[cfg(shm_model)]
unsafe impl Sync for ModelMemory {}

#[cfg(shm_model)]
impl Memory for ModelMemory {}

#[cfg(test)]
#[cfg(not(shm_model))]
mod tests {
    use super::sealed::Access;
    use super::*;
    use crate::plan::geometry::SlotBytes;
    use crate::plan::ring::{RingDepth, Seq};

    fn geometry(depth: u32, slot_bytes: usize) -> Geometry {
        Geometry::new(RingDepth::new(depth).unwrap(), SlotBytes::new(slot_bytes).unwrap()).unwrap()
    }

    #[test]
    fn a_fresh_heap_memory_holds_a_header_of_zeroes() {
        let m = HeapMemory::new(&geometry(4, 4096));
        let h = m.header();
        let words = [&h.req_head, &h.taken, &h.resp_head, &h.peer_sleeping, &h.host_sleeping];
        let start = [&h.start.attempt, &h.start.state, &h.start.note_len];
        assert!(words.iter().chain(start.iter()).all(|w| w.load(Relaxed) == 0));
        assert_eq!(h.peer_waits.read(), crate::channel::header::WaitTotals::default());
    }

    #[test]
    fn a_message_copied_in_is_the_message_taken_out_and_viewed() {
        let g = geometry(2, 64);
        let m = HeapMemory::new(&g);
        let slot = m.slot(g.answer_slot(Seq::new(1)));
        assert_eq!(slot.copy_in(0, &[1, 2, 3]), Ok(3));
        assert_eq!(slot.copy_in(3, &[4; 53]), Ok(56));
        slot.len_word().store(56, Relaxed);
        let mut out = BytesMut::from(&b"kept"[..]);
        assert_eq!(slot.take_message(&mut out), Ok(56));
        assert_eq!(out[..8], *b"kept\x01\x02\x03\x04");
        assert_eq!(slot.view_message().map(|v| v.to_vec()), Ok(out[4..].to_vec()));
        assert_eq!(m.slot(g.answer_slot(Seq::new(1))).len_word().load(Relaxed), 56);
    }

    #[test]
    fn bytes_up_to_the_slots_end_are_reached_and_one_past_it_is_refused() {
        let g = geometry(1, 64);
        let m = HeapMemory::new(&g);
        let slot = m.slot(g.request_slot(Seq::new(0)));
        let room = g.max_message();
        assert_eq!(slot.room(), room);
        assert_eq!(slot.copy_in(0, &vec![9; room as usize]), Ok(room));
        assert_eq!(
            slot.copy_in(1, &vec![9; room as usize]),
            Err(SlotRoomError {
                room,
                needed: u64::from(room) + 1
            })
        );
        assert_eq!(
            slot.copy_in(u32::MAX, &[9]),
            Err(SlotRoomError { room, needed: 1 << 32 })
        );
        assert_eq!(slot.copy_prefix_in(&vec![7; room as usize + 1]), room);
        let past = SlotLengthError::PastSlot {
            len: room + 1,
            max: room,
        };
        let mut out = BytesMut::new();
        assert_eq!(slot.append_to(room + 1, &mut out), Err(past));
        assert!(out.is_empty(), "a refused length copies nothing");
        slot.len_word().store(room + 1, Relaxed);
        assert_eq!(slot.take_message(&mut out), Err(past));
        assert_eq!(slot.view_message().map(|v| v.len()), Err(past));
        assert!(out.is_empty());
        slot.len_word().store(room, Relaxed);
        assert_eq!(slot.view_message().map(|v| v.len()), Ok(room as usize));
    }

    #[test]
    fn the_slots_and_the_note_of_one_memory_are_apart() {
        let g = geometry(2, 64);
        let m = HeapMemory::new(&g);
        let slots = [
            g.request_slot(Seq::new(0)),
            g.request_slot(Seq::new(1)),
            g.answer_slot(Seq::new(0)),
            g.answer_slot(Seq::new(1)),
        ];
        let refs = || slots.iter().map(|at| m.slot(*at)).chain([m.note()]);
        for (i, slot) in refs().enumerate() {
            slot.len_word().store(56, Relaxed);
            assert_eq!(slot.copy_in(0, &[i as u8; 56]), Ok(56));
        }
        for (i, slot) in refs().enumerate() {
            let mut out = BytesMut::new();
            assert_eq!(slot.take_message(&mut out), Ok(56));
            assert_eq!(out[..], [i as u8; 56]);
        }
        assert_eq!(m.note().room(), NOTE_BYTES);
        assert_eq!(
            m.header().start.note_len.load(Relaxed),
            56,
            "the note is measured by the header's note_len"
        );
    }
}
