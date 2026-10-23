//! Lays out one channel's memory: a header page, then the request ring, then the answer ring, each `depth` slots.
//!
//! A slot is an 8-byte header (the message length as a `u32`, then 4 bytes no one reads) and the message, so a
//! message starts word-aligned. Every offset into the memory comes from here, and only a [`Geometry`] makes one.

use super::ring::{RingDepth, Seq};

/// Bytes the header page takes: the largest page size the channel runs on, so the slots start page-aligned.
pub const HEADER_PAGE: usize = 1 << 14;

/// Bytes a slot's header takes before its message.
pub const SLOT_HEADER: usize = 8;

/// A slot's message length is a `u32`, so a slot past this would record a wrapped length.
pub const MAX_SLOT_BYTES: usize = u32::MAX as usize + SLOT_HEADER;

/// The largest file a channel's memory is: `off_t::MAX` on every platform the channel runs on, which the adapter
/// that sizes the file checks against its own `off_t`.
pub const MAX_FILE_BYTES: u64 = i64::MAX as u64;

/// Counts a slot's bytes, its header included; it holds at least the header, because only [`SlotBytes::new`] makes
/// one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotBytes(usize);

impl SlotBytes {
    /// Returns `None` below [`SLOT_HEADER`].
    pub const fn new(bytes: usize) -> Option<Self> {
        if bytes < SLOT_HEADER {
            return None;
        }
        Some(SlotBytes(bytes))
    }

    #[inline]
    pub const fn get(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GeometryError {
    #[error("a channel's slot holds at most {MAX_SLOT_BYTES} bytes (its message length is a u32), got {slot_bytes}")]
    SlotPastLengthPrefix { slot_bytes: usize },
    /// The sizes overflow `usize` or pass [`MAX_FILE_BYTES`]; a wrapped size would map less than the slots written
    /// into it.
    #[error("a channel of depth {depth} with {slot_bytes}-byte slots is too large to map")]
    TooLarge { depth: u32, slot_bytes: usize },
}

/// Where one slot starts, in bytes from the start of the channel's memory; only a [`Geometry`] makes one, and it
/// lies inside the memory that geometry lays out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotOffset(usize);

impl SlotOffset {
    #[inline]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// One channel's layout. The field types carry the lower bounds; [`Geometry::new`] checks the upper ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Geometry {
    depth: RingDepth,
    slot_bytes: SlotBytes,
    /// It is `slot_bytes` rounded up to a power of two, so a slot's place is a shift.
    slot_shift: u32,
    /// It is a multiple of [`HEADER_PAGE`].
    total_bytes: usize,
}

/// Returns `x · 2^k`, or `None` when it does not fit a `usize`.
const fn shifted(x: usize, k: u32) -> Option<usize> {
    if k >= usize::BITS || x.leading_zeros() < k {
        return None;
    }
    Some(x << k)
}

impl Geometry {
    pub fn new(depth: RingDepth, slot_bytes: SlotBytes) -> Result<Self, GeometryError> {
        let too_large = GeometryError::TooLarge {
            depth: depth.get(),
            slot_bytes: slot_bytes.get(),
        };
        let Some(stride) = slot_bytes.get().checked_next_power_of_two() else {
            return Err(too_large);
        };
        let slot_shift = stride.trailing_zeros();
        // Two rings of `depth` slots each.
        let rings = shifted(1, slot_shift + depth.get().trailing_zeros() + 1);
        let Some(total_bytes) = rings.and_then(|rings| rings.checked_add(HEADER_PAGE)) else {
            return Err(too_large);
        };
        // A ring of power-of-two slots is page-aligned once a slot is at least a page; below that the rings are
        // smaller than a page and their total is rounded up to one.
        let Some(total_bytes) = total_bytes.checked_next_multiple_of(HEADER_PAGE) else {
            return Err(too_large);
        };
        if total_bytes as u64 > MAX_FILE_BYTES || total_bytes > isize::MAX as usize {
            return Err(too_large);
        }
        // A layout too large to map is reported before a slot past its length prefix.
        if slot_bytes.get() > MAX_SLOT_BYTES {
            return Err(GeometryError::SlotPastLengthPrefix {
                slot_bytes: slot_bytes.get(),
            });
        }
        Ok(Geometry {
            depth,
            slot_bytes,
            slot_shift,
            total_bytes,
        })
    }

    #[inline]
    pub const fn depth(&self) -> RingDepth {
        self.depth
    }

    #[inline]
    pub const fn slot_bytes(&self) -> SlotBytes {
        self.slot_bytes
    }

    /// Returns how many bytes a message in a slot may take: the slot without its header.
    #[inline]
    pub const fn max_message(&self) -> u32 {
        // `new` refuses a slot past MAX_SLOT_BYTES, so this fits a u32.
        (self.slot_bytes.get() - SLOT_HEADER) as u32
    }

    #[inline]
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Returns where request `seq`'s slot starts.
    #[inline]
    pub const fn request_slot(&self, seq: Seq) -> SlotOffset {
        SlotOffset(HEADER_PAGE + (seq.slot_index(self.depth) << self.slot_shift))
    }

    /// Returns where answer `seq`'s slot starts: the answer ring follows the request ring.
    #[inline]
    pub const fn answer_slot(&self, seq: Seq) -> SlotOffset {
        let ring = (self.depth.get() as usize) << self.slot_shift;
        SlotOffset(HEADER_PAGE + ring + (seq.slot_index(self.depth) << self.slot_shift))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn geometry(depth: u32, slot_bytes: usize) -> Result<Geometry, GeometryError> {
        Geometry::new(
            RingDepth::new(depth).expect("a test asks for a power-of-two depth"),
            SlotBytes::new(slot_bytes).expect("a test asks for at least the slot header"),
        )
    }

    #[test]
    fn a_slot_holds_at_least_its_header() {
        assert_eq!(SlotBytes::new(SLOT_HEADER - 1), None);
        assert_eq!(SlotBytes::new(SLOT_HEADER).map(SlotBytes::get), Some(SLOT_HEADER));
    }

    #[test]
    fn the_rings_follow_the_header_page_one_after_the_other() {
        let g = geometry(4, 4097).unwrap();
        let stride = 8192;
        assert_eq!(g.total_bytes(), HEADER_PAGE + 2 * 4 * stride);
        assert_eq!(g.request_slot(Seq::new(0)).get(), HEADER_PAGE);
        assert_eq!(g.request_slot(Seq::new(5)).get(), HEADER_PAGE + stride);
        assert_eq!(g.answer_slot(Seq::new(0)).get(), HEADER_PAGE + 4 * stride);
        assert_eq!(g.answer_slot(Seq::new(7)).get(), HEADER_PAGE + 7 * stride);
        assert_eq!(g.max_message(), 4097 - 8);
    }

    #[test]
    fn rings_smaller_than_a_page_take_a_whole_one() {
        let g = geometry(1, SLOT_HEADER).unwrap();
        assert_eq!(g.total_bytes(), 2 * HEADER_PAGE);
        assert_eq!(g.max_message(), 0);
    }

    #[test]
    fn a_slot_up_to_the_length_prefix_is_laid_out_and_one_past_it_is_refused() {
        assert_eq!(geometry(1, MAX_SLOT_BYTES).unwrap().max_message(), u32::MAX);
        assert_eq!(
            geometry(1, MAX_SLOT_BYTES + 1),
            Err(GeometryError::SlotPastLengthPrefix {
                slot_bytes: MAX_SLOT_BYTES + 1
            })
        );
    }

    /// The rings are `2 · depth · stride` bytes beside the header page: with 2^31-byte slots that is `2^63` at depth
    /// 2^31, past `isize::MAX` by the header page, and `2^62` at depth 2^30, which fits.
    #[test]
    fn rings_that_fit_the_file_are_laid_out_and_one_slot_more_is_refused() {
        let slot = 1 << 31;
        let fits = geometry(1 << 30, slot).unwrap();
        assert_eq!(fits.total_bytes(), (1 << 62) + HEADER_PAGE);
        assert_eq!(
            geometry(1 << 31, slot),
            Err(GeometryError::TooLarge {
                depth: 1 << 31,
                slot_bytes: slot
            })
        );
    }

    #[test]
    fn a_slot_whose_size_cannot_round_up_is_too_large() {
        let err = geometry(1, usize::MAX / 2 + 2).unwrap_err();
        assert!(matches!(err, GeometryError::TooLarge { .. }), "{err}");
    }
}

#[cfg(kani)]
pub(super) mod proofs {
    use super::*;

    pub(crate) fn any_depth() -> RingDepth {
        let log2: u32 = kani::any();
        kani::assume(log2 < u32::BITS);
        RingDepth::new(1 << log2).expect("a power of two")
    }

    pub(crate) fn any_geometry() -> Geometry {
        let slot_bytes = SlotBytes::new(kani::any());
        kani::assume(slot_bytes.is_some());
        let made = Geometry::new(any_depth(), slot_bytes.expect("assumed"));
        kani::assume(made.is_ok());
        made.expect("assumed")
    }

    /// `ptr::add` spans at most `isize::MAX` bytes, and the file holding the memory at most `off_t::MAX`.
    #[kani::proof]
    fn a_channels_mapping_spans_one_pointer_offset() {
        let g = any_geometry();
        assert!(g.total_bytes() <= isize::MAX as usize);
        assert!(g.total_bytes() as u64 <= MAX_FILE_BYTES);
        assert!(g.total_bytes() % HEADER_PAGE == 0);
    }

    #[kani::proof]
    fn every_slot_lies_inside_the_mapping() {
        let g = any_geometry();
        let seq = Seq::new(kani::any());
        for at in [g.request_slot(seq), g.answer_slot(seq)] {
            assert!(at.get() >= HEADER_PAGE);
            assert!(at.get() + g.slot_bytes().get() <= g.total_bytes());
        }
        assert!(g.request_slot(seq).get() + g.slot_bytes().get() <= g.answer_slot(Seq::new(0)).get());
    }

    /// The message after a slot's header starts on an 8-byte boundary, which copying it by words needs.
    #[kani::proof]
    fn a_slots_payload_starts_word_aligned() {
        let g = any_geometry();
        let seq = Seq::new(kani::any());
        assert!((g.request_slot(seq).get() + SLOT_HEADER) % 8 == 0);
        assert!((g.answer_slot(seq).get() + SLOT_HEADER) % 8 == 0);
    }

    /// A ring holds at most `depth` consecutive sequence numbers at once, across the wrap of `u32`.
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
