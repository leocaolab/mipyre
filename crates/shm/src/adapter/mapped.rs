//! Keeps a channel's memory in a shared memory object both processes map: the host creates it, the peer attaches
//! to the fd it was handed.

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::ptr::NonNull;

use crate::channel::header::ChannelHeader;
use crate::channel::memory::{ByteRegion, Memory};
use crate::os::{self, Mapping, SyscallError};
use crate::plan::geometry::{Geometry, MAX_FILE_BYTES};

// The geometry keeps a channel's memory within `MAX_FILE_BYTES`, so the size handed to `ftruncate` fits this
// platform's `off_t`.
const _: () = assert!(libc::off_t::MAX as u64 == MAX_FILE_BYTES);
// A mapping starts on a page, and no page is smaller than 4 KiB.
const _: () = assert!(std::mem::align_of::<ChannelHeader>() <= 4096);

/// A channel's memory in a shared mapping, which no process this one forks inherits; dropping it unmaps it.
pub struct MappedMemory {
    mapping: Mapping,
    geometry: Geometry,
}

/// The shared memory object handed over cannot hold the channel.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AttachError {
    #[error("the channel's shared memory could not be mapped: {0}")]
    NotMapped(#[source] SyscallError),
    /// Mapping it anyway would put pages past the object's end in the mapping, and touching one raises SIGBUS.
    #[error("the channel's shared memory object is {file_bytes} bytes, but its layout needs {need}")]
    TooSmall { file_bytes: i64, need: usize },
}

impl MappedMemory {
    /// Creates a zeroed shared memory object the size of `geometry`'s layout, its size fixed for good, and maps it;
    /// the fd goes to the peer, which [`attach`](Self::attach)es to it.
    pub fn create(geometry: &Geometry) -> Result<(Self, OwnedFd), SyscallError> {
        let fd = os::shared_memory(geometry.total_bytes() as u64)?;
        let mapping = Mapping::new(fd.as_fd(), geometry.total_bytes())?;
        Ok((
            MappedMemory {
                mapping,
                geometry: *geometry,
            },
            fd,
        ))
    }

    /// Maps the shared memory object `fd` that [`create`](Self::create) made for `geometry`, once its size covers
    /// the layout.
    pub fn attach(geometry: &Geometry, fd: BorrowedFd<'_>) -> Result<Self, AttachError> {
        let file_bytes = os::file_bytes(fd).map_err(AttachError::NotMapped)?;
        check_covers(file_bytes, geometry.total_bytes())?;
        let mapping = Mapping::new(fd, geometry.total_bytes()).map_err(AttachError::NotMapped)?;
        Ok(MappedMemory {
            mapping,
            geometry: *geometry,
        })
    }
}

fn check_covers(file_bytes: i64, need: usize) -> Result<(), AttachError> {
    if u64::try_from(file_bytes).is_ok_and(|bytes| bytes >= need as u64) {
        return Ok(());
    }
    Err(AttachError::TooSmall { file_bytes, need })
}

// SAFETY: the mapping is `total_bytes()` long (created that size, or checked to cover it before mapping), readable
// and writable, page-aligned (which covers the header's alignment, asserted above), zero-filled by the OS, and stays
// until the mapping drops with `self`.
unsafe impl ByteRegion for MappedMemory {
    fn base(&self) -> NonNull<u8> {
        self.mapping.base()
    }

    fn region_geometry(&self) -> &Geometry {
        &self.geometry
    }
}

impl Memory for MappedMemory {}

// Miri runs no system call.
#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;
    use crate::channel::memory::sealed::Access;
    use crate::plan::geometry::{SlotBytes, HEADER_PAGE};
    use crate::plan::ring::{RingDepth, Seq};
    use crate::sync::Ordering::Relaxed;

    fn geometry(depth: u32, slot_bytes: usize) -> Geometry {
        Geometry::new(RingDepth::new(depth).unwrap(), SlotBytes::new(slot_bytes).unwrap()).unwrap()
    }

    #[test]
    fn memory_covers_a_layout_from_exactly_its_size() {
        let need = 1 << 20;
        assert_eq!(check_covers(need as i64, need), Ok(()));
        assert_eq!(check_covers(need as i64 + 1, need), Ok(()));
        assert_eq!(
            check_covers(need as i64 - 1, need),
            Err(AttachError::TooSmall {
                file_bytes: need as i64 - 1,
                need
            })
        );
        assert!(check_covers(-1, 0).is_err());
    }

    #[test]
    fn attach_refuses_memory_smaller_than_the_layout() {
        let g = geometry(2, 4096);
        // One header page short: macOS rounds a shared memory object up to whole pages.
        let fd = os::shared_memory((g.total_bytes() - HEADER_PAGE) as u64).unwrap();
        let refused = MappedMemory::attach(&g, fd.as_fd())
            .err()
            .expect("a short object is refused");
        assert_eq!(
            refused,
            AttachError::TooSmall {
                file_bytes: (g.total_bytes() - HEADER_PAGE) as i64,
                need: g.total_bytes()
            }
        );
    }

    #[test]
    fn a_memory_attached_to_the_fd_sees_what_the_creator_wrote() {
        let g = geometry(2, 4096);
        let (created, fd) = MappedMemory::create(&g).unwrap();
        let attached = MappedMemory::attach(&g, fd.as_fd()).unwrap();
        created.header().req_head.store(3, Relaxed);
        let slot = created.slot(g.request_slot(Seq::new(1)));
        assert_eq!(slot.copy_in(0, b"hello"), Ok(5));
        slot.len_word().store(5, Relaxed);
        assert_eq!(attached.header().req_head.load(Relaxed), 3);
        let seen = attached.slot(g.request_slot(Seq::new(1)));
        assert_eq!(seen.view_message().map(|v| v.to_vec()), Ok(b"hello".to_vec()));
    }

    /// macOS names each object for a moment before it unlinks it; creations side by side in one process pass over
    /// the names in use.
    #[test]
    fn creations_side_by_side_in_one_process_all_succeed() {
        let g = geometry(1, 64);
        let made = std::thread::scope(|s| {
            let each = || (0..1000).filter(|_| MappedMemory::create(&g).is_ok()).count();
            let (a, b) = (s.spawn(each), s.spawn(each));
            a.join().unwrap() + b.join().unwrap()
        });
        assert_eq!(made, 2000);
    }
}
