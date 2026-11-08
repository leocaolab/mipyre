//! Maps a channel's object a second time, as the peer process maps it, so a test writes the peer's words and bytes
//! by hand: the channel's own memory is sealed, and no code outside mipyre_shm reads or writes a slot through it.

use std::os::fd::{AsRawFd, OwnedFd};

/// A second shared mapping of a channel's object. Every access is a volatile one inside the mapping, and a word is
/// aligned, so the channel's atomics read what it wrote.
pub struct PeerView {
    base: *mut u8,
    len: usize,
}

impl PeerView {
    /// Maps the whole of `shm`'s `len` bytes.
    ///
    /// # Panics
    ///
    /// When the kernel refuses the mapping: the test that asked for it cannot run.
    pub fn new(shm: &OwnedFd, len: usize) -> Self {
        // SAFETY: a new shared mapping of the whole object at an address the kernel picks.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                shm.as_raw_fd(),
                0,
            )
        };
        assert_ne!(base, libc::MAP_FAILED, "mmap: {}", std::io::Error::last_os_error());
        PeerView { base: base.cast(), len }
    }

    pub fn put_u32(&self, at: usize, value: u32) {
        assert!(at.is_multiple_of(4) && at + 4 <= self.len);
        // SAFETY: an aligned word inside the mapping; the channel reads it only as an atomic.
        unsafe { self.base.add(at).cast::<u32>().write_volatile(value) };
    }

    pub fn u32_at(&self, at: usize) -> u32 {
        assert!(at.is_multiple_of(4) && at + 4 <= self.len);
        // SAFETY: as in `put_u32`.
        unsafe { self.base.add(at).cast::<u32>().read_volatile() }
    }

    pub fn put_u64(&self, at: usize, value: u64) {
        assert!(at.is_multiple_of(8) && at + 8 <= self.len);
        // SAFETY: as in `put_u32`, a word of 8 bytes.
        unsafe { self.base.add(at).cast::<u64>().write_volatile(value) };
    }

    pub fn put_bytes(&self, at: usize, bytes: &[u8]) {
        assert!(at + bytes.len() <= self.len);
        // SAFETY: the range lies inside the mapping, and private `bytes` cannot overlap it.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.base.add(at), bytes.len()) };
    }
}

// SAFETY: the view is a range of shared addresses read and written only by volatile accesses, from any thread.
unsafe impl Send for PeerView {}

impl Drop for PeerView {
    fn drop(&mut self) {
        // SAFETY: the whole of the mapping `new` made.
        unsafe { libc::munmap(self.base.cast(), self.len) };
    }
}
