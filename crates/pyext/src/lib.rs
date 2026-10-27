/// Miri runs no C, so under it the extension keeps Rust's own allocator.
#[cfg(not(any(feature = "bench", miri)))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// `bench` builds count allocations in a measurement window (`bench::alloc`).
#[cfg(feature = "bench")]
#[global_allocator]
static GLOBAL: bench::AllocationCounter = bench::AllocationCounter;

#[cfg(feature = "bench")]
mod bench;
mod boot;
mod db;
mod pyapi;
mod sync;
mod telemetry;

/// Checks that the extension allocates through mimalloc in both builds; the bench counter passes
/// every call on to mimalloc.
#[cfg(test)]
#[cfg(not(miri))]
mod allocator_tests {
    #[test]
    fn a_box_lives_in_mimallocs_heap() {
        let boxed = Box::new([7u8; 64]);
        // SAFETY: only asks whether the address lies in memory mimalloc manages; nothing is read
        // through it.
        let in_heap = unsafe { libmimalloc_sys::mi_is_in_heap_region(boxed.as_ptr().cast()) };
        assert!(
            in_heap,
            "a Box is not in mimalloc's heap: the global allocator is not mimalloc"
        );
    }

    /// Shows the check above can fail: memory from another allocator does not read as mimalloc's.
    #[test]
    fn a_block_from_the_system_allocator_is_not_in_mimallocs_heap() {
        use std::alloc::{GlobalAlloc, Layout, System};
        let layout = Layout::new::<[u8; 64]>();
        // SAFETY: a non-zero-sized layout; the block is freed below with the same allocator and layout.
        let block = unsafe { System.alloc(layout) };
        assert!(!block.is_null(), "the system allocator gave no block");
        // SAFETY: as above, only the address is asked about.
        let in_heap = unsafe { libmimalloc_sys::mi_is_in_heap_region(block.cast()) };
        // SAFETY: `block` came from `System.alloc(layout)` and is freed once.
        unsafe { System.dealloc(block, layout) };
        assert!(
            !in_heap,
            "a system-allocated block reads as mimalloc's: the check above proves nothing"
        );
    }
}
