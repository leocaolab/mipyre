//! Counts every allocation the process makes; a `realloc` counts as one, since the default
//! `GlobalAlloc::realloc` makes a new allocation.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

pub static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

pub struct AllocationCounter<A>(pub A);

// SAFETY: Every call goes to the inner allocator unchanged, so its guarantees hold.
unsafe impl<A: GlobalAlloc> GlobalAlloc for AllocationCounter<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller's layout is passed on unchanged.
        unsafe { self.0.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout the caller gives back are passed on unchanged.
        unsafe { self.0.dealloc(ptr, layout) }
    }
}
