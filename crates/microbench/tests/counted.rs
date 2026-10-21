//! A binary whose global allocator is `AllocationCounter` counts its allocations; the unit tests hold the
//! other side, a binary on the system allocator.

use std::alloc::System;

use mipyre_microbench::{allocations_counted, AllocationCounter};

#[global_allocator]
static GLOBAL: AllocationCounter<System> = AllocationCounter(System);

#[test]
fn under_the_counting_allocator_every_allocation_is_counted() {
    assert!(allocations_counted());
}
