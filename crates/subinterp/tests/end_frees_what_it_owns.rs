//! Ending a worker frees everything it owns on the Rust heap (b/771).
//!
//! An integration test because it needs its own global allocator, and this crate has none.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use mipyre_subinterp::{NewInterpreterError, Releasable, SubInterpreter, PYTHON_THREAD_STACK};
use pyo3::Python;

thread_local! {
    /// Bytes this thread allocated minus the bytes it freed.
    static LIVE_BYTES: Cell<isize> = const { Cell::new(0) };
}

fn add_live(delta: isize) {
    // `try_with` because the allocator also runs while this thread's locals are being torn down.
    let _counted = LIVE_BYTES.try_with(|live| live.set(live.get() + delta));
}

struct PerThreadLive;

// SAFETY: Every call is forwarded to `System` unchanged.
unsafe impl GlobalAlloc for PerThreadLive {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add_live(layout.size().cast_signed());
        // SAFETY: The caller's layout is passed on unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        add_live(-layout.size().cast_signed());
        // SAFETY: The pointer and layout the caller gives back are passed on unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        add_live(new_size.cast_signed() - layout.size().cast_signed());
        // SAFETY: The caller's arguments are passed on unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: PerThreadLive = PerThreadLive;

struct Nothing;

impl Releasable for Nothing {
    fn release(self, _py: Python<'_>) {}
}

fn create_and_end(label: String) -> Result<(), NewInterpreterError> {
    SubInterpreter::<Nothing>::create_on_this_thread::<NewInterpreterError>(label, std::convert::identity, |_py| {
        Ok(Nothing)
    })?
    .end_on_own_thread();
    Ok(())
}

#[test]
fn a_created_and_ended_worker_leaves_nothing_on_the_rust_heap() {
    Python::initialize();
    let live = std::thread::Builder::new()
        .stack_size(PYTHON_THREAD_STACK)
        .spawn(|| {
            // The first worker on a thread may set up what every later one reuses.
            create_and_end("warm-up".into()).expect("the warm-up worker");
            let before = LIVE_BYTES.with(Cell::get);
            create_and_end("x".repeat(64)).expect("the measured worker");
            LIVE_BYTES.with(Cell::get) - before
        })
        .expect("spawn the worker thread")
        .join()
        .expect("the worker thread ends");
    assert_eq!(live, 0, "bytes still allocated after the worker ended");
}
