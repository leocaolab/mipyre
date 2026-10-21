//! Counts heap allocations for the in-process benchmark by wrapping mimalloc in a counter.
//!
//! Outside a measurement window an allocation costs one relaxed load more than plain mimalloc.
//! Inside one, every `alloc`, `alloc_zeroed` and `realloc` is counted with its size and, when
//! asked, its call stack.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::LazyLock;

use parking_lot::Mutex;

/// The bench build's global allocator, which counts while a measurement window is open.
pub(crate) struct AllocationCounter;

static COUNTING: AtomicBool = AtomicBool::new(false);
static STACKS_ON: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

const DEPTH: usize = 24;
type CallStack = [usize; DEPTH];

/// Maps each recorded stack to its allocation count and bytes.
static STACKS: LazyLock<Mutex<HashMap<CallStack, (u64, u64)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

thread_local! {
    /// Is set while this thread records a stack, so the recording's own allocations are neither
    /// counted nor recursed into.
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

fn record(size: usize) {
    ALLOCS.fetch_add(1, Relaxed);
    BYTES.fetch_add(size as u64, Relaxed);
    if !STACKS_ON.load(Relaxed) {
        return;
    }
    let reentered = IN_HOOK.try_with(|h| h.replace(true)).unwrap_or(true);
    if reentered {
        return;
    }
    let mut stack: CallStack = [0; DEPTH];
    let mut i = 0;
    // SAFETY: `trace_unsynchronized` requires that no other thread unwinds concurrently in a way
    // the unwinder cannot handle; the bench measures one worker thread at a time, and a partial
    // stack only loses attribution.
    unsafe {
        backtrace::trace_unsynchronized(|frame| {
            stack[i] = frame.ip() as usize;
            i += 1;
            i < DEPTH
        });
    }
    {
        let mut stacks = STACKS.lock();
        let entry = stacks.entry(stack).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += size as u64;
    }
    #[expect(
        clippy::let_underscore_must_use,
        reason = "`try_with` fails only while the thread's locals are destroyed; the flag dies with them"
    )]
    let _ = IN_HOOK.try_with(|h| h.set(false));
}

unsafe impl GlobalAlloc for AllocationCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Relaxed) && !IN_HOOK.try_with(Cell::get).unwrap_or(true) {
            record(layout.size());
        }
        // SAFETY: the caller's contract for `alloc` is passed on unchanged.
        unsafe { mimalloc::MiMalloc.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Relaxed) && !IN_HOOK.try_with(Cell::get).unwrap_or(true) {
            record(layout.size());
        }
        // SAFETY: the caller's contract for `alloc_zeroed` is passed on unchanged.
        unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: every block came from mimalloc, so it is freed there with the caller's layout.
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Relaxed) && !IN_HOOK.try_with(Cell::get).unwrap_or(true) {
            record(new_size);
        }
        // SAFETY: every block came from mimalloc, so it is resized there under the caller's
        // contract.
        unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) }
    }
}

/// Starts a window that counts allocations from zero.
pub(crate) fn start_counting() {
    start(false);
}

/// Starts a window that counts allocations from zero and records the stack of each.
pub(crate) fn start_recording_stacks() {
    start(true);
}

fn start(stacks: bool) {
    ALLOCS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    STACKS.lock().clear();
    STACKS_ON.store(stacks, Relaxed);
    COUNTING.store(true, Relaxed);
}

/// Holds what one window counted.
pub(crate) struct Allocations {
    pub(crate) allocs: u64,
    pub(crate) bytes: u64,
    /// Lists every distinct stack, most allocations first; it is empty unless stacks were recorded.
    pub(crate) stacks: Vec<StackSite>,
}

/// Describes one call stack that allocated in the window.
pub(crate) struct StackSite {
    pub(crate) count: u64,
    pub(crate) bytes: u64,
    /// Lists the resolved frames, innermost first.
    pub(crate) frames: Vec<String>,
}

/// Ends the window and returns what it counted.
pub(crate) fn stop() -> Allocations {
    COUNTING.store(false, Relaxed);
    STACKS_ON.store(false, Relaxed);
    let allocs = ALLOCS.load(Relaxed);
    let bytes = BYTES.load(Relaxed);
    let raw = std::mem::take(&mut *STACKS.lock());
    let mut stacks: Vec<StackSite> = raw
        .into_iter()
        .map(|(stack, (count, bytes))| StackSite {
            count,
            bytes,
            frames: resolve(&stack),
        })
        .collect();
    stacks.sort_by_key(|s| std::cmp::Reverse(s.count));
    Allocations {
        allocs,
        bytes,
        stacks,
    }
}

fn resolve(stack: &CallStack) -> Vec<String> {
    let mut frames = Vec::new();
    for &ip in stack.iter().take_while(|&&ip| ip != 0) {
        backtrace::resolve(ip as *mut std::ffi::c_void, |symbol| {
            let name = symbol
                .name()
                .map(|n| format!("{n:#}"))
                .unwrap_or_else(|| format!("{ip:#x}"));
            let at = match (symbol.filename(), symbol.lineno()) {
                (Some(file), Some(line)) => format!(" ({}:{line})", file.display()),
                _ => String::new(),
            };
            frames.push(format!("{name}{at}"));
        });
    }
    frames
}
