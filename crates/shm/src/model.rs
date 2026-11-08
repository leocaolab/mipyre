//! The kernel under the Loom models: the futex a peer sleeps on and the pipe a caller's reactor waits on, as the
//! ports' implementations. Neither sleeps with a timeout, so a wake the protocol loses leaves a side asleep for
//! good, which Loom reports as a deadlock.

use std::convert::Infallible;
use std::sync::{Arc, PoisonError};

use crate::protocol::doorbell::{Doorbell, Parker};
use crate::sync::{AtomicU32, Condvar, Mutex, Ordering::Relaxed};

/// A futex: a sleeper compares the word under the lock and sleeps only while it holds what it expected, and a ring
/// takes the lock and wakes every sleeper, as the kernel compares and wakes under its hash bucket's lock. A clone is
/// the same futex.
#[derive(Clone, Default)]
pub(crate) struct FakeFutex(Arc<Bucket>);

/// What the kernel keeps for one futex: a lock and the sleepers under it.
#[derive(Default)]
struct Bucket {
    lock: Mutex<()>,
    woken: Condvar,
}

impl Parker for FakeFutex {
    type Error = Infallible;

    /// Sleeps until one ring, or not at all when `word` moved; the slice is ignored.
    fn park(&self, word: &AtomicU32, expect: u32, _slice_ns: u64) -> Result<(), Infallible> {
        let held = self.0.lock.lock().unwrap_or_else(PoisonError::into_inner);
        if word.load(Relaxed) == expect {
            drop(self.0.woken.wait(held).unwrap_or_else(PoisonError::into_inner));
        }
        Ok(())
    }
}

impl Doorbell for FakeFutex {
    type Outcome = ();
    type Error = Infallible;

    fn ring(&self, _word: &AtomicU32) -> Result<(), Infallible> {
        let _held = self.0.lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.0.woken.notify_all();
        Ok(())
    }
}

/// A caller's wake pipe: a ring writes a byte, and the caller's reactor sleeps until there is one to read, then reads
/// them all. A clone is the same pipe.
#[derive(Clone, Default)]
pub(crate) struct FakeWakePipe(Arc<Pipe>);

#[derive(Default)]
struct Pipe {
    bytes: Mutex<PipeBytes>,
    readable: Condvar,
}

/// The bytes in the pipe now, and every byte ever written to it.
#[derive(Clone, Copy, Default)]
struct PipeBytes {
    unread: u32,
    written: u32,
}

impl FakeWakePipe {
    /// Sleeps until a byte is in the pipe, then reads every byte there.
    pub(crate) fn wait_and_drain(&self) {
        let mut bytes = self.0.bytes.lock().unwrap_or_else(PoisonError::into_inner);
        while bytes.unread == 0 {
            bytes = self.0.readable.wait(bytes).unwrap_or_else(PoisonError::into_inner);
        }
        bytes.unread = 0;
    }

    /// Returns how many bytes were ever written: one per ring.
    pub(crate) fn written(&self) -> u32 {
        self.0.bytes.lock().unwrap_or_else(PoisonError::into_inner).written
    }
}

impl Doorbell for FakeWakePipe {
    type Outcome = ();
    type Error = Infallible;

    fn ring(&self, _word: &AtomicU32) -> Result<(), Infallible> {
        let mut bytes = self.0.bytes.lock().unwrap_or_else(PoisonError::into_inner);
        bytes.unread += 1;
        bytes.written += 1;
        self.0.readable.notify_all();
        Ok(())
    }
}
