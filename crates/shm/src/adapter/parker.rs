//! Sleeps on a word of a shared mapping through the OS's address wait.

use crate::os::{self, SyscallError};
use crate::protocol::doorbell::Parker;
use crate::sync::AtomicU32;

/// Sleeps through a futex (Linux) or `os_sync_wait_on_address` (macOS), shared across processes.
pub struct FutexParker;

impl Parker for FutexParker {
    type Error = SyscallError;

    fn park(&self, word: &AtomicU32, expect: u32, slice_ns: u64) -> Result<(), SyscallError> {
        os::wait(word, expect, slice_ns)
    }
}

// Miri runs no system call.
#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;
    use crate::adapter::doorbell::FutexDoorbell;
    use crate::protocol::doorbell::{park_slice, publish, ParkOutcome};
    use crate::sync::Ordering::SeqCst;

    #[test]
    fn a_park_on_a_word_already_moved_returns_at_once() {
        let word = AtomicU32::new(1);
        assert_eq!(FutexParker.park(&word, 0, 60_000_000_000), Ok(()));
    }

    #[test]
    fn a_publish_wakes_a_peer_parked_in_another_thread() {
        let (counter, sleeping) = (AtomicU32::new(0), AtomicU32::new(0));
        std::thread::scope(|s| {
            let peer = s.spawn(|| loop {
                match park_slice(&counter, &sleeping, 0, 60_000_000_000, &FutexParker) {
                    Ok(ParkOutcome::SliceOver) => continue,
                    other => return other,
                }
            });
            while sleeping.load(SeqCst) == 0 {
                std::hint::spin_loop();
            }
            assert!(publish(&counter, &sleeping, 1, &FutexDoorbell).is_ok());
            assert_eq!(peer.join().unwrap(), Ok(ParkOutcome::Changed(1)));
        });
    }
}
