//! Reads and sets how the OS handles one signal of this process.

use std::io;

/// How the OS handles one signal: its handler, flags and mask, as `sigaction` holds them.
#[derive(Clone, Copy)]
pub(super) struct SignalAction(libc::sigaction);

impl SignalAction {
    /// Returns how the OS handles `signal` now.
    pub(super) fn of(signal: libc::c_int) -> io::Result<Self> {
        // SAFETY: `sigaction` is plain integers and pointers, so all zeros is a valid value.
        let mut current: libc::sigaction = unsafe { std::mem::zeroed() };
        // SAFETY: a null new action only reads, into `current`, which is valid for one write.
        if unsafe { libc::sigaction(signal, std::ptr::null(), &mut current) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(current))
    }

    /// Makes this how the OS handles `signal`.
    pub(super) fn install(&self, signal: libc::c_int) -> io::Result<()> {
        // SAFETY: the new action is one `sigaction` read from the OS, whose handler is still in
        // this process; a null old action is not written.
        if unsafe { libc::sigaction(signal, &self.0, std::ptr::null_mut()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Says whether both call the same handler, whatever their flags and masks.
    pub(super) fn same_handler(&self, other: &Self) -> bool {
        self.0.sa_sigaction == other.0.sa_sigaction
    }
}
