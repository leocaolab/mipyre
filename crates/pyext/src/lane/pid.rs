//! Holds the pids of a lane's worker processes, typed so that 0 or a negative value never
//! reaches `kill`.

use std::fmt;

use crate::sync::{AtomicI32, Ordering::SeqCst};

/// The pid of a process. Never 0, which is no process, and never negative: a negative
/// `kill` argument names a process group, so a value like that must not reach `kill`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pid(rustix::process::Pid);

impl Pid {
    /// Returns `None` for 0 and for negative values.
    pub(crate) fn new(raw: i32) -> Option<Self> {
        if raw < 0 {
            return None;
        }
        rustix::process::Pid::from_raw(raw).map(Self)
    }

    pub(crate) fn as_raw(self) -> i32 {
        self.0.as_raw_nonzero().get()
    }
}

impl From<Pid> for rustix::process::Pid {
    fn from(pid: Pid) -> Self {
        pid.0
    }
}

impl fmt::Display for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_raw())
    }
}

/// A pid slot that may be empty; it stores 0 for empty, which never becomes a `Pid`, so it
/// cannot reach `kill`.
#[derive(Debug, Default)]
pub(crate) struct AtomicPid(AtomicI32);

impl AtomicPid {
    pub(crate) fn load(&self) -> Option<Pid> {
        Pid::new(self.0.load(SeqCst))
    }

    pub(crate) fn store(&self, pid: Option<Pid>) {
        self.0.store(raw_of(pid), SeqCst);
    }

    pub(crate) fn swap(&self, pid: Option<Pid>) -> Option<Pid> {
        Pid::new(self.0.swap(raw_of(pid), SeqCst))
    }
}

fn raw_of(pid: Option<Pid>) -> i32 {
    pid.map_or(0, Pid::as_raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_and_negative_are_not_pids() {
        assert_eq!(Pid::new(0), None);
        assert_eq!(Pid::new(-7), None);
        assert_eq!(Pid::new(7).map(Pid::as_raw), Some(7));
    }

    #[test]
    fn an_empty_atomic_pid_loads_none_and_round_trips() {
        let slot = AtomicPid::default();
        assert_eq!(slot.load(), None);
        let pid = Pid::new(42);
        slot.store(pid);
        assert_eq!(slot.load(), pid);
        assert_eq!(slot.swap(None), pid);
        assert_eq!(slot.load(), None);
    }
}
