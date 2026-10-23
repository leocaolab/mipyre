//! Holds the pid of a lane process, typed so that 0 or a negative value never reaches `kill`.

use std::fmt;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_and_negative_are_not_pids() {
        assert_eq!(Pid::new(0), None);
        assert_eq!(Pid::new(-7), None);
        assert_eq!(Pid::new(7).map(Pid::as_raw), Some(7));
    }
}
