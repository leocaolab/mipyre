//! Holds a lane's worker process as the server knows it: a process the kernel says the template
//! started, and the way to end it that cannot reach another process once its pid is reused.
//!
//! A worker is the template's child, not the server's: once it dies the template reaps it and its
//! pid is free for any process. So the server signals it through what it opened when it checked
//! the parent, never by the number alone. On Linux that is a pidfd. macOS has none: the server
//! stops signalling once the host has seen the exit, and the time between the template reaping a
//! worker and the host seeing its exit is a known window (`docs/design/shm-channel.md` §2.2).

use std::time::Instant;

use super::core::WorkerTag;
use super::os::ProcessHandle;
use super::pid::Pid;
use super::server::ForkError;
use crate::exchange::LaneIndex;

/// The process a lane's worker runs in. Made only by [`WorkerProcess::open`], which checks the
/// template is its parent: holding one means the process was the template's worker.
#[derive(Debug)]
pub(super) struct WorkerProcess {
    /// Its lane, start attempt and pid.
    tag: WorkerTag,
    /// When the server took the process as its lane's worker, right after the fork answered.
    opened_at: Instant,
    /// What the server signals it by and watches its exit through.
    handle: ProcessHandle,
}

impl WorkerProcess {
    /// Takes `pid`, which `template` answered to the fork request `tag`, once the kernel says
    /// `template` is its parent. A pid with no process names nothing a signal could hit: it is a
    /// worker that already ended and was reaped, and is taken, its death then seen as any
    /// worker's is.
    ///
    /// The handle is opened before the parent is read, so on Linux the parent read is of the
    /// process the pidfd holds unless that process was reaped and its pid taken by another in
    /// between, which takes the kernel going round the whole pid space. Even then a wrong reading
    /// only takes or refuses the worker: signals go to the pidfd.
    ///
    /// # Errors
    ///
    /// [`ForkError::NotTemplateChild`] for a process the template did not start,
    /// [`ForkError::ParentUnread`] when the kernel's record cannot be read, and
    /// [`ForkError::PidfdUnopened`] when the process has no pidfd (Linux; it is killed then: a
    /// worker left running would serve its lane unseen).
    pub(super) fn open(template: Pid, tag: WorkerTag) -> Result<Self, ForkError> {
        let pid = tag.pid;
        let handle = match ProcessHandle::open(pid.into()) {
            Ok(handle) => handle,
            Err(errno) => {
                // A child the template has not reaped still has its number, so the number names
                // it here, right after the parent is read.
                if check_parent(template, pid)?.is_some() {
                    signal_by_number(pid);
                }
                return Err(ForkError::PidfdUnopened {
                    template,
                    pid,
                    source: errno.into(),
                });
            }
        };
        if !handle.known_gone() && check_parent(template, pid)?.is_none() {
            handle.record_exit();
        }
        Ok(WorkerProcess {
            tag,
            opened_at: Instant::now(),
            handle,
        })
    }

    pub(super) fn lane(&self) -> LaneIndex {
        self.tag.lane
    }

    /// The worker this process is, as the host's decisions name it.
    pub(super) fn tag(&self) -> WorkerTag {
        self.tag
    }

    pub(super) fn pid(&self) -> Pid {
        self.tag.pid
    }

    pub(super) fn opened_at(&self) -> Instant {
        self.opened_at
    }

    /// Returns what the process is signalled by and its exit watched through.
    pub(super) fn handle(&self) -> &ProcessHandle {
        &self.handle
    }

    /// Records that the server saw the process exit: no signal goes to its pid after this.
    pub(super) fn record_exit(&self) {
        self.handle.record_exit();
    }

    /// Sends the process SIGKILL; one known to be gone is left alone.
    pub(super) fn kill(&self) {
        refused_kill(self.tag.pid, self.handle.kill(self.tag.pid.into()));
    }
}

/// Reads `pid`'s parent and returns it (`None`: no process has the pid) when it is `template`.
fn check_parent(template: Pid, pid: Pid) -> Result<Option<i32>, ForkError> {
    let parent =
        super::os::parent_of(pid.as_raw()).map_err(|source| ForkError::ParentUnread { template, pid, source })?;
    template_child(template, pid, parent)?;
    Ok(parent)
}

/// Takes `pid`, which the template answered to a fork request, as the lane's worker only when
/// the kernel says the template is its `parent`. A pid with no process (`None`) names nothing a
/// kill could hit, and is taken.
fn template_child(template: Pid, pid: Pid, parent: Option<i32>) -> Result<Pid, ForkError> {
    match parent {
        Some(parent) if parent != template.as_raw() => Err(ForkError::NotTemplateChild { template, pid, parent }),
        Some(_) | None => Ok(pid),
    }
}

/// Sends SIGKILL to `pid` by its number: only for a Linux worker no pidfd could be opened for,
/// which the template has not been seen to reap.
fn signal_by_number(pid: Pid) {
    refused_kill(
        pid,
        rustix::process::kill_process(pid.into(), rustix::process::Signal::KILL),
    );
}

/// Logs a SIGKILL the OS refused. A process already gone (ESRCH) is what the kill wanted; any
/// other refusal leaves it running, which only the log can tell.
fn refused_kill(pid: Pid, sent: rustix::io::Result<()>) {
    match sent {
        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
        Err(e) => tracing::error!(
            target: "mipyre::server",
            pid = pid.as_raw(),
            errno = e.raw_os_error(),
            error = %e,
            "SIGKILL to a lane worker was refused: it may still run"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pid the template answered is its worker only when the template is its parent; one with no
    /// process names nothing a kill could hit, and is taken.
    #[test]
    fn a_forked_pid_is_taken_only_as_a_child_of_the_template() {
        let template = Pid::new(100).unwrap();
        let pid = Pid::new(4242).unwrap();
        assert_eq!(template_child(template, pid, Some(100)).ok(), Some(pid));
        assert_eq!(template_child(template, pid, None).ok(), Some(pid));
        assert!(matches!(
            template_child(template, pid, Some(101)),
            Err(ForkError::NotTemplateChild { parent: 101, pid: p, template: t }) if p == pid && t == template
        ));
    }
}
