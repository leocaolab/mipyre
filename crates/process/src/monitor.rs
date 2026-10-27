//! Sees a host's workers exit, and says how one ended from the wait status the template reported.

use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::time::Duration;

use super::lane_failure::WorkerExit;
use super::worker_process::WorkerProcess;
use tokio::io::unix::AsyncFd;

/// Sees a host's workers exit through the host's reactor: the exit watch ([`super::os::ExitWatch`])
/// is itself one readable fd in the runtime. Workers are the template's children, not the server's:
/// the server only needs to know they are gone (the template reaps them and reports each wait
/// status over its socket).
pub(crate) struct ProcessMonitor {
    fd: AsyncFd<super::os::ExitWatch>,
    /// Holds the workers that were gone before they could be watched.
    early: RefCell<Vec<Arc<WorkerProcess>>>,
    /// Holds the workers watched, by the token their exit is reported under, each until it is.
    watched: RefCell<std::collections::HashMap<u64, Arc<WorkerProcess>>>,
    /// The token the next worker watched is given.
    next_token: Cell<u64>,
}

impl ProcessMonitor {
    pub(crate) fn new() -> std::io::Result<Self> {
        Ok(ProcessMonitor {
            fd: super::os::ExitWatch::new()?.registered()?,
            early: RefCell::new(Vec::new()),
            watched: RefCell::new(std::collections::HashMap::new()),
            next_token: Cell::new(0),
        })
    }

    pub(crate) async fn readable(&self) -> std::io::Result<()> {
        let mut guard = self.fd.readable().await?;
        guard.clear_ready();
        Ok(())
    }

    /// Watches `worker` for its exit; one already gone is reported by the next
    /// [`ProcessMonitor::exited`].
    pub(crate) fn add(&self, worker: Arc<WorkerProcess>) -> std::io::Result<()> {
        let token = self.next_token.get();
        match self.fd.get_ref().add(worker.handle(), worker.pid().into(), token)? {
            super::os::Registration::Watching => {
                self.next_token.set(token.wrapping_add(1));
                self.watched.borrow_mut().insert(token, worker);
            }
            super::os::Registration::Gone => {
                worker.record_exit();
                self.early.borrow_mut().push(worker);
            }
        }
        Ok(())
    }

    /// Returns every worker seen exiting since the last call, without blocking.
    pub(crate) fn exited(&self) -> std::io::Result<Vec<Arc<WorkerProcess>>> {
        const ROOM: usize = 16;
        let mut out = std::mem::take(&mut *self.early.borrow_mut());
        loop {
            let seen = self.fd.get_ref().exited::<ROOM>(Some(Duration::ZERO), |token| {
                let dead = self.watched.borrow_mut().remove(&token).ok_or_else(|| {
                    std::io::Error::other(format!(
                        "the exit watch reported token {token}, which no watched worker has"
                    ))
                })?;
                self.fd.get_ref().forget(dead.handle())?;
                dead.record_exit();
                out.push(dead);
                Ok(())
            })?;
            if seen < ROOM {
                return Ok(out);
            }
        }
    }
}

pub(crate) fn worker_exit(status: i32) -> WorkerExit {
    if libc::WIFSIGNALED(status) {
        let signal = libc::WTERMSIG(status);
        WorkerExit::Signaled {
            signal,
            name: super::os::signal_name(signal),
        }
    } else if libc::WIFEXITED(status) {
        WorkerExit::Exited(libc::WEXITSTATUS(status))
    } else {
        WorkerExit::Other(status)
    }
}
