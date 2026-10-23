//! Starts OS threads that can be joined by a deadline, and turns a panic's payload into text.

use std::convert::Infallible;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

/// How long a [`crate::StopWatcher`] dropped without `finish` waits for its thread.
pub(crate) const JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// How often [`OsThread::finished_by`] looks again at a thread whose body has returned but which
/// has not finished; that gap is only the thread storing its result and running its thread-local
/// destructors, so it is short.
const EXIT_POLL: Duration = Duration::from_millis(1);

/// A thread started by [`spawn`]. `JoinHandle` has no timed join, so the body holds the sender of
/// `ended`, and its drop when the body returns or unwinds wakes the waiter instead of a poll.
pub struct OsThread<T> {
    handle: std::thread::JoinHandle<T>,
    ended: mpsc::Receiver<Infallible>,
}

/// Starts `body` on a thread built by `builder`.
///
/// # Errors
///
/// Returns the OS's error when the thread cannot be started.
pub fn spawn<T: Send + 'static>(
    builder: std::thread::Builder,
    body: impl FnOnce() -> T + Send + 'static,
) -> std::io::Result<OsThread<T>> {
    let (ending, ended) = mpsc::channel::<Infallible>();
    let handle = builder.spawn(move || {
        // Its drop, after `body` returns or while it unwinds, is what wakes the waiter.
        let _ending = ending;
        body()
    })?;
    Ok(OsThread { handle, ended })
}

impl<T> OsThread<T> {
    /// Returns the thread's name, or its id if it was given none.
    pub fn name(&self) -> String {
        thread_name(self.handle.thread())
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Joins the thread if it ends by `deadline`, and otherwise lets it go on running, detached,
    /// because waiting longer could hang the process on a call that never returns.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadJoinError::StillRunning`] for a thread still running at `deadline`, and
    /// [`ThreadJoinError::Panicked`] with the panic's text for one that panicked.
    pub fn join_by(self, deadline: Instant) -> Result<T, ThreadJoinError> {
        if !self.finished_by(deadline) {
            return Err(ThreadJoinError::StillRunning);
        }
        self.handle
            .join()
            .map_err(|payload| ThreadJoinError::Panicked(into_panic_message(payload)))
    }

    /// Waits until the thread finishes or `deadline` passes, and returns whether it finished.
    fn finished_by(&self, deadline: Instant) -> bool {
        match self
            .ended
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            Ok(never) => match never {},
            Err(RecvTimeoutError::Timeout) => return false,
            Err(RecvTimeoutError::Disconnected) => {}
        }
        loop {
            if self.handle.is_finished() {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            std::thread::sleep(EXIT_POLL.min(remaining));
        }
    }
}

/// Why [`OsThread::join_by`] has no result.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ThreadJoinError {
    #[error("panicked: {0}")]
    Panicked(String),
    #[error("still running at the join deadline; left running")]
    StillRunning,
}

fn thread_name(thread: &std::thread::Thread) -> String {
    thread
        .name()
        .map_or_else(|| format!("{:?}", thread.id()), str::to_string)
}

/// Returns a panic payload's text; a payload that is neither `&str` nor `String` becomes a fixed
/// sentence. A `&Box<dyn Any + Send>` passed in place of the payload it holds is itself an `Any`,
/// so the box is looked into rather than called a payload of neither kind.
pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(inner) = payload.downcast_ref::<Box<dyn std::any::Any + Send>>() {
        return panic_message(inner.as_ref());
    }
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<panic payload is neither &str nor String>".to_string())
}

/// Returns [`panic_message`] of `payload` and drops it, catching a panic the drop raises, which
/// would otherwise escape the code that caught the first. The second panic's payload is leaked,
/// not dropped, so its drop cannot panic either.
pub(crate) fn into_panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    let message = panic_message(payload.as_ref());
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(payload))) {
        Ok(()) => message,
        Err(second) => {
            let said = format!(
                "{message}; dropping its payload panicked too: {}",
                panic_message(second.as_ref())
            );
            std::mem::forget(second);
            said
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_thread_that_ends_is_seen_finished_before_the_deadline() {
        let (release, released) = mpsc::channel::<()>();
        let thread = spawn(std::thread::Builder::new(), move || while released.recv().is_ok() {}).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        drop(release);
        assert!(thread.finished_by(deadline));
        assert!(Instant::now() < deadline, "woken by the end, not the deadline");
        thread.handle.join().unwrap();
    }

    #[test]
    fn a_thread_still_running_at_the_deadline_is_not_finished() {
        let (release, released) = mpsc::channel::<()>();
        let thread = spawn(std::thread::Builder::new(), move || while released.recv().is_ok() {}).unwrap();
        let timeout = Duration::from_millis(10);
        let started = Instant::now();
        assert!(!thread.finished_by(started + timeout));
        assert!(started.elapsed() >= timeout, "returned before the deadline");
        drop(release);
        thread.handle.join().unwrap();
    }

    #[test]
    fn a_panicking_body_wakes_the_wait_too() {
        let thread = spawn(std::thread::Builder::new(), || {
            if std::hint::black_box(true) {
                panic!("boom");
            }
        })
        .unwrap();
        assert_eq!(
            thread.join_by(Instant::now() + Duration::from_secs(10)),
            Err(ThreadJoinError::Panicked("boom".into()))
        );
    }

    #[test]
    fn a_thread_joined_by_a_passed_deadline_is_still_running() {
        let (release, released) = mpsc::channel::<()>();
        let thread = spawn(std::thread::Builder::new(), move || while released.recv().is_ok() {}).unwrap();
        assert_eq!(thread.join_by(Instant::now()), Err(ThreadJoinError::StillRunning));
        drop(release);
    }

    #[test]
    fn a_thread_that_ends_is_joined_with_its_result() {
        let thread = spawn(std::thread::Builder::new(), || 7).unwrap();
        assert_eq!(thread.join_by(Instant::now() + Duration::from_secs(10)), Ok(7));
    }

    #[test]
    fn a_payload_passed_as_its_box_is_read_through_it() {
        let payload = std::panic::catch_unwind(|| panic!("real message")).unwrap_err();
        assert_eq!(panic_message(&payload), "real message");
        assert_eq!(panic_message(&*payload), "real message");
        let neither: Box<dyn std::any::Any + Send> = Box::new(7_u8);
        assert_eq!(panic_message(&*neither), "<panic payload is neither &str nor String>");
    }
}
