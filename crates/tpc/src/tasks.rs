//! Counts one thread's connections until they end, sharing nothing with another thread.
//!
//! A thread's connections never leave its `LocalSet`, so the count is an `Rc<Cell>`: a connection
//! that starts or ends is one non-atomic add or subtract, and the one thing that waits on the
//! count, the drain at the stop, is one waker.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::FutureExt;
use mipyre_util::clock;
use mipyre_util::throttle::LogThrottle;

/// How often a thread logs a line for its tasks' panics; the lines between are counted, not logged.
const PANIC_LINE_PERIOD: Duration = Duration::from_secs(60);

struct TaskCount {
    running: Cell<usize>,
    panicked: Cell<u64>,
    /// Throttles the panic line, so a panic every connection hits is not one line per connection.
    panic_lines: LogThrottle,
    idle: RefCell<Option<Waker>>,
}

impl Default for TaskCount {
    fn default() -> Self {
        TaskCount {
            running: Cell::new(0),
            panicked: Cell::new(0),
            panic_lines: LogThrottle::new(PANIC_LINE_PERIOD),
            idle: RefCell::new(None),
        }
    }
}

/// Counts the tasks spawned on one thread's `LocalSet` that have not ended.
///
/// It is not `Clone`, and [`LocalTasks::idle`] borrows it mutably, so at most one [`IdleWaiter`]
/// waits at a time and one waker slot is enough.
#[derive(Default)]
pub struct LocalTasks(Rc<TaskCount>);

impl LocalTasks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawns `task` on the thread's `LocalSet` and counts it until it ends or is dropped
    /// unfinished with the `LocalSet`.
    ///
    /// # Panics
    ///
    /// Panics when called outside a `LocalSet`, as `tokio::task::spawn_local` does.
    pub fn spawn_local<F>(&self, task: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.0.running.set(self.0.running.get() + 1);
        let guard = TaskCountGuard(Rc::clone(&self.0));
        tokio::task::spawn_local(async move {
            if let Err(payload) = std::panic::AssertUnwindSafe(task).catch_unwind().await {
                record_panic(&guard.0, payload, clock::now());
            }
            drop(guard);
        });
    }

    pub fn len(&self) -> usize {
        self.0.running.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns how many of its tasks have ended by a panic; each one's text was logged where it
    /// was caught.
    pub fn panicked(&self) -> u64 {
        self.0.panicked.get()
    }

    /// Returns a future that completes once no task is running, at once if none is.
    ///
    /// A second waiter cannot be had while the first lives:
    ///
    /// ```compile_fail,E0499
    /// let mut tasks = mipyre_tpc::LocalTasks::new();
    /// let first = tasks.idle();
    /// let second = tasks.idle();
    /// drop((first, second));
    /// ```
    pub fn idle(&mut self) -> IdleWaiter<'_> {
        IdleWaiter(&self.0)
    }
}

/// Counts a task's panic and logs its text unless the thread's panic line is held back; returns
/// the lines held back since the last one when this one is logged.
fn record_panic(count: &TaskCount, payload: Box<dyn std::any::Any + Send>, now: Duration) -> Option<u64> {
    count.panicked.set(count.panicked.get() + 1);
    let panic = crate::threads::into_panic_message(payload);
    let held = count.panic_lines.admit(now)?;
    tracing::error!(%panic, held, "a connection task panicked");
    Some(held)
}

/// Lives inside its task, so its drop is the task's end.
struct TaskCountGuard(Rc<TaskCount>);

impl Drop for TaskCountGuard {
    fn drop(&mut self) {
        let left = self.0.running.get() - 1;
        self.0.running.set(left);
        if left == 0 {
            // The waker is taken out before it is woken, because it may run code that adds a task.
            let waker = self.0.idle.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

/// The future [`LocalTasks::idle`] returns.
pub struct IdleWaiter<'a>(&'a TaskCount);

impl Future for IdleWaiter<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.running.get() == 0 {
            return Poll::Ready(());
        }
        *self.0.idle.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local<F: Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&rt, f)
    }

    #[test]
    fn a_task_is_counted_until_it_ends() {
        local(async {
            let mut tasks = LocalTasks::new();
            assert!(tasks.is_empty());
            tasks.spawn_local(async {
                tokio::task::yield_now().await;
            });
            assert_eq!(tasks.len(), 1);
            tasks.idle().await;
            assert!(tasks.is_empty());
        });
    }

    #[test]
    fn a_task_dropped_unfinished_gives_its_count_back() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let set = tokio::task::LocalSet::new();
        let tasks = LocalTasks::new();
        set.block_on(&rt, async {
            tasks.spawn_local(std::future::pending::<()>());
            tokio::task::yield_now().await;
        });
        assert_eq!(tasks.len(), 1);
        drop(set);
        assert!(tasks.is_empty(), "the dropped task was counted down");
    }

    #[test]
    fn a_task_that_panics_is_counted_as_panicked() {
        local(async {
            let mut tasks = LocalTasks::new();
            tasks.spawn_local(async { panic!("the task's own failure") });
            tasks.spawn_local(async {});
            tasks.idle().await;
            assert_eq!(tasks.panicked(), 1);
        });
    }

    #[test]
    fn a_burst_of_panics_logs_one_line_per_period_and_counts_every_one() {
        let count = TaskCount::default();
        let panic = || -> Box<dyn std::any::Any + Send> { Box::new("the task's own failure") };
        let logged: Vec<_> = (0..5)
            .map(|ms| record_panic(&count, panic(), Duration::from_millis(ms)))
            .collect();
        assert_eq!(logged, [Some(0), None, None, None, None]);
        let next_period = PANIC_LINE_PERIOD + Duration::from_millis(5);
        assert_eq!(record_panic(&count, panic(), next_period), Some(4));
        assert_eq!(count.panicked.get(), 6);
    }

    /// The drain waits again after a timeout gave up; the second waiter must be woken by the task
    /// that ends after the first waiter is gone.
    #[test]
    fn a_waiter_after_an_earlier_one_is_woken_by_the_last_end() {
        local(async {
            let mut tasks = LocalTasks::new();
            let (release, released) = tokio::sync::oneshot::channel::<()>();
            tasks.spawn_local(async move {
                assert!(released.await.is_ok(), "the test sends before it ends");
            });
            let mut cx = Context::from_waker(Waker::noop());
            {
                let mut first = std::pin::pin!(tasks.idle());
                assert!(first.as_mut().poll(&mut cx).is_pending());
            }
            let second = tokio::task::spawn_local(async move {
                tasks.idle().await;
                tasks.len()
            });
            tokio::task::yield_now().await;
            release.send(()).expect("the task waits for this");
            assert_eq!(second.await.expect("the waiter ends"), 0);
        });
    }
}
