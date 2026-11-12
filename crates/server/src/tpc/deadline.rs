//! The budgets of a TPC thread's `async def` requests, kept in start order. Every request on a
//! server has the same budget, so deadlines come in the order requests start: one queue and one
//! tokio timer per thread (for the earliest) stand in for a timer per request.
//!
//! It does not know requests, Tasks or status codes: it wakes whoever watches a deadline once the
//! deadline passes. A watch that ends first (its request answered) leaves its queue entry behind,
//! which is skipped when it reaches the front.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use tokio::time::Instant;

/// Returns whether a queue holding `queued` entries for `watching` live watches is mostly entries
/// whose watch ended, so dropping those is worth one pass.
fn worth_compacting(queued: usize, watching: usize) -> bool {
    queued > 64 && queued > 4 * watching
}

/// One watch of a deadline; `generation` tells it from a later watch in the same slot.
struct Watcher {
    generation: u64,
    state: WatchState,
}

/// Says whether a watch's deadline passed, and while it has not, whom to wake when it does.
enum WatchState {
    Waiting(Option<Waker>),
    Passed,
}

/// The deadline of one watch, as the queue holds it: the watch is named by its slot and generation.
struct WatchDeadline {
    at: Instant,
    slot: usize,
    generation: u64,
}

/// The deadlines in start order, and the watches still waiting on them.
#[derive(Default)]
struct DeadlineQueue {
    order: VecDeque<WatchDeadline>,
    watchers: slab::Slab<Watcher>,
    generation: u64,
}

/// Returns whether `queued` still names a live watch in `watchers`.
fn watched(watchers: &slab::Slab<Watcher>, queued: &WatchDeadline) -> bool {
    watchers
        .get(queued.slot)
        .is_some_and(|watcher| watcher.generation == queued.generation)
}

impl DeadlineQueue {
    fn drop_ended_front(&mut self) {
        while self
            .order
            .front()
            .is_some_and(|queued| !watched(&self.watchers, queued))
        {
            self.order.pop_front();
        }
    }
}

/// One thread's queue of budget deadlines.
#[derive(Default)]
pub(crate) struct Deadlines {
    queue: RefCell<DeadlineQueue>,
    /// Wakes the sweeper when the queue gets a first entry.
    armed: tokio::sync::Notify,
}

impl Deadlines {
    /// Watches `at`, which is no earlier than any deadline watched before on this thread.
    pub(crate) fn watch(&self, at: Instant) -> Watch<'_> {
        let mut queue = self.queue.borrow_mut();
        queue.drop_ended_front();
        if worth_compacting(queue.order.len(), queue.watchers.len()) {
            let DeadlineQueue { order, watchers, .. } = &mut *queue;
            order.retain(|queued| watched(watchers, queued));
        }
        queue.generation += 1;
        let generation = queue.generation;
        let slot = queue.watchers.insert(Watcher {
            generation,
            state: WatchState::Waiting(None),
        });
        let was_empty = queue.order.is_empty();
        queue.order.push_back(WatchDeadline { at, slot, generation });
        drop(queue);
        if was_empty {
            self.armed.notify_one();
        }
        Watch {
            deadlines: self,
            slot,
            generation,
        }
    }

    /// Wakes the watches whose deadline passed by `now`, and returns the next deadline to wait for.
    fn expire(&self, now: Instant) -> Option<Instant> {
        let mut woken = Vec::new();
        let next = {
            let mut queue = self.queue.borrow_mut();
            loop {
                queue.drop_ended_front();
                let Some(front) = queue.order.front() else { break None };
                if front.at > now {
                    break Some(front.at);
                }
                let Some(queued) = queue.order.pop_front() else {
                    break None;
                };
                if let Some(watcher) = queue.watchers.get_mut(queued.slot) {
                    if let WatchState::Waiting(waker) = std::mem::replace(&mut watcher.state, WatchState::Passed) {
                        woken.extend(waker);
                    }
                }
            }
        };
        for waker in woken {
            waker.wake();
        }
        next
    }

    /// Wakes each watch when its deadline passes, until the thread stops.
    pub(crate) async fn sweep(&self) {
        let mut next = None;
        loop {
            match next {
                Some(at) => tokio::select! {
                    () = tokio::time::sleep_until(at) => {}
                    () = self.armed.notified() => {}
                },
                None => self.armed.notified().await,
            }
            next = self.expire(Instant::now());
        }
    }
}

/// Completes once its deadline passes; dropping it stops the watch.
pub(crate) struct Watch<'d> {
    deadlines: &'d Deadlines,
    slot: usize,
    generation: u64,
}

impl Future for Watch<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut queue = self.deadlines.queue.borrow_mut();
        let Some(watcher) = queue
            .watchers
            .get_mut(self.slot)
            .filter(|watcher| watcher.generation == self.generation)
        else {
            return Poll::Ready(());
        };
        let WatchState::Waiting(waker) = &mut watcher.state else {
            return Poll::Ready(());
        };
        match waker {
            Some(waker) if waker.will_wake(cx.waker()) => {}
            _ => *waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        let mut queue = self.deadlines.queue.borrow_mut();
        if queue
            .watchers
            .get(self.slot)
            .is_some_and(|watcher| watcher.generation == self.generation)
        {
            queue.watchers.remove(self.slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_queue_mostly_of_ended_watches_is_compacted() {
        assert!(worth_compacting(65, 16));
        assert!(!worth_compacting(64, 0));
        assert!(!worth_compacting(65, 17));
    }

    #[tokio::test(start_paused = true)]
    async fn a_watch_wakes_once_its_deadline_passes_and_one_dropped_first_never_does() {
        let deadlines = Deadlines::default();
        let sweeper = async {
            deadlines.sweep().await;
        };
        tokio::pin!(sweeper);
        let start = Instant::now();
        let dropped = deadlines.watch(start + std::time::Duration::from_secs(1));
        drop(dropped);
        let kept = deadlines.watch(start + std::time::Duration::from_secs(2));
        tokio::select! {
            () = &mut sweeper => unreachable!("the sweeper runs until the thread stops"),
            () = kept => {}
        }
        assert_eq!(Instant::now() - start, std::time::Duration::from_secs(2));
        assert!(deadlines.queue.borrow().order.is_empty());
    }

    #[test]
    fn every_watch_kept_completes_once_its_deadline_passes_and_not_before() {
        use std::time::Duration;

        let deadlines = Deadlines::default();
        let start = Instant::now();
        let mut cx = Context::from_waker(Waker::noop());
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut kept = Vec::new();
        let mut at = start;
        for _ in 0..2_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            at += Duration::from_micros(state % 3);
            let mut watch = Box::pin(deadlines.watch(at));
            assert!(watch.as_mut().poll(&mut cx).is_pending());
            if !state.is_multiple_of(4) {
                kept.push((at, watch));
            }
        }
        let mut now = start;
        while now <= at {
            deadlines.expire(now);
            kept.retain_mut(|(due, watch)| {
                let done = watch.as_mut().poll(&mut cx).is_ready();
                assert_eq!(done, *due <= now, "a watch of {:?} at {:?}", *due - start, now - start);
                !done
            });
            now += Duration::from_micros(7);
        }
        deadlines.expire(now);
        assert!(kept.is_empty());
        assert!(deadlines.queue.borrow().order.is_empty());
    }
}
