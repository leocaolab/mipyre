//! Gives each per-core thread its own view of the server's stop, so waiting for it shares
//! nothing with another thread.
//!
//! The server's stop is one `CancellationToken`, and waiting on it is not free: tokio-util's
//! `cancelled()` locks the token's tree mutex on every poll and its `Notify`'s mutex to refresh
//! the waker, and a clone or drop locks the tree mutex again. A connection polls its stop at
//! least once per request, so a token shared by every thread put two process-wide mutexes on
//! every request of every core.
//!
//! [`LocalStop`] has one task per thread wait on the token and fire a thread-local signal. Each
//! [`StopWaiter`] holds a waker slot of its own, so a poll reads the flag and compares one waker.
//! An ended waiter's slot is kept for the next one, so a connection usually allocates none; after
//! a flood the spare slots are let go until no more are kept than are held, or [`SPARE_SLOTS`].

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};

use tokio_util::sync::CancellationToken;

/// How many free slots a thread keeps however few waiters it has, so a thread whose connections
/// come and go a few dozen at a time reuses its slots instead of allocating one per connection.
const SPARE_SLOTS: usize = 64;

/// One thread's stop signal; cloning it is a non-atomic `Rc` increment.
#[derive(Clone)]
pub struct LocalStop(Rc<StopState>);

struct StopState {
    stopped: Cell<bool>,
    slots: RefCell<WakerSlots>,
    /// The task that waits on the shared token; a signal nothing fires has none.
    watcher: Option<tokio::task::AbortHandle>,
}

impl Drop for StopState {
    fn drop(&mut self) {
        // A `LocalStop` dropped before the stop must not leave its task waiting on the shared token.
        if let Some(watcher) = &self.watcher {
            watcher.abort();
        }
    }
}

/// One waiter's waker, shared by the waiter and the state that wakes it.
struct WakerSlot {
    waker: RefCell<Waker>,
}

/// The waker slots of one thread's waiters. A waiter holds its slot; a slot no waiter holds is
/// kept in `free` for the next waiter, holding the no-op waker; the stop wakes every slot still
/// alive through `made`, which holds each weakly so that a slot let go is gone.
///
/// The free list is bounded by its own capacity, which is grown and shrunk only when it is full:
/// a waiter's give-back is then the same `push` it would be on an unbounded list, and the bound
/// costs a connection nothing.
#[derive(Default)]
struct WakerSlots {
    made: Vec<Weak<WakerSlot>>,
    free: Vec<Rc<WakerSlot>>,
    /// The slots alive: held by a waiter or in `free`. Only `take` and `free_full` change it.
    live: usize,
}

impl WakerSlots {
    /// Returns a slot holding `waker` for a new waiter: a free one if there is one.
    fn take(&mut self, waker: Waker) -> Rc<WakerSlot> {
        match self.free.pop() {
            Some(slot) => {
                slot.waker.replace(waker);
                slot
            }
            None => self.make(waker),
        }
    }

    /// Makes a slot, which a thread does only when it has more waiters than ever since its last
    /// cut, so it is off the path a connection takes.
    #[cold]
    fn make(&mut self, waker: Waker) -> Rc<WakerSlot> {
        let made = Rc::new(WakerSlot {
            waker: RefCell::new(waker),
        });
        // A slot let go leaves its entry behind; the dead ones go before the list grows, so it
        // stays within twice the slots alive.
        if self.made.len() == self.made.capacity() {
            self.made.retain(|slot| slot.strong_count() > 0);
        }
        self.made.push(Rc::downgrade(&made));
        self.live += 1;
        made
    }

    /// Takes back a slot a waiter held, which already holds the no-op waker.
    fn give_back(&mut self, slot: Rc<WakerSlot>) {
        if self.free.len() < self.free.capacity() {
            self.free.push(slot);
        } else {
            self.free_full(slot);
        }
    }

    /// Keeps `slot` when the free list is still smaller than `max(held, SPARE_SLOTS)`, and
    /// otherwise lets it go; a free list past twice that is cut back to it.
    ///
    /// The list grows by half its shortfall each time, so it is full, and looked at here again,
    /// before the waiters still held could all have ended: without that, the last give-backs of
    /// a flood would fill a list nobody looks at again. Each cut halves the list, so the work
    /// done here is paid for by the give-backs that filled it.
    #[cold]
    fn free_full(&mut self, slot: Rc<WakerSlot>) {
        let free = self.free.len();
        // `slot` is alive and not free, so `live` counts it beside the free ones.
        let held = self.live - free - 1;
        let keep = held.max(SPARE_SLOTS);
        if free < keep {
            self.free.reserve_exact((keep - free).div_ceil(2));
            self.free.push(slot);
            return;
        }
        drop(slot);
        self.live -= 1;
        if free > 2 * keep {
            self.live -= free - keep;
            self.free.truncate(keep);
            self.free.shrink_to(keep);
            self.made.retain(|slot| slot.strong_count() > 0);
        }
    }

    /// Takes every live slot's waker out, leaving the no-op one; a free slot's is that already.
    fn take_wakers(&self) -> Vec<Waker> {
        self.made
            .iter()
            .filter_map(Weak::upgrade)
            .map(|slot| slot.waker.replace(Waker::noop().clone()))
            .collect()
    }
}

impl LocalStop {
    /// Returns a signal nothing fires, for connections never told to stop, such as the in-memory
    /// bench's, whose server side is dropped with its runtime.
    pub fn never() -> Self {
        LocalStop(Rc::new(StopState {
            stopped: Cell::new(false),
            slots: RefCell::default(),
            watcher: None,
        }))
    }

    /// Returns this thread's signal, fired when `shutdown` is cancelled, and spawns the one task
    /// that waits on the token. The task holds the state weakly, so dropping the last
    /// `LocalStop` and waiter aborts the task and lets go of the token.
    ///
    /// # Panics
    ///
    /// Panics when called outside a `LocalSet`, as `tokio::task::spawn_local` does.
    pub fn follow(shutdown: &CancellationToken) -> Self {
        LocalStop(Rc::new_cyclic(|weak: &Weak<StopState>| {
            let (weak, token) = (weak.clone(), shutdown.clone());
            let watcher = tokio::task::spawn_local(async move {
                token.cancelled().await;
                if let Some(state) = weak.upgrade() {
                    state.fire();
                }
            });
            StopState {
                stopped: Cell::new(shutdown.is_cancelled()),
                slots: RefCell::default(),
                watcher: Some(watcher.abort_handle()),
            }
        }))
    }

    /// Returns a future that completes once the server stops, at once if it already has.
    pub fn stopped(&self) -> StopWaiter {
        StopWaiter {
            inner: Rc::clone(&self.0),
            slot: None,
        }
    }
}

impl StopState {
    fn fire(&self) {
        self.stopped.set(true);
        // The wakers are taken out before any is woken, because a waker may run code that polls
        // or drops a `StopWaiter`.
        let wakers = self.slots.borrow().take_wakers();
        for waker in wakers {
            waker.wake();
        }
    }

    fn slot_for(&self, waker: &Waker) -> Rc<WakerSlot> {
        // The waker is cloned before the slots are borrowed, because a waker's clone may run code
        // that polls another waiter of this stop.
        let waker = waker.clone();
        self.slots.borrow_mut().take(waker)
    }

    fn release(&self, slot: Rc<WakerSlot>) {
        // The waiter's waker is dropped before the slots are borrowed, because dropping it may
        // drop a task that holds another `StopWaiter`, whose drop releases its own slot.
        drop(slot.waker.replace(Waker::noop().clone()));
        self.slots.borrow_mut().give_back(slot);
    }
}

/// The future [`LocalStop::stopped`] returns.
pub struct StopWaiter {
    inner: Rc<StopState>,
    /// Set at the first pending poll; only this waiter writes the waker until the stop takes it.
    slot: Option<Rc<WakerSlot>>,
}

impl Future for StopWaiter {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.inner.stopped.get() {
            // The slot goes back now rather than at the drop, so a ready waiter holds none.
            if let Some(slot) = this.slot.take() {
                this.inner.release(slot);
            }
            return Poll::Ready(());
        }
        match &this.slot {
            Some(slot) => {
                let mut waker = slot.waker.borrow_mut();
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
            }
            None => this.slot = Some(this.inner.slot_for(cx.waker())),
        }
        Poll::Pending
    }
}

impl Drop for StopWaiter {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.inner.release(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns how many slots a waiter holds.
    fn held_count(slots: &WakerSlots) -> usize {
        slots.live - slots.free.len()
    }

    fn local<F: Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new().block_on(&rt, f)
    }

    #[test]
    fn waiting_futures_complete_when_the_token_is_cancelled() {
        local(async {
            let token = CancellationToken::new();
            let stop = LocalStop::follow(&token);
            let waiters: Vec<_> = (0..3)
                .map(|_| tokio::task::spawn_local(stop.stopped()))
                .collect();
            tokio::task::yield_now().await;
            for w in &waiters {
                assert!(!w.is_finished());
            }
            token.cancel();
            for w in waiters {
                tokio::time::timeout(std::time::Duration::from_secs(5), w)
                    .await
                    .expect("woken by the cancel")
                    .unwrap();
            }
            stop.stopped().await;
        });
    }

    #[test]
    fn a_dropped_waiter_frees_its_slot() {
        local(async {
            let stop = LocalStop::never();
            let first = tokio::task::spawn_local(stop.stopped());
            tokio::task::yield_now().await;
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());
            let second = tokio::task::spawn_local(stop.stopped());
            tokio::task::yield_now().await;
            let held = held_count(&stop.0.slots.borrow());
            assert_eq!(held, 1, "only the second waiter holds a slot");
            second.abort();
        });
    }

    #[test]
    fn a_new_waiter_reuses_a_dropped_waiters_slot() {
        local(async {
            let stop = LocalStop::never();
            let (_counter, waker) = counting();
            let mut cx = Context::from_waker(&waker);
            for _ in 0..100 {
                let mut waiter = std::pin::pin!(stop.stopped());
                assert!(waiter.as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(stop.0.slots.borrow().live, 1, "one slot serves them all");
            assert_eq!(held_count(&stop.0.slots.borrow()), 0, "and is free again");
        });
    }

    #[test]
    fn a_ready_waiter_gives_its_slot_back_when_it_completes() {
        local(async {
            let token = CancellationToken::new();
            let stop = LocalStop::follow(&token);
            let waiter = tokio::task::spawn_local(stop.stopped());
            tokio::task::yield_now().await;
            token.cancel();
            waiter.await.unwrap();
            // `stop` is still alive, so the slot was given back by the ready poll, not by a drop.
            assert_eq!(held_count(&stop.0.slots.borrow()), 0);
        });
    }

    #[test]
    fn dropping_the_last_local_stop_lets_go_of_its_task() {
        local(async {
            let token = CancellationToken::new();
            let stop = LocalStop::follow(&token);
            let weak = Rc::downgrade(&stop.0);
            let task = stop.0.watcher.clone().expect("the watcher task");
            assert!(!task.is_finished(), "the watcher waits on the token");
            drop(stop);
            assert!(
                weak.upgrade().is_none(),
                "the task held no strong reference"
            );
            tokio::task::yield_now().await;
            assert!(
                task.is_finished(),
                "dropping the last LocalStop aborts its task"
            );
            token.cancel();
        });
    }

    struct CountingWaker(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for CountingWaker {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn counting() -> (std::sync::Arc<CountingWaker>, Waker) {
        let counter = std::sync::Arc::new(CountingWaker(std::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(std::sync::Arc::clone(&counter));
        (counter, waker)
    }

    fn wakes(counter: &CountingWaker) -> usize {
        counter.0.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[test]
    fn a_registered_waiter_is_woken_by_the_stop() {
        local(async {
            let token = CancellationToken::new();
            let stop = LocalStop::follow(&token);
            let (counter, waker) = counting();
            let mut cx = Context::from_waker(&waker);
            let mut waiter = std::pin::pin!(stop.stopped());
            assert!(waiter.as_mut().poll(&mut cx).is_pending());
            token.cancel();
            tokio::task::yield_now().await;
            assert_eq!(wakes(&counter), 1, "the registered waker was woken once");
            assert!(waiter.as_mut().poll(&mut cx).is_ready());
        });
    }

    #[test]
    fn a_waiter_polled_with_a_new_waker_wakes_the_new_one() {
        local(async {
            let token = CancellationToken::new();
            let stop = LocalStop::follow(&token);
            let (old, old_waker) = counting();
            let (new, new_waker) = counting();
            let mut waiter = std::pin::pin!(stop.stopped());
            assert!(waiter
                .as_mut()
                .poll(&mut Context::from_waker(&old_waker))
                .is_pending());
            assert!(waiter
                .as_mut()
                .poll(&mut Context::from_waker(&new_waker))
                .is_pending());
            token.cancel();
            tokio::task::yield_now().await;
            assert_eq!(wakes(&new), 1, "the waker of the latest poll is woken");
            assert_eq!(wakes(&old), 0, "the replaced waker is not");
        });
    }

    #[test]
    fn a_registered_waiter_completes_after_the_stop() {
        local(async {
            let stop = LocalStop::never();
            let (_counter, waker) = counting();
            let mut cx = Context::from_waker(&waker);
            let mut waiter = std::pin::pin!(stop.stopped());
            assert!(waiter.as_mut().poll(&mut cx).is_pending());
            stop.0.fire();
            assert!(waiter.as_mut().poll(&mut cx).is_ready());
        });
    }

    #[test]
    fn a_never_signal_never_completes() {
        local(async {
            let stop = LocalStop::never();
            let waited =
                tokio::time::timeout(std::time::Duration::from_millis(5), stop.stopped()).await;
            assert!(waited.is_err());
        });
    }

    fn taken(slots: &mut WakerSlots, n: usize) -> Vec<Rc<WakerSlot>> {
        (0..n).map(|_| slots.take(Waker::noop().clone())).collect()
    }

    /// Gives every slot back, checking after each that the free list is within twice what it
    /// may keep.
    fn give_all_back(slots: &mut WakerSlots, held: Vec<Rc<WakerSlot>>) {
        for slot in held {
            slots.give_back(slot);
            let bound = 2 * held_count(slots).max(SPARE_SLOTS);
            assert!(slots.free.len() <= bound, "{} free for {} held", slots.free.len(), held_count(slots));
        }
    }

    #[test]
    fn a_thread_with_no_more_waiters_than_the_spare_reuses_every_slot() {
        let mut slots = WakerSlots::default();
        let held = taken(&mut slots, SPARE_SLOTS);
        give_all_back(&mut slots, held);
        let held = taken(&mut slots, SPARE_SLOTS);
        assert_eq!(slots.live, SPARE_SLOTS, "no slot was made for the second round");
        give_all_back(&mut slots, held);
        assert_eq!(slots.live, SPARE_SLOTS, "and none was let go");
    }

    #[test]
    fn waiters_coming_and_going_beside_many_held_ones_make_no_new_slot() {
        let mut slots = WakerSlots::default();
        let mut held = taken(&mut slots, 4 * SPARE_SLOTS);
        let live = slots.live;
        for _ in 0..10 * SPARE_SLOTS {
            let gone = held.pop().expect("one is held");
            slots.give_back(gone);
            held.push(slots.take(Waker::noop().clone()));
        }
        assert_eq!(slots.live, live);
    }

    /// Each burst past the free list makes slots that are let go again; their entries must not
    /// pile up from one burst to the next.
    #[test]
    fn bursts_past_the_free_list_leave_no_growing_trail() {
        let mut slots = WakerSlots::default();
        let steady = taken(&mut slots, SPARE_SLOTS);
        for _ in 0..1_000 {
            let burst = taken(&mut slots, 3 * SPARE_SLOTS);
            give_all_back(&mut slots, burst);
            assert!(slots.made.len() <= 2 * slots.live.max(SPARE_SLOTS) + 4 * SPARE_SLOTS, "{} entries for {} slots", slots.made.len(), slots.live);
        }
        give_all_back(&mut slots, steady);
    }

    #[test]
    fn after_a_flood_a_thread_keeps_at_most_twice_the_spare() {
        for flood in [SPARE_SLOTS, SPARE_SLOTS + 1, 2 * SPARE_SLOTS, 2 * SPARE_SLOTS + 1, 10 * SPARE_SLOTS, 100 * SPARE_SLOTS] {
            let mut slots = WakerSlots::default();
            let held = taken(&mut slots, flood);
            give_all_back(&mut slots, held);
            assert!(slots.live <= 2 * SPARE_SLOTS, "a flood of {flood} left {} slots", slots.live);
            assert!(slots.made.len() <= 2 * SPARE_SLOTS + flood.min(2 * SPARE_SLOTS), "{} entries", slots.made.len());
        }
    }

    #[test]
    fn a_flood_of_waiters_leaves_at_most_twice_the_spare_slots_behind() {
        local(async {
            let stop = LocalStop::never();
            let mut cx = Context::from_waker(Waker::noop());
            let mut waiters: Vec<_> = (0..10 * SPARE_SLOTS).map(|_| Box::pin(stop.stopped())).collect();
            for waiter in &mut waiters {
                assert!(waiter.as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(stop.0.slots.borrow().live, 10 * SPARE_SLOTS);
            drop(waiters);
            assert!(stop.0.slots.borrow().live <= 2 * SPARE_SLOTS);
        });
    }

    #[test]
    fn the_stop_wakes_every_held_waiter_and_no_freed_one() {
        local(async {
            let token = CancellationToken::new();
            let stop = LocalStop::follow(&token);
            let counters: Vec<_> = (0..3).map(|_| counting()).collect();
            let mut waiters: Vec<_> = counters
                .iter()
                .map(|(_, waker)| {
                    let mut waiter = Box::pin(stop.stopped());
                    assert!(waiter.as_mut().poll(&mut Context::from_waker(waker)).is_pending());
                    waiter
                })
                .collect();
            drop(waiters.remove(1));
            token.cancel();
            tokio::task::yield_now().await;
            let woken: Vec<usize> = counters.iter().map(|(counter, _)| wakes(counter)).collect();
            assert_eq!(woken, [1, 0, 1]);
        });
    }

    #[test]
    fn a_stop_followed_after_the_cancel_is_stopped_at_the_first_poll() {
        local(async {
            let token = CancellationToken::new();
            token.cancel();
            let stop = LocalStop::follow(&token);
            let mut waiter = std::pin::pin!(stop.stopped());
            assert!(waiter.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        });
    }

    #[test]
    fn a_stop_followed_before_the_cancel_is_not_stopped_until_it() {
        local(async {
            let token = CancellationToken::new();
            let stop = LocalStop::follow(&token);
            let mut waiter = std::pin::pin!(stop.stopped());
            assert!(waiter.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        });
    }

    thread_local! {
        static ON_CLONE: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    /// Returns a waker whose first clone runs what `ON_CLONE` holds.
    fn reentrant_waker() -> Waker {
        fn clone(_: *const ()) -> std::task::RawWaker {
            if let Some(run) = ON_CLONE.with(|on| on.borrow_mut().take()) {
                run();
            }
            std::task::RawWaker::new(std::ptr::null(), &VTABLE)
        }
        fn noop(_: *const ()) {}
        static VTABLE: std::task::RawWakerVTable = std::task::RawWakerVTable::new(clone, noop, noop, noop);
        // SAFETY: every function of the vtable ignores the data pointer, so a null one is valid.
        unsafe { Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    /// Waking code may poll; a waker whose clone polls another waiter of the same stop must find
    /// the slots unborrowed, whether the new slot is a free one or a new one.
    #[test]
    fn a_waker_whose_clone_polls_another_waiter_is_registered() {
        for reuse in [true, false] {
            let stop = LocalStop::never();
            if reuse {
                let mut first = std::pin::pin!(stop.stopped());
                assert!(first.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            }
            let other = stop.clone();
            ON_CLONE.with(|on| {
                *on.borrow_mut() = Some(Box::new(move || {
                    let mut waiter = Box::pin(other.stopped());
                    assert!(waiter.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
                }));
            });
            let waker = reentrant_waker();
            let mut waiter = std::pin::pin!(stop.stopped());
            assert!(waiter.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        }
    }
}
