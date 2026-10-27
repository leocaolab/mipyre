//! Hands requests from any thread to one lane host, at most a fixed number at a time.
//!
//! A full inbox hands the item back at once instead of growing, so the sender decides what the
//! request gets. Closing refuses every later send and drops what is left; a send racing the close
//! drops its item as well, so nothing is left in a closed inbox for a host that no longer takes.

use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::task::Poll;

use crossbeam_queue::ArrayQueue;
use futures_util::task::AtomicWaker;

use crate::sync::{fence, AtomicBool, Ordering};

pub(super) struct Inbox<T> {
    /// Is allocated for its capacity when the inbox is made and never grows.
    items: ArrayQueue<T>,
    closed: AtomicBool,
    taker: AtomicWaker,
}

/// Why [`Inbox::send`] gave its item back.
#[derive(Debug)]
pub(super) enum InboxRefusal<T> {
    Full(T),
    Closed(T),
}

impl<T> Inbox<T> {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Inbox {
            items: ArrayQueue::new(capacity.get()),
            closed: AtomicBool::new(false),
            taker: AtomicWaker::new(),
        }
    }

    /// Puts `item` in and wakes the task waiting in [`Inbox::next`]. Never waits. An item that
    /// meets a close on its way in is dropped, as if the close had come just after it.
    pub(super) fn send(&self, item: T) -> Result<(), InboxRefusal<T>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(InboxRefusal::Closed(item));
        }
        self.items.push(item).map_err(InboxRefusal::Full)?;
        // Pairs with the fence in `close`: either this load sees the close, or the close's drain
        // sees this item.
        fence(Ordering::SeqCst);
        if self.closed.load(Ordering::Relaxed) {
            self.drain();
        } else {
            self.taker.wake();
        }
        Ok(())
    }

    pub(super) fn take(&self) -> Option<T> {
        self.items.pop()
    }

    /// Waits for the next item. Only one task waits at a time: a second one's waker replaces the first's.
    pub(super) async fn next(&self) -> T {
        poll_fn(|cx| {
            if let Some(item) = self.items.pop() {
                return Poll::Ready(item);
            }
            self.taker.register(cx.waker());
            self.items.pop().map_or(Poll::Pending, Poll::Ready)
        })
        .await
    }

    /// Refuses every later send and drops what is left.
    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        self.drain();
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    fn drain(&self) {
        while self.items.pop().is_some() {}
    }
}

#[cfg(test)]
#[cfg(all(sync_model, not(sync_shuttle)))]
#[path = "inbox/loom_model.rs"]
mod loom_model;

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use std::future::Future as _;

    fn inbox(capacity: usize) -> Inbox<u32> {
        Inbox::new(NonZeroUsize::new(capacity).expect("a test inbox holds at least one"))
    }

    #[test]
    fn an_inbox_takes_up_to_its_capacity_and_hands_the_next_back_as_full() {
        let inbox = inbox(2);
        assert!(inbox.send(1).is_ok());
        assert!(inbox.send(2).is_ok());
        assert!(matches!(inbox.send(3), Err(InboxRefusal::Full(3))));
        assert_eq!(inbox.take(), Some(1));
        assert!(inbox.send(3).is_ok(), "a taken item makes room for one");
        assert_eq!(inbox.take(), Some(2));
        assert_eq!(inbox.take(), Some(3));
        assert_eq!(inbox.take(), None);
    }

    #[test]
    fn a_closed_inbox_drops_what_is_left_and_hands_every_later_item_back() {
        let dropped = std::sync::Arc::new(());
        let inbox = Inbox::new(NonZeroUsize::MIN);
        assert!(inbox.send(std::sync::Arc::clone(&dropped)).is_ok());
        inbox.close();
        assert_eq!(
            std::sync::Arc::strong_count(&dropped),
            1,
            "the item left in it was dropped"
        );
        assert!(inbox.is_closed());
        assert!(matches!(
            inbox.send(std::sync::Arc::clone(&dropped)),
            Err(InboxRefusal::Closed(_))
        ));
        assert!(inbox.take().is_none());
    }

    struct WakeFlag(AtomicBool);

    impl futures_util::task::ArcWake for WakeFlag {
        fn wake_by_ref(flag: &std::sync::Arc<Self>) {
            flag.0.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_waiting_taker_is_woken_by_a_send_and_gets_the_item() {
        let inbox = inbox(1);
        let flag = std::sync::Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = futures_util::task::waker(std::sync::Arc::clone(&flag));
        let mut cx = std::task::Context::from_waker(&waker);
        let mut next = std::pin::pin!(inbox.next());
        assert!(next.as_mut().poll(&mut cx).is_pending());
        assert!(!flag.0.load(Ordering::Relaxed));
        assert!(inbox.send(7).is_ok());
        assert!(flag.0.load(Ordering::Relaxed), "the send woke the waiting taker");
        assert_eq!(next.as_mut().poll(&mut cx), Poll::Ready(7));
    }
}
