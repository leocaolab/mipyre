//! Provides the bounded many-to-many queue that hands requests to the threads running Python (the
//! main interpreter's threads and the pool's workers), where a receiver with nothing to take sleeps
//! at once. A sender may also add a receiver when none sleeps, and an idle receiver past the fewest
//! kept may retire, so the threads behind it can grow and shrink with the load.
//!
//! It replaces crossbeam's channel, whose blocking receive spins and then calls `sched_yield`
//! several times before it sleeps: a worker that has just answered finds the queue empty, so every
//! request paid for that tail, and how much of it followed timing.
//!
//! No wake-up is lost because every change of the queue and every decision to sleep happens under
//! `JobQueue::state`. A receiver counts itself in `sleepers` before `Condvar::wait` releases the
//! lock, so a sender that queues a job or drops the last sender sees under the same lock every
//! receiver that decided to sleep before it, and one that decides after it finds the job or the
//! close. The wake is sent after the lock is released; the
//! sleeper is already waiting, so it is woken either way.

use std::collections::VecDeque;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use crate::sync::poisoning::{Condvar, Mutex, MutexGuard};

struct JobQueue<T> {
    state: Mutex<QueueState<T>>,
    ready: Condvar,
}

struct QueueState<T> {
    /// Is allocated for `capacity` at the start and never grows, so nothing allocates under the
    /// lock.
    jobs: VecDeque<T>,
    capacity: usize,
    senders: usize,
    receivers: usize,
    /// Counts receivers asleep in `ready`, and woken ones not yet back under the lock.
    sleepers: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum QueueFront<T> {
    Job(T),
    /// The receiver sleeps until a job is queued or the last sender goes.
    Empty,
    /// Nothing more will come, because every sender is gone and the queue is drained.
    Closed,
}

/// What a sender does once its job is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushEffect {
    WakeSleeper,
    /// No receiver sleeps and fewer than the most allowed exist, so one more is counted and started.
    AddReceiver,
    Nothing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobSendError {
    QueueFull,
    NoReceiverLeft,
}

/// Is one sending end; once every clone is dropped, receivers drain the queue and then get `None`.
pub(crate) struct JobSender<T>(Arc<JobQueue<T>>);

/// Is one receiving end, counted in `receivers` while it exists.
pub(crate) struct JobReceiver<T> {
    queue: Arc<JobQueue<T>>,
    /// Is set once the receiver retired, which counted it out under the lock, so its drop does not
    /// count it out again.
    retired: bool,
}

/// Returns the first sender and receiver of a queue holding up to `capacity` jobs; a capacity of 0
/// admits none.
pub(crate) fn job_queue<T>(capacity: usize) -> (JobSender<T>, JobReceiver<T>) {
    let queue = Arc::new(JobQueue {
        state: Mutex::new(QueueState {
            jobs: VecDeque::with_capacity(capacity),
            capacity,
            senders: 1,
            receivers: 1,
            sleepers: 0,
        }),
        ready: Condvar::new(),
    });
    (JobSender(Arc::clone(&queue)), JobReceiver::counted(queue))
}

/// Returns what a sender does after queueing a job: wake a sleeping receiver, or else add one while
/// fewer than `max` exist.
fn push_effect(sleepers: usize, receivers: usize, max: usize) -> PushEffect {
    if sleepers > 0 {
        PushEffect::WakeSleeper
    } else if receivers < max {
        PushEffect::AddReceiver
    } else {
        PushEffect::Nothing
    }
}

/// Returns whether an idle receiver is one more than the `min` the queue keeps.
fn is_surplus(receivers: usize, min: usize) -> bool {
    receivers > min
}

impl<T> QueueState<T> {
    fn admit(&self) -> Result<(), JobSendError> {
        if self.receivers == 0 {
            Err(JobSendError::NoReceiverLeft)
        } else if self.jobs.len() >= self.capacity {
            Err(JobSendError::QueueFull)
        } else {
            Ok(())
        }
    }

    /// Takes the next job or says why there is none.
    fn front(&mut self) -> QueueFront<T> {
        match self.jobs.pop_front() {
            Some(job) => QueueFront::Job(job),
            None if self.senders == 0 => QueueFront::Closed,
            None => QueueFront::Empty,
        }
    }
}

impl<T> JobQueue<T> {
    fn lock(&self) -> MutexGuard<'_, QueueState<T>> {
        // No section under this lock can panic (counter steps and queue operations within the
        // capacity allocated at the start; a job is dropped only after the lock is released), so a
        // poisoned lock still guards a consistent state.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn recv(&self) -> Option<T> {
        let mut state = self.lock();
        loop {
            match state.front() {
                QueueFront::Job(job) => return Some(job),
                QueueFront::Closed => return None,
                QueueFront::Empty => {
                    state.sleepers += 1;
                    state = self.ready.wait(state).unwrap_or_else(PoisonError::into_inner);
                    state.sleepers -= 1;
                }
            }
        }
    }

    /// Takes the next job, or returns `None` once the queue is closed and drained, or once the
    /// receiver waited `keepalive` with nothing to take while it was one more than `min`; it is then
    /// counted out under the same lock, so two receivers timing out together never leave fewer than
    /// `min`.
    fn recv_or_retire(&self, min: usize, keepalive: Duration) -> Taken<T> {
        let mut state = self.lock();
        loop {
            match state.front() {
                QueueFront::Job(job) => return Taken::Job(job),
                QueueFront::Closed => return Taken::Closed,
                QueueFront::Empty if !is_surplus(state.receivers, min) => {
                    state.sleepers += 1;
                    state = self.ready.wait(state).unwrap_or_else(PoisonError::into_inner);
                    state.sleepers -= 1;
                }
                QueueFront::Empty => {
                    state.sleepers += 1;
                    let (woken, waited) = self
                        .ready
                        .wait_timeout(state, keepalive)
                        .unwrap_or_else(PoisonError::into_inner);
                    state = woken;
                    state.sleepers -= 1;
                    // A wake sent as the wait timed out may have been this receiver's, so the queue
                    // is looked at again before it retires.
                    if waited.timed_out() && state.jobs.is_empty() && is_surplus(state.receivers, min) {
                        state.receivers -= 1;
                        return Taken::Retired;
                    }
                }
            }
        }
    }
}

/// What a receiver that may retire gets from the queue.
enum Taken<T> {
    Job(T),
    Closed,
    Retired,
}

impl<T> JobSender<T> {
    /// Queues `job` without waiting; a refused job is dropped here, after the lock is released.
    pub(crate) fn try_send(&self, job: T) -> Result<(), JobSendError> {
        self.try_send_or_grow(job, 0, drop)
    }

    /// Queues `job` as [`try_send`](Self::try_send) does and, when no receiver sleeps and fewer than
    /// `max` exist, counts one more and hands it to `start` after the lock is released. Dropping it
    /// counts it out again, which is what a receiver `start` could not give a thread comes to.
    #[inline(always)]
    pub(crate) fn try_send_or_grow(
        &self,
        job: T,
        max: usize,
        start: impl FnOnce(JobReceiver<T>),
    ) -> Result<(), JobSendError> {
        let mut state = self.0.lock();
        if let Err(refused) = state.admit() {
            drop(state);
            drop(job);
            return Err(refused);
        }
        state.jobs.push_back(job);
        match push_effect(state.sleepers, state.receivers, max) {
            PushEffect::WakeSleeper => {
                drop(state);
                self.0.ready.notify_one();
            }
            PushEffect::AddReceiver => {
                state.receivers += 1;
                drop(state);
                start(JobReceiver::counted(Arc::clone(&self.0)));
            }
            PushEffect::Nothing => {}
        }
        Ok(())
    }

    /// Returns how many jobs the queue holds at most.
    pub(crate) fn capacity(&self) -> usize {
        self.0.lock().capacity
    }
}

impl<T> JobReceiver<T> {
    fn counted(queue: Arc<JobQueue<T>>) -> Self {
        JobReceiver { queue, retired: false }
    }

    /// Returns the next job, sleeping while there is none, or `None` once every sender is gone and
    /// the queue is drained.
    pub(crate) fn recv(&self) -> Option<T> {
        self.queue.recv()
    }

    /// Works as [`recv`](Self::recv), and also returns `None` once this receiver waited
    /// `keepalive` with nothing to take while more than `min` receivers existed; the receiver is
    /// then already counted out.
    pub(crate) fn recv_or_retire(mut self, min: usize, keepalive: Duration) -> Option<(T, Self)> {
        match self.queue.recv_or_retire(min, keepalive) {
            Taken::Job(job) => Some((job, self)),
            Taken::Closed => None,
            Taken::Retired => {
                self.retired = true;
                None
            }
        }
    }

    pub(crate) fn try_recv(&self) -> Option<T> {
        self.queue.lock().jobs.pop_front()
    }
}

impl<T> Clone for JobSender<T> {
    fn clone(&self) -> Self {
        self.0.lock().senders += 1;
        JobSender(Arc::clone(&self.0))
    }
}

impl<T> Clone for JobReceiver<T> {
    fn clone(&self) -> Self {
        self.queue.lock().receivers += 1;
        JobReceiver::counted(Arc::clone(&self.queue))
    }
}

impl<T> Drop for JobSender<T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.senders -= 1;
        let wake = state.senders == 0 && state.sleepers > 0;
        drop(state);
        if wake {
            self.0.ready.notify_all();
        }
    }
}

impl<T> Drop for JobReceiver<T> {
    fn drop(&mut self) {
        if !self.retired {
            self.queue.lock().receivers -= 1;
        }
    }
}

#[cfg(test)]
#[cfg(all(sync_model, not(sync_shuttle)))]
mod loom_model;

#[cfg(test)]
#[cfg(all(sync_model, sync_shuttle))]
mod shuttle_model;

#[cfg(test)]
#[cfg(not(sync_model))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn state(len: usize, capacity: usize, senders: usize, receivers: usize) -> QueueState<usize> {
        QueueState {
            jobs: (0..len).collect(),
            capacity,
            senders,
            receivers,
            sleepers: 0,
        }
    }

    #[test]
    fn a_queue_one_short_of_capacity_admits_and_a_full_one_does_not() {
        assert_eq!(state(3, 4, 1, 1).admit(), Ok(()));
        assert_eq!(state(4, 4, 1, 1).admit(), Err(JobSendError::QueueFull));
        assert_eq!(state(0, 0, 1, 1).admit(), Err(JobSendError::QueueFull));
    }

    #[test]
    fn a_queue_with_one_receiver_admits_and_with_none_refuses_before_it_is_full() {
        assert_eq!(state(0, 4, 1, 1).admit(), Ok(()));
        assert_eq!(state(0, 4, 1, 0).admit(), Err(JobSendError::NoReceiverLeft));
        assert_eq!(state(4, 4, 1, 0).admit(), Err(JobSendError::NoReceiverLeft));
    }

    #[test]
    fn an_empty_queue_with_a_sender_left_sleeps_and_with_none_is_closed() {
        assert_eq!(state(0, 4, 1, 1).front(), QueueFront::Empty);
        assert_eq!(state(0, 4, 0, 1).front(), QueueFront::Closed);
    }

    #[test]
    fn jobs_are_taken_in_order_after_the_last_sender_is_gone_too() {
        let mut s = state(2, 4, 0, 1);
        assert_eq!(s.front(), QueueFront::Job(0));
        assert_eq!(s.front(), QueueFront::Job(1));
        assert_eq!(s.front(), QueueFront::Closed);
    }

    #[test]
    fn a_push_wakes_a_sleeper_before_it_adds_a_receiver() {
        assert_eq!(push_effect(1, 4, 8), PushEffect::WakeSleeper);
        assert_eq!(push_effect(1, 8, 8), PushEffect::WakeSleeper);
    }

    #[test]
    fn a_push_with_no_sleeper_adds_a_receiver_one_short_of_the_most_and_not_at_it() {
        assert_eq!(push_effect(0, 7, 8), PushEffect::AddReceiver);
        assert_eq!(push_effect(0, 8, 8), PushEffect::Nothing);
        assert_eq!(push_effect(0, 0, 0), PushEffect::Nothing);
    }

    #[test]
    fn an_idle_receiver_one_past_the_fewest_kept_is_surplus_and_at_it_is_not() {
        assert!(is_surplus(5, 4));
        assert!(!is_surplus(4, 4));
    }

    #[test]
    fn a_push_with_no_sleeper_and_room_adds_a_receiver_that_takes_it() {
        let (tx, rx) = job_queue::<u32>(4);
        let mut added = None;
        assert_eq!(tx.try_send_or_grow(7, 2, |receiver| added = Some(receiver)), Ok(()));
        let added = added.expect("one receiver and none asleep, with room for two");
        assert_eq!(added.try_recv(), Some(7));
        assert_eq!(
            tx.try_send_or_grow(8, 2, |_| panic!("two receivers is the most")),
            Ok(())
        );
        assert_eq!(rx.try_recv(), Some(8));
    }

    #[test]
    fn a_receiver_a_thread_could_not_take_is_counted_out() {
        let (tx, rx) = job_queue::<u32>(4);
        assert_eq!(tx.try_send_or_grow(1, 2, drop), Ok(()));
        let mut added = false;
        assert_eq!(tx.try_send_or_grow(2, 2, |_| added = true), Ok(()));
        assert!(added, "the dropped receiver left room for another");
        drop(rx);
    }

    /// Four receivers past the two kept time out together; the count goes down under one lock, so
    /// exactly two retire and the other two stay for the next job.
    #[test]
    fn surplus_receivers_retire_down_to_the_fewest_kept_and_no_further() {
        const MIN: usize = 2;
        let (tx, rx) = job_queue::<u32>(4);
        let takers: Vec<_> = (0..4)
            .map(|_| {
                let rx = rx.clone();
                std::thread::spawn(move || {
                    let mut taken = 0;
                    let mut receiver = rx;
                    while let Some((_, next)) = receiver.recv_or_retire(MIN, std::time::Duration::from_millis(20)) {
                        taken += 1;
                        receiver = next;
                    }
                    taken
                })
            })
            .collect();
        drop(rx);
        while tx.0.lock().receivers > MIN {
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(tx.0.lock().receivers, MIN, "a receiver retired past the fewest kept");
        assert_eq!(tx.try_send(9), Ok(()));
        drop(tx);
        let taken: usize = takers
            .into_iter()
            .map(|t| t.join().expect("a taker does not panic"))
            .sum();
        assert_eq!(taken, 1);
    }

    #[test]
    fn try_send_refuses_past_capacity_and_once_every_receiver_is_gone() {
        let (tx, rx) = job_queue::<u32>(1);
        assert_eq!(tx.try_send(1), Ok(()));
        assert_eq!(tx.try_send(2), Err(JobSendError::QueueFull));
        assert_eq!(rx.try_recv(), Some(1));
        drop(rx);
        assert_eq!(tx.try_send(3), Err(JobSendError::NoReceiverLeft));
    }

    #[test]
    fn a_sleeping_receiver_wakes_for_a_job_and_for_the_last_sender_going() {
        let (tx, rx) = job_queue::<u32>(1);
        let taker = std::thread::spawn(move || (rx.recv(), rx.recv()));
        // Whether the taker sleeps before the job comes or finds it queued, it gets it.
        tx.try_send(7).expect("the receiver is alive and the queue empty");
        drop(tx);
        assert_eq!(taker.join().expect("the taker does not panic"), (Some(7), None));
    }

    /// Many senders and receivers on a small queue: every job queued is taken exactly once and
    /// every receiver ends once the senders are gone, whichever way the sleeps and wakes fall. A
    /// lost wake-up hangs it. `JOB_QUEUE_STRESS_N` sets the jobs per sender (to run it long).
    #[test]
    fn every_queued_job_is_taken_once_and_every_receiver_ends() {
        const SENDERS: usize = 4;
        const RECEIVERS: usize = 4;
        let per_sender: usize = std::env::var("JOB_QUEUE_STRESS_N")
            .ok()
            .and_then(|n| n.parse().ok())
            .unwrap_or(2_000);
        let (tx, rx) = job_queue::<usize>(2);
        let queued = Arc::new(AtomicUsize::new(0));
        let senders: Vec<_> = (0..SENDERS)
            .map(|s| {
                let (tx, queued) = (tx.clone(), Arc::clone(&queued));
                std::thread::spawn(move || {
                    let mut sum = 0;
                    for i in 0..per_sender {
                        let job = s * per_sender + i;
                        // A full queue is retried: the sender side of the test, not of the queue.
                        while tx.try_send(job) == Err(JobSendError::QueueFull) {
                            std::thread::yield_now();
                        }
                        sum += job;
                    }
                    queued.fetch_add(per_sender, Ordering::SeqCst);
                    sum
                })
            })
            .collect();
        drop(tx);
        let receivers: Vec<_> = (0..RECEIVERS)
            .map(|_| {
                let rx = rx.clone();
                std::thread::spawn(move || {
                    let (mut count, mut sum) = (0, 0);
                    while let Some(job) = rx.recv() {
                        count += 1;
                        sum += job;
                    }
                    (count, sum)
                })
            })
            .collect();
        drop(rx);
        let sent: usize = senders
            .into_iter()
            .map(|t| t.join().expect("a sender does not panic"))
            .sum();
        let (mut count, mut sum) = (0, 0);
        for t in receivers {
            let (c, s) = t.join().expect("a receiver does not panic");
            count += c;
            sum += s;
        }
        assert_eq!(count, SENDERS * per_sender);
        assert_eq!(queued.load(Ordering::SeqCst), count);
        assert_eq!(sum, sent);
    }
}
