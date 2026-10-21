//! Provides the bounded many-to-many queue that hands requests to the threads running Python (the
//! main-interpreter bridge and the pool's workers), where a receiver with nothing to take sleeps at
//! once.
//!
//! It replaces crossbeam's channel, whose blocking receive spins and then calls `sched_yield`
//! several times before it sleeps: a worker that has just answered finds the queue empty, so every
//! request paid for that tail, and how much of it followed timing.
//!
//! No wake-up is lost because every change of the queue and every decision to sleep happens under
//! `JobQueue::state`. A receiver counts itself in `sleepers` before `Condvar::wait` releases the
//! lock, so a sender that queues a job, drops the last sender or calls [`JobReceiver::wake_all`]
//! sees under the same lock every receiver that decided to sleep before it, and one that decides
//! after it finds the job, the close or the stop. The wake is sent after the lock is released; the
//! sleeper is already waiting, so it is woken either way.

use std::collections::VecDeque;
use std::sync::{Arc, PoisonError};

use crate::sync::poisoning::{Condvar, Mutex, MutexGuard};
use crate::sync::{AtomicBool, Ordering};

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
    /// Nothing more will come, because every sender is gone and the queue is drained, or the
    /// receiver's stop is set.
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobSendError {
    QueueFull,
    NoReceiverLeft,
}

/// Is one sending end; once every clone is dropped, receivers drain the queue and then get `None`.
pub(crate) struct JobSender<T>(Arc<JobQueue<T>>);

pub(crate) struct JobReceiver<T>(Arc<JobQueue<T>>);

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
    (JobSender(Arc::clone(&queue)), JobReceiver(queue))
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

    /// Takes the next job or says why there is none; a set `stopped` ends the receiver even with
    /// jobs queued.
    fn front(&mut self, stopped: bool) -> QueueFront<T> {
        if stopped {
            return QueueFront::Closed;
        }
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

    fn recv(&self, stop: Option<&AtomicBool>) -> Option<T> {
        let mut state = self.lock();
        loop {
            let stopped = stop.is_some_and(|s| s.load(Ordering::SeqCst));
            match state.front(stopped) {
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
}

impl<T> JobSender<T> {
    /// Queues `job` without waiting; a refused job is dropped here, after the lock is released.
    pub(crate) fn try_send(&self, job: T) -> Result<(), JobSendError> {
        let mut state = self.0.lock();
        if let Err(refused) = state.admit() {
            drop(state);
            drop(job);
            return Err(refused);
        }
        state.jobs.push_back(job);
        let wake = state.sleepers > 0;
        drop(state);
        if wake {
            self.0.ready.notify_one();
        }
        Ok(())
    }
}

impl<T> JobReceiver<T> {
    /// Returns the next job, sleeping while there is none, or `None` once every sender is gone and
    /// the queue is drained.
    pub(crate) fn recv(&self) -> Option<T> {
        self.0.recv(None)
    }

    /// Works as [`recv`](Self::recv) but returns `None` once `stop` is set; whoever sets it must
    /// call [`wake_all`](Self::wake_all) afterwards so sleepers see it.
    pub(crate) fn recv_unless(&self, stop: &AtomicBool) -> Option<T> {
        self.0.recv(Some(stop))
    }

    pub(crate) fn try_recv(&self) -> Option<T> {
        self.0.lock().jobs.pop_front()
    }

    /// Wakes every sleeping receiver so it looks at its stop again.
    pub(crate) fn wake_all(&self) {
        let wake = self.0.lock().sleepers > 0;
        if wake {
            self.0.ready.notify_all();
        }
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
        self.0.lock().receivers += 1;
        JobReceiver(Arc::clone(&self.0))
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
        self.0.lock().receivers -= 1;
    }
}

#[cfg(test)]
#[cfg(sync_model)]
mod loom_model;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

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
        assert_eq!(state(0, 4, 1, 1).front(false), QueueFront::Empty);
        assert_eq!(state(0, 4, 0, 1).front(false), QueueFront::Closed);
    }

    #[test]
    fn jobs_are_taken_in_order_after_the_last_sender_is_gone_too() {
        let mut s = state(2, 4, 0, 1);
        assert_eq!(s.front(false), QueueFront::Job(0));
        assert_eq!(s.front(false), QueueFront::Job(1));
        assert_eq!(s.front(false), QueueFront::Closed);
    }

    #[test]
    fn a_stopped_receiver_is_closed_with_a_job_queued() {
        assert_eq!(state(1, 4, 1, 1).front(false), QueueFront::Job(0));
        assert_eq!(state(1, 4, 1, 1).front(true), QueueFront::Closed);
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

    #[test]
    fn a_sleeping_receiver_wakes_for_its_stop() {
        let (tx, rx) = job_queue::<u32>(1);
        let stop = Arc::new(AtomicBool::new(false));
        let (for_taker, waker) = (Arc::clone(&stop), rx.clone());
        let taker = std::thread::spawn(move || rx.recv_unless(&for_taker));
        stop.store(true, Ordering::SeqCst);
        waker.wake_all();
        assert_eq!(taker.join().expect("the taker does not panic"), None);
        drop(tx);
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
        let sent: usize = senders.into_iter().map(|t| t.join().expect("a sender does not panic")).sum();
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
