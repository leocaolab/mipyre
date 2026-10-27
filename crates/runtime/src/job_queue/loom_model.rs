//! The Loom model of the job queue's no-lost-wake-up protocol: senders, receivers and the last
//! sender's drop, over every interleaving, running the shipping queue (its lock and condition
//! variable are `crate::sync`'s, Loom's here). A receiver left
//! asleep with nothing coming to wake it is a deadlock, which Loom reports, so every model keeps
//! its senders alive until the receivers return unless the close is what it proves. Run with
//! `RUSTFLAGS="--cfg sync_model" cargo test -p mipyre_pyext --lib --profile model --target-dir target/loom loom_model`.
//!
//! It proves: a job sent while a receiver sleeps or is about to is taken; two jobs sent to two
//! receivers that sleep are taken one each, also when the second comes before the first woken
//! receiver is back under the lock; the last sender going wakes every sleeping receiver to `None`,
//! after the jobs queued before it are taken; two senders racing for the last free place get exactly one in and the other is told
//! `QueueFull` at once.

use loom::thread;

use super::{job_queue, JobSendError};

#[test]
fn a_job_sent_while_the_receiver_sleeps_or_not_is_taken() {
    loom::model(|| {
        let (tx, rx) = job_queue::<u32>(1);
        let taker = thread::spawn(move || rx.recv());

        assert_eq!(tx.try_send(7), Ok(()));

        assert_eq!(taker.join().unwrap(), Some(7));
        drop(tx);
    });
}

#[test]
fn two_jobs_sent_to_two_sleeping_receivers_are_taken_one_each() {
    loom::model(|| {
        let (tx, rx) = job_queue::<u32>(2);
        let first = thread::spawn({
            let rx = rx.clone();
            move || rx.recv()
        });
        let second = thread::spawn(move || rx.recv());

        assert_eq!(tx.try_send(1), Ok(()));
        assert_eq!(tx.try_send(2), Ok(()));

        let mut taken = [first.join().unwrap(), second.join().unwrap()];
        taken.sort_unstable();
        assert_eq!(taken, [Some(1), Some(2)]);
        drop(tx);
    });
}

#[test]
fn jobs_from_two_senders_reach_one_receiver() {
    loom::model(|| {
        let (tx, rx) = job_queue::<u32>(2);
        let other = thread::spawn({
            let tx = tx.clone();
            move || {
                assert_eq!(tx.try_send(1), Ok(()));
                tx
            }
        });

        assert_eq!(tx.try_send(2), Ok(()));
        let mut taken = [rx.recv(), rx.recv()];

        taken.sort_unstable();
        assert_eq!(taken, [Some(1), Some(2)]);
        drop(other.join().unwrap());
        drop(tx);
    });
}

#[test]
fn the_last_sender_going_wakes_every_sleeping_receiver_to_none() {
    loom::model(|| {
        let (tx, rx) = job_queue::<u32>(1);
        let first = thread::spawn({
            let rx = rx.clone();
            move || rx.recv()
        });
        let second = thread::spawn(move || rx.recv());
        let other_sender = thread::spawn({
            let tx = tx.clone();
            move || drop(tx)
        });

        drop(tx);

        other_sender.join().unwrap();
        assert_eq!(first.join().unwrap(), None);
        assert_eq!(second.join().unwrap(), None);
    });
}

#[test]
fn a_job_queued_before_the_last_sender_goes_is_taken_before_none() {
    loom::model(|| {
        let (tx, rx) = job_queue::<u32>(1);
        let sender = thread::spawn(move || {
            assert_eq!(tx.try_send(7), Ok(()));
        });

        let taken = (rx.recv(), rx.recv());

        assert_eq!(taken, (Some(7), None));
        sender.join().unwrap();
    });
}

#[test]
fn of_two_senders_racing_for_the_last_place_one_gets_in_and_the_other_is_told_full() {
    loom::model(|| {
        let (tx, rx) = job_queue::<u32>(1);
        let other = thread::spawn({
            let tx = tx.clone();
            move || tx.try_send(1)
        });

        let mine = tx.try_send(2);
        let theirs = other.join().unwrap();

        let admitted = match (mine, theirs) {
            (Ok(()), Err(JobSendError::QueueFull)) => 2,
            (Err(JobSendError::QueueFull), Ok(())) => 1,
            outcome => panic!("one of two sends to one free place gets in: {outcome:?}"),
        };
        assert_eq!(rx.try_recv(), Some(admitted));
        assert_eq!(rx.try_recv(), None);
    });
}
