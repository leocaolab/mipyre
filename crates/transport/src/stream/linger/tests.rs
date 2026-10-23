//! Tests the lingering close over an in-memory pipe on a paused clock.

use super::*;

use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;

const TICK: Duration = Duration::from_millis(1);
const LINGER_IDLE: Duration = Duration::from_secs(2);
const LINGER_LIMIT: Duration = Duration::from_secs(10);
const BOUNDS: LingerBounds = LingerBounds {
    idle: LINGER_IDLE,
    limit: LINGER_LIMIT,
};

/// Serializes the tests that read the process-wide [`StreamEvent::LingerCutShort`] count, so one
/// that asserts it did not move is not moved by another.
static CUT_SHORT_COUNT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn deadline(started: Instant, last_bytes: Instant) -> Instant {
    BOUNDS.deadline(started, last_bytes)
}

#[test]
fn the_wait_ends_the_idle_gap_after_the_last_bytes_while_that_is_within_the_limit() {
    let started = Instant::now();
    let last = started + LINGER_LIMIT - LINGER_IDLE;
    assert_eq!(deadline(started, last), started + LINGER_LIMIT);
    assert_eq!(deadline(started, last - TICK), last - TICK + LINGER_IDLE);
}

#[test]
fn bytes_that_come_past_the_limit_less_the_idle_gap_do_not_extend_the_wait() {
    let started = Instant::now();
    let last = started + LINGER_LIMIT - LINGER_IDLE + TICK;
    assert_eq!(deadline(started, last), started + LINGER_LIMIT);
}

#[test]
fn the_limit_and_the_idle_gap_are_the_ones_given() {
    let started = Instant::now();
    let short = LingerBounds {
        idle: Duration::from_secs(1),
        limit: Duration::from_secs(3),
    };
    assert_eq!(short.deadline(started, started), started + Duration::from_secs(1));
    assert_eq!(
        short.deadline(started, started + Duration::from_secs(5)),
        started + Duration::from_secs(3)
    );
    assert_eq!(
        BOUNDS.deadline(started, started + Duration::from_secs(5)),
        started + Duration::from_secs(7)
    );
}

/// Starts lingering on the server's end of a pipe whose buffer holds `capacity` bytes, and
/// returns the client's end with the lingering task.
fn lingering(capacity: usize) -> (DuplexStream, JoinHandle<()>) {
    lingering_until(capacity, std::future::pending())
}

/// Works like [`lingering`] for a wait that `stop` ends.
fn lingering_until(capacity: usize, stop: impl Future<Output = ()> + Send + 'static) -> (DuplexStream, JoinHandle<()>) {
    let (client, mut server) = tokio::io::duplex(capacity);
    let done = tokio::spawn(async move { linger(&mut server, BOUNDS, stop).await });
    (client, done)
}

#[tokio::test(start_paused = true)]
async fn a_client_still_sending_more_than_the_buffer_holds_gets_to_finish_and_close() {
    let (mut client, done) = lingering(1024);
    client
        .write_all(&vec![b'x'; 64 * 1024])
        .await
        .expect("the server reads what the client sends");
    drop(client);
    tokio::time::timeout(TICK, done)
        .await
        .expect("the wait ends at the client's close")
        .expect("the lingering task ends without a panic");
}

#[tokio::test(start_paused = true)]
async fn a_client_that_sends_nothing_more_is_waited_for_the_idle_gap_and_no_longer() {
    let (mut client, mut done) = lingering(1024);
    client
        .write_all(b"the rest of a body")
        .await
        .expect("the pipe takes it");
    let last = Instant::now();
    assert!(
        tokio::time::timeout_at(last + LINGER_IDLE - TICK, &mut done)
            .await
            .is_err(),
        "the wait ended before the idle gap"
    );
    tokio::time::timeout_at(last + LINGER_IDLE + TICK, &mut done)
        .await
        .expect("the wait ends at the idle gap")
        .expect("the lingering task ends without a panic");
}

#[tokio::test(start_paused = true)]
async fn a_client_that_keeps_sending_is_cut_off_at_the_limit_and_counted() {
    let _counting = CUT_SHORT_COUNT.lock().await;
    let counted = events::total(StreamEvent::LingerCutShort);
    let (mut client, mut done) = lingering(1024);
    let started = Instant::now();
    let sender = tokio::spawn(async move {
        loop {
            if client.write_all(b"more").await.is_err() {
                return;
            }
            tokio::time::sleep(LINGER_IDLE / 2).await;
        }
    });
    assert!(
        tokio::time::timeout_at(started + LINGER_LIMIT - TICK, &mut done)
            .await
            .is_err(),
        "the wait ended before the limit"
    );
    tokio::time::timeout_at(started + LINGER_LIMIT + TICK, &mut done)
        .await
        .expect("the wait ends at the limit")
        .expect("the lingering task ends without a panic");
    assert!(events::total(StreamEvent::LingerCutShort) > counted);
    sender.abort();
}

#[tokio::test(start_paused = true)]
async fn a_stop_ends_the_wait_at_once_and_is_not_counted_as_cut_short() {
    let _counting = CUT_SHORT_COUNT.lock().await;
    let counted = events::total(StreamEvent::LingerCutShort);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let (mut client, mut done) = lingering_until(1024, async move {
        stopped.await.expect("the test sends the stop");
    });
    client
        .write_all(b"the rest of a body")
        .await
        .expect("the pipe takes it");
    assert!(
        tokio::time::timeout(LINGER_IDLE - TICK, &mut done).await.is_err(),
        "the wait ended before the stop"
    );
    stop.send(()).expect("the lingering task waits for the stop");
    let stopped_at = Instant::now();
    done.await.expect("the lingering task ends without a panic");
    assert_eq!(Instant::now() - stopped_at, Duration::ZERO);
    assert_eq!(events::total(StreamEvent::LingerCutShort), counted);
}
