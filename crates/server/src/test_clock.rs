//! Runs the connection tests' clock on tokio's paused time.

use mipyre_http::io::clock::Clock;

/// Is held by each test that ends a connection without a first request, since such a test counts
/// the process's `FirstRequestTimedOut` and one running beside it would add to the count.
pub(crate) static NO_FIRST_REQUEST: std::sync::Mutex<()> = std::sync::Mutex::new(());

std::thread_local! {
    static TEST_CLOCK: std::cell::Cell<Option<&'static Clock>> = const { std::cell::Cell::new(None) };
}

/// Returns a paused current-thread runtime whose thread's [`ticking_clock`] reads tokio's time
/// each time the runtime wakes, so a deadline is armed on the paused time, not a stale tick.
pub(crate) fn paused_runtime() -> tokio::runtime::Runtime {
    // A clock left by an earlier runtime on this thread has lost its task with that runtime.
    TEST_CLOCK.set(None);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .on_thread_unpark(|| {
            if let Some(clock) = TEST_CLOCK.get() {
                clock.tick(tokio::time::Instant::now().into_std());
            }
        })
        .build()
        .expect("a test runtime builds")
}

/// Returns this test thread's clock, ticked on tokio's clock: at each wake of a
/// [`paused_runtime`], and by a local task at each armed deadline, so a deadline fires at its time
/// and the paused clock jumps to it as to any tokio timer.
pub(crate) fn ticking_clock() -> &'static Clock {
    // The task looks again this often for a deadline armed after it went to sleep.
    const RECHECK: std::time::Duration = std::time::Duration::from_millis(100);
    if let Some(clock) = TEST_CLOCK.get() {
        return clock;
    }
    let clock: &'static Clock = Box::leak(Box::new(Clock::starting_at(tokio::time::Instant::now().into_std())));
    TEST_CLOCK.set(Some(clock));
    tokio::task::spawn_local(async move {
        loop {
            let recheck = tokio::time::Instant::now() + RECHECK;
            let next = clock
                .next_deadline()
                .map_or(recheck, |at| tokio::time::Instant::from_std(at).min(recheck));
            tokio::time::sleep_until(next).await;
            clock.tick(tokio::time::Instant::now().into_std());
        }
    });
    clock
}
