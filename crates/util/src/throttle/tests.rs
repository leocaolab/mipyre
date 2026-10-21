use super::*;

use proptest::prelude::*;

const PERIOD: Duration = Duration::from_secs(60);

fn at(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

fn just_before(secs: u64) -> Duration {
    Duration::from_secs(secs) - Duration::from_nanos(1)
}

#[test]
fn one_line_per_period_then_the_held_count() {
    let throttle = LogThrottle::new(PERIOD);
    assert_eq!(throttle.admit(at(0)), Some(0), "the first line goes out");
    assert_eq!(throttle.admit(at(1)), None);
    assert_eq!(throttle.admit(just_before(60)), None, "the period is not over");
    assert_eq!(throttle.admit(at(60)), Some(2), "the period is over: both held lines counted");
    assert_eq!(throttle.admit(just_before(120)), None, "a new period from the last line");
    assert_eq!(throttle.admit(at(120)), Some(1), "and only the lines held since");
}

#[test]
fn a_new_throttle_admits_its_first_line_at_any_time() {
    assert_eq!(LogThrottle::new(PERIOD).admit(at(0)), Some(0));
    assert_eq!(LogThrottle::new(PERIOD).admit(at(12_345)), Some(0));
}

#[test]
fn a_zero_period_lets_every_line_through_and_counts_none_held() {
    let throttle = LogThrottle::new(Duration::ZERO);
    for _ in 0..3 {
        assert_eq!(throttle.admit(at(5)), Some(0));
    }
}

/// Lets threads race with clock readings that arrive out of order, each crossing many periods.
#[test]
fn racing_threads_let_one_line_out_per_period_and_lose_no_held_line() {
    const THREADS: u64 = 8;
    const CALLS: u64 = 20_000;
    let throttle = LogThrottle::new(Duration::from_millis(1));
    let admitted: Vec<(u64, u64)> = std::thread::scope(|scope| {
        let runs: Vec<_> = (0..THREADS)
            .map(|thread| {
                let throttle = &throttle;
                scope.spawn(move || {
                    (0..CALLS)
                        .filter_map(|i| {
                            let now = (i * 10_000 + (i * 7919 + thread * 104_729) % 600_000).saturating_sub(300_000);
                            throttle.admit(Duration::from_nanos(now)).map(|held| (now, held))
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        runs.into_iter().flat_map(|run| run.join().expect("a counting thread does not panic")).collect()
    });

    let mut times: Vec<u64> = admitted.iter().map(|(now, _)| *now).collect();
    times.sort_unstable();
    for pair in times.windows(2) {
        assert!(pair[1] - pair[0] >= 1_000_000, "two lines within one period: {pair:?}");
    }
    let reported: u64 = admitted.iter().map(|(_, held)| held).sum();
    let residual = throttle.admit(Duration::MAX).expect("no line is open at the end of time");
    assert_eq!(admitted.len() as u64 + reported + residual, THREADS * CALLS);
}


/// Throttles one key-less line as a plain value, as a reference for the atomic throttle.
fn throttled(times: &[u64], period_ns: u64) -> Vec<Option<u64>> {
    let mut state = ThrottleState::new();
    let period = ThrottlePeriod::new(Duration::from_nanos(period_ns));
    times.iter().map(|at| state.admit(Duration::from_nanos(*at), period)).collect()
}

proptest! {
    #[test]
    fn the_atomic_throttle_decides_as_the_plain_state_does(
        times in prop::collection::vec(0u64..1_000, 0..64),
        period_ns in 0u64..200,
    ) {
        let throttle = LogThrottle::new(Duration::from_nanos(period_ns));
        let decided: Vec<Option<u64>> = times.iter().map(|t| throttle.admit(Duration::from_nanos(*t))).collect();
        prop_assert_eq!(decided, throttled(&times, period_ns));
    }
}
