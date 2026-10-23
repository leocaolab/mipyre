use super::*;

use proptest::prelude::*;

const PERIOD: Duration = Duration::from_secs(60);

fn at(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

fn just_before(secs: u64) -> Duration {
    Duration::from_secs(secs) - Duration::from_nanos(1)
}

fn capacity(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a test capacity is not zero")
}

#[test]
fn a_keys_logged_line_carries_its_held_count() {
    let mut throttles: PerKeyThrottles<String> = PerKeyThrottles::new(PERIOD, capacity(8));
    assert_eq!(throttles.admit("a", at(0)), Some(HeldLines::OfKey(0)));
    assert_eq!(throttles.admit("a", at(1)), None);
    assert_eq!(throttles.admit("a", just_before(60)), None);
    assert_eq!(
        throttles.admit("a", at(60)),
        Some(HeldLines::OfKey(2)),
        "both held lines are counted in the next one"
    );
    assert_eq!(
        throttles.admit("a", at(120)),
        Some(HeldLines::OfKey(0)),
        "and only once"
    );
}

#[test]
fn a_key_logs_once_per_period_and_others_are_not_held_back() {
    let mut throttles: PerKeyThrottles<String> = PerKeyThrottles::new(PERIOD, capacity(8));
    assert!(throttles.admit("a", at(0)).is_some());
    assert!(throttles.admit("a", at(1)).is_none());
    assert!(throttles.admit("b", at(1)).is_some(), "another key has its own line");
    assert!(throttles.admit("b", at(2)).is_none());
}

#[test]
fn keys_beyond_capacity_in_rotation_stay_throttled() {
    let mut throttles: PerKeyThrottles<u32> = PerKeyThrottles::new(PERIOD, capacity(2));
    let logged = (0..30)
        .filter(|i| throttles.admit(&(i % 3), at(u64::from(*i))).is_some())
        .count();
    assert_eq!(logged, 3, "0 and 1 once each, and one overflow line for the third key");
}

#[test]
fn the_least_recently_seen_key_is_forgotten_past_capacity() {
    let mut throttles: PerKeyThrottles<u32> = PerKeyThrottles::new(PERIOD, capacity(2));
    assert!(throttles.admit(&1, at(0)).is_some());
    assert!(throttles.admit(&2, at(0)).is_some());
    assert!(throttles.admit(&1, at(1)).is_none(), "1 is still remembered");
    assert!(throttles.admit(&3, at(1)).is_some());
    assert!(
        throttles.admit(&2, at(2)).is_none(),
        "2 was not forgotten while in its period"
    );
    assert!(
        throttles.admit(&3, at(2)).is_none(),
        "3's overflow gate already logged this period"
    );
}

#[test]
fn a_new_key_takes_the_place_of_one_whose_period_is_over() {
    let mut throttles: PerKeyThrottles<u32> = PerKeyThrottles::new(PERIOD, capacity(2));
    assert!(throttles.admit(&1, at(0)).is_some());
    assert!(throttles.admit(&2, at(10)).is_some());
    assert_eq!(
        throttles.admit(&3, just_before(60)),
        Some(HeldLines::OfUnrememberedKeys(0)),
        "no place yet: the overflow line"
    );
    assert_eq!(
        throttles.admit(&3, at(60)),
        Some(HeldLines::OfKey(0)),
        "3 takes 1's place"
    );
    assert!(throttles.admit(&3, at(61)).is_none(), "and is remembered");
    assert!(throttles.admit(&2, at(61)).is_none(), "2 kept its place");
}

#[test]
fn a_forgotten_keys_held_lines_go_out_with_the_next_overflow_line() {
    let mut throttles: PerKeyThrottles<String> = PerKeyThrottles::new(PERIOD, capacity(1));
    assert_eq!(throttles.admit("a", at(0)), Some(HeldLines::OfKey(0)));
    assert_eq!(throttles.admit("a", at(1)), None);
    assert_eq!(
        throttles.admit("b", at(60)),
        Some(HeldLines::OfKey(0)),
        "b takes the place of a, whose period is over"
    );
    assert_eq!(
        throttles.admit("c", at(61)),
        Some(HeldLines::OfUnrememberedKeys(1)),
        "the line a held is not lost with a's place"
    );
}

/// Returns the held lines a `PerKeyThrottles` still keeps, which no line has carried out yet.
fn residual<K: Hash + Eq>(throttles: &PerKeyThrottles<K>) -> u64 {
    throttles.remembered.values().map(|e| e.throttle.held()).sum::<u64>() + throttles.unremembered.held()
}

fn carried(held: HeldLines) -> u64 {
    match held {
        HeldLines::OfKey(n) | HeldLines::OfUnrememberedKeys(n) => n,
    }
}

proptest! {
    #[test]
    fn keyed_throttles_carry_every_held_line_exactly_once(
        calls in prop::collection::vec((0u8..7, 0u64..400), 0..200),
        capacity_n in 1usize..5,
        period_ns in 1u64..100,
    ) {
        let mut throttles: PerKeyThrottles<u8> = PerKeyThrottles::new(Duration::from_nanos(period_ns), capacity(capacity_n));
        let decided: Vec<Option<HeldLines>> = calls.iter().map(|(key, at)| throttles.admit(key, Duration::from_nanos(*at))).collect();
        let held = decided.iter().filter(|d| d.is_none()).count() as u64;
        let reported: u64 = decided.iter().flatten().map(|h| carried(*h)).sum();
        prop_assert_eq!(reported + residual(&throttles), held);
        prop_assert!(throttles.remembered.len() <= capacity_n);
    }

    /// Checks, with time moving forward, that no window one period long lets out more than one
    /// line per remembered key plus one for the keys not remembered, and that a key's own lines
    /// are a period apart.
    #[test]
    fn keyed_throttles_let_out_at_most_capacity_and_one_lines_per_period(
        calls in prop::collection::vec((0u8..9, 0u64..30), 0..200),
        capacity_n in 1usize..5,
        period_ns in 1u64..200,
    ) {
        let mut throttles: PerKeyThrottles<u8> = PerKeyThrottles::new(Duration::from_nanos(period_ns), capacity(capacity_n));
        let mut now = 0u64;
        let mut lines: Vec<(u64, u8, HeldLines)> = Vec::new();
        for (key, step) in &calls {
            now += step;
            if let Some(held) = throttles.admit(key, Duration::from_nanos(now)) {
                lines.push((now, *key, held));
            }
        }
        for (start, _, _) in &lines {
            let within = lines.iter().filter(|(t, _, _)| *t >= *start && *t - *start < period_ns).count();
            prop_assert!(within <= capacity_n + 1, "{within} lines from {start}: {lines:?}");
        }
        for key in 0u8..9 {
            let own: Vec<u64> = lines
                .iter()
                .filter(|(_, k, held)| *k == key && matches!(held, HeldLines::OfKey(_)))
                .map(|(t, _, _)| *t)
                .collect();
            for pair in own.windows(2) {
                prop_assert!(pair[1] - pair[0] >= period_ns, "key {key}: {own:?}");
            }
        }
    }
}

/// Reads the doc of `PerKeyThrottles` plainly: a list in the order keys were last seen.
struct KeyList {
    period_ns: u64,
    capacity: usize,
    /// Holds each key with when its period is over and its held lines, least recently seen first.
    keys: Vec<(u8, u64, u64)>,
    unremembered: (u64, u64),
}

impl KeyList {
    fn admit(&mut self, key: u8, at: u64) -> Option<HeldLines> {
        let open_at = at.saturating_add(self.period_ns);
        if let Some(i) = self.keys.iter().position(|k| k.0 == key) {
            let (_, over_at, held) = self.keys.remove(i);
            let (entry, admitted) = if at >= over_at {
                ((key, open_at, 0), Some(held))
            } else {
                ((key, over_at, held + 1), None)
            };
            self.keys.push(entry);
            return admitted.map(HeldLines::OfKey);
        }
        if self.keys.len() >= self.capacity {
            match self.keys.iter().position(|k| at >= k.1) {
                Some(i) => self.unremembered.1 += self.keys.remove(i).2,
                None if at >= self.unremembered.0 => {
                    let held = std::mem::take(&mut self.unremembered.1);
                    self.unremembered.0 = open_at;
                    return Some(HeldLines::OfUnrememberedKeys(held));
                }
                None => {
                    self.unremembered.1 += 1;
                    return None;
                }
            }
        }
        self.keys.push((key, open_at, 0));
        Some(HeldLines::OfKey(0))
    }
}

proptest! {
    #[test]
    fn keyed_throttles_decide_as_their_doc_says(
        calls in prop::collection::vec((0u8..7, 0u64..400), 0..200),
        capacity_n in 1usize..5,
        period_ns in 0u64..100,
    ) {
        let mut throttles: PerKeyThrottles<u8> = PerKeyThrottles::new(Duration::from_nanos(period_ns), capacity(capacity_n));
        let mut listed = KeyList { period_ns, capacity: capacity_n, keys: Vec::new(), unremembered: (0, 0) };
        for (key, at) in &calls {
            prop_assert_eq!(throttles.admit(key, Duration::from_nanos(*at)), listed.admit(*key, *at), "key {} at {}", key, at);
        }
    }
}
