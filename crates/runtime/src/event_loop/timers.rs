//! Keeps one loop's timers in a heap ordered by their exact deadlines, and says when the loop's
//! poller timer must be set again: the kernel wakes the driver at the earliest deadline.
//!
//! It does not know what a timer does, which thread takes the due ones, or what the poller timer is
//! set for: the fd table, which sets it, records that.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use slab::Slab;

use super::Due;

/// Bounds how far ahead a timer is set, about thirty years.
pub(super) const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);

/// Names one queued timer; `seq` tells it from a later timer in the same slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Key {
    slot: usize,
    seq: u64,
}

/// Holds the loop's timers, each what `D` says it does when it comes due. A timer taken out leaves
/// its heap entry behind, skipped when it comes to the top; the heap is rebuilt once such entries
/// outnumber the live ones.
pub(super) struct Timers<D = Due> {
    slots: Slab<(Instant, u64, D)>,
    heap: BinaryHeap<Reverse<(Instant, u64, usize)>>,
    seq: u64,
}

impl<D> Default for Timers<D> {
    fn default() -> Self {
        Timers {
            slots: Slab::new(),
            heap: BinaryHeap::new(),
            seq: 0,
        }
    }
}

impl<D> Timers<D> {
    /// Adds `due` at `at` and returns its key, which takes it out again.
    pub(super) fn push(&mut self, at: Instant, due: D) -> Key {
        self.seq += 1;
        let slot = self.slots.insert((at, self.seq, due));
        self.heap.push(Reverse((at, self.seq, slot)));
        Key { slot, seq: self.seq }
    }

    /// Takes the timer `key` names out, or returns `None` when it came due already.
    pub(super) fn remove(&mut self, key: &Key) -> Option<D> {
        let queued = self.slots.get(key.slot).is_some_and(|&(_, seq, _)| seq == key.seq);
        let removed = queued.then(|| self.slots.remove(key.slot).2);
        if worth_rebuilding(self.heap.len(), self.slots.len()) {
            self.heap = self
                .slots
                .iter()
                .map(|(slot, &(at, seq, _))| Reverse((at, seq, slot)))
                .collect();
        }
        removed
    }

    /// Takes the timers due at `now`, earliest first and those of one instant in the order they
    /// were set.
    pub(super) fn take_due(&mut self, now: Instant) -> Vec<D> {
        let mut due = Vec::new();
        while let Some(&Reverse((at, seq, slot))) = self.heap.peek() {
            let live = self.slots.get(slot).is_some_and(|&(_, s, _)| s == seq);
            if live && at > now {
                break;
            }
            self.heap.pop();
            if live {
                due.push(self.slots.remove(slot).2);
            }
        }
        due
    }

    /// Returns the earliest deadline of a timer still queued, dropping the entries of timers taken
    /// out that are ahead of it.
    pub(super) fn earliest(&mut self) -> Option<Instant> {
        loop {
            match self.heap.peek() {
                Some(&Reverse((at, seq, slot))) if self.slots.get(slot).is_some_and(|&(_, s, _)| s == seq) => {
                    return Some(at);
                }
                Some(_) => {
                    self.heap.pop();
                }
                None => return None,
            }
        }
    }
}

/// Says what the poller timer must be told after a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TimerChange {
    /// It is set for the earliest timer already and has not fired.
    Keep,
    SetFor(Instant),
    Disarm,
}

/// Says whether a timer added at `at` comes before the deadline the poller timer is set for.
pub(super) fn sooner(armed: Option<Instant>, at: Instant) -> bool {
    armed.is_none_or(|armed| at < armed)
}

/// Says what the poller timer set for `armed` must be told when the earliest timer left is `next`.
/// A timer that fired, by `now` or by the poller's word, is always set again: setting it is what
/// clears its readiness, and a kqueue timer fires once: the scan that reads its event takes it off
/// the queue, whatever this thread's clock reads.
pub(super) fn change(armed: Option<Instant>, next: Option<Instant>, now: Instant, fired: bool) -> TimerChange {
    let fired = fired || armed.is_some_and(|armed| armed <= now);
    match next {
        _ if !fired && next == armed => TimerChange::Keep,
        Some(at) => TimerChange::SetFor(at),
        None => TimerChange::Disarm,
    }
}

/// Says whether the heap holds so many entries of timers taken out that rebuilding it from the
/// live ones is cheaper than skipping them as they come to the top.
fn worth_rebuilding(entries: usize, live: usize) -> bool {
    entries > 64 && entries > 2 * live
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timer_sets_the_poller_timer_only_when_it_is_sooner_than_what_it_is_set_for() {
        let at = Instant::now();
        assert!(sooner(None, at));
        assert!(sooner(Some(at + Duration::from_nanos(1)), at));
        assert!(!sooner(Some(at), at));
    }

    #[test]
    fn a_poller_timer_set_right_is_kept_and_one_that_fired_or_is_wrong_is_told_again() {
        let now = Instant::now();
        let later = now + Duration::from_millis(5);
        assert_eq!(change(Some(later), Some(later), now, false), TimerChange::Keep);
        assert_eq!(change(None, None, now, false), TimerChange::Keep);
        assert_eq!(change(Some(now), Some(now), now, false), TimerChange::SetFor(now));
        assert_eq!(change(Some(later), Some(later), now, true), TimerChange::SetFor(later));
        assert_eq!(change(Some(now), None, now, false), TimerChange::Disarm);
        assert_eq!(change(Some(later), None, now, false), TimerChange::Disarm);
        assert_eq!(change(None, Some(later), now, false), TimerChange::SetFor(later));
    }

    #[test]
    fn the_heap_is_rebuilt_only_once_entries_of_taken_out_timers_outnumber_the_live_ones() {
        assert!(!worth_rebuilding(64, 0));
        assert!(worth_rebuilding(65, 32));
        assert!(!worth_rebuilding(66, 33));
    }

    /// Steps a xorshift generator, so the model test runs the same sequence every time.
    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[test]
    fn timers_come_due_as_an_ordered_map_of_their_deadlines_says_through_adds_removals_and_rebuilds() {
        use std::collections::BTreeMap;

        let start = Instant::now();
        let mut timers = Timers::<u32>::default();
        let mut model: BTreeMap<(Instant, u32), Key> = BTreeMap::new();
        let mut state = 0x9E37_79B9_7F4A_7C15;
        let mut now = start;
        for id in 0..20_000u32 {
            match next(&mut state) % 8 {
                0..=3 => {
                    let at = now + Duration::from_micros(next(&mut state) % 50);
                    let key = timers.push(at, id);
                    model.insert((at, id), key);
                }
                4..=5 => {
                    let Some((&taken, &key)) = model.iter().nth(next(&mut state) as usize % model.len().max(1)) else {
                        continue;
                    };
                    assert_eq!(timers.remove(&key), Some(taken.1));
                    assert_eq!(timers.remove(&key), None, "a key takes its timer out once");
                    model.remove(&taken);
                }
                _ => {
                    now += Duration::from_micros(next(&mut state) % 20);
                    let expected: Vec<u32> = model.range(..(now, u32::MAX)).map(|(&(_, id), _)| id).collect();
                    let keys: Vec<Key> = model.range(..(now, u32::MAX)).map(|(_, &key)| key).collect();
                    assert_eq!(timers.take_due(now), expected);
                    model.retain(|&(at, _), _| at > now);
                    for key in keys {
                        assert_eq!(timers.remove(&key), None, "a timer that came due is out");
                    }
                }
            }
            assert_eq!(timers.earliest(), model.keys().next().map(|&(at, _)| at));
        }
    }
}
