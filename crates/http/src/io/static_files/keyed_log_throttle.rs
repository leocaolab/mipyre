//! Throttles a log line per key, for the static-file errors a client can repeat: the decision is
//! [`PerKeyThrottles`], a plain state machine over the time handed in, and [`KeyedLogThrottle`] is
//! its lock, so the statics can be shared across threads.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, DefaultHasher, Hash};
use std::num::NonZeroUsize;
use std::time::Duration;

use mipyre_util::throttle::{ThrottlePeriod, ThrottleState};

/// Counts the lines a keyed line that goes out was held back for, and says whose they were.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeldLines {
    /// The lines of this key held back since its last line.
    OfKey(u64),
    /// The lines held back since the last such line of every key not remembered, this one
    /// included: the table was full, or a key was forgotten with lines still held.
    OfUnrememberedKeys(u64),
}

/// Holds the throttle of each remembered key and the one shared by every key that is not.
///
/// A new key replaces the least recently seen key whose period is over, and that key's held lines
/// move to the shared throttle. When no key's period is over, the new key goes through the shared
/// throttle, so rotating through more keys than `capacity` cannot log every line.
pub struct PerKeyThrottles<K> {
    period: ThrottlePeriod,
    capacity: NonZeroUsize,
    /// It is never longer than `capacity`, and no two entries have the same `seen`.
    remembered: HashMap<K, RecentKey, BuildHasherDefault<DefaultHasher>>,
    /// Numbers the calls, so a larger `seen` means more recently seen.
    calls: u64,
    unremembered: ThrottleState,
}

/// Holds a remembered key's throttle and the call it was last seen on.
struct RecentKey {
    throttle: ThrottleState,
    seen: u64,
}

impl<K: Hash + Eq> PerKeyThrottles<K> {
    pub const fn new(period: Duration, capacity: NonZeroUsize) -> Self {
        PerKeyThrottles {
            period: ThrottlePeriod::new(period),
            capacity,
            remembered: HashMap::with_hasher(BuildHasherDefault::new()),
            calls: 0,
            unremembered: ThrottleState::new(),
        }
    }

    /// Returns the lines held back that this line carries when it should be logged, and `None`
    /// when it is held back. The key is copied only when it is not remembered yet.
    pub fn admit<Q>(&mut self, key: &Q, now: Duration) -> Option<HeldLines>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq + ToOwned<Owned = K>,
    {
        self.calls += 1;
        let seen = self.calls;
        if let Some(entry) = self.remembered.get_mut(key) {
            entry.seen = seen;
            return entry.throttle.admit(now, self.period).map(HeldLines::OfKey);
        }
        if self.remembered.len() >= self.capacity.get() {
            let expired = self
                .remembered
                .values()
                .filter(|e| e.throttle.is_over(now))
                .min_by_key(|e| e.seen)
                .map(|e| (e.seen, e.throttle.held()));
            let Some((forgotten, held)) = expired else {
                return self.unremembered.admit(now, self.period).map(HeldLines::OfUnrememberedKeys);
            };
            self.remembered.retain(|_, e| e.seen != forgotten);
            self.unremembered.add_held(held);
        }
        let mut throttle = ThrottleState::new();
        let admitted = throttle.admit(now, self.period);
        self.remembered.insert(key.to_owned(), RecentKey { throttle, seen });
        admitted.map(HeldLines::OfKey)
    }
}

/// Throttles each key separately, remembering up to `capacity` keys; it takes a lock, so it is
/// for error paths only.
pub(crate) struct KeyedLogThrottle<K> {
    throttles: parking_lot::Mutex<PerKeyThrottles<K>>,
}

impl<K: Hash + Eq> KeyedLogThrottle<K> {
    pub(crate) const fn new(period: Duration, capacity: NonZeroUsize) -> Self {
        KeyedLogThrottle {
            throttles: parking_lot::Mutex::new(PerKeyThrottles::new(period, capacity)),
        }
    }

    /// Decides `key`'s line as [`PerKeyThrottles::admit`] does, under the lock.
    pub(crate) fn admit<Q>(&self, key: &Q, now: Duration) -> Option<HeldLines>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq + ToOwned<Owned = K>,
    {
        self.throttles.lock().admit(key, now)
    }
}

#[cfg(test)]
mod tests;
