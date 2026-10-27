//! The atomics, locks and concurrent maps this crate's cross-thread state is made of: std's,
//! parking_lot's and dashmap's in a normal build, Loom's under `--cfg sync_model`, where the models
//! (`io/static_files/cache/loom_model.rs`) run the same code over every interleaving. Only the
//! names switch; nothing wraps the real ones, so a build pays nothing for them. Shipping code names
//! an atomic or a lock only through this module.
//!
//! State Loom cannot take — what a `static` holds (no `const` constructor in Loom, and a static
//! outlives the model's executions) — names std's and parking_lot's itself, each with why.

#[cfg(not(sync_model))]
pub(crate) use dashmap::{mapref::entry::Entry, DashMap};
#[cfg(not(sync_model))]
pub(crate) use parking_lot::{Mutex, MutexGuard};
#[cfg(not(sync_model))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64};

#[cfg(sync_model)]
pub(crate) use self::model::{DashMap, Entry};
#[cfg(sync_model)]
pub(crate) use self::model::{Mutex, MutexGuard};
#[cfg(sync_model)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64};

pub(crate) use std::sync::atomic::Ordering;

/// Loom's lock with the parking_lot calls this crate makes (`lock` without poisoning,
/// `into_inner`, `default`), and a map with the dashmap calls it makes, one lock over the whole
/// map: a dashmap with a single shard, which is every shard a model's few keys can tell apart.
#[cfg(sync_model)]
mod model {
    use std::borrow::Borrow;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::ops::{Deref, DerefMut};
    use std::sync::PoisonError;

    // `pub`: `coalesce::SendShare` names them in a public (hidden) associated type; the module is private.
    #[derive(Debug)]
    pub struct Mutex<T>(loom::sync::Mutex<T>);

    pub struct MutexGuard<'a, T>(loom::sync::MutexGuard<'a, T>);

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Mutex(loom::sync::Mutex::new(value))
        }

        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.lock().unwrap_or_else(PoisonError::into_inner))
        }

        pub(crate) fn into_inner(self) -> T {
            self.0.into_inner().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Mutex::new(T::default())
        }
    }

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    const HELD: &str = "the guard has held the map's lock since the key was found in it";

    pub struct DashMap<K, V>(Mutex<HashMap<K, V>>);

    /// A value read under the map's lock, which it holds until dropped.
    pub struct Ref<'a, K, V> {
        map: MutexGuard<'a, HashMap<K, V>>,
        key: K,
    }

    pub enum Entry<'a, K, V> {
        Occupied(FullSlot<'a, K, V>),
        Vacant(EmptySlot<'a, K, V>),
    }

    pub struct FullSlot<'a, K, V> {
        map: MutexGuard<'a, HashMap<K, V>>,
        key: K,
    }

    pub struct EmptySlot<'a, K, V> {
        map: MutexGuard<'a, HashMap<K, V>>,
        key: K,
    }

    impl<K: Hash + Eq + Clone, V> DashMap<K, V> {
        pub(crate) fn new() -> Self {
            DashMap(Mutex::new(HashMap::new()))
        }

        pub(crate) fn len(&self) -> usize {
            self.0.lock().len()
        }

        pub(crate) fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let map = self.0.lock();
            let key = map.get_key_value(key)?.0.clone();
            Some(Ref { map, key })
        }

        pub(crate) fn entry(&self, key: K) -> Entry<'_, K, V> {
            let map = self.0.lock();
            if map.contains_key(&key) {
                Entry::Occupied(FullSlot { map, key })
            } else {
                Entry::Vacant(EmptySlot { map, key })
            }
        }

        pub(crate) fn retain(&self, mut keep: impl FnMut(&K, &mut V) -> bool) {
            self.0.lock().retain(|key, value| keep(key, value));
        }

        pub(crate) fn remove_if_mut<Q>(&self, key: &Q, remove: impl FnOnce(&K, &mut V) -> bool) -> Option<(K, V)>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let mut map = self.0.lock();
            let (found, mut value) = map.remove_entry(key)?;
            if remove(&found, &mut value) {
                return Some((found, value));
            }
            map.insert(found, value);
            None
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "a model, built only under `--cfg sync_model`: the key was found under the lock the guard still holds"
    )]
    impl<K: Hash + Eq, V> Deref for Ref<'_, K, V> {
        type Target = V;
        fn deref(&self) -> &V {
            self.map.get(&self.key).expect(HELD)
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "a model, built only under `--cfg sync_model`: the key was found under the lock the guard still holds"
    )]
    impl<K: Hash + Eq, V> FullSlot<'_, K, V> {
        pub(crate) fn get(&self) -> &V {
            self.map.get(&self.key).expect(HELD)
        }

        pub(crate) fn get_mut(&mut self) -> &mut V {
            self.map.get_mut(&self.key).expect(HELD)
        }
    }

    impl<K: Hash + Eq, V> EmptySlot<'_, K, V> {
        pub(crate) fn insert(mut self, value: V) {
            self.map.insert(self.key, value);
        }
    }
}
