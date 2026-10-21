//! Builds a value on first use, one builder at a time, and builds again after a failed build.

use std::sync::OnceLock;

/// A second caller waits for the first build and takes its value, so no value is built only to
/// be thrown away: dropping a Tokio runtime blocks on its threads, which panics in a Tokio context.
pub(super) struct BuildSlot<T> {
    value: OnceLock<T>,
    building: parking_lot::Mutex<()>,
}

impl<T> BuildSlot<T> {
    pub(super) const fn new() -> Self {
        BuildSlot {
            value: OnceLock::new(),
            building: parking_lot::const_mutex(()),
        }
    }

    /// Returns the value, running `build` if no build has succeeded yet.
    ///
    /// # Errors
    ///
    /// Returns the error of a failed build; the slot stays empty, so the next use builds again.
    pub(super) fn get_or_try_init<E>(
        &self,
        build: impl FnOnce() -> Result<T, E>,
    ) -> Result<&T, E> {
        if let Some(built) = self.value.get() {
            return Ok(built);
        }
        let _building = self.building.lock();
        if let Some(built) = self.value.get() {
            return Ok(built);
        }
        let built = build()?;
        // The slot is empty under the lock, so this stores `built` and never drops it.
        Ok(self.value.get_or_init(|| built))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    #[test]
    fn a_failed_build_is_returned_and_not_kept_and_the_next_use_builds() {
        let slot = BuildSlot::<u32>::new();
        assert_eq!(slot.get_or_try_init(|| Err("refused")), Err("refused"));
        assert_eq!(slot.get_or_try_init(|| Ok::<_, &str>(7)), Ok(&7));
    }

    #[test]
    fn once_built_the_value_stands_and_no_build_runs_again() {
        let slot = BuildSlot::<u32>::new();
        assert_eq!(slot.get_or_try_init(|| Ok::<_, &str>(7)), Ok(&7));
        let builds = AtomicUsize::new(0);
        let again = slot.get_or_try_init(|| {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok::<_, &str>(8)
        });
        assert_eq!(again, Ok(&7));
        assert_eq!(builds.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn callers_at_once_build_one_value_and_all_take_it() {
        const CALLERS: usize = 8;
        let slot = Arc::new(BuildSlot::<Box<u32>>::new());
        let builds = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(CALLERS));
        let taken: Vec<usize> = (0..CALLERS)
            .map(|_| {
                let (slot, builds, start) =
                    (Arc::clone(&slot), Arc::clone(&builds), Arc::clone(&start));
                std::thread::spawn(move || {
                    start.wait();
                    let value = slot.get_or_try_init(|| {
                        builds.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, &str>(Box::new(42))
                    });
                    value.map(|b| &**b as *const u32 as usize)
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|t| t.join().unwrap().unwrap())
            .collect();
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(taken.iter().all(|&at| at == taken[0]), "{taken:?}");
    }
}
