//! Splits a pool's workers between `def` and `async def` handlers.

use mipyre_http::plan::route::PlaceCounts;

/// The pool's workers split by kind: `def` handlers run only on sync workers and `async def`
/// handlers only on async workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerCounts {
    pub sync_workers: usize,
    pub async_workers: usize,
}

impl WorkerCounts {
    pub(crate) fn total(&self) -> usize {
        self.sync_workers + self.async_workers
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SplitError {
    #[error("subinterpreters=0: the sub-interpreter pool needs at least one")]
    NoWorkers,
    #[error(
        "subinterpreters=1 cannot serve both sync (`def`) and async (`async def`) routes: \
         the sub-interpreter pool runs each kind on sub-interpreters of its own, so it needs \
         at least 2 (or handlers of one kind only)"
    )]
    OneWorkerForBothKinds,
}

/// The handler kinds a route table's pooled routes need; lane routes count as sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteKinds {
    Empty,
    Sync,
    Async,
    Both,
}

impl RouteKinds {
    pub fn of(counts: &PlaceCounts) -> RouteKinds {
        match (counts.worker_sync + counts.lane > 0, counts.worker_async > 0) {
            (false, false) => RouteKinds::Empty,
            (true, false) => RouteKinds::Sync,
            (false, true) => RouteKinds::Async,
            (true, true) => RouteKinds::Both,
        }
    }

    pub fn has_sync(self) -> bool {
        matches!(self, RouteKinds::Sync | RouteKinds::Both)
    }
}

/// Splits `n` workers between the kinds needed, giving async the smaller half when both are.
pub(crate) fn split_workers(n: usize, kinds: RouteKinds) -> Result<WorkerCounts, SplitError> {
    let split = |sync_workers, async_workers| WorkerCounts {
        sync_workers,
        async_workers,
    };
    match kinds {
        RouteKinds::Empty => Ok(split(n, 0)),
        _ if n == 0 => Err(SplitError::NoWorkers),
        RouteKinds::Both if n == 1 => Err(SplitError::OneWorkerForBothKinds),
        RouteKinds::Both => Ok(split(n - n / 2, n / 2)),
        RouteKinds::Sync => Ok(split(n, 0)),
        RouteKinds::Async => Ok(split(0, n)),
    }
}

/// Splits `n` workers for a route table; a `runtime="main"` route needs no pool worker.
pub fn split_workers_for_routes(n: usize, counts: &PlaceCounts) -> Result<WorkerCounts, SplitError> {
    split_workers(n, RouteKinds::of(counts))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(sync_workers: usize, async_workers: usize) -> Result<WorkerCounts, SplitError> {
        Ok(WorkerCounts {
            sync_workers,
            async_workers,
        })
    }

    #[test]
    fn both_kinds_share_the_workers_async_half_rounded_down() {
        assert_eq!(split_workers(0, RouteKinds::Both), Err(SplitError::NoWorkers));
        assert_eq!(
            split_workers(1, RouteKinds::Both),
            Err(SplitError::OneWorkerForBothKinds)
        );
        assert_eq!(split_workers(2, RouteKinds::Both), split(1, 1));
        assert_eq!(split_workers(3, RouteKinds::Both), split(2, 1));
        assert_eq!(split_workers(4, RouteKinds::Both), split(2, 2));
        assert_eq!(split_workers(5, RouteKinds::Both), split(3, 2));
    }

    #[test]
    fn one_kind_gets_every_worker() {
        for n in 1..=4 {
            assert_eq!(split_workers(n, RouteKinds::Sync), split(n, 0));
            assert_eq!(split_workers(n, RouteKinds::Async), split(0, n));
            assert_eq!(split_workers(n, RouteKinds::Empty), split(n, 0));
        }
        assert_eq!(split_workers(0, RouteKinds::Sync), Err(SplitError::NoWorkers));
        assert_eq!(split_workers(0, RouteKinds::Async), Err(SplitError::NoWorkers));
    }

    #[test]
    fn a_needed_kind_never_gets_zero_workers() {
        for n in 0..=8 {
            for kinds in [RouteKinds::Both, RouteKinds::Sync, RouteKinds::Async] {
                if let Ok(s) = split_workers(n, kinds) {
                    assert_eq!(s.total(), n);
                    assert!(!kinds.has_sync() || s.sync_workers > 0, "n={n}: {s:?}");
                    assert!(
                        !matches!(kinds, RouteKinds::Async | RouteKinds::Both) || s.async_workers > 0,
                        "n={n}: {s:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn gil_routes_need_no_pool_worker() {
        assert_eq!(
            split_workers_for_routes(
                1,
                &PlaceCounts {
                    worker_sync: 1,
                    main_buffered: 2,
                    ..PlaceCounts::default()
                }
            ),
            split(1, 0)
        );
        assert_eq!(
            split_workers_for_routes(
                1,
                &PlaceCounts {
                    worker_sync: 1,
                    worker_async: 1,
                    ..PlaceCounts::default()
                }
            ),
            Err(SplitError::OneWorkerForBothKinds)
        );
        assert_eq!(
            split_workers_for_routes(
                3,
                &PlaceCounts {
                    worker_async: 2,
                    ..PlaceCounts::default()
                }
            ),
            split(0, 3)
        );
    }

    #[test]
    fn split_errors_render_their_reason() {
        assert!(SplitError::NoWorkers.to_string().starts_with("subinterpreters=0"));
        assert!(SplitError::OneWorkerForBothKinds
            .to_string()
            .contains("needs at least 2"));
    }
}
