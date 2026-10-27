//! The Postgres pools a server run owns, and the handles code holds to them.
//!
//! `PgPool.connect()` makes a [`PgHandle`]: what the call asked for, bound to no pool yet. The
//! engine records it with the interpreter that made it; when a run seals it takes every live
//! handle its main interpreter recorded, opens one pool per DSN on a `mipyre-db` runtime of its own
//! ([`RunPools`]) and binds the handles to them, and it closes them when it stops. A worker of the
//! run is given the run's pools before its script runs, so its `connect()` binds to them at
//! once. A handle no run holds opens a pool of its own on its first query and closes it when it
//! is dropped. No request does any of this: binding happens at the seal, a worker's start and a
//! handle's first query.
//!
//! It knows no Python: the engine reads which module made a handle and hands it in as a
//! [`ModuleKey`], and gives the stack size of the runtime's threads, since it hands results to
//! Python on them.

mod db_runtime;
mod dsn;
mod pool;
mod sync;

use std::sync::{Arc, Weak};

pub use self::db_runtime::{DbRuntime, TaskError};
pub use self::dsn::Dsn;
use self::pool::PoolSettings;
pub use self::pool::{Expiry, PoolConflict, PoolRequest, PoolSettingDifferences};

/// Names a run in an error, so two runs that met there can be told apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLabel(pub String);

impl std::fmt::Display for RunLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A pool a query can run on now, with the runtime it runs on.
#[derive(Clone)]
pub struct OpenPool {
    pub pool: sqlx::PgPool,
    pub runtime: Arc<DbRuntime>,
}

/// One run's pools, one per DSN its main interpreter connected to, on the run's own runtime.
pub struct RunPools {
    label: RunLabel,
    runtime: Arc<DbRuntime>,
    pools: Vec<(PoolSettings, sqlx::PgPool)>,
}

/// Why a run's pools could not be opened; the run does not start.
#[derive(Debug, thiserror::Error)]
pub enum RunPoolsError {
    /// Two `connect()` calls asked for one DSN with different values: every one, a line each.
    #[error("{}", lines(.0))]
    Conflicts(Vec<PoolConflict>),
    #[error(transparent)]
    Runtime(#[from] TaskError),
    #[error("PgPool connect to {dsn}: {source}")]
    ConnectFailed {
        dsn: Dsn,
        #[source]
        source: sqlx::Error,
    },
    /// Handles another run that is still serving holds: a handle is one run's at a time.
    #[error("{}", lines(.0))]
    Shared(Vec<HandleConflict>),
}

/// A handle bound to a run still serving, which another run sealing now also wants.
#[derive(Debug)]
pub struct HandleConflict {
    dsn: Dsn,
    holder: RunLabel,
    wanted_by: RunLabel,
}

impl std::fmt::Display for HandleConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the PgPool handle for {} belongs to {} while it serves, so {} cannot use it too: \
             a handle is one run's at a time",
            self.dsn, self.holder, self.wanted_by
        )
    }
}

fn lines<T: std::fmt::Display>(items: &[T]) -> String {
    items.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n")
}

impl RunPools {
    /// Seals a run's pools from `handles`, every live `connect()` of its main interpreter in call
    /// order: plans one pool per DSN, reports every conflicting setting at once, builds the
    /// run's runtime with threads of `stack_size` bytes, opens the pools and binds the handles.
    /// `None` when there are no handles. Call it with the GIL released: it waits for the
    /// connections.
    ///
    /// # Errors
    ///
    /// Returns [`RunPoolsError`]: the conflicts, the runtime or a connection that failed, or
    /// handles another run still holds.
    pub fn seal(
        label: RunLabel,
        handles: &[Arc<PgHandle>],
        stack_size: usize,
    ) -> Result<Option<Arc<RunPools>>, RunPoolsError> {
        if handles.is_empty() {
            return Ok(None);
        }
        let requests: Vec<&PoolRequest> = handles.iter().map(|h| &h.request).collect();
        let default_threads =
            db_runtime::default_runtime_threads().map_err(|e| RunPoolsError::Runtime(TaskError::RuntimeNotBuilt(e)))?;
        let plan = pool::plan_run(&requests, default_threads).map_err(RunPoolsError::Conflicts)?;
        let shared: Vec<HandleConflict> = handles
            .iter()
            .filter_map(|handle| {
                handle.serving_run().map(|holder| HandleConflict {
                    dsn: handle.request.dsn.clone(),
                    holder,
                    wanted_by: label.clone(),
                })
            })
            .collect();
        if !shared.is_empty() {
            return Err(RunPoolsError::Shared(shared));
        }
        let runtime = Arc::new(DbRuntime::build(Some(plan.threads), stack_size)?);
        let mut pools = Vec::with_capacity(plan.pools.len());
        for settings in plan.pools {
            let pool = settings
                .open(&runtime)?
                .map_err(|source| RunPoolsError::ConnectFailed {
                    dsn: settings.dsn.clone(),
                    source,
                })?;
            pools.push((settings, pool));
        }
        let run = Arc::new(RunPools { label, runtime, pools });
        for handle in handles {
            if let Some(index) = run.index_of(&handle.request.dsn) {
                handle.bind(&run, index);
            }
        }
        Ok(Some(run))
    }

    fn index_of(&self, dsn: &Dsn) -> Option<usize> {
        self.pools.iter().position(|(settings, _)| settings.dsn == *dsn)
    }

    fn open(&self, index: usize) -> Option<OpenPool> {
        self.pools.get(index).map(|(_, pool)| OpenPool {
            pool: pool.clone(),
            runtime: Arc::clone(&self.runtime),
        })
    }

    /// Binds a worker's `connect()` to this run's pool for its DSN, if the run has one.
    ///
    /// # Errors
    ///
    /// Returns [`PoolSettingDifferences`] when the call names a setting the run's pool does not
    /// have.
    pub fn adopt(self: &Arc<Self>, handle: &PgHandle) -> Result<(), PoolSettingDifferences> {
        let Some((index, (settings, _))) = self
            .pools
            .iter()
            .enumerate()
            .find(|(_, (settings, _))| settings.dsn == handle.request.dsn)
        else {
            return Ok(());
        };
        let differences = handle.request.differences(settings, self.runtime.threads());
        if let Some(differ) = PoolSettingDifferences::of(differences) {
            return Err(differ);
        }
        handle.bind(self, index);
        Ok(())
    }

    /// Closes every pool as the run stops, waiting until their connections are closed. Call it
    /// with the GIL released.
    pub fn close(&self) {
        let pools: Vec<sqlx::PgPool> = self.pools.iter().map(|(_, pool)| pool.clone()).collect();
        if let Err(e) = self.runtime.run(async move {
            for pool in pools {
                pool.close().await;
            }
        }) {
            tracing::warn!(error = %e, run = %self.label,
                "the run's database pools could not be closed; their connections end with the process");
        }
    }
}

/// The module whose code made a handle or an app: its file (absolute), or its name when it
/// has none (`python -c`'s `__main__`). A run takes only the handles its own app's modules made,
/// so two apps of one process never touch each other's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleKey(String);

impl From<String> for ModuleKey {
    fn from(key: String) -> Self {
        ModuleKey(key)
    }
}

/// What a `PgPool.connect()` made: what it asked for, the module that asked, and the pool it is
/// bound to.
pub struct PgHandle {
    request: PoolRequest,
    owner: ModuleKey,
    binding: crate::sync::RwLock<HandlePool>,
}

enum HandlePool {
    /// No run has it, and it has not queried yet.
    Unbound,
    /// A run's pool; when the run is gone the handle opens its own on its next query.
    Run { run: Weak<RunPools>, index: usize },
    /// Its own pool, open until the handle is dropped.
    Own(OpenPool),
}

/// Why a handle's own pool could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OwnPoolError {
    #[error(transparent)]
    Runtime(#[from] TaskError),
    #[error("PgPool connect: {0}")]
    ConnectFailed(#[source] sqlx::Error),
}

impl PgHandle {
    pub fn new(request: PoolRequest, owner: ModuleKey) -> Arc<Self> {
        Arc::new(PgHandle {
            request,
            owner,
            binding: crate::sync::RwLock::new(HandlePool::Unbound),
        })
    }

    /// Says whether one of `modules` made this handle.
    pub fn made_by(&self, modules: &[ModuleKey]) -> bool {
        modules.contains(&self.owner)
    }

    /// The run this handle is bound to while it serves.
    fn serving_run(&self) -> Option<RunLabel> {
        match &*self.binding.read() {
            HandlePool::Run { run, .. } => run.upgrade().map(|run| run.label.clone()),
            HandlePool::Unbound | HandlePool::Own(_) => None,
        }
    }

    fn bind(&self, run: &Arc<RunPools>, index: usize) {
        *self.binding.write() = HandlePool::Run {
            run: Arc::downgrade(run),
            index,
        };
    }

    /// The pool a query runs on now: the run's it is bound to, else its own, opened on this
    /// first query outside a run, on a runtime with threads of `stack_size` bytes. Call it with the
    /// GIL released: opening waits for a connection.
    ///
    /// # Errors
    ///
    /// Returns [`OwnPoolError`] when its own pool cannot be opened.
    pub fn open(&self, stack_size: usize) -> Result<OpenPool, OwnPoolError> {
        match &*self.binding.read() {
            HandlePool::Own(open) => return Ok(open.clone()),
            HandlePool::Run { run, index } => {
                if let Some(open) = run.upgrade().and_then(|run| run.open(*index)) {
                    return Ok(open);
                }
            }
            HandlePool::Unbound => {}
        }
        // Opened outside the lock, so a slow connect holds up no other query; a second thread
        // racing here keeps whichever was installed first and drops its own.
        let runtime = Arc::new(DbRuntime::build(self.request.runtime_threads, stack_size)?);
        let pool = self
            .request
            .settings()
            .open(&runtime)?
            .map_err(OwnPoolError::ConnectFailed)?;
        let opened = OpenPool { pool, runtime };
        let mut binding = self.binding.write();
        match &*binding {
            HandlePool::Own(installed) => return Ok(installed.clone()),
            HandlePool::Run { run, index } => {
                if let Some(open) = run.upgrade().and_then(|run| run.open(*index)) {
                    return Ok(open);
                }
            }
            HandlePool::Unbound => {}
        }
        *binding = HandlePool::Own(opened.clone());
        Ok(opened)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STACK: usize = 2 << 20;

    fn handle(dsn: &str) -> Arc<PgHandle> {
        PgHandle::new(
            PoolRequest {
                dsn: Dsn::new(dsn.to_owned()),
                max_connections: None,
                acquire_timeout: None,
                runtime_threads: None,
                max_lifetime: None,
                idle_timeout: None,
                ping_on_acquire: None,
            },
            ModuleKey("app.py".into()),
        )
    }

    /// A run holding `handles` without opening anything: what the binding rules see.
    fn serving(label: &str, handles: &[&Arc<PgHandle>]) -> Arc<RunPools> {
        let run = Arc::new(RunPools {
            label: RunLabel(label.to_owned()),
            runtime: Arc::new(DbRuntime::build(Some(std::num::NonZeroUsize::MIN), STACK).unwrap()),
            pools: Vec::new(),
        });
        for handle in handles {
            handle.bind(&run, 0);
        }
        run
    }

    #[test]
    fn a_run_without_handles_opens_nothing() {
        assert!(RunPools::seal(RunLabel("server run 1".into()), &[], STACK)
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_handle_another_serving_run_holds_stops_the_seal_naming_both_runs() {
        let shared = handle("postgres://u:secret@h/db");
        let _first = serving("server run 1 of a.py", &[&shared]);
        let refused = RunPools::seal(RunLabel("server run 1 of b.py".into()), &[Arc::clone(&shared)], STACK);
        let Err(RunPoolsError::Shared(shared_handles)) = refused else {
            panic!("the seal goes on")
        };
        assert_eq!(
            shared_handles.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "the PgPool handle for postgres://u:***@h/db belongs to server run 1 of a.py while it serves, so \
              server run 1 of b.py cannot use it too: a handle is one run's at a time"
            ]
        );
    }

    #[test]
    fn a_handle_belongs_to_the_modules_that_made_it() {
        let held = handle("postgres://h/db");
        assert!(held.made_by(&[ModuleKey("other.py".into()), ModuleKey("app.py".into())]));
        assert!(!held.made_by(&[ModuleKey("other.py".into())]));
    }

    #[test]
    fn a_handle_whose_run_stopped_is_free_for_the_next() {
        let held = handle("postgres://h/db");
        let first = serving("server run 1", &[&held]);
        assert_eq!(held.serving_run(), Some(RunLabel("server run 1".into())));
        drop(first);
        assert_eq!(held.serving_run(), None);
    }

    #[test]
    fn conflicting_calls_stop_the_seal_before_anything_opens() {
        let mut two = handle("postgres://h/db");
        Arc::get_mut(&mut two).unwrap().request.max_connections = Some(2);
        let mut three = handle("postgres://h/db");
        Arc::get_mut(&mut three).unwrap().request.max_connections = Some(3);
        let refused = RunPools::seal(RunLabel("server run 1".into()), &[two, three], STACK);
        let Err(e @ RunPoolsError::Conflicts(_)) = refused else {
            panic!("the seal goes on")
        };
        assert_eq!(
            e.to_string(),
            "postgres://h/db: PgPool is already connected with max_connections=2, not 3"
        );
    }
}
