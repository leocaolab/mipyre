//! What a `PgPool.connect()` asks for, the settings a pool opens with, and how two asks for one
//! DSN can disagree.
//!
//! A run opens one pool per DSN from every `connect()` its main interpreter made (b/335 D17):
//! the DSN's first call opens it, and every later call naming another value of a setting is
//! refused, all of them together, when the run seals.

use std::num::NonZeroUsize;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

use super::db_runtime::DbRuntime;
use super::dsn::Dsn;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// Closes a connection this old, so none outlives a server-side or middlebox lifetime limit for
/// long; it is sqlx's default, and PgBouncer's `server_lifetime` is an hour.
const DEFAULT_MAX_LIFETIME: Expiry = Expiry::After(Duration::from_secs(30 * 60));
/// Closes an idle connection after five minutes, under the roughly 350 s after which NATs and
/// cloud load balancers drop an idle TCP flow silently; asyncpg uses the same value.
const DEFAULT_IDLE_TIMEOUT: Expiry = Expiry::After(Duration::from_secs(5 * 60));
/// Skips the test round trip before handing out a connection; a query on a connection that died
/// while idle fails with the server's or the socket's own error.
const DEFAULT_PING_ON_ACQUIRE: bool = false;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Expiry {
    After(Duration),
    Never,
}

impl Expiry {
    /// Reads `PgPool.connect()` seconds, where 0 means never.
    pub(crate) fn from_secs(secs: u64) -> Self {
        match secs {
            0 => Self::Never,
            n => Self::After(Duration::from_secs(n)),
        }
    }

    fn duration(self) -> Option<Duration> {
        match self {
            Self::After(d) => Some(d),
            Self::Never => None,
        }
    }
}

impl std::fmt::Display for Expiry {
    /// Writes seconds as `PgPool.connect()` takes them, with 0 for never.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.duration().map_or(0, |d| d.as_secs()))
    }
}

/// The settings one pool opens with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PoolSettings {
    pub(crate) dsn: Dsn,
    max_connections: u32,
    acquire_timeout: Duration,
    max_lifetime: Expiry,
    idle_timeout: Expiry,
    ping_on_acquire: bool,
}

impl PoolSettings {
    /// Opens the pool on `runtime`, waiting for its first connection.
    ///
    /// # Errors
    ///
    /// Returns the runtime's failure, or the connection's.
    pub(crate) fn open(&self, runtime: &DbRuntime) -> Result<Result<sqlx::PgPool, sqlx::Error>, super::TaskError> {
        // Without `ping_on_acquire`, a connection that died while idle fails its query with the
        // raw error, and sqlx's ping on release then closes it, so the next query gets a fresh one.
        let options = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .acquire_timeout(self.acquire_timeout)
            .max_lifetime(self.max_lifetime.duration())
            .idle_timeout(self.idle_timeout.duration())
            .test_before_acquire(self.ping_on_acquire);
        let dsn = self.dsn.expose_secret().to_owned();
        runtime.run(async move { options.connect(&dsn).await })
    }
}

/// What a `PgPool.connect()` call asked for; `None` is the default on the DSN's first call of
/// a run, and whatever the pool has on a later one.
#[derive(Clone, Debug)]
pub(crate) struct PoolRequest {
    pub(crate) dsn: Dsn,
    pub(crate) max_connections: Option<u32>,
    pub(crate) acquire_timeout: Option<Duration>,
    pub(crate) runtime_threads: Option<NonZeroUsize>,
    pub(crate) max_lifetime: Option<Expiry>,
    pub(crate) idle_timeout: Option<Expiry>,
    pub(crate) ping_on_acquire: Option<bool>,
}

impl PoolRequest {
    /// The settings of a pool opened for this call alone.
    pub(crate) fn settings(&self) -> PoolSettings {
        PoolSettings {
            dsn: self.dsn.clone(),
            max_connections: self.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS),
            acquire_timeout: self.acquire_timeout.unwrap_or(DEFAULT_ACQUIRE_TIMEOUT),
            max_lifetime: self.max_lifetime.unwrap_or(DEFAULT_MAX_LIFETIME),
            idle_timeout: self.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT),
            ping_on_acquire: self.ping_on_acquire.unwrap_or(DEFAULT_PING_ON_ACQUIRE),
        }
    }

    /// Lists how an open pool's settings (`existing`, on a runtime of `threads`) differ from
    /// what this call asked for; a setting it left out never differs.
    pub(crate) fn differences(&self, existing: &PoolSettings, threads: NonZeroUsize) -> Vec<PoolSettingDifference> {
        [
            differs(self.max_connections, existing.max_connections)
                .map(|(existing, asked)| PoolSettingDifference::MaxConnections { existing, asked }),
            differs(self.acquire_timeout, existing.acquire_timeout)
                .map(|(existing, asked)| PoolSettingDifference::AcquireTimeout { existing, asked }),
            differs(self.runtime_threads, threads)
                .map(|(existing, asked)| PoolSettingDifference::RuntimeThreads { existing, asked }),
            differs(self.max_lifetime, existing.max_lifetime)
                .map(|(existing, asked)| PoolSettingDifference::MaxLifetime { existing, asked }),
            differs(self.idle_timeout, existing.idle_timeout)
                .map(|(existing, asked)| PoolSettingDifference::IdleTimeout { existing, asked }),
            differs(self.ping_on_acquire, existing.ping_on_acquire)
                .map(|(existing, asked)| PoolSettingDifference::PingOnAcquire { existing, asked }),
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

/// The value an open pool has and the different one a call asked for, if it asked for one.
fn differs<T: PartialEq + Copy>(asked: Option<T>, existing: T) -> Option<(T, T)> {
    asked.filter(|asked| *asked != existing).map(|asked| (existing, asked))
}

/// What a run opens: one pool per DSN, in the order the DSNs were first asked for, and the
/// size of its runtime.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RunPlan {
    pub(crate) threads: NonZeroUsize,
    pub(crate) pools: Vec<PoolSettings>,
}

/// Plans a run's pools from every `connect()` its main interpreter made, in call order: a
/// DSN's first call opens its pool, with the defaults for what it left out, and every later
/// call for that DSN naming another value is a conflict, as a later `connect()` to an open
/// pool always was. The run has one runtime: the first call naming `runtime_threads` sizes it,
/// else `default_threads`, and every other call naming another size is a conflict.
///
/// # Errors
///
/// Returns every conflict, each naming its DSN (password masked).
pub(crate) fn plan_run(requests: &[&PoolRequest], default_threads: NonZeroUsize) -> Result<RunPlan, Vec<PoolConflict>> {
    let threads = requests
        .iter()
        .find_map(|r| r.runtime_threads)
        .unwrap_or(default_threads);
    let mut pools: Vec<PoolSettings> = Vec::new();
    let mut conflicts = Vec::new();
    for request in requests {
        let Some(first) = pools.iter().find(|pool| pool.dsn == request.dsn) else {
            pools.push(request.settings());
            // The first call for a DSN may still name another runtime size than the run's.
            conflicts.extend(
                request
                    .differences(&request.settings(), threads)
                    .into_iter()
                    .map(|difference| PoolConflict {
                        dsn: request.dsn.clone(),
                        difference,
                    }),
            );
            continue;
        };
        for difference in request.differences(first, threads) {
            let conflict = PoolConflict {
                dsn: request.dsn.clone(),
                difference,
            };
            if !conflicts.contains(&conflict) {
                conflicts.push(conflict);
            }
        }
    }
    if conflicts.is_empty() {
        Ok(RunPlan { threads, pools })
    } else {
        Err(conflicts)
    }
}

/// Two `connect()` calls of one run asked for one DSN with different values of a setting.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PoolConflict {
    dsn: Dsn,
    difference: PoolSettingDifference,
}

impl std::fmt::Display for PoolConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.dsn, self.difference)
    }
}

/// Every way an open pool differs from what a `connect()` asked for. It is never empty,
/// because only [`PoolSettingDifferences::of`] makes one.
#[derive(Debug)]
pub(crate) struct PoolSettingDifferences(Vec<PoolSettingDifference>);

impl PoolSettingDifferences {
    /// Returns `None` when `differences` is empty.
    pub(crate) fn of(differences: Vec<PoolSettingDifference>) -> Option<Self> {
        (!differences.is_empty()).then_some(PoolSettingDifferences(differences))
    }
}

impl std::fmt::Display for PoolSettingDifferences {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, difference) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{difference}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PoolSettingDifferences {}

/// One way a `PgPool.connect()` differs from the pool its DSN already has in the run. The DSN
/// is never echoed here because it may carry a password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PoolSettingDifference {
    MaxConnections {
        existing: u32,
        asked: u32,
    },
    AcquireTimeout {
        existing: Duration,
        asked: Duration,
    },
    RuntimeThreads {
        existing: NonZeroUsize,
        asked: NonZeroUsize,
    },
    MaxLifetime {
        existing: Expiry,
        asked: Expiry,
    },
    IdleTimeout {
        existing: Expiry,
        asked: Expiry,
    },
    PingOnAcquire {
        existing: bool,
        asked: bool,
    },
}

impl std::fmt::Display for PoolSettingDifference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const ALREADY: &str = "PgPool is already connected";
        match self {
            Self::MaxConnections { existing, asked } => {
                write!(f, "{ALREADY} with max_connections={existing}, not {asked}")
            }
            Self::AcquireTimeout { existing, asked } => write!(
                f,
                "{ALREADY} with acquire_timeout_secs={}, not {}",
                existing.as_secs(),
                asked.as_secs()
            ),
            Self::RuntimeThreads { existing, asked } => {
                write!(f, "{ALREADY} with runtime_threads={existing}, not {asked}")
            }
            Self::MaxLifetime { existing, asked } => {
                write!(f, "{ALREADY} with max_lifetime_secs={existing}, not {asked}")
            }
            Self::IdleTimeout { existing, asked } => {
                write!(f, "{ALREADY} with idle_timeout_secs={existing}, not {asked}")
            }
            Self::PingOnAcquire { existing, asked } => {
                write!(f, "{ALREADY} with ping_on_acquire={existing}, not {asked}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(dsn: &str, max: Option<u32>, timeout: Option<u64>) -> PoolRequest {
        PoolRequest {
            dsn: Dsn::new(dsn.to_owned()),
            max_connections: max,
            acquire_timeout: timeout.map(Duration::from_secs),
            runtime_threads: None,
            max_lifetime: None,
            idle_timeout: None,
            ping_on_acquire: None,
        }
    }

    fn two() -> NonZeroUsize {
        NonZeroUsize::new(2).unwrap()
    }

    #[test]
    fn a_request_matches_the_pool_it_describes() {
        let existing = request("postgres://a", Some(4), None).settings();
        assert!(request("postgres://a", None, None)
            .differences(&existing, two())
            .is_empty());
        assert!(request("postgres://a", Some(4), Some(30))
            .differences(&existing, two())
            .is_empty());
    }

    #[test]
    fn every_difference_is_named() {
        let existing = request("postgres://a", Some(4), None).settings();
        let found = request("postgres://a", Some(5), Some(5)).differences(&existing, two());
        assert!(matches!(
            found.as_slice(),
            [
                PoolSettingDifference::MaxConnections { existing: 4, asked: 5 },
                PoolSettingDifference::AcquireTimeout { .. },
            ]
        ));
    }

    #[test]
    fn another_runtime_size_expiry_or_ping_setting_differs() {
        let existing = request("postgres://a", None, None).settings();
        let mut asked = request("postgres://a", None, None);
        asked.runtime_threads = two().checked_add(1);
        asked.max_lifetime = Some(Expiry::Never);
        asked.idle_timeout = Some(Expiry::from_secs(1));
        asked.ping_on_acquire = Some(true);
        assert!(matches!(
            asked.differences(&existing, two()).as_slice(),
            [
                PoolSettingDifference::RuntimeThreads { .. },
                PoolSettingDifference::MaxLifetime { .. },
                PoolSettingDifference::IdleTimeout { .. },
                PoolSettingDifference::PingOnAcquire { .. },
            ]
        ));
        asked.runtime_threads = Some(two());
        asked.max_lifetime = Some(DEFAULT_MAX_LIFETIME);
        asked.idle_timeout = Some(DEFAULT_IDLE_TIMEOUT);
        asked.ping_on_acquire = Some(false);
        assert!(asked.differences(&existing, two()).is_empty());
    }

    #[test]
    fn zero_seconds_is_never() {
        assert_eq!(Expiry::from_secs(0), Expiry::Never);
        assert_eq!(Expiry::from_secs(0).to_string(), "0");
        assert_eq!(Expiry::from_secs(300).duration(), Some(Duration::from_secs(300)));
    }

    #[test]
    fn a_run_opens_one_pool_per_dsn_with_its_first_call_s_settings() {
        let first = request("postgres://a", None, Some(5));
        let later = request("postgres://a", None, Some(5));
        let other = request("postgres://b", Some(3), None);
        let plan = plan_run(&[&first, &later, &other], two()).unwrap();
        assert_eq!(plan.threads, two());
        assert_eq!(plan.pools, vec![first.settings(), other.settings()]);
    }

    #[test]
    fn every_conflict_of_a_run_is_named_once_with_its_dsn() {
        let first = request("postgres://u:secret@h/a", Some(2), None);
        let more = request("postgres://u:secret@h/a", Some(3), Some(5));
        let fine = request("postgres://u:secret@h/a", Some(2), None);
        let conflicts = plan_run(&[&first, &more, &fine, &more], two()).unwrap_err();
        let lines: Vec<String> = conflicts.iter().map(ToString::to_string).collect();
        assert_eq!(
            lines,
            [
                "postgres://u:***@h/a: PgPool is already connected with max_connections=2, not 3",
                // The first call left the timeout out: the pool has the default.
                "postgres://u:***@h/a: PgPool is already connected with acquire_timeout_secs=30, not 5",
            ]
        );
    }

    #[test]
    fn the_run_s_runtime_size_is_one_across_dsns() {
        let mut a = request("postgres://a", None, None);
        a.runtime_threads = Some(two());
        let mut b = request("postgres://b", None, None);
        b.runtime_threads = two().checked_add(1);
        let conflicts = plan_run(&[&a, &b], NonZeroUsize::MIN).unwrap_err();
        assert!(matches!(
            conflicts.as_slice(),
            [PoolConflict {
                difference: PoolSettingDifference::RuntimeThreads { .. },
                ..
            }]
        ));
        b.runtime_threads = None;
        assert_eq!(plan_run(&[&a, &b], NonZeroUsize::MIN).unwrap().threads, two());
        assert_eq!(
            plan_run(&[&b], two()).unwrap().threads,
            two(),
            "nobody named it: the default"
        );
    }
}
