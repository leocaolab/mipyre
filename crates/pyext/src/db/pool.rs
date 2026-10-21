//! Keeps the process's open pools, one per DSN, and decides whether a `PgPool.connect()` may
//! reuse its DSN's pool.
//!
//! A later `connect()` to an open DSN reuses its pool only when every setting it names matches,
//! and is refused with every setting that differs.

use std::num::NonZeroUsize;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

use pyo3::Python;

use super::dsn::Dsn;
use super::error::{DbError, TaskError};
use super::runtime::{db_runtime, default_runtime_threads, run_on_db_rt, runtime};

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

/// Holds the process's open pools, one per DSN; a `Vec` suffices because there are a handful at
/// most and it is searched once per `connect()`.
static PG_POOLS: parking_lot::Mutex<Vec<PoolHandle>> = parking_lot::Mutex::new(Vec::new());

/// Keeps an open pool with the settings it was opened with, so a later `connect` to its DSN can
/// be checked against them.
struct PoolHandle {
    settings: PoolSettings,
    pool: sqlx::PgPool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Expiry {
    After(Duration),
    Never,
}

impl Expiry {
    /// Reads `PgPool.connect()` seconds, where 0 means never.
    pub(super) fn from_secs(secs: u64) -> Self {
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

#[derive(Clone)]
struct PoolSettings {
    dsn: Dsn,
    max_connections: u32,
    acquire_timeout: Duration,
    runtime_threads: NonZeroUsize,
    max_lifetime: Expiry,
    idle_timeout: Expiry,
    ping_on_acquire: bool,
}

/// Holds what a `PgPool.connect()` call asked for; `None` accepts whatever the pool has.
pub(super) struct PoolRequest {
    pub(super) dsn: Dsn,
    pub(super) max_connections: Option<u32>,
    pub(super) acquire_timeout: Option<Duration>,
    pub(super) runtime_threads: Option<NonZeroUsize>,
    pub(super) max_lifetime: Option<Expiry>,
    pub(super) idle_timeout: Option<Expiry>,
    pub(super) ping_on_acquire: Option<bool>,
}

impl PoolRequest {
    fn settings(&self) -> Result<PoolSettings, TaskError> {
        let runtime_threads = match self.runtime_threads {
            Some(threads) => threads,
            None => default_runtime_threads().map_err(TaskError::RuntimeNotBuilt)?,
        };
        Ok(PoolSettings {
            dsn: self.dsn.clone(),
            max_connections: self.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS),
            acquire_timeout: self.acquire_timeout.unwrap_or(DEFAULT_ACQUIRE_TIMEOUT),
            runtime_threads,
            max_lifetime: self.max_lifetime.unwrap_or(DEFAULT_MAX_LIFETIME),
            idle_timeout: self.idle_timeout.unwrap_or(DEFAULT_IDLE_TIMEOUT),
            ping_on_acquire: self.ping_on_acquire.unwrap_or(DEFAULT_PING_ON_ACQUIRE),
        })
    }

    /// Lists how the open pool's `existing` settings differ from what this call asked for.
    fn differences(&self, existing: &PoolSettings) -> Vec<PoolSettingDifference> {
        let max_connections = self
            .max_connections
            .filter(|&asked| asked != existing.max_connections)
            .map(|asked| PoolSettingDifference::MaxConnections {
                existing: existing.max_connections,
                asked,
            });
        let acquire_timeout = self
            .acquire_timeout
            .filter(|&asked| asked != existing.acquire_timeout)
            .map(|asked| PoolSettingDifference::AcquireTimeout {
                existing: existing.acquire_timeout,
                asked,
            });
        let runtime_threads = self
            .runtime_threads
            .filter(|&asked| asked != existing.runtime_threads)
            .map(|asked| PoolSettingDifference::RuntimeThreads {
                existing: existing.runtime_threads,
                asked,
            });
        let max_lifetime = self
            .max_lifetime
            .filter(|&asked| asked != existing.max_lifetime)
            .map(|asked| PoolSettingDifference::MaxLifetime {
                existing: existing.max_lifetime,
                asked,
            });
        let idle_timeout = self
            .idle_timeout
            .filter(|&asked| asked != existing.idle_timeout)
            .map(|asked| PoolSettingDifference::IdleTimeout {
                existing: existing.idle_timeout,
                asked,
            });
        let ping_on_acquire = self
            .ping_on_acquire
            .filter(|&asked| asked != existing.ping_on_acquire)
            .map(|asked| PoolSettingDifference::PingOnAcquire {
                existing: existing.ping_on_acquire,
                asked,
            });
        [
            max_connections,
            acquire_timeout,
            runtime_threads,
            max_lifetime,
            idle_timeout,
            ping_on_acquire,
        ]
        .into_iter()
        .flatten()
        .collect()
    }
}

/// Allows a call to use its DSN's open pool only if every setting it names matches, because a
/// DSN has one pool and a call asking for another sizing could not get it.
///
/// # Errors
///
/// Returns [`PoolSettingDifferences`] listing every setting that differs.
fn reuse(request: &PoolRequest, existing: &PoolSettings) -> Result<(), PoolSettingDifferences> {
    match PoolSettingDifferences::of(request.differences(existing)) {
        None => Ok(()),
        Some(mismatch) => Err(mismatch),
    }
}

fn open_pool(dsn: &Dsn) -> Option<(sqlx::PgPool, PoolSettings)> {
    PG_POOLS
        .lock()
        .iter()
        .find(|open| open.settings.dsn == *dsn)
        .map(|open| (open.pool.clone(), open.settings.clone()))
}

/// Returns the DSN's open pool if it can serve `request`, else opens a new one.
///
/// # Errors
///
/// Returns [`DbError`] when the settings differ from the open pool's, the runtime cannot be
/// built, or the connection fails.
pub(super) fn connect_pool(py: Python<'_>, request: PoolRequest) -> Result<sqlx::PgPool, DbError> {
    if let Some((pool, settings)) = open_pool(&request.dsn) {
        reuse(&request, &settings)?;
        return Ok(pool);
    }

    // The runtime may already run for another pool, so the pool gets the threads it has and a
    // request naming another count is refused.
    let mut settings = request.settings()?;
    settings.runtime_threads = db_runtime(Some(settings.runtime_threads))?.threads;
    reuse(&request, &settings)?;
    // Without `ping_on_acquire`, a connection that died while idle fails its query with the raw
    // error, and sqlx's ping on release then closes it, so the next query gets a fresh one.
    let options = PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .acquire_timeout(settings.acquire_timeout)
        .max_lifetime(settings.max_lifetime.duration())
        .idle_timeout(settings.idle_timeout.duration())
        .test_before_acquire(settings.ping_on_acquire);
    let dsn = settings.dsn.expose_secret().to_owned();
    let pool = py
        .detach(|| run_on_db_rt(async move { options.connect(&dsn).await }))?
        .map_err(DbError::ConnectFailed)?;

    // Two first calls for one DSN can race because each worker runs the app script, so the
    // loser closes its pool and is checked against the winner.
    let mut pools = PG_POOLS.lock();
    if let Some(winner) = pools.iter().find(|open| open.settings.dsn == settings.dsn) {
        let (winner, winner_settings) = (winner.pool.clone(), winner.settings.clone());
        drop(pools);
        runtime()?.spawn(async move { pool.close().await });
        reuse(&request, &winner_settings)?;
        return Ok(winner);
    }
    pools.push(PoolHandle {
        settings,
        pool: pool.clone(),
    });
    Ok(pool)
}

/// Every way the process's pool differs from what a `connect()` asked for. It is never empty,
/// because only [`PoolSettingDifferences::of`] makes one.
#[derive(Debug)]
pub(super) struct PoolSettingDifferences(Vec<PoolSettingDifference>);

impl PoolSettingDifferences {
    /// Returns `None` when `differences` is empty.
    fn of(differences: Vec<PoolSettingDifference>) -> Option<Self> {
        (!differences.is_empty()).then_some(PoolSettingDifferences(differences))
    }

    /// Returns the differences for a test; outside tests only the message carries them.
    #[cfg(test)]
    fn into_differences(self) -> Vec<PoolSettingDifference> {
        self.0
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

/// One way a later `PgPool.connect()` differs from the pool its DSN already has. Any difference
/// refuses the call, and the DSN is never echoed because it may carry a password.
#[derive(Debug)]
pub(super) enum PoolSettingDifference {
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
                write!(
                    f,
                    "{ALREADY} with max_lifetime_secs={existing}, not {asked}"
                )
            }
            Self::IdleTimeout { existing, asked } => {
                write!(
                    f,
                    "{ALREADY} with idle_timeout_secs={existing}, not {asked}"
                )
            }
            Self::PingOnAcquire { existing, asked } => {
                write!(f, "{ALREADY} with ping_on_acquire={existing}, not {asked}")
            }
        }
    }
}

/// Pool settings are checked.
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

    #[test]
    fn a_request_matches_the_pool_it_describes() {
        let existing = request("postgres://a", Some(4), None).settings().unwrap();
        assert!(request("postgres://a", None, None)
            .differences(&existing)
            .is_empty());
        assert!(request("postgres://a", Some(4), Some(30))
            .differences(&existing)
            .is_empty());
    }

    #[test]
    fn every_difference_is_named() {
        let existing = request("postgres://a", Some(4), None).settings().unwrap();
        let found = request("postgres://a", Some(5), Some(5)).differences(&existing);
        assert!(matches!(
            found.as_slice(),
            [
                PoolSettingDifference::MaxConnections {
                    existing: 4,
                    asked: 5
                },
                PoolSettingDifference::AcquireTimeout { .. },
            ]
        ));
    }

    fn refused(reused: Result<(), PoolSettingDifferences>) -> Vec<PoolSettingDifference> {
        reused.expect_err("refused").into_differences()
    }

    #[test]
    fn any_difference_is_refused() {
        let existing = request("postgres://a", Some(4), None).settings().unwrap();
        assert!(matches!(
            refused(reuse(&request("postgres://a", Some(5), None), &existing)).as_slice(),
            [PoolSettingDifference::MaxConnections {
                existing: 4,
                asked: 5
            }]
        ));
        assert!(matches!(
            refused(reuse(&request("postgres://a", None, Some(5)), &existing)).as_slice(),
            [PoolSettingDifference::AcquireTimeout { .. }]
        ));
    }

    #[test]
    fn another_runtime_size_is_refused() {
        let existing = request("postgres://a", None, None).settings().unwrap();
        let mut asked = request("postgres://a", None, None);
        asked.runtime_threads = existing.runtime_threads.checked_add(1);
        assert!(matches!(
            refused(reuse(&asked, &existing)).as_slice(),
            [PoolSettingDifference::RuntimeThreads { .. }]
        ));
        asked.runtime_threads = Some(existing.runtime_threads);
        assert!(reuse(&asked, &existing).is_ok());
    }

    #[test]
    fn another_expiry_or_ping_setting_is_refused() {
        let existing = request("postgres://a", None, None).settings().unwrap();
        let mut asked = request("postgres://a", None, None);
        asked.max_lifetime = Some(Expiry::Never);
        assert!(matches!(
            refused(reuse(&asked, &existing)).as_slice(),
            [PoolSettingDifference::MaxLifetime { .. }]
        ));
        asked.max_lifetime = Some(DEFAULT_MAX_LIFETIME);
        asked.idle_timeout = Some(Expiry::from_secs(1));
        assert!(matches!(
            refused(reuse(&asked, &existing)).as_slice(),
            [PoolSettingDifference::IdleTimeout { .. }]
        ));
        asked.idle_timeout = Some(DEFAULT_IDLE_TIMEOUT);
        asked.ping_on_acquire = Some(true);
        assert!(matches!(
            refused(reuse(&asked, &existing)).as_slice(),
            [PoolSettingDifference::PingOnAcquire { .. }]
        ));
        asked.ping_on_acquire = Some(false);
        assert!(reuse(&asked, &existing).is_ok());
    }

    #[test]
    fn zero_seconds_is_never() {
        assert_eq!(Expiry::from_secs(0), Expiry::Never);
        assert_eq!(Expiry::from_secs(0).to_string(), "0");
        assert_eq!(
            Expiry::from_secs(300).duration(),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn settings_left_out_or_equal_are_reused() {
        let existing = request("postgres://a", Some(4), None).settings().unwrap();
        assert!(reuse(&request("postgres://a", None, None), &existing).is_ok());
        assert!(reuse(&request("postgres://a", Some(4), Some(30)), &existing).is_ok());
    }
}
