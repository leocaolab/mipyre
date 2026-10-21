//! Parses the engine's environment variables once into the server's typed configuration.
//!
//! Nothing deeper in the run path reads the environment. A value that does not parse is a
//! startup error naming the variable and the raw text, never a silent default. Host, port,
//! workers and TLS are read on the Python side (`_ServeSettings`) and passed in.

use std::num::{NonZeroU64, NonZeroUsize};

use pyo3::prelude::*;

use crate::runtime::BridgeConfig;
use mipyre_subinterp::gc::{GcMode, GcPolicy};
use crate::serve::AcceptMode;

/// Names an environment variable the engine reads at start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvVar {
    Tpc,
    TpcDarwin,
    GcMode,
    GcThreshold,
    GcOomFailsafe,
    GcIdleMs,
    BridgeWorkers,
    BridgeCapacity,
    AsyncQueueCapacity,
    Metrics,
}

impl EnvVar {
    pub(crate) fn name(self) -> &'static str {
        match self {
            EnvVar::Tpc => "MIPYRE_TPC",
            EnvVar::TpcDarwin => "MIPYRE_TPC_DARWIN",
            EnvVar::GcMode => "MIPYRE_GC_MODE",
            EnvVar::GcThreshold => "MIPYRE_GC_THRESHOLD",
            EnvVar::GcOomFailsafe => "MIPYRE_GC_OOM_FAILSAFE",
            EnvVar::GcIdleMs => "MIPYRE_GC_IDLE_MS",
            EnvVar::BridgeWorkers => "MIPYRE_GIL_BRIDGE_WORKERS",
            EnvVar::BridgeCapacity => "MIPYRE_GIL_BRIDGE_CAPACITY",
            EnvVar::AsyncQueueCapacity => "MIPYRE_ASYNC_QUEUE_CAPACITY",
            EnvVar::Metrics => "MIPYRE_METRICS",
        }
    }

    /// Returns what the value must be, in the words an error uses.
    pub(crate) fn expected(self) -> &'static str {
        match self {
            EnvVar::Tpc => "expected 1/true/yes/on or 0/false/no/off",
            EnvVar::TpcDarwin => "expected \"fanout\" or \"listener\"",
            EnvVar::GcMode => "expected \"count\", \"idle\" or \"off\"",
            EnvVar::GcThreshold | EnvVar::GcOomFailsafe => "expected a request count (0 or more)",
            EnvVar::GcIdleMs => "expected a positive number of milliseconds",
            EnvVar::BridgeWorkers | EnvVar::BridgeCapacity | EnvVar::AsyncQueueCapacity => {
                "expected a positive integer"
            }
            EnvVar::Metrics => "expected 1 or 0",
        }
    }
}

impl std::fmt::Display for EnvVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Counts the requests a worker serves between `gc.collect()`s in count mode; on the baseline
/// test 5000 gave p99 = 2.0 ms, 100_000 gave about 300 µs, and never collecting gave 240 µs.
const DEFAULT_GC_THRESHOLD: u64 = 100_000;
/// Counts the requests after which idle mode collects even without a lull.
const DEFAULT_GC_OOM_FAILSAFE: u64 = 50_000;
const DEFAULT_GC_IDLE_MS: NonZeroU64 = NonZeroU64::new(100).unwrap();
/// Runs four main-interpreter bridge threads because handlers mix CPU and I/O, and an
/// I/O-bound handler releases the GIL for its peers.
const DEFAULT_BRIDGE_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
/// Gives each bridge thread this many queue slots when the capacity is not set.
const BRIDGE_QUEUE_PER_WORKER: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// Says which set variable cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EnvError {
    #[error("environment variable {var}={raw:?} is invalid: {}", .var.expected())]
    Invalid { var: EnvVar, raw: String },
    /// Carries the value with each invalid UTF-8 sequence replaced.
    #[error("environment variable {var}={lossy:?} is invalid: {}", .var.expected())]
    NotUnicode { var: EnvVar, lossy: String },
    #[cfg(not(target_os = "macos"))]
    #[error("environment variable {}=\"fanout\" is invalid: the fanout topology exists on macOS only", EnvVar::TpcDarwin)]
    FanoutOffMacos,
}

impl From<EnvError> for PyErr {
    fn from(e: EnvError) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

/// Names a size argument, so a note about an unused size can say which.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SizeArg {
    Workers,
    IoWorkers,
}

impl SizeArg {
    pub(super) fn arg(self) -> &'static str {
        match self {
            SizeArg::Workers => "workers",
            SizeArg::IoWorkers => "io_workers",
        }
    }

    fn env(self) -> &'static str {
        match self {
            SizeArg::Workers => "MIPYRE_WORKERS",
            SizeArg::IoWorkers => "MIPYRE_IO_WORKERS",
        }
    }
}

/// Says where a size came from; Python sees it as `mipyre.engine.SizeOrigin`.
#[pyclass(module = "mipyre.engine", name = "SizeOrigin", eq, eq_int, frozen, hash, from_py_object)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SizeOrigin {
    RunArg,
    StartArg,
    EnvVar,
}

impl SizeOrigin {
    /// Returns the origin as the user wrote it, such as `run(workers=)` or `MIPYRE_WORKERS`.
    pub(super) fn describe(self, size: SizeArg) -> String {
        match self {
            SizeOrigin::RunArg => format!("run({}=)", size.arg()),
            SizeOrigin::StartArg => format!("engine.start({}=)", size.arg()),
            SizeOrigin::EnvVar => size.env().to_string(),
        }
    }
}

/// Holds a size with where it came from, so a note about an unused size can name its origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GivenSize {
    pub(crate) value: NonZeroUsize,
    pub(crate) origin: SizeOrigin,
}

/// Holds the sizes `run()` was given; `None` takes the topology's default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ServerSizes {
    /// Sizes the TPC threads, or the pool's sub-interpreters.
    pub(crate) workers: Option<GivenSize>,
    /// Sizes the Tokio threads of the multi-thread (`MIPYRE_TPC=0`) server.
    pub(crate) io_workers: Option<GivenSize>,
}

/// Says which server `MIPYRE_TPC` selects; only [`Topology::resolve`] reads it.
///
/// [`Topology::resolve`]: super::topology::Topology::resolve
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ServerKind {
    /// Runs thread-per-core, which is the default.
    Tpc(AcceptMode),
    /// Runs the channel pool, or multi-thread Tokio in GIL mode, for `MIPYRE_TPC=0`.
    MultiThread,
}

/// Holds everything the engine takes from the environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EnvConfig {
    pub(super) server: ServerKind,
    pub(crate) tuning: EngineSettings,
}

/// Holds the environment settings a resolved server runs with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EngineSettings {
    pub(crate) gc: GcPolicy,
    pub(crate) bridge: BridgeConfig,
    /// Counts the requests the async worker pool queues before it answers 503; `None` keeps the
    /// pool's default.
    pub(crate) async_queue_capacity: Option<NonZeroUsize>,
    /// Turns on the hot-path counters and the RSS sampler.
    pub(crate) metrics: bool,
}

impl EnvConfig {
    /// Parses the process environment.
    ///
    /// # Errors
    ///
    /// Returns [`EnvError`] for the first set variable that cannot be used.
    pub(crate) fn from_env() -> Result<Self, EnvError> {
        Self::parse(|var| {
            std::env::var_os(var).map(|raw| {
                raw.into_string()
                    .map_err(|raw| raw.to_string_lossy().into_owned())
            })
        })
    }

    /// Parses the variables `lookup` returns, which gives `None` when unset and `Err` with the
    /// lossy text when not UTF-8.
    ///
    /// # Errors
    ///
    /// Returns [`EnvError`] for the first set variable that cannot be used.
    pub(crate) fn parse(
        lookup: impl Fn(&'static str) -> Option<Result<String, String>>,
    ) -> Result<Self, EnvError> {
        let get = |var: EnvVar| -> Result<Option<String>, EnvError> {
            match lookup(var.name()) {
                None => Ok(None),
                Some(Ok(raw)) => Ok(Some(raw)),
                Some(Err(lossy)) => Err(EnvError::NotUnicode { var, lossy }),
            }
        };
        let invalid = |var: EnvVar, raw: String| EnvError::Invalid { var, raw };

        let tpc = match get(EnvVar::Tpc)? {
            None => true,
            Some(raw) => parse_bool(&raw).ok_or_else(|| invalid(EnvVar::Tpc, raw))?,
        };

        // The value is checked even when TPC is off and ignores it.
        let accept = match get(EnvVar::TpcDarwin)?.as_deref() {
            None | Some("listener") => AcceptMode::PerThreadListener,
            #[cfg(target_os = "macos")]
            Some("fanout") => AcceptMode::Fanout,
            #[cfg(not(target_os = "macos"))]
            Some("fanout") => return Err(EnvError::FanoutOffMacos),
            Some(other) => return Err(invalid(EnvVar::TpcDarwin, other.into())),
        };

        let mode = match get(EnvVar::GcMode)? {
            None => GcMode::Count,
            Some(raw) => match raw.parse::<GcMode>() {
                Ok(mode) => mode,
                Err(_unknown) => return Err(invalid(EnvVar::GcMode, raw)),
            },
        };
        let count = |var: EnvVar, default: u64| -> Result<u64, EnvError> {
            match get(var)? {
                None => Ok(default),
                Some(raw) => raw.parse().map_err(|_| invalid(var, raw)),
            }
        };
        let idle_ms = match get(EnvVar::GcIdleMs)? {
            None => DEFAULT_GC_IDLE_MS,
            Some(raw) => raw
                .parse::<NonZeroU64>()
                .map_err(|_| invalid(EnvVar::GcIdleMs, raw))?,
        };
        // Every number is checked whichever mode uses it, so a bad value is an error even when
        // this mode ignores it.
        let threshold = NonZeroU64::new(count(EnvVar::GcThreshold, DEFAULT_GC_THRESHOLD)?);
        let failsafe = NonZeroU64::new(count(EnvVar::GcOomFailsafe, DEFAULT_GC_OOM_FAILSAFE)?);
        let gc = match mode {
            // A threshold of 0 never collects, so it is spelled as `off`.
            GcMode::Count => match threshold {
                Some(every) => GcPolicy::Count { every },
                None => GcPolicy::Off,
            },
            GcMode::Idle => GcPolicy::Idle {
                failsafe,
                tick: std::time::Duration::from_millis(idle_ms.get()),
            },
            GcMode::Off => GcPolicy::Off,
        };

        let positive = |var: EnvVar| -> Result<Option<NonZeroUsize>, EnvError> {
            get(var)?
                .map(|raw| raw.parse::<NonZeroUsize>().map_err(|_| invalid(var, raw)))
                .transpose()
        };
        let workers = positive(EnvVar::BridgeWorkers)?.unwrap_or(DEFAULT_BRIDGE_WORKERS);
        let capacity = match positive(EnvVar::BridgeCapacity)? {
            Some(capacity) => capacity,
            None => workers.saturating_mul(BRIDGE_QUEUE_PER_WORKER),
        };

        let async_queue_capacity = positive(EnvVar::AsyncQueueCapacity)?;

        let metrics = match get(EnvVar::Metrics)?.as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            Some(other) => return Err(invalid(EnvVar::Metrics, other.into())),
        };

        Ok(EnvConfig {
            server: if tpc {
                ServerKind::Tpc(accept)
            } else {
                ServerKind::MultiThread
            },
            tuning: EngineSettings {
                gc,
                bridge: BridgeConfig { workers, capacity },
                async_queue_capacity,
                metrics,
            },
        })
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    /// Parses `vars` as the whole environment, every other variable unset.
    pub(in crate::boot) fn parse(vars: &[(&'static str, &str)]) -> Result<EnvConfig, EnvError> {
        EnvConfig::parse(|var| {
            vars.iter()
                .find(|(k, _)| *k == var)
                .map(|(_, v)| Ok(v.to_string()))
        })
    }

    struct Invalid {
        var: &'static str,
        raw: String,
    }

    fn env_error(vars: &[(&'static str, &str)]) -> Invalid {
        match parse(vars).expect_err("expected an EnvError") {
            EnvError::Invalid { var, raw } => Invalid { var: var.name(), raw },
            other => panic!("expected EnvError::Invalid, got {other:?}"),
        }
    }

    #[test]
    fn nothing_set_is_the_documented_defaults() {
        let config = parse(&[]).unwrap();
        assert_eq!(
            config.server,
            ServerKind::Tpc(AcceptMode::PerThreadListener)
        );
        assert_eq!(
            config.tuning.gc,
            GcPolicy::Count {
                every: NonZeroU64::new(100_000).expect("Proof: 100_000 is not 0"),
            }
        );
        assert_eq!(
            parse(&[("MIPYRE_GC_MODE", "idle")]).unwrap().tuning.gc,
            GcPolicy::Idle {
                failsafe: NonZeroU64::new(50_000),
                tick: std::time::Duration::from_millis(100),
            }
        );
        assert_eq!(config.tuning.bridge.workers.get(), 4);
        assert_eq!(config.tuning.bridge.capacity.get(), 64);
        assert!(!config.tuning.metrics);
    }

    #[test]
    fn tpc_is_a_boolean() {
        let server = |raw| parse(&[("MIPYRE_TPC", raw)]).unwrap().server;
        assert_eq!(server("0"), ServerKind::MultiThread);
        assert_eq!(server("off"), ServerKind::MultiThread);
        assert_eq!(server("1"), ServerKind::Tpc(AcceptMode::PerThreadListener));
        let e = env_error(&[("MIPYRE_TPC", "maybe")]);
        assert_eq!((e.var, e.raw.as_str()), ("MIPYRE_TPC", "maybe"));
    }

    #[test]
    fn gc_numbers_must_parse() {
        let config = parse(&[("MIPYRE_GC_THRESHOLD", "0")]).unwrap();
        assert_eq!(config.tuning.gc, GcPolicy::Off);
        let config = parse(&[
            ("MIPYRE_GC_MODE", "idle"),
            ("MIPYRE_GC_OOM_FAILSAFE", "7"),
            ("MIPYRE_GC_IDLE_MS", "5"),
        ])
        .unwrap();
        assert_eq!(
            config.tuning.gc,
            GcPolicy::Idle {
                failsafe: NonZeroU64::new(7),
                tick: std::time::Duration::from_millis(5),
            }
        );
        for (var, raw) in [
            ("MIPYRE_GC_THRESHOLD", "5k"),
            ("MIPYRE_GC_THRESHOLD", "-1"),
            ("MIPYRE_GC_OOM_FAILSAFE", "lots"),
            ("MIPYRE_GC_IDLE_MS", "0"),
            ("MIPYRE_GC_IDLE_MS", "1.5"),
        ] {
            let e = env_error(&[(var, raw)]);
            assert_eq!((e.var, e.raw.as_str()), (var, raw));
        }
        let e = env_error(&[("MIPYRE_GC_MODE", "idel")]);
        assert_eq!((e.var, e.raw.as_str()), ("MIPYRE_GC_MODE", "idel"));
    }

    #[test]
    fn each_gc_mode_takes_its_own_numbers() {
        let numbers = [
            ("MIPYRE_GC_THRESHOLD", "10"),
            ("MIPYRE_GC_OOM_FAILSAFE", "20"),
        ];
        let policy = |mode| {
            let mut vars = numbers.to_vec();
            vars.push(("MIPYRE_GC_MODE", mode));
            parse(&vars).unwrap().tuning.gc
        };
        assert_eq!(
            policy("count"),
            GcPolicy::Count {
                every: NonZeroU64::new(10).expect("Proof: 10 is not 0"),
            }
        );
        assert_eq!(
            policy("idle"),
            GcPolicy::Idle {
                failsafe: NonZeroU64::new(20),
                tick: std::time::Duration::from_millis(100),
            }
        );
        assert_eq!(policy("off"), GcPolicy::Off);
    }

    #[test]
    fn bridge_sizes_are_positive() {
        let config = parse(&[("MIPYRE_GIL_BRIDGE_WORKERS", "2")]).unwrap();
        assert_eq!(config.tuning.bridge.capacity.get(), 32);
        let config = parse(&[("MIPYRE_GIL_BRIDGE_CAPACITY", "5")]).unwrap();
        assert_eq!(config.tuning.bridge.capacity.get(), 5);
        for (var, raw) in [
            ("MIPYRE_GIL_BRIDGE_CAPACITY", "0"),
            ("MIPYRE_GIL_BRIDGE_CAPACITY", "big"),
            ("MIPYRE_GIL_BRIDGE_WORKERS", "0"),
            ("MIPYRE_GIL_BRIDGE_WORKERS", "four"),
        ] {
            let e = env_error(&[(var, raw)]);
            assert_eq!((e.var, e.raw.as_str()), (var, raw));
        }
    }

    #[test]
    fn async_queue_capacity_is_positive_and_unset_by_default() {
        assert_eq!(parse(&[]).unwrap().tuning.async_queue_capacity, None);
        let config = parse(&[("MIPYRE_ASYNC_QUEUE_CAPACITY", "40000")]).unwrap();
        assert_eq!(
            config.tuning.async_queue_capacity.map(NonZeroUsize::get),
            Some(40000)
        );
        for raw in ["0", "-1", "lots"] {
            let e = env_error(&[("MIPYRE_ASYNC_QUEUE_CAPACITY", raw)]);
            assert_eq!(
                (e.var, e.raw.as_str()),
                ("MIPYRE_ASYNC_QUEUE_CAPACITY", raw)
            );
        }
    }

    #[test]
    fn metrics_and_topology_take_their_values_only() {
        assert!(parse(&[("MIPYRE_METRICS", "1")]).unwrap().tuning.metrics);
        assert_eq!(
            env_error(&[("MIPYRE_METRICS", "yes")]).var,
            "MIPYRE_METRICS"
        );
        assert_eq!(
            env_error(&[("MIPYRE_TPC_DARWIN", "fan-out")]).var,
            "MIPYRE_TPC_DARWIN"
        );
        let fanout = parse(&[("MIPYRE_TPC_DARWIN", "fanout")]);
        #[cfg(target_os = "macos")]
        assert_eq!(fanout.unwrap().server, ServerKind::Tpc(AcceptMode::Fanout));
        #[cfg(not(target_os = "macos"))]
        assert!(fanout.is_err());
    }

    #[test]
    fn gc_mode_parses_the_three_modes_and_nothing_else() {
        assert_eq!("count".parse::<GcMode>().ok(), Some(GcMode::Count));
        assert_eq!("idle".parse::<GcMode>().ok(), Some(GcMode::Idle));
        assert_eq!("off".parse::<GcMode>().ok(), Some(GcMode::Off));
        for raw in ["idel", "", "IDLE", " idle"] {
            assert_eq!(raw.parse::<GcMode>().ok(), None);
        }
        let message = parse(&[("MIPYRE_GC_MODE", "idel")])
            .expect_err("expected an EnvError")
            .to_string();
        assert!(message.contains("MIPYRE_GC_MODE") && message.contains("\"idel\""));
    }

    #[test]
    fn a_non_utf8_value_is_an_error_not_unset() {
        let result = EnvConfig::parse(|var| {
            (var == "MIPYRE_GIL_BRIDGE_WORKERS").then(|| Err("\u{fffd}".to_string()))
        });
        assert!(matches!(
            result,
            Err(EnvError::NotUnicode {
                var: EnvVar::BridgeWorkers,
                ..
            })
        ));
    }
}
