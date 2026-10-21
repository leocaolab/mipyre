//! Decides how one server serves: thread-per-core or multi-thread, with or without
//! sub-interpreter workers, how many threads and accept loops.
//!
//! Every decision here is pure: it reads the interpreter mode the code gave, the settings
//! [`EnvConfig`] parsed, the sizes `run()` was given and the CPUs [`super::cpus`] read.

use std::num::NonZeroUsize;

use pyo3::prelude::*;

use mipyre_cpu::Cpus;

use super::config::{EnvConfig, EnvVar, GivenSize, ServerKind, ServerSizes, SizeArg};
use crate::runtime::RouteKinds;
use crate::serve::AcceptMode;
use mipyre_subinterp::gc::GcMode;

/// Says where handlers without `runtime="main"` run; Python sees it as `mipyre.engine.Mode`.
#[pyclass(module = "mipyre.engine", name = "Mode", eq, eq_int, frozen, hash)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum InterpreterMode {
    Gil,
    /// Runs handlers in sub-interpreter workers, except `runtime="main"` routes.
    Subinterp,
}

impl InterpreterMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            InterpreterMode::Gil => "gil",
            InterpreterMode::Subinterp => "subinterp",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("mode {0:?} is not a serving mode; expected \"gil\" (or \"default\") or \"subinterp\" (or \"auto\")")]
pub(crate) struct UnknownInterpreterMode(String);

impl From<UnknownInterpreterMode> for PyErr {
    fn from(e: UnknownInterpreterMode) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

impl std::str::FromStr for InterpreterMode {
    type Err = UnknownInterpreterMode;

    fn from_str(name: &str) -> Result<Self, UnknownInterpreterMode> {
        match name {
            "gil" | "default" => Ok(InterpreterMode::Gil),
            "subinterp" | "auto" => Ok(InterpreterMode::Subinterp),
            _ => Err(UnknownInterpreterMode(name.to_string())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}={mode} is not supported by {topology}", EnvVar::GcMode)]
pub(crate) struct UnsupportedGcMode {
    mode: GcMode,
    topology: Topology,
}

impl From<UnsupportedGcMode> for PyErr {
    fn from(e: UnsupportedGcMode) -> Self {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    }
}

/// Says how one server serves; it is resolved once in `run()` and every serving decision reads
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Topology {
    /// Runs thread-per-core with every handler on the main interpreter.
    TpcGil { threads: NonZeroUsize },
    /// Runs thread-per-core with one sub-interpreter per thread, taking connections as `accept`
    /// says.
    TpcWorkers {
        threads: NonZeroUsize,
        accept: AcceptMode,
    },
    /// Runs a multi-thread Tokio runtime, for `MIPYRE_TPC=0` in GIL mode.
    MultiThreadGil {
        io_threads: NonZeroUsize,
        accept_loops: NonZeroUsize,
    },
    /// Runs a channel pool of workers behind a multi-thread Tokio runtime, for `MIPYRE_TPC=0`
    /// in sub-interpreter mode.
    Pool {
        workers: NonZeroUsize,
        io_threads: NonZeroUsize,
        accept_loops: NonZeroUsize,
    },
}

impl Topology {
    /// Picks the topology for `mode` and `env`, sized from `sizing` or else `cpus`.
    ///
    /// TPC defaults to one thread per physical core, because two TPC threads on SMT siblings
    /// run the same code and thrash their shared L1 (-50% on a 7840HS). The multi-thread
    /// server defaults to the logical count, because work-stealing puts IO and bytecode, with
    /// different footprints, on the siblings.
    ///
    /// # Errors
    ///
    /// Returns [`UnsupportedGcMode`] when the topology cannot run the configured GC mode, before
    /// anything is bound.
    pub(crate) fn resolve(
        mode: InterpreterMode,
        env: &EnvConfig,
        sizing: ServerSizes,
        cpus: Cpus,
    ) -> Result<Self, UnsupportedGcMode> {
        let topology = if let ServerKind::Tpc(accept) = env.server {
            let threads = sizing.workers.map_or(cpus.physical(), |g| g.value);
            match mode {
                InterpreterMode::Gil => Topology::TpcGil { threads },
                InterpreterMode::Subinterp => Topology::TpcWorkers { threads, accept },
            }
        } else {
            let io_threads = sizing.io_workers.map_or(cpus.logical(), |g| g.value);
            let accept_loops = multi_thread_accept_loops(io_threads, cpus.logical());
            match mode {
                InterpreterMode::Gil => Topology::MultiThreadGil {
                    io_threads,
                    accept_loops,
                },
                InterpreterMode::Subinterp => Topology::Pool {
                    workers: sizing.workers.map_or(cpus.logical(), |g| g.value),
                    io_threads,
                    accept_loops,
                },
            }
        };
        let gc = env.tuning.gc.mode();
        if topology.supports(gc) {
            Ok(topology)
        } else {
            Err(UnsupportedGcMode { mode: gc, topology })
        }
    }

    /// Drops the sub-interpreters when no route runs on one, since each would execute the
    /// app's code and load its extensions for nothing.
    pub(crate) fn without_workers(self) -> Self {
        match self {
            Topology::Pool {
                io_threads,
                accept_loops,
                ..
            } => Topology::MultiThreadGil {
                io_threads,
                accept_loops,
            },
            other => other,
        }
    }

    /// Returns one line per size this server does not use, naming where it was given and why.
    /// It is not an error because one script runs under several `MIPYRE_TPC` / `MIPYRE_RUNTIME`
    /// settings and its size is right for one of them.
    pub(crate) fn unused_sizes(self, sizing: ServerSizes) -> Vec<String> {
        let thread_per_core = match self {
            Topology::TpcGil { .. } | Topology::TpcWorkers { .. } => true,
            Topology::MultiThreadGil { .. } | Topology::Pool { .. } => false,
        };
        let line = |size: SizeArg, given: GivenSize, why: &str| {
            format!(
                "{arg}={value} is ignored: it was given as {origin}, and {self} does not use it: {why}.",
                arg = size.arg(),
                value = given.value,
                origin = given.origin.describe(size),
            )
        };
        let mut notes = Vec::new();
        if let (true, Some(given)) = (thread_per_core, sizing.io_workers) {
            notes.push(line(
                SizeArg::IoWorkers,
                given,
                "each thread-per-core thread does its own IO, so there are no IO threads to size \
                 (workers= sizes the TPC threads)",
            ));
        }
        if let (Topology::MultiThreadGil { .. }, Some(given)) = (self, sizing.workers) {
            notes.push(line(
                SizeArg::Workers,
                given,
                "every handler runs on the main interpreter, or no route needs a sub-interpreter, \
                 so no sub-interpreter worker starts (io_workers= sizes its IO threads)",
            ));
        }
        notes
    }

    /// Returns one line per environment setting this server does not use, in the form of
    /// [`unused_sizes`][Self::unused_sizes].
    pub(crate) fn unused_settings(self, env: &EnvConfig) -> Vec<String> {
        match (self, env.server) {
            #[cfg(target_os = "macos")]
            (Topology::TpcGil { .. }, ServerKind::Tpc(AcceptMode::Fanout)) => vec![format!(
                "{}=fanout is ignored: it was given in the environment, and {self} \
                 does not use it: in GIL mode every TPC thread accepts on its own listeners; the \
                 fanout acceptor is for the sub-interpreter TPC server only.",
                EnvVar::TpcDarwin
            )],
            _ => Vec::new(),
        }
    }

    /// Counts the accept loops, each of which opens its own socket per listener.
    pub(crate) fn accept_loops(self) -> NonZeroUsize {
        match self {
            Topology::TpcGil { threads }
            | Topology::TpcWorkers {
                threads,
                accept: AcceptMode::PerThreadListener,
            } => threads,
            #[cfg(target_os = "macos")]
            Topology::TpcWorkers {
                accept: AcceptMode::Fanout,
                ..
            } => NonZeroUsize::MIN,
            Topology::MultiThreadGil { accept_loops, .. } | Topology::Pool { accept_loops, .. } => {
                accept_loops
            }
        }
    }

    /// Says whether this topology can schedule `mode`'s collections. Only the per-thread TPC
    /// listener has the idle tick; GIL topologies run no workers and accept every mode.
    pub(crate) fn supports(self, mode: GcMode) -> bool {
        match self {
            Topology::TpcGil { .. }
            | Topology::TpcWorkers {
                accept: AcceptMode::PerThreadListener,
                ..
            } => true,
            Topology::MultiThreadGil { .. } => true,
            #[cfg(target_os = "macos")]
            Topology::TpcWorkers {
                accept: AcceptMode::Fanout,
                ..
            } => matches!(mode, GcMode::Count | GcMode::Off),
            Topology::Pool { .. } => matches!(mode, GcMode::Count | GcMode::Off),
        }
    }
}

impl std::fmt::Display for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Topology::TpcGil { .. } => "the thread-per-core GIL server",
            Topology::TpcWorkers {
                accept: AcceptMode::PerThreadListener,
                ..
            } => "the thread-per-core sub-interpreter server",
            #[cfg(target_os = "macos")]
            Topology::TpcWorkers {
                accept: AcceptMode::Fanout,
                ..
            } => "the Darwin fanout TPC topology (MIPYRE_TPC_DARWIN=fanout)",
            Topology::MultiThreadGil { .. } => "the multi-thread GIL server (MIPYRE_TPC=0)",
            Topology::Pool { .. } => "the sub-interpreter pool (MIPYRE_TPC=0)",
        })
    }
}

/// Counts the multi-thread server's accept loops; macOS gets one because its `SO_REUSEPORT`
/// does not load-balance connections the way Linux's does.
#[cfg(target_os = "linux")]
fn multi_thread_accept_loops(io_threads: NonZeroUsize, logical: NonZeroUsize) -> NonZeroUsize {
    io_threads.min(logical)
}

#[cfg(not(target_os = "linux"))]
fn multi_thread_accept_loops(_io_threads: NonZeroUsize, _logical: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::MIN
}

/// Counts the async-engine workers for a TPC sub-interpreter server's `async def` routes: one
/// per TPC thread, or half as many (at least one) when `def` routes share the cores.
pub(crate) fn tpc_async_workers(threads: NonZeroUsize, kinds: RouteKinds) -> NonZeroUsize {
    if kinds.has_sync() {
        NonZeroUsize::new(threads.get() / 2).unwrap_or(NonZeroUsize::MIN)
    } else {
        threads
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::boot::config::tests::parse as parse_env;
    use crate::boot::config::SizeOrigin;

    #[test]
    fn mode_names_parse_and_a_typo_is_an_error() {
        assert_eq!("gil".parse(), Ok(InterpreterMode::Gil));
        assert_eq!("default".parse(), Ok(InterpreterMode::Gil));
        assert_eq!("subinterp".parse(), Ok(InterpreterMode::Subinterp));
        assert_eq!("auto".parse(), Ok(InterpreterMode::Subinterp));
        for typo in ["subinterpreter", "async", "GIL", "", "hybrid"] {
            assert_eq!(
                typo.parse::<InterpreterMode>(),
                Err(UnknownInterpreterMode(typo.into()))
            );
        }
        let text = "subinterpter"
            .parse::<InterpreterMode>()
            .unwrap_err()
            .to_string();
        assert!(
            text.contains("\"subinterpter\"") && text.contains("\"gil\""),
            "{text}"
        );
    }

    fn run_sized(value: usize) -> GivenSize {
        GivenSize {
            value: n(value),
            origin: SizeOrigin::RunArg,
        }
    }

    fn n(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    const CPUS: Cpus = Cpus::new(
        NonZeroUsize::new(16).unwrap(),
        NonZeroUsize::new(8).unwrap(),
    );

    fn resolve(
        mode: InterpreterMode,
        vars: &[(&'static str, &str)],
        sizing: ServerSizes,
    ) -> Result<Topology, UnsupportedGcMode> {
        Topology::resolve(mode, &parse_env(vars).unwrap(), sizing, CPUS)
    }

    #[test]
    fn tpc_threads_default_to_what_a_cpu_quota_runs_at_once_not_the_cores_in_the_mask() {
        // With `docker --cpus=2` on a 32-core cpuset, 32 threads would share 2 CPUs of time.
        let quota = Cpus::new(n(2), n(32));
        let env = parse_env(&[]).unwrap();
        let t = Topology::resolve(InterpreterMode::Gil, &env, ServerSizes::default(), quota);
        assert_eq!(t.unwrap(), Topology::TpcGil { threads: n(2) });
    }

    #[test]
    fn tpc_defaults_to_one_thread_per_physical_core() {
        let t = resolve(InterpreterMode::Subinterp, &[], ServerSizes::default()).unwrap();
        assert_eq!(
            t,
            Topology::TpcWorkers {
                threads: n(8),
                accept: AcceptMode::PerThreadListener
            }
        );
        assert_eq!(t.accept_loops(), n(8));
        let t = resolve(InterpreterMode::Gil, &[], ServerSizes::default()).unwrap();
        assert_eq!(t, Topology::TpcGil { threads: n(8) });
        let sized = ServerSizes {
            workers: Some(run_sized(3)),
            io_workers: Some(run_sized(99)),
        };
        let t = resolve(InterpreterMode::Subinterp, &[], sized).unwrap();
        assert_eq!(
            t,
            Topology::TpcWorkers {
                threads: n(3),
                accept: AcceptMode::PerThreadListener
            }
        );
        assert_eq!(t.accept_loops(), n(3));
    }

    #[test]
    fn a_size_the_server_does_not_use_is_named_with_its_source_and_why() {
        // TPC has no IO threads to size, so the size is named rather than refused.
        let io = ServerSizes {
            workers: None,
            io_workers: Some(GivenSize {
                value: n(99),
                origin: SizeOrigin::EnvVar,
            }),
        };
        let topology = resolve(InterpreterMode::Subinterp, &[], io).unwrap();
        let notes = topology.unused_sizes(io);
        assert_eq!(notes.len(), 1, "{notes:?}");
        let note = &notes[0];
        assert!(
            note.contains("io_workers=99")
                && note.contains("MIPYRE_IO_WORKERS")
                && note.contains("does not use it")
                && note.contains("thread-per-core"),
            "{note}"
        );
        let sized = ServerSizes {
            workers: Some(run_sized(3)),
            io_workers: None,
        };
        let topology = resolve(InterpreterMode::Subinterp, &[], sized).unwrap();
        assert!(topology.unused_sizes(sized).is_empty());
        // The pool has no workers to size once no route runs on a sub-interpreter.
        let off = [("MIPYRE_TPC", "0")];
        let workers = ServerSizes {
            workers: Some(run_sized(2)),
            io_workers: None,
        };
        let served = resolve(InterpreterMode::Subinterp, &off, workers)
            .unwrap()
            .without_workers();
        let notes = served.unused_sizes(workers);
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(
            notes[0].contains("workers=2") && notes[0].contains("run(workers=)"),
            "{}",
            notes[0]
        );
    }

    #[test]
    fn tpc_off_sizes_from_the_logical_count() {
        let off = [("MIPYRE_TPC", "0")];
        let t = resolve(InterpreterMode::Subinterp, &off, ServerSizes::default()).unwrap();
        let loops = if cfg!(target_os = "linux") { 16 } else { 1 };
        assert_eq!(
            t,
            Topology::Pool {
                workers: n(16),
                io_threads: n(16),
                accept_loops: n(loops),
            }
        );
        assert_eq!(t.accept_loops(), n(loops));
        let sized = ServerSizes {
            workers: Some(run_sized(2)),
            io_workers: Some(run_sized(4)),
        };
        let t = resolve(InterpreterMode::Gil, &off, sized).unwrap();
        let loops = if cfg!(target_os = "linux") { 4 } else { 1 };
        assert_eq!(
            t,
            Topology::MultiThreadGil {
                io_threads: n(4),
                accept_loops: n(loops),
            }
        );
    }

    #[test]
    fn the_pool_runs_count_and_off_but_not_idle() {
        let off = ("MIPYRE_TPC", "0");
        for mode in ["count", "off"] {
            assert!(resolve(
                InterpreterMode::Subinterp,
                &[off, ("MIPYRE_GC_MODE", mode)],
                ServerSizes::default()
            )
            .is_ok());
        }
        let mode = "idle";
        let err = resolve(
            InterpreterMode::Subinterp,
            &[off, ("MIPYRE_GC_MODE", mode)],
            ServerSizes::default(),
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                UnsupportedGcMode {
                    topology: Topology::Pool { .. },
                    ..
                }
            ),
            "{err:?}"
        );
        let text = err.to_string();
        assert!(
            text.contains(mode) && text.contains("MIPYRE_TPC=0"),
            "{text}"
        );
        assert!(resolve(
            InterpreterMode::Gil,
            &[off, ("MIPYRE_GC_MODE", "idle")],
            ServerSizes::default()
        )
        .is_ok());
    }

    #[test]
    fn per_thread_tpc_runs_every_gc_mode() {
        for mode in ["count", "idle", "off"] {
            assert!(resolve(
                InterpreterMode::Subinterp,
                &[("MIPYRE_GC_MODE", mode)],
                ServerSizes::default()
            )
            .is_ok());
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn darwin_fanout_has_one_accept_loop_and_no_idle_mode() {
        let fanout = ("MIPYRE_TPC_DARWIN", "fanout");
        let t = resolve(
            InterpreterMode::Subinterp,
            &[fanout],
            ServerSizes::default(),
        )
        .unwrap();
        assert_eq!(
            t,
            Topology::TpcWorkers {
                threads: n(8),
                accept: AcceptMode::Fanout
            }
        );
        assert_eq!(t.accept_loops(), NonZeroUsize::MIN);
        assert!(t.supports(GcMode::Off));
        let err = resolve(
            InterpreterMode::Subinterp,
            &[fanout, ("MIPYRE_GC_MODE", "idle")],
            ServerSizes::default(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("idle") && err.to_string().contains("fanout"));
        // Fanout is a sub-interpreter topology, so GIL mode stays per-thread.
        let t = resolve(InterpreterMode::Gil, &[fanout], ServerSizes::default()).unwrap();
        assert_eq!(t, Topology::TpcGil { threads: n(8) });
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_fanout_setting_the_gil_tpc_server_does_not_use_is_named_with_why() {
        let fanout = [("MIPYRE_TPC_DARWIN", "fanout")];
        let env = parse_env(&fanout).unwrap();
        let gil = resolve(InterpreterMode::Gil, &fanout, ServerSizes::default()).unwrap();
        let notes = gil.unused_settings(&env);
        assert_eq!(notes.len(), 1, "{notes:?}");
        let note = &notes[0];
        assert!(
            note.starts_with("MIPYRE_TPC_DARWIN=fanout is ignored")
                && note.contains("given in the environment")
                && note.contains("the thread-per-core GIL server does not use it")
                && note.contains("sub-interpreter TPC server only"),
            "{note}"
        );
        let workers = resolve(InterpreterMode::Subinterp, &fanout, ServerSizes::default()).unwrap();
        assert!(workers.unused_settings(&env).is_empty(), "the fanout server uses it");
        let unset = parse_env(&[]).unwrap();
        let gil = resolve(InterpreterMode::Gil, &[], ServerSizes::default()).unwrap();
        assert!(gil.unused_settings(&unset).is_empty(), "nothing given, nothing named");
    }

    #[test]
    fn tpc_async_pool_shares_the_cores_with_sync_routes() {
        assert_eq!(tpc_async_workers(n(8), RouteKinds::Async), n(8));
        assert_eq!(tpc_async_workers(n(8), RouteKinds::Both), n(4));
        assert_eq!(tpc_async_workers(n(3), RouteKinds::Both), n(1));
        assert_eq!(tpc_async_workers(n(1), RouteKinds::Both), n(1));
        assert_eq!(tpc_async_workers(n(1), RouteKinds::Async), n(1));
    }

}
