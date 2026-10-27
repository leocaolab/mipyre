//! Decides how one server serves: thread-per-core or multi-thread, with or without
//! sub-interpreter workers, how many threads and accept loops. The thread-per-core server
//! starts workers exactly when a route runs on one; its main-interpreter routes go to the
//! bridge either way.
//!
//! Every decision here is pure: it reads the interpreter mode the route table implies, the settings
//! the server's flags settled ([`ServerRecipe`]) and the CPUs [`super::cpus`] read.

use std::num::NonZeroUsize;

use pyo3::prelude::*;

use mipyre_cpu::Cpus;

use super::config::{FlagError, ServerKind, ServerRecipe};
use super::settings::{FlagUse, ServerFlagValues};
use mipyre_runtime::RouteKinds;
use mipyre_server::AcceptMode;
use mipyre_subinterp::gc::GcMode;

/// Says whether the multi-thread server runs a pool of sub-interpreter workers: it does exactly
/// when a route of its sealed table runs on one, so one table is always served by one topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InterpreterMode {
    /// Runs every handler on the main interpreter or a process lane.
    Gil,
    /// Runs handlers in sub-interpreter workers, except `runtime="main"` and process routes.
    Subinterp,
}

impl InterpreterMode {
    pub(crate) fn of(routes: &mipyre_runtime::Routes) -> Self {
        if routes.uses_workers() {
            InterpreterMode::Subinterp
        } else {
            InterpreterMode::Gil
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("gc_mode={mode} is not supported by {topology}")]
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
    /// Runs thread-per-core, taking connections as `accept` says: with one sub-interpreter per
    /// thread when a route runs on one, and with none otherwise.
    Tpc { threads: NonZeroUsize, accept: AcceptMode },
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
    /// Picks the topology for `env`, and for `mode` when the server is multi-thread, sized from
    /// `sizing` or else `cpus`.
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
    pub(crate) fn resolve(mode: InterpreterMode, recipe: &ServerRecipe, cpus: Cpus) -> Result<Self, UnsupportedGcMode> {
        let sizing = &recipe.sizes;
        let topology = if let ServerKind::Tpc(accept) = recipe.server {
            let threads = sizing.threads.as_ref().map_or(cpus.physical(), |g| *g.value());
            Topology::Tpc { threads, accept }
        } else {
            let io_threads = sizing.threads.as_ref().map_or(cpus.logical(), |g| *g.value());
            let accept_loops = io_threads.min(cpus.logical());
            match mode {
                InterpreterMode::Gil => Topology::MultiThreadGil {
                    io_threads,
                    accept_loops,
                },
                InterpreterMode::Subinterp => Topology::Pool {
                    workers: sizing
                        .subinterpreters
                        .as_ref()
                        .map_or(cpus.logical().max(POOL_DEFAULT_MIN), |g| *g.value()),
                    io_threads,
                    accept_loops,
                },
            }
        };
        let gc = recipe.tuning.gc.mode();
        if topology.supports(gc) {
            Ok(topology)
        } else {
            Err(UnsupportedGcMode { mode: gc, topology })
        }
    }

    /// Counts the accept loops, each of which opens its own socket per listener.
    pub(crate) fn accept_loops(self) -> NonZeroUsize {
        match self {
            Topology::Tpc {
                threads,
                accept: AcceptMode::PerThreadListener,
            } => threads,
            #[cfg(target_os = "macos")]
            Topology::Tpc {
                accept: AcceptMode::Fanout,
                ..
            } => NonZeroUsize::MIN,
            Topology::MultiThreadGil { accept_loops, .. } | Topology::Pool { accept_loops, .. } => accept_loops,
        }
    }

    /// Says whether this topology can schedule `mode`'s collections. Only the per-thread TPC
    /// listener has the idle tick; the multi-thread GIL server runs no workers and accepts every
    /// mode.
    pub(crate) fn supports(self, mode: GcMode) -> bool {
        match self {
            Topology::Tpc {
                accept: AcceptMode::PerThreadListener,
                ..
            }
            | Topology::MultiThreadGil { .. } => true,
            #[cfg(target_os = "macos")]
            Topology::Tpc {
                accept: AcceptMode::Fanout,
                ..
            } => matches!(mode, GcMode::Count | GcMode::Off),
            Topology::Pool { .. } => matches!(mode, GcMode::Count | GcMode::Off),
        }
    }
}

/// What decides which of the given flags a server uses: its topology, what its routes need
/// and how it takes connections.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ServerLayout {
    pub(crate) topology: Topology,
    /// Some route runs on a sub-interpreter worker: the thread-per-core server then gives each
    /// thread one.
    pub(crate) worker_routes: bool,
    /// Some route runs on the main interpreter.
    pub(crate) main_routes: bool,
    /// Some route on a sub-interpreter worker is `async def`.
    pub(crate) async_worker_routes: bool,
    /// Some route has `runtime="process"`.
    pub(crate) process_routes: bool,
    /// The server takes TCP connections (not only in-memory ones).
    pub(crate) tcp: bool,
}

impl ServerLayout {
    /// Says whether this server uses a value of a flag that is used as `used` says.
    pub(crate) fn uses(&self, used: FlagUse) -> bool {
        match used {
            FlagUse::Always => true,
            FlagUse::Pool => matches!(self.topology, Topology::Pool { .. }),
            FlagUse::MacosTpc => cfg!(target_os = "macos") && matches!(self.topology, Topology::Tpc { .. }),
            FlagUse::SubinterpreterWorkers => self.workers() > 0,
            FlagUse::MainRoutes => self.main_routes,
            FlagUse::AsyncWorkerRoutes => self.async_worker_routes && self.workers() > 0,
            FlagUse::ProcessRoutes => self.process_routes,
            FlagUse::TcpTransport => self.tcp,
            // A server flag is never used by entry point; the entry checks those.
            FlagUse::ReloadingEntry => true,
        }
    }

    /// Counts the sub-interpreter workers: one per thread-per-core thread when a route runs on
    /// one, the pool's size, or none.
    pub(crate) fn workers(&self) -> usize {
        match self.topology {
            Topology::Tpc { threads, .. } if self.worker_routes => threads.get(),
            Topology::Pool { workers, .. } => workers.get(),
            Topology::Tpc { .. } | Topology::MultiThreadGil { .. } => 0,
        }
    }

    /// Returns one error per flag `values` gives (from the deployment or from code, not the
    /// default) that this server does not use.
    pub(crate) fn unused(&self, values: &ServerFlagValues) -> Vec<FlagError> {
        values
            .given()
            .into_iter()
            .filter(|(flag, _)| !self.uses(flag.used))
            .map(|(flag, given)| FlagError::UnusedByTopology {
                flag: flag.name,
                given,
                user: self.topology.to_string(),
                needs: flag.used.users(),
            })
            .collect()
    }
}

impl std::fmt::Display for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Topology::Tpc {
                accept: AcceptMode::PerThreadListener,
                ..
            } => "the thread-per-core server",
            #[cfg(target_os = "macos")]
            Topology::Tpc {
                accept: AcceptMode::Fanout,
                ..
            } => "the Darwin fanout TPC topology (MIPYRE_TPC_ACCEPT=fanout)",
            Topology::MultiThreadGil { .. } => "the multi-thread GIL server (MIPYRE_TPC=0)",
            Topology::Pool { .. } => "the sub-interpreter pool (MIPYRE_TPC=0)",
        })
    }
}

/// The fewest sub-interpreters the pool defaults to: `def` and `async def` handlers each need one
/// of their own, and a one-CPU host must still start an app that has both.
const POOL_DEFAULT_MIN: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1);

/// Counts the async-engine workers for a TPC server's `async def` routes: one
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
    use std::ffi::OsString;

    use crate::boot::config::ServerFlagsError;
    use crate::boot::settings::{CodeCall, Deployment, EntryPoint, ServerFlags};

    fn n(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    const CPUS: Cpus = Cpus::new(NonZeroUsize::new(16).unwrap(), NonZeroUsize::new(8).unwrap());

    /// The recipe of a server whose environment is `vars` and whose code gives `code`
    /// (`ServerFlags(...)`'s keywords).
    fn recipe(vars: &[(&str, &str)], code: &[(&str, &str)], entry: EntryPoint) -> ServerRecipe {
        let env: Vec<(OsString, OsString)> = vars
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v)))
            .collect();
        let mut flags = ServerFlags::default();
        for (keyword, raw) in code {
            flags
                .give(CodeCall::ServerFlags, keyword, raw)
                .map_err(|e: ServerFlagsError| e.to_string())
                .unwrap();
        }
        ServerRecipe::of(Deployment::resolve(&env, &[], None, entry).unwrap().over(&flags)).unwrap()
    }

    fn resolve(
        mode: InterpreterMode,
        vars: &[(&str, &str)],
        code: &[(&str, &str)],
    ) -> Result<Topology, UnsupportedGcMode> {
        Topology::resolve(mode, &recipe(vars, code, EntryPoint::Script), CPUS)
    }

    #[test]
    fn tpc_threads_default_to_what_a_cpu_quota_runs_at_once_not_the_cores_in_the_mask() {
        // With `docker --cpus=2` on a 32-core cpuset, 32 threads would share 2 CPUs of time.
        let quota = Cpus::new(n(2), n(32));
        let t = Topology::resolve(InterpreterMode::Gil, &recipe(&[], &[], EntryPoint::Script), quota);
        assert_eq!(
            t.unwrap(),
            Topology::Tpc {
                threads: n(2),
                accept: AcceptMode::PerThreadListener
            }
        );
    }

    #[test]
    fn tpc_defaults_to_one_thread_per_physical_core() {
        let t = resolve(InterpreterMode::Subinterp, &[], &[]).unwrap();
        assert_eq!(
            t,
            Topology::Tpc {
                threads: n(8),
                accept: AcceptMode::PerThreadListener
            }
        );
        assert_eq!(t.accept_loops(), n(8));
        // Without a sub-interpreter route the TPC server is the same one, with no workers.
        let gil = resolve(InterpreterMode::Gil, &[], &[]).unwrap();
        assert_eq!(gil, t);
        let t = resolve(
            InterpreterMode::Subinterp,
            &[],
            &[("threads", "3"), ("subinterpreters", "99")],
        )
        .unwrap();
        assert_eq!(
            t,
            Topology::Tpc {
                threads: n(3),
                accept: AcceptMode::PerThreadListener
            }
        );
        assert_eq!(t.accept_loops(), n(3));
    }

    /// A layout of `topology` with none of the route facts, taking TCP connections.
    fn layout(topology: Topology) -> ServerLayout {
        let worker_routes = !matches!(topology, Topology::MultiThreadGil { .. });
        ServerLayout {
            topology,
            worker_routes,
            main_routes: false,
            async_worker_routes: false,
            process_routes: false,
            tcp: true,
        }
    }

    /// The thread-per-core server of a table with no sub-interpreter route: its threads have no
    /// workers.
    fn tpc_without_workers() -> ServerLayout {
        ServerLayout {
            worker_routes: false,
            ..layout(TPC)
        }
    }

    fn unused_names(layout: &ServerLayout, recipe: &ServerRecipe) -> Vec<&'static str> {
        layout
            .unused(&recipe.values)
            .iter()
            .map(|e| match e {
                FlagError::UnusedByTopology { flag, .. } => *flag,
                other => panic!("{other}"),
            })
            .collect()
    }

    const TPC: Topology = Topology::Tpc {
        threads: NonZeroUsize::MIN,
        accept: AcceptMode::PerThreadListener,
    };
    const POOL: Topology = Topology::Pool {
        workers: NonZeroUsize::MIN,
        io_threads: NonZeroUsize::MIN,
        accept_loops: NonZeroUsize::MIN,
    };
    const MULTI_GIL: Topology = Topology::MultiThreadGil {
        io_threads: NonZeroUsize::MIN,
        accept_loops: NonZeroUsize::MIN,
    };

    #[test]
    fn a_size_the_server_does_not_use_is_an_error_with_its_source_and_who_uses_it() {
        let given = recipe(&[("MIPYRE_SUBINTERPRETERS", "99")], &[], EntryPoint::Script);
        assert!(layout(POOL).unused(&given.values).is_empty());
        let errors = layout(TPC).unused(&given.values);
        assert_eq!(errors.len(), 1, "{errors:?}");
        let line = errors[0].to_string();
        assert!(
            line.starts_with("subinterpreters is given (MIPYRE_SUBINTERPRETERS")
                && line.contains("the thread-per-core server does not use it")
                && line.contains("sized by threads"),
            "{line}"
        );
        // From code too; the default is never named.
        let code = recipe(&[], &[("subinterpreters", "2")], EntryPoint::Script);
        assert_eq!(unused_names(&layout(MULTI_GIL), &code), ["subinterpreters"]);
        assert!(layout(TPC)
            .unused(&recipe(&[], &[], EntryPoint::Script).values)
            .is_empty());
    }

    #[test]
    fn a_size_given_by_a_command_line_flag_is_named_as_that_flag() {
        let env: Vec<(OsString, OsString)> = Vec::new();
        let deployment =
            Deployment::resolve(&env, &[("subinterpreters", "3".to_string())], None, EntryPoint::CliRun).unwrap();
        let pool = ServerRecipe::of(deployment.over(&ServerFlags::default())).unwrap();
        let errors = layout(TPC).unused(&pool.values);
        assert_eq!(errors.len(), 1, "{errors:?}");
        let line = errors[0].to_string();
        assert!(
            line.contains("mipyre run --subinterpreters") && !line.contains("ServerFlags(subinterpreters=)"),
            "{line}"
        );
    }

    #[test]
    fn each_kind_of_flag_is_used_by_its_servers_only() {
        // (flag given, a layout that uses it, one that does not)
        let mut main = tpc_without_workers();
        main.main_routes = true;
        let mut pool_async = layout(POOL);
        pool_async.async_worker_routes = true;
        let mut tpc_async = layout(TPC);
        tpc_async.async_worker_routes = true;
        let mut lanes = layout(TPC);
        lanes.process_routes = true;
        let mut memory = layout(TPC);
        memory.tcp = false;
        let pairs: [(&str, &str, ServerLayout, ServerLayout); 8] = [
            ("MIPYRE_GC_MODE", "idle", layout(TPC), tpc_without_workers()),
            ("MIPYRE_ISOLATE_DIR", "/srv/iso", layout(POOL), layout(MULTI_GIL)),
            ("MIPYRE_BRIDGE_WORKERS", "2", main, layout(MULTI_GIL)),
            ("MIPYRE_ASYNC_QUEUE_CAPACITY", "100", pool_async, layout(POOL)),
            ("MIPYRE_ASYNC_QUEUE_CAPACITY", "100", tpc_async, layout(TPC)),
            ("MIPYRE_PROCESS_WORKERS", "2", lanes, layout(TPC)),
            ("MIPYRE_PORT", "8001", layout(TPC), memory),
            ("MIPYRE_THREADS", "2", layout(MULTI_GIL), layout(MULTI_GIL)),
        ];
        for (variable, value, using, not_using) in pairs {
            let given = recipe(&[(variable, value)], &[], EntryPoint::Script);
            assert!(using.unused(&given.values).is_empty(), "{variable}");
            if variable == "MIPYRE_THREADS" {
                // Every server uses its threads.
                assert!(not_using.unused(&given.values).is_empty());
            } else {
                assert_eq!(not_using.unused(&given.values).len(), 1, "{variable}");
            }
        }
    }

    #[test]
    fn the_main_thread_sizes_are_used_by_every_server_with_main_routes() {
        let given = recipe(
            &[("MIPYRE_BRIDGE_WORKERS", "2"), ("MIPYRE_BRIDGE_CAPACITY", "8")],
            &[],
            EntryPoint::Script,
        );
        for topology in [TPC, POOL, MULTI_GIL] {
            let with_main = ServerLayout {
                main_routes: true,
                ..layout(topology)
            };
            assert!(with_main.unused(&given.values).is_empty(), "{topology:?}");
            assert_eq!(layout(topology).unused(&given.values).len(), 2, "{topology:?}");
        }
    }

    #[test]
    fn tpc_off_sizes_from_the_logical_count() {
        let off = [("MIPYRE_TPC", "0")];
        let t = resolve(InterpreterMode::Subinterp, &off, &[]).unwrap();
        assert_eq!(
            t,
            Topology::Pool {
                workers: n(16),
                io_threads: n(16),
                accept_loops: n(16),
            }
        );
        assert_eq!(t.accept_loops(), n(16));
        let t = resolve(
            InterpreterMode::Gil,
            &off,
            &[("threads", "4"), ("subinterpreters", "2")],
        )
        .unwrap();
        assert_eq!(
            t,
            Topology::MultiThreadGil {
                io_threads: n(4),
                accept_loops: n(4),
            }
        );
    }

    #[test]
    fn multi_thread_accept_loops_stop_at_the_logical_count() {
        let off = [("MIPYRE_TPC", "0")];
        for (threads, loops) in [("16", 16), ("17", 16)] {
            let t = resolve(InterpreterMode::Gil, &off, &[("threads", threads)]).unwrap();
            assert_eq!(t.accept_loops(), n(loops), "threads={threads}");
        }
    }

    #[test]
    fn the_pool_defaults_to_two_sub_interpreters_on_one_cpu_but_takes_one_when_given() {
        let off = [("MIPYRE_TPC", "0")];
        let one = Cpus::new(n(1), n(1));
        let pool_workers = |code: &[(&str, &str)]| match Topology::resolve(
            InterpreterMode::Subinterp,
            &recipe(&off, code, EntryPoint::Script),
            one,
        )
        .unwrap()
        {
            Topology::Pool { workers, .. } => workers,
            other => panic!("{other:?}"),
        };
        assert_eq!(pool_workers(&[]), n(2));
        assert_eq!(pool_workers(&[("subinterpreters", "1")]), n(1));
    }

    #[test]
    fn the_pool_runs_count_and_off_but_not_idle() {
        let off = ("MIPYRE_TPC", "0");
        for mode in ["count", "off"] {
            assert!(resolve(InterpreterMode::Subinterp, &[off, ("MIPYRE_GC_MODE", mode)], &[]).is_ok());
        }
        let mode = "idle";
        let err = resolve(InterpreterMode::Subinterp, &[off, ("MIPYRE_GC_MODE", mode)], &[]).unwrap_err();
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
        assert!(text.contains(mode) && text.contains("MIPYRE_TPC=0"), "{text}");
        assert!(resolve(InterpreterMode::Gil, &[off, ("MIPYRE_GC_MODE", "idle")], &[]).is_ok());
    }

    #[test]
    fn per_thread_tpc_runs_every_gc_mode() {
        for mode in ["count", "idle", "off"] {
            assert!(resolve(InterpreterMode::Subinterp, &[("MIPYRE_GC_MODE", mode)], &[]).is_ok());
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn darwin_fanout_has_one_accept_loop_and_no_idle_mode() {
        let fanout = ("MIPYRE_TPC_ACCEPT", "fanout");
        let t = resolve(InterpreterMode::Subinterp, &[fanout], &[]).unwrap();
        assert_eq!(
            t,
            Topology::Tpc {
                threads: n(8),
                accept: AcceptMode::Fanout
            }
        );
        assert_eq!(t.accept_loops(), NonZeroUsize::MIN);
        assert!(t.supports(GcMode::Off));
        let err = resolve(InterpreterMode::Subinterp, &[fanout, ("MIPYRE_GC_MODE", "idle")], &[]).unwrap_err();
        assert!(err.to_string().contains("idle") && err.to_string().contains("fanout"));
        // A table without a sub-interpreter route is served by the same fanout server.
        let gil = resolve(InterpreterMode::Gil, &[fanout], &[]).unwrap();
        assert_eq!(gil, t);
    }

    #[test]
    fn an_accept_mode_is_used_only_by_the_macos_tpc_server() {
        let given = recipe(&[("MIPYRE_TPC_ACCEPT", "listener")], &[], EntryPoint::Script);
        for on in [layout(TPC), tpc_without_workers()] {
            if cfg!(target_os = "macos") {
                assert!(unused_names(&on, &given).is_empty());
            } else {
                assert_eq!(unused_names(&on, &given), ["tpc_accept"]);
            }
        }
        assert_eq!(unused_names(&layout(POOL), &given), ["tpc_accept"]);
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
