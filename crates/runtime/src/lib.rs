//! Runs Python handlers: sub-interpreter workers and their pool, the main interpreter's threads, a
//! WebSocket connection's task on an async engine,
//! a process lane's worker (`lane_worker`: handlers in worker processes, `runtime="process"`), and
//! the hook chain around each call.

mod declaration;
mod engine_queue;
pub mod exchange;
mod hook_chain;
mod isolation;
mod job_queue;
mod lane_worker;
#[cfg(feature = "leak_detect")]
pub mod leak_detect;
mod live_workers;
mod main_call;
mod main_gil;
mod main_threads;
mod pool;
mod request_context;
mod rules;
mod shared_map;
mod start;
mod sync;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
mod websocket;
mod worker;
mod worker_api;
mod worker_app;

pub use self::declaration::DeclarationError;
pub use self::isolation::_note_isolated;
pub(crate) use self::isolation::report_startup;
pub use self::lane_worker::serve as serve_lane_worker;
pub use self::live_workers::{LiveWorkers, WorkerCounters, WorkerKind, WorkerKindLoss, WorkerLife, WorkerTally};
pub(crate) use self::main_call::{call_handler_with_hooks, close_thread_event_loop, run_with_hooks};
pub use self::main_gil::MAIN_GIL;
pub use self::main_threads::{MainSizes, MainThreadHandles, MainThreads};
pub(crate) use self::pool::answer_if_awaited;
pub use self::pool::{
    pools_at_abandon_limit, split_workers_for_routes, workers_abandoned, AbandonedWorker, InterpreterPool, PoolError,
    PoolJob, PoolThreads, RouteKinds, WorkerCounts,
};
pub(crate) use self::request_context::in_reused_context;
pub use self::rules::{copy_rules, Handler, Routes};
pub use self::shared_map::{map_for_new, AppStore, StateValue};
pub use self::start::{start_here, StartError, StartGate, StartHandle, ThreadResident};
pub use self::websocket::{NoLiveEngine, WsConnection, WsEngines};
pub use self::worker::program::{app_file, CommandLine, MissingAppFileError, WorkerProgram};
pub use self::worker::start::{WorkerException, WorkerRecipe};
pub use self::worker::sync_runner::{Retirement, SyncRunner};
pub use self::worker::{SubInterpreterWorker, WorkerIndex};
pub use self::worker_api::{_worker_fail, _worker_send, _worker_take, _worker_to_response, ConnectionInbox, WsJob};
pub use self::worker_app::{
    _routers_to_load, _set_routers_to_load, record_worker_app, AppRoute, AppWebSocket, EngineModule,
    ScriptRegistrations,
};
