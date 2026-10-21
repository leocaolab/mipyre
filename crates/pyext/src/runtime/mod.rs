//! Runs Python handlers: sub-interpreter workers and their pool, the main interpreter's bridge,
//! and the hook chain around each call.

mod hook_chain;
mod isolation;
mod job_queue;
mod live_workers;
mod main_bridge;
mod main_call;
mod pool;
mod request_context;
mod rules;
mod shared_map;
mod start;
mod worker;
mod worker_api;
mod worker_app;

pub(crate) use self::isolation::{_note_isolated, report_startup};
pub(crate) use self::live_workers::{LiveWorkers, ServerRunStop, ServerStop, WorkerKind, WorkerLife};
pub(crate) use self::main_bridge::{BridgeConfig, BridgeJob, MainInterpBridge};
pub(crate) use self::main_call::{
    call_handler_with_hooks, close_thread_event_loop, main_chain, run_with_hooks, MainReply,
    StreamReply,
};
pub(crate) use self::pool::{
    answer_if_awaited, split_workers_for_routes, AbandonedWorker, InterpreterPool, PoolJob,
    PoolThreads, RouteKinds, WorkerCounts,
};
pub(crate) use self::request_context::{in_request_context, RequestContext};
pub(crate) use self::rules::copy_rules;
pub(crate) use self::shared_map::{hand_to_worker, map_for_new, AppStore, StateValue};
pub(crate) use self::start::{start_here, StartHandle, ThreadResident, StartError, StartGate};
pub(crate) use self::worker::program::{main_script, NoWorkerScript, WorkerProgram};
pub(crate) use self::worker::start::{WorkerException, WorkerRecipe};
pub(crate) use self::worker::sync_runner::{Retirement, ServeOutcome, SyncRunner};
pub(crate) use self::worker::{SubInterpreterWorker, WorkerIndex};
pub(crate) use self::worker_api::{
    _worker_app_handlers, _worker_app_hooks, _worker_close, _worker_fail, _worker_recv,
    _worker_send, _worker_timed_out, _worker_to_response,
};
pub(crate) use self::worker_app::{
    _routers_to_load, _set_routers_to_load, record_worker_app, AppRoute, ScriptRegistrations,
};
