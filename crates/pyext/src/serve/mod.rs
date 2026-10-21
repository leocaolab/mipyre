//! Serves HTTP over hyper and Tokio: the request pipeline, the TPC and multi-thread servers'
//! connections, WebSocket and gRPC, response compression, and the stop signal.

mod compression;
mod conn;
mod drain;
mod error_response;
mod grpc;
mod listener;
mod multi_thread;
mod on_lane;
mod on_main;
mod on_pool;
mod on_tpc;
mod pipeline;
mod respond;
mod site;
mod stop;
mod tpc;
mod websocket;

#[cfg(test)]
mod coalesce_tests;
#[cfg(test)]
mod conn_bench;
#[cfg(test)]
mod opening_tests;

pub(crate) use self::multi_thread::serve_multi_thread;
pub(crate) use self::listener::{
    accept_stopped_error, bind_options, handed_by_systemd, listener_error, print_listening,
};
pub(crate) use self::on_main::handle_request;
pub(crate) use self::on_pool::handle_request_subinterp;
#[cfg(feature = "bench")]
pub(crate) use self::on_tpc::handle_request_tpc_inline;
pub(crate) use self::site::{Routes, Site};
pub(crate) use self::drain::{drain_timeout, report_drain_timeout, teardown_timeout, TEARDOWN_MARGIN};
pub(crate) use self::stop::{until_stopped, StopSignals};
pub(crate) use self::tpc::{
    report_pinning, run_tpc_gil, run_tpc_workers, thread_cpus, tpc_banner, AcceptMode, OffThreadRunners, ServeError,
    TpcServer, TpcThreads,
};
#[cfg(feature = "bench")]
pub(crate) use self::tpc::{drive_conn, serve_on_workers, tpc_accept_loop_inline, TpcThread};
pub(crate) use self::websocket::{OpenConnections, Upgrades};
