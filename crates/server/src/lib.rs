//! Serves HTTP over hyper and Tokio: the request pipeline, the TPC and multi-thread servers'
//! connections, WebSocket and gRPC, response compression, and the stop signal.

mod access_line;
mod compression;
mod conn;
mod counters;
mod drain;
mod error_response;
mod grpc;
mod listener;
mod multi_thread;
mod on_main;
mod on_pool;
mod on_process;
mod on_tpc;
mod pipeline;
mod request;
mod respond;
mod site;
mod stop;
mod sync;
mod tpc;
mod websocket;

#[cfg(test)]
#[cfg(not(sync_model))]
mod coalesce_tests;
#[cfg(test)]
#[cfg(not(sync_model))]
mod conn_bench;
#[cfg(test)]
#[cfg(not(sync_model))]
mod opening_tests;
#[cfg(test)]
#[cfg(not(sync_model))]
mod test_clock;

pub(crate) use self::access_line::log_access;
pub use self::access_line::{JsonLines, ACCESS_TARGET};
pub use self::counters::{AnswerCounts, ServerCounters, ServerCounts, SiteCounters};
pub(crate) use self::drain::{drain_timeout, report_drain_timeout, teardown_timeout};
pub use self::listener::{bind_options, handed_by_systemd, print_listening};
pub use self::multi_thread::{serve_multi_thread, IoThreadServers};
pub use self::on_main::MultiThreadServer;
pub use self::on_pool::LargeBodyPermits;
pub use self::request::handle_request;
pub use self::site::Site;
pub use self::stop::{watching_stop_signals, Disposition, ServerRunStop, ServerStop, StopSignalDispositions};
#[cfg(feature = "bench")]
pub use self::tpc::{drive_conn, serve_on_workers, tpc_accept_loop_inline, TpcThread};
pub use self::tpc::{
    log_startup, report_pinning, run_tpc, thread_cpus, tpc_banner, AcceptMode, ServeError, TpcServer, TpcThreads,
};
pub use self::websocket::{upgrade_target, OpenConnections, WsUpgrade};
