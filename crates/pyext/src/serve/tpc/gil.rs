//! Serves GIL mode on the TPC threads: every handler runs on the main interpreter.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use mipyre_cpu::CoreId;
use mipyre_tpc::{LocalStop, LocalTasks, ShutdownDeadline, Spawner};
use mipyre_transport::driver::TransportDriver;

use super::startup::{log_startup, with_cores};
use super::threads::{drain_connections, join_run, serve_on_this_thread, settle, start_tpc_threads, with_stop_watcher};
use super::ServeError;
use crate::runtime::ServerRunStop;
use super::conn::serve_accepted;
use crate::serve::on_main::handle_request;
use crate::serve::site::Site;
use crate::serve::websocket::Upgrades;

/// Serves every handler on the main interpreter from one pinned thread per accept loop of
/// `transport`, thread `i` with `sites[i]`. It prints `banner` only once every thread has started,
/// so a failed start says nothing of the server. A thread that fails stops the server, and its
/// error is returned.
///
/// # Panics
///
/// Panics unless `sites` has one entry per accept loop.
pub(crate) fn run_tpc_gil<D: TransportDriver>(
    transport: D,
    sites: Vec<Arc<Site>>,
    cpus: Vec<CoreId>,
    banner: &str,
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    spawner: &mut impl Spawner,
) -> Result<(), ServeError> {
    assert_eq!(
        sites.len(),
        transport.accept_loops(),
        "one site per TPC thread"
    );
    // Every site holds a copy of the run's counters.
    let counters = Arc::clone(&sites[0].counters);
    with_stop_watcher(run, Arc::clone(&counters), deadline, || {
        let listening = transport.listening();
        let payloads: Vec<_> = transport.into_loops().into_iter().zip(sites).collect();
        let thread_deadline = deadline.clone();
        let handles = start_tpc_threads(
            spawner,
            with_cores(payloads, &cpus),
            run,
            deadline,
            &counters,
            move |slot, (share, site), stop| {
                let counters = Arc::clone(&site.counters);
                serve_on_this_thread(slot.core(), &counters, stop, &thread_deadline, || {
                    tpc_accept_loop_gil(share, site, stop.token(), slot.index(), thread_deadline.clone())
                })
            },
            // The threads wait only for the run's stop, so a failed spawn has nothing else to
            // release.
            || {},
        )?;
        log_startup("gil", &listening, banner);
        settle(join_run(handles, run, deadline, &counters), run.cause())
    })
}

async fn tpc_accept_loop_gil<D: TransportDriver>(
    share: D,
    site: Arc<Site>,
    shutdown: CancellationToken,
    thread: usize,
    deadline: ShutdownDeadline,
) -> Result<(), ServeError> {
    let mut source = share.register()?;
    let upgrades = Upgrades::of(&site);
    let stop = LocalStop::follow(&shutdown);
    let stopped = stop.stopped();
    tokio::pin!(stopped);
    let lane_host = crate::lane::host_tpc_thread(site.process.as_ref(), &site.rules, thread)
        .map_err(ServeError::LaneHost)?;
    let tracker = LocalTasks::new();
    let ended = loop {
        tokio::select! {
            biased;
            () = &mut stopped => break Ok(()),
            accepted = D::accept(&mut source) => match accepted {
                Ok(accepted) => {
                    let site = Arc::clone(&site);
                    let handle = move |req, peer| handle_request(req, Arc::clone(&site), peer);
                    tracker.spawn_local(serve_accepted::<D, _, _>(accepted, upgrades.clone(), handle, stop.stopped()));
                }
                Err(stopped_accepting) => break Err(stopped_accepting.into()),
            }
        }
    };
    drain_connections(tracker, &deadline, &site.counters).await;
    if let Some(host) = lane_host {
        host.shutdown().await;
    }
    ended
}
