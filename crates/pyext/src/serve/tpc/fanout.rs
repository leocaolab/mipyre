//! Serves sub-interpreter mode on macOS through one acceptor thread that fans connections out.

use std::rc::Rc;
use std::sync::{Arc, OnceLock};

use tokio_util::sync::CancellationToken;

use mipyre_tpc::{LocalStop, LocalTasks, ShutdownDeadline};
use mipyre_transport::driver::TransportDriver;

use super::conn::{serve_accepted, TpcThread};
use super::inline::{connections_stop, retired, stopped_accepting};
use super::threads::{
    drain_connections, join_run, left_running, serve_on_this_thread, settle, spawn_worker_threads, stopped_error,
};
use super::{topology_label, AcceptMode, ServeError, TpcServer};
use crate::runtime::ServerRunStop;
use crate::serve::on_tpc::handle_request_tpc_inline;
use crate::serve::websocket::upgrade_site;

/// Serves through one acceptor thread that hands connections round-robin to the worker threads
/// over bounded queues, at one cross-thread wake per connection. It has no idle tick, so it takes
/// count or off GC only (`Topology::supports`).
///
/// Darwin's `SO_REUSEPORT` sends nearly all traffic to one listener, which this spreads. It is
/// opt-in (`MIPYRE_TPC_DARWIN=fanout`) because on localhost benchmarks per-thread listeners still
/// win: the wake cost and client/server CPU contention outweigh the spread.
pub(super) fn run_fanout<D: TransportDriver>(
    transport: D,
    server: TpcServer,
    announce: impl FnOnce(&'static str, &[String]),
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
    spawner: &mut impl mipyre_tpc::Spawner,
) -> Result<(), ServeError> {
    // Past this many queued connections the acceptor drops new ones (RST), so open file
    // descriptors stay bounded at threads × this.
    const WORKER_INBOX_CAP: usize = 1024;

    let label = topology_label(&server.threads, AcceptMode::Fanout);
    let TpcServer {
        threads,
        main_bridge,
        async_pool,
        cpus,
        counters,
        ..
    } = server;
    let threads = threads.into_setups(main_bridge, async_pool);
    let (txs, threads): (Vec<_>, Vec<_>) = threads
        .into_iter()
        .map(|setup| {
            let (tx, rx) = tokio::sync::mpsc::channel::<D::Connection>(WORKER_INBOX_CAP);
            let retirement = setup.plan.as_ref().map(|plan| Arc::clone(&plan.retired));
            (FanoutInbox { tx, retirement }, (setup, rx))
        })
        .unzip();
    // The acceptor shares the threads' run, so whichever fails first is the run's cause.
    let mut handles = spawn_worker_threads(
        spawner,
        threads,
        &cpus,
        run,
        deadline,
        &counters,
        |tpc_thread, rx, stop, deadline| fanout_worker_loop::<D>(rx, tpc_thread, stop, deadline),
    )?;
    announce(label, &transport.listening());

    // The acceptor has its own thread and runtime so polling accept does not contend with any
    // worker. It runs unpinned because macOS has no CPU pinning.
    let (acceptor_counters, acceptor_deadline) = (Arc::clone(&counters), deadline.clone());
    let spawned = mipyre_tpc::spawn_in_run(
        "mipyre-acceptor",
        mipyre_subinterp::PYTHON_THREAD_STACK,
        run,
        move |own| {
            serve_on_this_thread(None, &acceptor_counters, own, &acceptor_deadline, || {
                fanout_accept_loop(transport, txs, own.token())
            })
        },
    );
    match spawned {
        Ok(handle) => handles.push(handle),
        // The refusal stopped the run.
        Err(not_spawned) => {
            let joined = join_run(handles, run, deadline, &counters);
            return Err(stopped_error(Some(not_spawned.into()), left_running(&joined)));
        }
    }
    settle(join_run(handles, run, deadline, &counters), run.cause())
}

/// The acceptor's end of one thread's inbox; the worker thread registers each connection with its
/// own runtime.
struct FanoutInbox<C> {
    tx: tokio::sync::mpsc::Sender<C>,
    /// It is set once the thread's worker retires, and is `None` for a thread with no worker.
    retirement: Option<Arc<OnceLock<Arc<str>>>>,
}

impl<C> FanoutInbox<C> {
    /// Returns whether the thread closed its inbox because its worker retired.
    fn retired(&self) -> bool {
        self.retirement.as_ref().is_some_and(|r| r.get().is_some())
    }
}

async fn fanout_accept_loop<D: TransportDriver>(
    transport: D,
    mut txs: Vec<FanoutInbox<D::Connection>>,
    shutdown: CancellationToken,
) -> Result<(), ServeError> {
    use tokio::sync::mpsc::error::TrySendError;

    let mut source = transport.register()?;
    let mut next: usize = 0;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            accepted = D::accept(&mut source) => {
                let mut connection = accepted?;
                // A thread whose worker retired closed its inbox, so it leaves the rotation and the
                // connection goes to the next thread. The workers stop the server once the last
                // one retires.
                let sent = loop {
                    match txs[next].tx.try_send(connection) {
                        Err(TrySendError::Closed(unsent)) if txs[next].retired() => {
                            txs.remove(next);
                            if txs.is_empty() {
                                break None;
                            }
                            next %= txs.len();
                            connection = unsent;
                        }
                        other => break Some(other),
                    }
                };
                let Some(sent) = sent else {
                    break;
                };
                match sent {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        tracing::warn!(
                            target: "mipyre::server",
                            worker = next,
                            "TPC worker inbox full — dropping connection (load shed)"
                        );
                    }
                    // A worker closes its inbox without a retirement only once it has seen the
                    // stop, which is cancelled before the worker sees it, or when its serve ended
                    // on a failure. A stopped run is no failure of the acceptor's; otherwise the
                    // acceptor fails, so the run stops with a cause and the server exits non-zero
                    // rather than serve on with no one accepting. The worker's own failure, logged
                    // at the join, is the run's cause instead when it reached the run first.
                    Err(TrySendError::Closed(_)) if shutdown.is_cancelled() => break,
                    Err(TrySendError::Closed(_)) => return Err(ServeError::WorkerInboxClosed { worker: next }),
                }
                next = (next + 1) % txs.len();
            }
        }
    }
    // Dropping `txs` hangs up every receiver, which ends the workers along with the token.
    Ok(())
}

async fn fanout_worker_loop<D: TransportDriver>(
    mut rx: tokio::sync::mpsc::Receiver<D::Connection>,
    tpc_thread: Rc<TpcThread>,
    shutdown: CancellationToken,
    deadline: ShutdownDeadline,
) -> Result<(), ServeError> {
    let ws_site = upgrade_site(&tpc_thread.site);
    let stop = LocalStop::follow(&shutdown);
    let stopped = stop.stopped();
    tokio::pin!(stopped);
    let (retiring, conn_stop) = connections_stop(&shutdown);
    let worker = tpc_thread.worker.as_ref();
    let thread = tpc_thread.index.get();
    let lane_host =
        crate::runtime::process::host_tpc_thread(tpc_thread.site.process.as_ref(), &tpc_thread.site.rules, thread)
            .map_err(ServeError::LaneHost)?;
    let tracker = LocalTasks::new();
    let mut accepting = true;
    loop {
        tokio::select! {
            biased;
            () = &mut stopped => break,
            // Closing the inbox makes the acceptor hand connections to the other threads; the
            // open ones finish their current request and close.
            () = retired(worker), if accepting => {
                accepting = false;
                rx.close();
                retiring.cancel();
                stopped_accepting(&tpc_thread);
            }
            received = rx.recv(), if accepting => match received {
                Some(connection) => {
                    tracker.spawn_local(serve_accepted::<D, _, _>(
                        connection,
                        ws_site.clone(),
                        {
                            let tpc_thread = Rc::clone(&tpc_thread);
                            move |req, peer| handle_request_tpc_inline(req, Rc::clone(&tpc_thread), peer)
                        },
                        conn_stop.stopped(),
                    ));
                }
                None => break,
            }
        }
    }
    drain_connections(tracker, &deadline, &tpc_thread.site.counters).await;
    if let Some(host) = lane_host {
        host.shutdown().await;
    }
    Ok(())
}
