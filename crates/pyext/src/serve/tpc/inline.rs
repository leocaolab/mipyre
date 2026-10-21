//! Serves sub-interpreter mode on the TPC threads: each thread owns a worker and runs its `def`
//! handlers inline on it.

use std::rc::Rc;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use mipyre_subinterp::gc::GcPolicy;
use mipyre_tpc::{LocalStop, LocalTasks, ShutdownDeadline};
use mipyre_transport::driver::{AcceptError, TransportDriver};

use super::startup::log_startup;
use super::threads::{drain_connections, serve_on_workers, with_stop_watcher};
use super::{hybrid_mode, AcceptMode, ServeError, TpcServer};
use super::conn::{serve_accepted, ThreadWorker, TpcThread};
use crate::runtime::ServerRunStop;
use crate::serve::on_tpc::handle_request_tpc_inline;
use crate::serve::websocket::Upgrades;

/// Serves `server` with one `SO_REUSEPORT` listener per TPC thread.
pub(super) fn run_per_thread_listener<D: TransportDriver>(
    transport: D,
    server: TpcServer,
    banner: &str,
    run: &ServerRunStop,
    deadline: &ShutdownDeadline,
) -> Result<(), ServeError> {
    let (gc, counters) = (server.gc, Arc::clone(&server.counters));
    with_stop_watcher(run, counters, deadline, || {
        let listening = transport.listening();
        serve_on_workers(
            server,
            transport.into_loops(),
            run,
            deadline,
            move |tpc_thread, share, stop, deadline| tpc_accept_loop_inline(share, tpc_thread, stop, gc, deadline),
            || log_startup(hybrid_mode(AcceptMode::PerThreadListener), &listening, banner),
        )
    })
}

/// Accepts on this thread's listeners and serves each connection inline on `tpc_thread`'s worker
/// until `shutdown`, running the idle GC tick in idle mode.
///
/// Once the worker retires the thread stops accepting: its listeners close, so the kernel
/// hands new connections to the other threads, and its open connections finish the request
/// they are on and close. The thread itself runs until `shutdown`, for the process lanes it
/// hosts.
pub(crate) async fn tpc_accept_loop_inline<D: TransportDriver>(
    share: D,
    tpc_thread: Rc<TpcThread>,
    shutdown: CancellationToken,
    gc: GcPolicy,
    deadline: ShutdownDeadline,
) -> Result<(), ServeError> {
    let mut source = Some(share.register()?);
    // The worker fires its own count trigger (the threshold, or idle mode's OOM failsafe); this
    // loop adds only the idle tick.
    let mut ticker = gc.idle_tick().map(|tick| {
        // Nothing needs collecting before any request ran, so the first tick is one period out.
        tokio::time::interval_at(tokio::time::Instant::now() + tick, tick)
    });
    let upgrades = Upgrades::of(&tpc_thread.site);
    let stop = LocalStop::follow(&shutdown);
    let stopped = stop.stopped();
    tokio::pin!(stopped);
    let (retiring, conn_stop) = connections_stop(&shutdown);
    let worker = tpc_thread.slot.worker();
    let thread = tpc_thread.slot.index().get();
    let lane_host = crate::lane::host_tpc_thread(tpc_thread.site.process.as_ref(), &tpc_thread.site.rules, thread)
        .map_err(ServeError::LaneHost)?;
    let tracker = LocalTasks::new();
    let ended = loop {
        let wake = tokio::select! {
            biased;
            () = &mut stopped => break Ok(()),
            () = retired(worker), if source.is_some() => AcceptWake::Retired,
            accepted = accept::<D>(&mut source) => match accepted {
                Ok(accepted) => AcceptWake::Accepted(accepted),
                Err(stopped_accepting) => break Err(stopped_accepting.into()),
            },
            () = next_tick(&mut ticker) => AcceptWake::Tick,
        };
        match wake {
            AcceptWake::Accepted(accepted) => {
                tracker.spawn_local(serve_accepted::<D, _, _>(
                    accepted,
                    upgrades.clone(),
                    {
                        let tpc_thread = Rc::clone(&tpc_thread);
                        move |req, peer| handle_request_tpc_inline(req, Rc::clone(&tpc_thread), peer)
                    },
                    conn_stop.stopped(),
                ));
            }
            AcceptWake::Tick => {
                if let Some(worker) = worker {
                    idle_gc_tick(worker);
                }
            }
            AcceptWake::Retired => {
                source = None;
                ticker = None;
                retiring.cancel();
                stopped_accepting(&tpc_thread);
            }
        }
    };
    drain_connections(tracker, &deadline, &tpc_thread.site.counters).await;
    if let Some(host) = lane_host {
        host.shutdown().await;
    }
    ended
}

enum AcceptWake<C> {
    Accepted(C),
    Tick,
    Retired,
}

/// Returns a stop for a thread's connections that fires on shutdown or when the returned token is
/// cancelled, which the thread does once its worker retires.
pub(super) fn connections_stop(shutdown: &CancellationToken) -> (CancellationToken, LocalStop) {
    let retiring = shutdown.child_token();
    let stop = LocalStop::follow(&retiring);
    (retiring, stop)
}

/// Waits for the next connection on `source`, forever once the thread has stopped accepting.
async fn accept<D: TransportDriver>(source: &mut Option<D::AcceptSource>) -> Result<D::Connection, AcceptError> {
    match source {
        Some(source) => D::accept(source).await,
        None => std::future::pending().await,
    }
}

/// Completes once `worker` retires, and never for a thread without one.
pub(super) async fn retired(worker: Option<&ThreadWorker>) {
    match worker {
        Some(worker) => worker.retired().await,
        None => std::future::pending().await,
    }
}

/// Logs that the thread takes no more connections; why its worker retired was logged then.
pub(super) fn stopped_accepting(tpc_thread: &TpcThread) {
    tracing::warn!(
        target: "mipyre::server",
        worker = tpc_thread.slot.index().get(),
        "this TPC thread's worker retired: the thread takes no more connections and closes \
         the ones it has; the other threads serve"
    );
}

/// Waits for the idle GC tick, forever in count and off modes.
async fn next_tick(ticker: &mut Option<tokio::time::Interval>) {
    match ticker {
        Some(ticker) => {
            ticker.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// Runs the idle tick, which collects if the worker went quiet; a failed collect retires it.
fn idle_gc_tick(thread_worker: &ThreadWorker) {
    let Ok(worker) = thread_worker.serving() else {
        return;
    };
    if let Err(retirement) = worker.borrow_mut().on_idle_tick() {
        thread_worker.retire(&retirement);
    }
}
