//! Says why a server run stops, and waits for its stop: SIGINT, SIGTERM (what systemd sends), or
//! `Server.shutdown()`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use mipyre_tpc::{RunStop, ShutdownDeadline, StopWatcherError, ThreadJoinError};
use signal_hook::flag::register_conditional_default;
use signal_hook::low_level::{pipe, unregister};
use signal_hook::SigId;
use tokio_util::sync::CancellationToken;

use crate::ServerCounters;
use mipyre_process::LaneRunError;
use mipyre_runtime::WorkerKindLoss;

use self::signal_action::SignalAction;

mod signal_action;

/// Why a server stopped, for the reasons beyond its threads' that its run's stop ([`RunStop`])
/// records. `run()` raises it, so the process exits non-zero for its supervisor to restart rather
/// than answer 503 forever.
#[derive(Clone, Debug)]
pub enum ServerStop {
    NoWorkerLeft(WorkerKindLoss),
    LanesFailed(LaneRunError),
}

impl std::fmt::Display for ServerStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerStop::NoWorkerLeft(WorkerKindLoss { kind, first }) => {
                write!(
                    f,
                    "the server stopped: no {kind} is left serving; the first stopped: {first}"
                )
            }
            ServerStop::LanesFailed(why) => write!(f, "the process lanes failed, so the server stopped: {why}"),
        }
    }
}

/// A server run's stop, with the server's own causes.
pub type ServerRunStop<S = CancellationToken> = RunStop<ServerStop, S>;

/// The name of the thread that stops a server run on SIGINT or SIGTERM.
const STOP_WATCHER: &str = "mipyre-stop";

/// Runs `run` (the whole of a server run, its start included) while a thread stops it on SIGINT
/// or SIGTERM, then ends that thread by `deadline`; a watcher that did not end cleanly is logged
/// and counted in `counters`. Once no run listens, a stop signal is handled as `before` says it
/// was before the run. A signal that comes while the run is still starting stops it as soon
/// as the start reaches the stop, instead of being left to Python, which would not see it until
/// the run ends.
///
/// # Errors
///
/// Returns why the watcher could not start; `run` is not called then, because a server that
/// would ignore its stop signals must not start.
pub fn watching_stop_signals<R>(
    stop: &ServerRunStop,
    counters: &ServerCounters,
    deadline: &ShutdownDeadline,
    before: StopSignalDispositions,
    run: impl FnOnce() -> R,
) -> Result<R, StopWatcherError> {
    let install = move || StopSignals::install(before);
    let watcher = mipyre_tpc::spawn_stop_watcher(STOP_WATCHER, stop, install, until_stopped)?;
    let ran = run();
    if let Err(failure) = watcher.finish(deadline.fix(Instant::now())) {
        report_stop_watcher_failure(&failure, counters);
    }
    Ok(ran)
}

/// Logs and counts a stop watcher that panicked or was left running at its join deadline; the
/// run's stop was cancelled either way.
fn report_stop_watcher_failure(failure: &ThreadJoinError, counters: &ServerCounters) {
    counters.stop_watcher_failure();
    tracing::error!(
        target: "mipyre::server",
        thread = STOP_WATCHER,
        error = %failure,
        "the stop watcher thread did not end cleanly"
    );
}

/// The stop signals registered for one run: each SIGINT or SIGTERM from the registration on
/// writes a byte to `arrived`. The run has its own, because tokio's signals keep a signal that came
/// while nothing listened and hand it to the next listener, which stopped the next run in the same
/// process as soon as it started. They are registered inside the runtime that waits on them,
/// before anything is spawned, so a signal that cannot be registered fails the start instead of
/// leaving a server that cannot be stopped.
struct StopSignals {
    arrived: tokio::net::UnixStream,
    registrations: [SigId; 2],
    before: StopSignalDispositions,
    fallbacks: Arc<Fallbacks>,
}

impl StopSignals {
    /// Registers both signals and counts this run among those listening; the error names the
    /// first that failed and keeps the OS error's kind.
    fn install(before: StopSignalDispositions) -> std::io::Result<Self> {
        let mut listeners = LISTENERS.lock();
        let fallbacks = listeners.fallbacks(before)?;
        let (arrived, written) = std::os::unix::net::UnixStream::pair()?;
        arrived.set_nonblocking(true)?;
        let arrived = tokio::net::UnixStream::from_std(arrived)?;
        let for_terminate = written.try_clone()?;
        let interrupt = registered(StopSignal::Interrupt, pipe::register(libc::SIGINT, written))?;
        let terminate =
            registered(StopSignal::Terminate, pipe::register(libc::SIGTERM, for_terminate)).inspect_err(|_| {
                unregister(interrupt);
            })?;
        listeners.reclaim(&fallbacks.hooks).inspect_err(|_| {
            unregister(interrupt);
            unregister(terminate);
            listeners.release();
        })?;
        listeners.runs += 1;
        fallbacks.set(before, listeners.runs);
        Ok(Self {
            arrived,
            registrations: [interrupt, terminate],
            before,
            fallbacks,
        })
    }

    /// Waits until a signal has written its byte; a wake with no byte to read is spurious.
    ///
    /// # Errors
    ///
    /// Returns the socket's error, or `UnexpectedEof` if its writing end is closed, which only
    /// the drop of this value does.
    async fn arrival(&self) -> std::io::Result<()> {
        let mut byte = [0u8; 1];
        loop {
            self.arrived.readable().await?;
            match self.arrived.try_read(&mut byte) {
                Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }
    }
}

impl Drop for StopSignals {
    fn drop(&mut self) {
        let mut listeners = LISTENERS.lock();
        listeners.runs -= 1;
        self.fallbacks.set(self.before, listeners.runs);
        for registration in self.registrations {
            unregister(registration);
        }
        listeners.release();
    }
}

/// How a stop signal was handled before a run, as Python reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// The OS default, which signal-hook does not run once it has taken the signal over.
    Default,
    /// A handler, which signal-hook calls itself before a run's action, or ignored.
    Other,
}

/// How SIGINT and SIGTERM were handled before a run.
#[derive(Debug, Clone, Copy)]
pub struct StopSignalDispositions {
    pub interrupt: Disposition,
    pub terminate: Disposition,
}

/// Decides whether a stop signal that arrives while `listening_runs` runs wait on it takes the
/// OS default action, as it would have before any run.
fn takes_default_action(before: Disposition, listening_runs: usize) -> bool {
    before == Disposition::Default && listening_runs == 0
}

/// The runs of this process that listen for the stop signals, the fallbacks the first of them
/// registered, and the handlers runs took the signals back from. signal-hook keeps its handler on
/// a signal after the last action is unregistered and never runs the default action it took over,
/// so without a fallback a SIGTERM that no run hears would be ignored.
static LISTENERS: parking_lot::Mutex<Listeners> = parking_lot::Mutex::new(Listeners {
    runs: 0,
    fallbacks: None,
    displaced: [None; 2],
});

struct Listeners {
    runs: usize,
    fallbacks: Option<Arc<Fallbacks>>,
    /// Holds, for SIGINT and SIGTERM, the handler a run start replaced with signal-hook's.
    displaced: [Option<SignalAction>; 2],
}

impl Listeners {
    /// Returns the fallbacks, registering them the first time.
    fn fallbacks(&mut self, before: StopSignalDispositions) -> std::io::Result<Arc<Fallbacks>> {
        if let Some(fallbacks) = &self.fallbacks {
            return Ok(Arc::clone(fallbacks));
        }
        let fallbacks = Arc::new(Fallbacks::register(before, self.runs)?);
        self.fallbacks = Some(Arc::clone(&fallbacks));
        Ok(fallbacks)
    }

    /// Puts signal-hook's handler back on each stop signal whose handler something else replaced,
    /// such as Python's `signal.signal` between two runs, and keeps the replacement to put back
    /// once no run listens. signal-hook installs its handler only at a signal's first registration,
    /// so without this a run would register actions the OS never calls. While a run listens the
    /// server owns the signal: the displaced handler is not called.
    fn reclaim(&mut self, hooks: &[SignalAction; 2]) -> std::io::Result<()> {
        for ((signal, hook), displaced) in STOP_SIGNALS.into_iter().zip(hooks).zip(&mut self.displaced) {
            let current = registered(signal, SignalAction::of(signal.number()))?;
            if !current.same_handler(hook) {
                registered(signal, hook.install(signal.number()))?;
                *displaced = Some(current);
            }
        }
        Ok(())
    }

    /// Puts back, once no run listens, the handlers run starts displaced; one that cannot be put
    /// back is logged, and the signal stays with signal-hook and the fallbacks.
    fn release(&mut self) {
        if self.runs > 0 {
            return;
        }
        for (signal, displaced) in STOP_SIGNALS.into_iter().zip(&mut self.displaced) {
            let Some(handler) = displaced.take() else { continue };
            if let Err(error) = handler.install(signal.number()) {
                tracing::error!(target: "mipyre::server", signal = %signal, error = %error,
                    "could not put back the handler a run took the stop signal from");
            }
        }
    }
}

/// The fallback flags on SIGINT and SIGTERM: while one is set, its signal takes the OS default
/// action. Their registrations live as long as the process.
struct Fallbacks {
    interrupt: Arc<AtomicBool>,
    terminate: Arc<AtomicBool>,
    /// Holds signal-hook's handlers on SIGINT and SIGTERM as their first registration installed
    /// them.
    hooks: [SignalAction; 2],
}

impl Fallbacks {
    /// Registers both fallbacks, each set as `before` and `listening_runs` decide, so a signal
    /// that comes before the run's own registrations is handled as before the run.
    fn register(before: StopSignalDispositions, listening_runs: usize) -> std::io::Result<Self> {
        let interrupt = Arc::new(AtomicBool::new(takes_default_action(before.interrupt, listening_runs)));
        let terminate = Arc::new(AtomicBool::new(takes_default_action(before.terminate, listening_runs)));
        let interrupt_fallback = registered(
            StopSignal::Interrupt,
            register_conditional_default(libc::SIGINT, Arc::clone(&interrupt)),
        )?;
        let terminate_fallback = registered(
            StopSignal::Terminate,
            register_conditional_default(libc::SIGTERM, Arc::clone(&terminate)),
        )
        .inspect_err(|_| {
            unregister(interrupt_fallback);
        })?;
        let hook = |signal: StopSignal| {
            registered(signal, SignalAction::of(signal.number())).inspect_err(|_| {
                unregister(interrupt_fallback);
                unregister(terminate_fallback);
            })
        };
        let interrupt_hook = hook(StopSignal::Interrupt)?;
        let terminate_hook = hook(StopSignal::Terminate)?;
        Ok(Self {
            interrupt,
            terminate,
            hooks: [interrupt_hook, terminate_hook],
        })
    }

    fn set(&self, before: StopSignalDispositions, listening_runs: usize) {
        self.interrupt
            .store(takes_default_action(before.interrupt, listening_runs), Ordering::SeqCst);
        self.terminate
            .store(takes_default_action(before.terminate, listening_runs), Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopSignal {
    Interrupt,
    Terminate,
}

const STOP_SIGNALS: [StopSignal; 2] = [StopSignal::Interrupt, StopSignal::Terminate];

impl StopSignal {
    fn number(self) -> libc::c_int {
        match self {
            StopSignal::Interrupt => libc::SIGINT,
            StopSignal::Terminate => libc::SIGTERM,
        }
    }
}

impl std::fmt::Display for StopSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            StopSignal::Interrupt => "SIGINT",
            StopSignal::Terminate => "SIGTERM",
        })
    }
}

/// A stop signal's handler could not be registered, so the server does not start.
#[derive(Debug, thiserror::Error)]
#[error("could not register the {signal} handler: {source}")]
struct StopSignalError {
    signal: StopSignal,
    #[source]
    source: std::io::Error,
}

/// Wraps a failed registration in an `io::Error` that names the signal and keeps the OS error's
/// kind.
fn registered<S>(signal: StopSignal, result: std::io::Result<S>) -> std::io::Result<S> {
    result.map_err(|source| std::io::Error::new(source.kind(), StopSignalError { signal, source }))
}

/// Resolves once the server should stop, then cancels `stop` so every accept loop and
/// connection sees it: on SIGINT or SIGTERM, or when `Server.shutdown()` cancelled `stop`.
async fn until_stopped(stop: CancellationToken, signals: StopSignals) {
    tokio::select! {
        arrived = signals.arrival() => {
            if let Err(error) = arrived {
                tracing::error!(target: "mipyre::server", error = %error,
                    "the stop signals can no longer be watched, so the server stops");
            }
        }
        () = stop.cancelled() => {}
    }
    tracing::info!(target: "mipyre::server", "Shutting down gracefully...");
    println!("\n  Shutting down gracefully...");
    stop.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failed registration names the signal and keeps the OS error; a registered handler
    /// passes through as it is.
    #[test]
    fn a_stop_signal_that_cannot_be_registered_is_named_in_the_start_error() {
        for (signal, name) in [(StopSignal::Interrupt, "SIGINT"), (StopSignal::Terminate, "SIGTERM")] {
            let refused = std::io::Error::from_raw_os_error(libc::EINVAL);
            let err = registered::<()>(signal, Err(refused)).expect_err("a failed registration");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{err}");
            let text = err.to_string();
            assert!(text.contains(name), "{text}");
            assert!(
                text.contains(&std::io::Error::from_raw_os_error(libc::EINVAL).to_string()),
                "{text}"
            );
            let inner = err
                .get_ref()
                .and_then(|e| e.downcast_ref::<StopSignalError>())
                .expect("the io::Error carries which signal");
            assert_eq!(inner.signal, signal);
            assert_eq!(registered(signal, Ok(7)).expect("registered"), 7);
        }
    }

    /// Checks that a stop watcher that did not end cleanly is logged with its own error and counted.
    #[test]
    fn a_stop_watcher_that_did_not_end_cleanly_is_said_and_counted() {
        let counters = ServerCounters::new(false);
        let failure = ThreadJoinError::StillRunning;
        let log = mipyre_runtime::testkit::captured_log(|| report_stop_watcher_failure(&failure, &counters));
        assert_eq!(counters.read().stop_watcher_failures, 1);
        let line = log
            .lines()
            .find(|l| l.contains("stop watcher thread did not end cleanly"))
            .unwrap_or_else(|| panic!("the failure was not said:\n{log}"));
        assert!(line.contains("\"thread\":\"mipyre-stop\""), "{line}");
        assert!(line.contains("still running at the join deadline"), "{line}");
        assert!(line.contains("\"level\":\"ERROR\""), "{line}");
    }

    #[test]
    fn a_stop_signal_takes_its_default_action_only_when_it_was_the_default_and_no_run_listens() {
        assert!(takes_default_action(Disposition::Default, 0));
        assert!(!takes_default_action(Disposition::Default, 1));
        assert!(!takes_default_action(Disposition::Other, 0));
    }

    /// A real SIGTERM to this process stops the server the way SIGINT does; only `kill` makes the
    /// OS run the registered handler. One that came before a run registered is not that run's:
    /// its socket has no byte for it. An earlier run listens throughout, because with none the
    /// signal's default action would end the test process. One test sends both, so no other
    /// signal test's SIGTERM lands in the middle of it.
    #[test]
    fn sigterm_stops_the_run_registered_before_it_and_no_later_one() {
        const DEFAULT: StopSignalDispositions = StopSignalDispositions {
            interrupt: Disposition::Default,
            terminate: Disposition::Default,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let _earlier_run = StopSignals::install(DEFAULT).unwrap();
            // SAFETY: `kill` on this process's own pid touches no memory of ours.
            assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
            let signals = StopSignals::install(DEFAULT).unwrap();
            let mut byte = [0u8; 1];
            assert_eq!(
                signals.arrived.try_read(&mut byte).map_err(|e| e.kind()),
                Err(std::io::ErrorKind::WouldBlock),
                "a signal from before the install reached the run"
            );

            let stop = CancellationToken::new();
            let waiting = tokio::spawn(until_stopped(stop.clone(), signals));
            // SAFETY: as above.
            assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
            tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
                .await
                .expect("the stop did not come within 10 s")
                .unwrap();
            assert!(stop.is_cancelled());
        });
    }
}
