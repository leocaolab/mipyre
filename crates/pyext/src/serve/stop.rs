//! Waits for a running server's stop: SIGINT, SIGTERM (what systemd sends), or
//! `Server.shutdown()`.

use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio_util::sync::CancellationToken;

/// The registered stop signals. They are registered inside the runtime that waits on them,
/// before anything is spawned, so a signal that cannot be registered fails the start instead of
/// leaving a server that cannot be stopped.
pub(crate) struct StopSignals {
    interrupt: Signal,
    terminate: Signal,
}

impl StopSignals {
    /// Registers both signals; the error names the first that failed and keeps the OS error's
    /// kind.
    pub(crate) fn install() -> std::io::Result<Self> {
        Ok(Self {
            interrupt: registered(StopSignal::Interrupt, signal(SignalKind::interrupt()))?,
            terminate: registered(StopSignal::Terminate, signal(SignalKind::terminate()))?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopSignal {
    Interrupt,
    Terminate,
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
    result.map_err(|source| {
        std::io::Error::new(source.kind(), StopSignalError { signal, source })
    })
}

/// Resolves once the server should stop, then cancels `stop` so every accept loop and
/// connection sees it: on SIGINT or SIGTERM, or when `Server.shutdown()` cancelled `stop`.
pub(crate) async fn until_stopped(stop: CancellationToken, mut signals: StopSignals) {
    tokio::select! {
        _ = signals.interrupt.recv() => {}
        _ = signals.terminate.recv() => {}
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

    /// A real SIGTERM to this process stops the server the way SIGINT does; only `kill` makes the
    /// OS deliver the signal to the handler tokio registered.
    #[test]
    fn sigterm_stops_the_server_like_sigint() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stop = CancellationToken::new();
            let signals = StopSignals::install().unwrap();
            let waiting = tokio::spawn(until_stopped(stop.clone(), signals));
            // SAFETY: `kill` on this process's own pid touches no memory of ours.
            assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
            tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
                .await
                .expect("the stop did not come within 10 s")
                .unwrap();
            assert!(stop.is_cancelled());
        });
    }
}
