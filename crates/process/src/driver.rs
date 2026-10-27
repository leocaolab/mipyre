//! Starts the lane's template and worker processes, by fork on Linux and by spawning a new
//! Python on macOS; a test server's template is spawned on both (`_driver_spawn_template`).
//!
//! Which strategy to use is decided in the launcher (`mipyre._process_lane.template`), not here.

use std::io;
use std::os::fd::RawFd;

use super::os::StartedProcess;

/// Says how a worker gets the app.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartStrategy {
    /// The worker imports the app itself, so forking stays safe whatever the app's libraries do
    /// at import.
    Import,
    /// The template imports the app and forks workers, which replaces them faster and shares
    /// memory; the template must be single-threaded when it forks.
    Fork,
}

impl std::fmt::Display for StartStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            StartStrategy::Import => "import",
            StartStrategy::Fork => "fork",
        })
    }
}

/// Text that names no start strategy.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("start={0:?} is not one of \"import\", \"fork\"")]
pub struct ParseStartStrategyError(pub(crate) String);

impl std::str::FromStr for StartStrategy {
    type Err = ParseStartStrategyError;

    fn from_str(text: &str) -> Result<Self, ParseStartStrategyError> {
        match text {
            "import" => Ok(StartStrategy::Import),
            "fork" => Ok(StartStrategy::Fork),
            other => Err(ParseStartStrategyError(other.to_string())),
        }
    }
}

/// How this platform starts a new template or `start="import"` worker ([`super::os::SPAWNS_NEW_PROCESSES`]).
pub const DRIVER_NAME: &str = if super::os::SPAWNS_NEW_PROCESSES {
    "spawn"
} else {
    "fork"
};

/// A template or worker that did not start. A failed fork or spawn keeps the OS error, whose errno the template sends
/// to the server as its answer to a fork request.
#[derive(Debug, thiserror::Error)]
pub enum ProcessStartError {
    #[error("NUL in {0:?}")]
    NulInArgument(String),
    #[error("NUL in the environment entry {0:?}")]
    NulInEnvironment(Vec<u8>),
    #[error("posix_spawn {python} -m {module} keeping fds {keep_fds:?}: {source}")]
    Spawn {
        python: String,
        module: String,
        keep_fds: Vec<RawFd>,
        source: io::Error,
    },
    #[error("fork: {0}")]
    Fork(io::Error),
}

/// Starts the template or a `start="import"` worker: it calls `fork`, which takes about a millisecond, or, where
/// nothing may fork ([`super::os::SPAWNS_NEW_PROCESSES`]), spawns `python -m <module> <args>`.
///
/// # Errors
///
/// Returns why the process did not start.
pub fn start_new_process(
    fork: impl FnOnce() -> io::Result<StartedProcess>,
    python: &str,
    module: &str,
    args: &[String],
    keep_fds: &[RawFd],
    env: &[(String, String)],
) -> Result<StartedProcess, ProcessStartError> {
    if super::os::SPAWNS_NEW_PROCESSES {
        spawn_python(python, module, args, keep_fds, env)
    } else {
        fork().map_err(ProcessStartError::Fork)
    }
}

/// Spawns `python -m <module> <args>`, which inherits `keep_fds`, the standard streams, and this
/// environment with `env` overriding it. Safe while other threads run: this process's fds keep
/// their close-on-exec flag (`os::spawn`).
///
/// # Errors
///
/// Refuses an argument or environment entry holding a NUL, and returns the OS error `posix_spawn` met.
pub fn spawn_python(
    python: &str,
    module: &str,
    args: &[String],
    keep_fds: &[RawFd],
    env: &[(String, String)],
) -> Result<StartedProcess, ProcessStartError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let cstr = |s: &str| CString::new(s).map_err(|_| ProcessStartError::NulInArgument(s.to_string()));
    let mut argv: Vec<CString> = vec![cstr(python)?, cstr("-m")?, cstr(module)?];
    for a in args {
        argv.push(cstr(a)?);
    }
    let mut envp: Vec<CString> = std::env::vars_os()
        .filter(|(k, _)| !env.iter().any(|(name, _)| k.to_str() == Some(name.as_str())))
        .map(|(k, v)| {
            let entry = [k.as_bytes(), b"=", v.as_bytes()].concat();
            CString::new(entry).map_err(|e| ProcessStartError::NulInEnvironment(e.into_vec()))
        })
        .collect::<Result<_, _>>()?;
    for (k, v) in env {
        envp.push(cstr(&format!("{k}={v}"))?);
    }
    let pid = super::os::spawn(&argv, &envp, keep_fds).map_err(|source| ProcessStartError::Spawn {
        python: python.to_string(),
        module: module.to_string(),
        keep_fds: keep_fds.to_vec(),
        source,
    })?;
    Ok(StartedProcess::Parent { pid })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nul_in_an_argument_is_refused_before_anything_is_spawned() {
        let refused = spawn_python("python", "a\0b", &[], &[], &[]).err();
        assert_eq!(refused.map(|e| e.to_string()), Some(r#"NUL in "a\0b""#.to_string()));
    }

    #[test]
    fn a_failed_spawn_keeps_the_os_errno() {
        let Err(refused) = spawn_python("/nonexistent/python", "m", &[], &[], &[]) else {
            panic!("a python that does not exist was spawned");
        };
        assert!(refused
            .to_string()
            .starts_with("posix_spawn /nonexistent/python -m m keeping fds []: "));
        assert!(
            matches!(refused, ProcessStartError::Spawn { source, .. } if source.raw_os_error() == Some(libc::ENOENT))
        );
    }
}
