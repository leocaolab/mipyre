//! Every mipyre setting, declared once: its name, from which its command-line option and its
//! environment variable are spelled, its `pyproject.toml` key, the places app code gives it, the
//! shape its value has, its default and its help.
//!
//! A value the deployment must be able to override is a flag and has one entry here; one it must
//! not is a constant where it is used. The `--help` text, the settings table in the docs and
//! the Python-side declarations are read from this table, never written a second time.

use std::fmt::Display;
use std::num::{NonZeroU16, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use mipyre_subinterp::gc::GcMode;
use pyo3::prelude::*;

use mipyre_process::{FailurePolicy, LaneSlotBytes, StartStrategy};

use super::config::{
    code_value, given_line, read_flag, settled_line, take, CodeValue, DeploymentSources, DeploymentValue, FlagError,
    FlagSource, FlagValue, ServerFlagsError,
};
use mipyre_server::AcceptMode;

/// Prefixes of `MIPYRE_` variables that are not ours to read: the test and benchmark tools' own.
/// Written here so that a deployment that sets one is not refused as unknown.
pub(crate) const RESERVED_PREFIXES: [&str; 2] = ["MIPYRE_TEST_", "MIPYRE_BENCH_"];

/// One setting, declared once: every source's key, its shape, its default and its help. Python
/// reads the table as `mipyre.engine._flags()`, a list of these.
#[pyclass(module = "mipyre.engine", name = "Flag", frozen)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Flag {
    /// What docs, errors and summaries call it: `threads`, `process.workers`.
    pub(crate) name: &'static str,
    /// The key in `pyproject.toml`, in its table.
    pub(crate) file: PyprojectKey,
    /// Every place app code gives it; empty when code cannot.
    pub(crate) code: &'static [CodeKey],
    pub(crate) shape: FlagShape,
    /// The default as `--help` and the docs print it; a computed default names its rule.
    pub(crate) default: &'static str,
    pub(crate) scope: FlagScope,
    /// When a server uses the flag's value; a value given for a server that does not use it is
    /// an error.
    pub(crate) used: FlagUse,
    pub(crate) doc: &'static str,
}

/// Returns a setting's `mipyre run` / `mipyre dev` option: `process.queue_per_worker` is
/// `--process-queue-per-worker`.
pub(crate) fn cli_option(setting: &str) -> String {
    format!("--{}", setting.replace(['.', '_'], "-"))
}

/// Returns a setting's environment variable: `process.queue_per_worker` is
/// `MIPYRE_PROCESS_QUEUE_PER_WORKER`.
pub(crate) fn env_variable(setting: &str) -> String {
    format!("MIPYRE_{}", setting.replace('.', "_").to_ascii_uppercase())
}

/// When a flag's value is used: by any server, or only by one of a kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlagUse {
    /// Every server, or (a process-wide flag) every process.
    Always,
    /// A sub-interpreter pool: the multi-thread server with sub-interpreter routes. The
    /// thread-per-core server runs one sub-interpreter per thread, sized by `threads`.
    Pool,
    /// The thread-per-core server on macOS, the one place its listeners can be fanned out.
    MacosTpc,
    /// A server with sub-interpreter workers, which collect garbage and keep isolated copies.
    SubinterpreterWorkers,
    /// A server with main-interpreter routes (or a fallback).
    MainRoutes,
    /// A server with `async def` routes on sub-interpreter workers, whose async pool the capacity
    /// sizes: the pool, and the thread-per-core server, which hands them to an async pool too.
    AsyncWorkerRoutes,
    /// A server with `runtime="process"` routes.
    ProcessRoutes,
    /// A server taking TCP connections.
    TcpTransport,
    /// A process an entry that reloads may start: a script or `mipyre run` (`mipyre dev`
    /// reloads by itself, and an embedded engine never does).
    ReloadingEntry,
}

impl FlagUse {
    /// The servers (or processes) that use a flag, said after "it is used only by".
    pub(crate) fn users(self) -> &'static str {
        match self {
            FlagUse::Always => "every server",
            FlagUse::Pool => {
                "the sub-interpreter pool (tpc off, with sub-interpreter routes); the thread-per-core \
                 server runs one sub-interpreter per thread, sized by threads"
            }
            FlagUse::MacosTpc => "the thread-per-core server on macOS",
            FlagUse::SubinterpreterWorkers => "a server with sub-interpreter workers",
            FlagUse::MainRoutes => "a server with main-interpreter routes",
            FlagUse::AsyncWorkerRoutes => "a server with async def routes on sub-interpreter workers",
            FlagUse::ProcessRoutes => "a server with runtime=\"process\" routes",
            FlagUse::TcpTransport => "a server taking TCP connections",
            FlagUse::ReloadingEntry => "a script's app.run() and mipyre run (mipyre dev reloads by itself)",
        }
    }
}

impl EntryPoint {
    /// Says whether this entry point reloads when its `reload` flag says so.
    pub(crate) fn reloads(self) -> bool {
        matches!(self, EntryPoint::Script | EntryPoint::CliRun)
    }
}

impl std::fmt::Display for EntryPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            EntryPoint::Script => "a script's app.run()",
            EntryPoint::CliRun => "mipyre run",
            EntryPoint::CliDev => "mipyre dev",
            EntryPoint::Embedded => "the embedded engine (TestClient, start())",
        })
    }
}

/// A key of `pyproject.toml`, in the table it belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PyprojectKey {
    /// A key of `[tool.mipyre]`.
    Top(&'static str),
    /// A key of `[tool.mipyre.process]`.
    Process(&'static str),
}

impl Display for PyprojectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PyprojectKey::Top(key) => write!(f, "[tool.mipyre] {key}"),
            PyprojectKey::Process(key) => write!(f, "[tool.mipyre.process] {key}"),
        }
    }
}

/// One place app code gives a flag: the call and its keyword (`ProcessLanes` + `"workers"` reads
/// `ProcessLanes(workers=)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodeKey {
    pub(crate) call: CodeCall,
    pub(crate) keyword: &'static str,
}

/// The calls through which app code gives flag values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodeCall {
    /// `app.run(reload=)`.
    Run,
    /// `ProcessLanes(...)`.
    ProcessLanes,
    /// `ServerFlags(...)`, the programmatic one (`TestClient(config=)`, `start(config=)`).
    ServerFlags,
    /// The app's constructor, `Mipyre(...)`.
    AppConstructor,
}

impl Display for CodeCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CodeCall::Run => "run()",
            CodeCall::ProcessLanes => "ProcessLanes()",
            CodeCall::ServerFlags => "ServerFlags()",
            CodeCall::AppConstructor => "Mipyre()",
        })
    }
}

impl Display for CodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let call = self.call.to_string();
        write!(f, "{}({}=)", call.trim_end_matches("()"), self.keyword)
    }
}

/// The unit a duration is written in, the same in every source of one flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeUnit {
    /// A decimal number of seconds: `0.5`.
    Seconds,
    /// A whole number of milliseconds.
    Millis,
    /// A whole number of microseconds.
    Micros,
}

impl TimeUnit {
    fn words(self) -> &'static str {
        match self {
            TimeUnit::Seconds => "seconds",
            TimeUnit::Millis => "milliseconds",
            TimeUnit::Micros => "microseconds",
        }
    }
}

/// What a flag's text parses into; each parses into the value's owner type, and an option names
/// its owner, whose [`FlagChoice::ALL`], `FromStr` and `Display` are its choices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlagShape {
    /// `1` / `true` / `yes` / `on` or `0` / `false` / `no` / `off`.
    Bool,
    /// A whole number, 0 or more.
    Count,
    /// A whole number, 1 or more.
    Positive,
    /// A time, 0 or more.
    Duration {
        unit: TimeUnit,
    },
    /// A time above 0.
    PositiveDuration {
        unit: TimeUnit,
    },
    /// A TCP port, 0 to 65535; 0 binds whichever port the kernel picks.
    Port,
    /// Comma-separated TCP ports.
    Ports,
    /// A host name or address to bind.
    Host,
    /// Comma-separated DNS names.
    Hosts,
    /// Comma-separated IP addresses and CIDR blocks of reverse proxies.
    Proxies,
    /// A filesystem path.
    Path,
    /// A process lane's slot size in bytes, at least [`mipyre_process::MIN_SLOT_BYTES`].
    SlotBytes,
    GcMode,
    AcceptMode,
    StartStrategy,
    FailurePolicy,
}

impl FlagShape {
    /// Returns the shape's name, one word: what a command line builds its option from.
    pub(crate) fn word(self) -> &'static str {
        match self {
            FlagShape::Bool => "bool",
            FlagShape::Count => "count",
            FlagShape::Positive => "positive",
            FlagShape::Duration { .. } => "duration",
            FlagShape::PositiveDuration { .. } => "positive_duration",
            FlagShape::Port => "port",
            FlagShape::Ports => "ports",
            FlagShape::Host => "host",
            FlagShape::Hosts => "hosts",
            FlagShape::Proxies => "proxies",
            FlagShape::Path => "path",
            FlagShape::SlotBytes => "slot_bytes",
            FlagShape::GcMode | FlagShape::AcceptMode | FlagShape::StartStrategy | FlagShape::FailurePolicy => "choice",
        }
    }

    /// Returns what a value of this shape must be, in the words an error uses.
    pub(crate) fn expected(self) -> String {
        match self {
            FlagShape::Bool => "expected 1/true/yes/on or 0/false/no/off".into(),
            FlagShape::Count => "expected a whole number (0 or more)".into(),
            FlagShape::Positive => "expected a positive whole number".into(),
            FlagShape::Duration { unit } => format!("expected a number of {} (0 or more)", unit.words()),
            FlagShape::PositiveDuration {
                unit: TimeUnit::Seconds,
            } => "expected a number of seconds, at least 0.001".into(),
            FlagShape::PositiveDuration { unit } => format!("expected a positive number of {}", unit.words()),
            FlagShape::Port => "expected a port from 0 to 65535 (0: the kernel picks one)".into(),
            FlagShape::Ports => "expected comma-separated ports from 1 to 65535".into(),
            FlagShape::Host => "expected a host name or address".into(),
            FlagShape::Hosts => "expected comma-separated host names".into(),
            FlagShape::Proxies => "expected comma-separated IP addresses or CIDR blocks".into(),
            FlagShape::Path => "expected a path".into(),
            FlagShape::SlotBytes => format!(
                "expected a number of bytes, at least {}",
                mipyre_process::MIN_SLOT_BYTES
            ),
            FlagShape::GcMode => one_of::<GcMode>(),
            #[cfg(target_os = "macos")]
            FlagShape::AcceptMode => one_of::<AcceptMode>(),
            #[cfg(not(target_os = "macos"))]
            FlagShape::AcceptMode => format!("{} (fanout exists on macOS only)", one_of::<AcceptMode>()),
            FlagShape::StartStrategy => one_of::<StartStrategy>(),
            FlagShape::FailurePolicy => one_of::<FailurePolicy>(),
        }
    }
}

fn one_of<C: FlagChoice>() -> String {
    let choices: Vec<String> = C::ALL.iter().map(|c| format!("{c:?}", c = c.to_string())).collect();
    format!("expected one of {}", choices.join(", "))
}

/// An option flag's owner type: every value it has. The owner's `FromStr` and `Display` read and
/// print it, so the table writes no choice a second time.
pub(crate) trait FlagChoice: Copy + FromStr + Display + 'static {
    const ALL: &'static [Self];
}

impl FlagChoice for GcMode {
    const ALL: &'static [Self] = &[GcMode::Count, GcMode::Idle, GcMode::Off];
}

impl FlagChoice for AcceptMode {
    #[cfg(target_os = "macos")]
    const ALL: &'static [Self] = &[AcceptMode::PerThreadListener, AcceptMode::Fanout];
    #[cfg(not(target_os = "macos"))]
    const ALL: &'static [Self] = &[AcceptMode::PerThreadListener];
}

impl FlagChoice for StartStrategy {
    const ALL: &'static [Self] = &[StartStrategy::Import, StartStrategy::Fork];
}

impl FlagChoice for FailurePolicy {
    const ALL: &'static [Self] = &[FailurePolicy::Isolate, FailurePolicy::Exit];
}

/// A Rust type a flag's value is: which shapes it is written in, and how text of one of them
/// reads into it.
pub(crate) trait FlagType: Sized {
    /// Whether a flag of `shape` can hold this type.
    fn fits(shape: FlagShape) -> bool;
    /// Reads `raw`, written in `shape`; `None` when it is not a value of that shape (the error
    /// says [`FlagShape::expected`]).
    fn read(raw: &str, shape: FlagShape) -> Option<Self>;
    /// Writes the value as a flag of its shape is written, for summaries and errors.
    fn written(&self) -> String;
}

impl FlagType for bool {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::Bool
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        }
    }

    fn written(&self) -> String {
        if *self {
            "on".into()
        } else {
            "off".into()
        }
    }
}

impl FlagType for u64 {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::Count
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        raw.parse().ok()
    }

    fn written(&self) -> String {
        self.to_string()
    }
}

impl FlagType for NonZeroUsize {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::Positive
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        raw.parse().ok()
    }

    fn written(&self) -> String {
        self.to_string()
    }
}

impl FlagType for Duration {
    fn fits(shape: FlagShape) -> bool {
        matches!(shape, FlagShape::Duration { .. } | FlagShape::PositiveDuration { .. })
    }

    fn read(raw: &str, shape: FlagShape) -> Option<Self> {
        let (unit, positive) = match shape {
            FlagShape::Duration { unit } => (unit, false),
            FlagShape::PositiveDuration { unit } => (unit, true),
            _ => return None,
        };
        let time = match unit {
            TimeUnit::Seconds => {
                let seconds: f64 = raw.parse().ok()?;
                Duration::try_from_secs_f64(seconds).ok()?
            }
            TimeUnit::Millis => Duration::from_millis(raw.parse().ok()?),
            TimeUnit::Micros => Duration::from_micros(raw.parse().ok()?),
        };
        // A positive time in seconds is used in whole milliseconds: below one, it would be 0.
        let least = match unit {
            TimeUnit::Seconds => Duration::from_millis(1),
            TimeUnit::Millis => Duration::from_millis(1),
            TimeUnit::Micros => Duration::from_micros(1),
        };
        (!positive || time >= least).then_some(time)
    }

    fn written(&self) -> String {
        format!("{self:?}")
    }
}

impl FlagType for u16 {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::Port
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        raw.parse().ok()
    }

    fn written(&self) -> String {
        self.to_string()
    }
}

impl FlagType for Vec<NonZeroU16> {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::Ports
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        let ports: Option<Vec<NonZeroU16>> = raw
            .split(',')
            .map(str::trim)
            .filter(|port| !port.is_empty())
            .map(|port| port.parse().ok())
            .collect();
        ports.filter(|ports| !ports.is_empty())
    }

    fn written(&self) -> String {
        self.iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
    }
}

impl FlagType for String {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::Host
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        (!raw.is_empty()).then(|| raw.to_string())
    }

    fn written(&self) -> String {
        self.clone()
    }
}

impl FlagType for Vec<String> {
    fn fits(shape: FlagShape) -> bool {
        matches!(shape, FlagShape::Hosts | FlagShape::Proxies)
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        let hosts: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .map(String::from)
            .collect();
        (!hosts.is_empty()).then_some(hosts)
    }

    fn written(&self) -> String {
        self.join(",")
    }
}

impl FlagType for PathBuf {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::Path
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        (!raw.is_empty()).then(|| PathBuf::from(raw))
    }

    fn written(&self) -> String {
        self.display().to_string()
    }
}

impl FlagType for LaneSlotBytes {
    fn fits(shape: FlagShape) -> bool {
        shape == FlagShape::SlotBytes
    }

    fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
        LaneSlotBytes::new(raw.parse().ok()?)
    }

    fn written(&self) -> String {
        self.get().to_string()
    }
}

macro_rules! choice_flag_type {
    ($($owner:ident),*) => {$(
        impl FlagType for $owner {
            fn fits(shape: FlagShape) -> bool {
                shape == FlagShape::$owner
            }

            fn read(raw: &str, _shape: FlagShape) -> Option<Self> {
                raw.parse().ok()
            }

            fn written(&self) -> String {
                self.to_string()
            }
        }
    )*};
}

choice_flag_type!(GcMode, AcceptMode, StartStrategy, FailurePolicy);

/// Whether a flag is about one server or about the whole process: only the first kind is in
/// `ServerFlags`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlagScope {
    Server,
    /// `reload` and `log`, each read once by its one reader.
    Process,
}

/// Which entry point started this run; it says how a command-line flag is named in an error
/// (`mipyre run --threads`) and, later, which flags it can use (`reload`).
#[pyclass(module = "mipyre.engine", eq, eq_int, frozen, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum EntryPoint {
    /// A script's `app.run()`.
    Script,
    /// `mipyre run`.
    CliRun,
    /// `mipyre dev`.
    CliDev,
    /// No entry point: the engine embedded (`TestClient`, a raw `start()`).
    #[default]
    Embedded,
}

impl EntryPoint {
    /// Returns the command whose options carry flags, or `None` when this entry point has no
    /// command line of mipyre's.
    pub(crate) fn command(self) -> Option<&'static str> {
        match self {
            EntryPoint::CliRun => Some("mipyre run"),
            EntryPoint::CliDev => Some("mipyre dev"),
            EntryPoint::Script | EntryPoint::Embedded => None,
        }
    }
}

const fn run(keyword: &'static str) -> CodeKey {
    CodeKey {
        call: CodeCall::Run,
        keyword,
    }
}

const fn server_flags(keyword: &'static str) -> CodeKey {
    CodeKey {
        call: CodeCall::ServerFlags,
        keyword,
    }
}

const fn process_lanes(keyword: &'static str) -> CodeKey {
    CodeKey {
        call: CodeCall::ProcessLanes,
        keyword,
    }
}

/// Declares every flag once, with the Rust type its value has: [`FLAGS`], the typed fields of
/// [`Deployment`] (every flag), of [`ServerFlags`] and of [`ServerFlagValues`] (the server flags),
/// all in the same order.
macro_rules! flags {
    (@flag $scope:ident {
        name: $name:literal,
        file: $file:expr,
        code: [$($code:expr),* $(,)?],
        shape: $shape:expr,
        used: $used:ident,
        default: $default:literal,
        doc: $doc:literal $(,)?
    }) => {
        Flag {
            name: $name,
            file: $file,
            code: &[$($code),*],
            shape: $shape,
            default: $default,
            scope: FlagScope::$scope,
            used: FlagUse::$used,
            doc: $doc,
        }
    };
    (
        server { $($sfield:ident: $sty:ty = $sdecl:tt),* $(,)? }
        process { $($pfield:ident: $pty:ty = $pdecl:tt),* $(,)? }
    ) => {
        /// Every setting mipyre has, in the order `--help` and the docs list them: the server
        /// flags, then the process-wide ones.
        pub(crate) const FLAGS: &[Flag] = &[
            $(flags!(@flag Server $sdecl),)*
            $(flags!(@flag Process $pdecl),)*
        ];

        /// What the deployment gave, by flag: each value with the source it came from, or `None`
        /// where the deployment gave nothing. One of these is made per entry point.
        #[pyclass(module = "mipyre.engine", frozen)]
        #[derive(Debug, Clone, Default)]
        pub(crate) struct Deployment {
            pub(crate) entry: EntryPoint,
            $(pub(crate) $sfield: Option<DeploymentValue<$sty>>,)*
            $(pub(crate) $pfield: Option<DeploymentValue<$pty>>,)*
        }

        /// The flag values app code gives one server (`ServerFlags(...)`, the `ProcessLanes`), each
        /// with the place in the code that gave it; `None` where code gave nothing.
        #[pyclass(module = "mipyre.engine", frozen)]
        #[derive(Debug, Clone, Default)]
        pub(crate) struct ServerFlags {
            $(pub(crate) $sfield: Option<CodeValue<$sty>>,)*
        }

        /// One server's flag values once the deployment is laid over the code: from the
        /// deployment (with the code's different value it overrode), from code, or `None` for
        /// the default, which the owner of each value works out.
        #[derive(Debug, Clone)]
        pub(crate) struct ServerFlagValues {
            $(pub(crate) $sfield: Option<FlagValue<$sty>>,)*
        }

        impl Deployment {
            /// Reads every flag from `sources`, collecting each value that cannot be used rather
            /// than stopping at the first.
            pub(super) fn read_flags(entry: EntryPoint, sources: &DeploymentSources<'_>, errors: &mut Vec<FlagError>) -> Self {
                Deployment {
                    entry,
                    $($sfield: take(read_flag::<$sty>(&const { flags!(@flag Server $sdecl) }, sources), errors),)*
                    $($pfield: take(read_flag::<$pty>(&const { flags!(@flag Process $pdecl) }, sources), errors),)*
                }
            }

            /// Returns one line per value the deployment gave: the flag, the value and where it came
            /// from.
            pub(crate) fn summary(&self) -> Vec<String> {
                let mut lines = Vec::new();
                $(given_line(&mut lines, &const { flags!(@flag Server $sdecl) }, &self.$sfield);)*
                $(given_line(&mut lines, &const { flags!(@flag Process $pdecl) }, &self.$pfield);)*
                lines
            }

            /// Lays this deployment over `code`, one server's flags: a deployment value wins
            /// and keeps the code's value it replaced when the two differ.
            pub(crate) fn over(&self, code: &ServerFlags) -> ServerFlagValues {
                ServerFlagValues {
                    $($sfield: FlagValue::over(self.$sfield.clone(), code.$sfield.clone()),)*
                }
            }
        }

        impl ServerFlags {
            /// Sets the flag given in code as `keyword` of `call` to `raw`, written as Python wrote
            /// it.
            pub(crate) fn give(&mut self, call: CodeCall, keyword: &str, raw: &str) -> Result<(), ServerFlagsError> {
                $({
                    let flag: &'static Flag = &const { flags!(@flag Server $sdecl) };
                    if let Some(at) = flag.code.iter().find(|key| key.call == call && key.keyword == keyword) {
                        self.$sfield = Some(code_value::<$sty>(flag, at, raw)?);
                        return Ok(());
                    }
                })*
                Err(ServerFlagsError::UnknownKeyword { call, keyword: keyword.to_string() })
            }
        }

        impl ServerFlagValues {
            /// Returns every server flag somebody gave, with where it came from.
            pub(crate) fn given(&self) -> Vec<(&'static Flag, FlagSource)> {
                let mut given = Vec::new();
                $(if let Some(value) = &self.$sfield {
                    given.push((&const { flags!(@flag Server $sdecl) }, value.source()));
                })*
                given
            }

            /// Returns one line per server flag somebody gave: its value and where it came from,
            /// and for a deployment value that replaced the code's, what it replaced. A start
            /// prints these.
            pub(crate) fn summary(&self) -> Vec<String> {
                let mut lines = Vec::new();
                $(settled_line(&mut lines, &const { flags!(@flag Server $sdecl) }, &self.$sfield);)*
                lines
            }
        }
    };
}

flags! {
    server {
    threads: NonZeroUsize = {
        name: "threads", file: PyprojectKey::Top("threads"),
        code: [server_flags("threads")],
        shape: FlagShape::Positive,
        used: Always,
        default: "one per physical core (thread-per-core); with tpc off, one per logical CPU",
        doc: "the server's threads: thread-per-core threads, or with tpc off the I/O threads",
    },
    subinterpreters: NonZeroUsize = {
        name: "subinterpreters",
        file: PyprojectKey::Top("subinterpreters"),
        code: [server_flags("subinterpreters")],
        shape: FlagShape::Positive,
        used: Pool,
        default: "one per logical CPU, at least 2",
        doc: "the sub-interpreter pool's size, with tpc off; thread-per-core runs one per thread",
    },
    tpc: bool = {
        name: "tpc", file: PyprojectKey::Top("tpc"),
        code: [server_flags("tpc")],
        shape: FlagShape::Bool,
        used: Always,
        default: "on",
        doc: "thread-per-core; off runs the multi-thread server",
    },
    tpc_accept: AcceptMode = {
        name: "tpc_accept", file: PyprojectKey::Top("tpc_accept"),
        code: [server_flags("tpc_accept")],
        shape: FlagShape::AcceptMode,
        used: MacosTpc,
        default: "listener",
        doc: "how thread-per-core threads take connections on macOS: a listener each, or one fanout acceptor",
    },
    gc_mode: GcMode = {
        name: "gc_mode", file: PyprojectKey::Top("gc_mode"),
        code: [server_flags("gc_mode")],
        shape: FlagShape::GcMode,
        used: SubinterpreterWorkers,
        default: "count",
        doc: "when a sub-interpreter worker runs gc.collect()",
    },
    gc_threshold: u64 = {
        name: "gc_threshold",
        file: PyprojectKey::Top("gc_threshold"),
        code: [server_flags("gc_threshold")],
        shape: FlagShape::Count,
        used: SubinterpreterWorkers,
        default: "100000",
        doc: "requests between collections in count mode; 0 never collects",
    },
    gc_idle_ms: Duration = {
        name: "gc_idle_ms", file: PyprojectKey::Top("gc_idle_ms"),
        code: [server_flags("gc_idle_ms")],
        shape: FlagShape::PositiveDuration { unit: TimeUnit::Millis },
        used: SubinterpreterWorkers,
        default: "100",
        doc: "the lull, in milliseconds, after which idle mode collects",
    },
    gc_oom_failsafe: u64 = {
        name: "gc_oom_failsafe",
        file: PyprojectKey::Top("gc_oom_failsafe"),
        code: [server_flags("gc_oom_failsafe")],
        shape: FlagShape::Count,
        used: SubinterpreterWorkers,
        default: "50000",
        doc: "requests after which idle mode collects even without a lull; 0 collects only in lulls",
    },
    bridge_workers: NonZeroUsize = {
        name: "bridge_workers",
        file: PyprojectKey::Top("bridge_workers"),
        code: [server_flags("bridge_workers")],
        shape: FlagShape::Positive,
        used: MainRoutes,
        default: "4 (thread-per-core) / 512 (tpc off)",
        doc: "most threads that run main-interpreter def routes",
    },
    bridge_capacity: NonZeroUsize = {
        name: "bridge_capacity",
        file: PyprojectKey::Top("bridge_capacity"),
        code: [server_flags("bridge_capacity")],
        shape: FlagShape::Positive,
        used: MainRoutes,
        default: "16 per main-interpreter thread",
        doc: "main-interpreter requests queued (async def: queued or running) before 503",
    },
    async_queue_capacity: NonZeroUsize = {
        name: "async_queue_capacity",
        file: PyprojectKey::Top("async_queue_capacity"),
        code: [server_flags("async_queue_capacity")],
        shape: FlagShape::Positive,
        used: AsyncWorkerRoutes,
        default: "128 per pool worker",
        doc: "async def requests queued or running before 503",
    },
    metrics: bool = {
        name: "metrics", file: PyprojectKey::Top("metrics"),
        code: [server_flags("metrics")],
        shape: FlagShape::Bool,
        used: Always,
        default: "off",
        doc: "count every answer and sample RSS",
    },
    host: String = {
        name: "host", file: PyprojectKey::Top("host"),
        code: [server_flags("host")],
        shape: FlagShape::Host,
        used: TcpTransport,
        default: "127.0.0.1",
        doc: "the address to bind",
    },
    port: u16 = {
        name: "port", file: PyprojectKey::Top("port"),
        code: [server_flags("port")],
        shape: FlagShape::Port,
        used: TcpTransport,
        default: "8000",
        doc: "the port to bind",
    },
    tls_cert: PathBuf = {
        name: "tls_cert", file: PyprojectKey::Top("tls_cert"),
        code: [server_flags("tls_cert")],
        shape: FlagShape::Path,
        used: TcpTransport,
        default: "none (plain HTTP)",
        doc: "the TLS certificate chain (PEM); given with tls_key",
    },
    tls_key: PathBuf = {
        name: "tls_key", file: PyprojectKey::Top("tls_key"),
        code: [server_flags("tls_key")],
        shape: FlagShape::Path,
        used: TcpTransport,
        default: "none (plain HTTP)",
        doc: "the TLS private key (PEM); given with tls_cert",
    },
    extra_tls_ports: Vec<NonZeroU16> = {
        name: "extra_tls_ports",
        file: PyprojectKey::Top("extra_tls_ports"),
        code: [server_flags("extra_tls_ports")],
        shape: FlagShape::Ports,
        used: TcpTransport,
        default: "none",
        doc: "more ports served over TLS",
    },
    extra_plain_ports: Vec<NonZeroU16> = {
        name: "extra_plain_ports",
        file: PyprojectKey::Top("extra_plain_ports"),
        code: [server_flags("extra_plain_ports")],
        shape: FlagShape::Ports,
        used: TcpTransport,
        default: "none",
        doc: "more ports served over plain HTTP",
    },
    allowed_hosts: Vec<String> = {
        name: "allowed_hosts",
        file: PyprojectKey::Top("allowed_hosts"),
        code: [server_flags("allowed_hosts")],
        shape: FlagShape::Hosts,
        used: TcpTransport,
        default: "localhost and IP addresses",
        doc: "DNS names this server is reached by, besides localhost and IP addresses ('*' for any)",
    },
    trusted_proxies: Vec<String> = {
        name: "trusted_proxies",
        file: PyprojectKey::Top("trusted_proxies"),
        code: [server_flags("trusted_proxies")],
        shape: FlagShape::Proxies,
        used: TcpTransport,
        default: "none: the TCP peer is the client",
        doc: "reverse proxies (IP addresses and CIDR blocks) whose X-Forwarded-For names the client",
    },
    isolate_dir: PathBuf = {
        name: "isolate_dir", file: PyprojectKey::Top("isolate_dir"),
        code: [server_flags("isolate_dir")],
        shape: FlagShape::Path,
        used: SubinterpreterWorkers,
        default: "mipyre in the user's cache directory",
        doc: "where each sub-interpreter worker keeps its copy of an isolated extension",
    },
    process_workers: NonZeroUsize = {
        name: "process.workers",
        file: PyprojectKey::Process("workers"),
        code: [process_lanes("workers"), server_flags("process_workers")],
        shape: FlagShape::Positive,
        used: ProcessRoutes,
        default: "one per physical core",
        doc: "worker processes for runtime=\"process\" routes",
    },
    process_queue: NonZeroUsize = {
        name: "process.queue_per_worker",
        file: PyprojectKey::Process("queue_per_worker"),
        code: [process_lanes("queue_per_worker"), server_flags("process_queue_per_worker")],
        shape: FlagShape::Positive,
        used: ProcessRoutes,
        default: "128",
        doc: "requests each process worker holds before 503",
    },
    process_slot_bytes: LaneSlotBytes = {
        name: "process.slot_bytes",
        file: PyprojectKey::Process("slot_bytes"),
        code: [process_lanes("slot_bytes"), server_flags("process_slot_bytes")],
        shape: FlagShape::SlotBytes,
        used: ProcessRoutes,
        default: "262144",
        doc: "the largest request and answer a process worker takes, in bytes",
    },
    process_start: StartStrategy = {
        name: "process.start", file: PyprojectKey::Process("start"),
        code: [process_lanes("start"), server_flags("process_start")],
        shape: FlagShape::StartStrategy,
        used: ProcessRoutes,
        default: "import",
        doc: "how a process worker gets the app: import it, or fork from a template that did",
    },
    process_timeout: Duration = {
        name: "process.timeout",
        file: PyprojectKey::Process("timeout"),
        code: [process_lanes("timeout"), server_flags("process_timeout")],
        shape: FlagShape::PositiveDuration { unit: TimeUnit::Seconds },
        used: ProcessRoutes,
        default: "30",
        doc: "a process handler's time, in seconds; past it the worker is killed and the request answered 504",
    },
    process_spin_us: Duration = {
        name: "process.spin_us",
        file: PyprojectKey::Process("spin_us"),
        code: [server_flags("process_spin_us")],
        shape: FlagShape::Duration { unit: TimeUnit::Micros },
        used: ProcessRoutes,
        default: "50",
        doc: "how long a process worker spins for the next request, in microseconds, before it sleeps",
    },
    process_host_failure: FailurePolicy = {
        name: "process.host_failure",
        file: PyprojectKey::Process("host_failure"),
        code: [server_flags("process_host_failure")],
        shape: FlagShape::FailurePolicy,
        used: ProcessRoutes,
        default: "isolate",
        doc: "what a failed process lane or host does: take it out of service, or end the server",
    },
    }
    process {
    reload: bool = {
        name: "reload", file: PyprojectKey::Top("reload"),
        code: [run("reload")],
        shape: FlagShape::Bool,
        used: ReloadingEntry,
        default: "off",
        doc: "restart the app when its files change",
    },
    log: bool = {
        name: "log", file: PyprojectKey::Top("log"),
        code: [CodeKey { call: CodeCall::AppConstructor, keyword: "debug" }],
        shape: FlagShape::Bool,
        used: Always,
        default: "off; on under mipyre dev",
        doc: "turn Python logging on",
    },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn every_source_key_names_one_flag() {
        let unique = |keys: Vec<String>| keys.len() == keys.iter().collect::<HashSet<_>>().len();
        assert!(unique(FLAGS.iter().map(|f| f.name.to_string()).collect()));
        assert!(unique(FLAGS.iter().map(|f| cli_option(f.name)).collect()));
        assert!(unique(FLAGS.iter().map(|f| env_variable(f.name)).collect()));
        assert!(unique(FLAGS.iter().map(|f| format!("{:?}", f.file)).collect()));
        assert!(unique(
            FLAGS.iter().flat_map(|f| f.code).map(ToString::to_string).collect()
        ));
    }

    #[test]
    fn only_server_flags_are_given_through_server_flags() {
        for flag in FLAGS {
            let through = flag.code.iter().any(|c| c.call == CodeCall::ServerFlags);
            assert_eq!(through, flag.scope == FlagScope::Server, "{}", flag.name);
        }
    }

    #[test]
    fn an_option_expects_its_owners_choices() {
        assert_eq!(
            FlagShape::StartStrategy.expected(),
            "expected one of \"import\", \"fork\""
        );
        assert_eq!(
            FlagShape::GcMode.expected(),
            "expected one of \"count\", \"idle\", \"off\""
        );
    }

    #[test]
    fn a_settings_option_and_variable_are_spelled_from_its_name() {
        assert_eq!(cli_option("process.queue_per_worker"), "--process-queue-per-worker");
        assert_eq!(
            env_variable("process.queue_per_worker"),
            "MIPYRE_PROCESS_QUEUE_PER_WORKER"
        );
        assert_eq!(cli_option("tpc"), "--tpc");
        assert_eq!(env_variable("tpc"), "MIPYRE_TPC");
    }

    #[test]
    fn every_choice_prints_and_parses_back_as_itself() {
        fn round_trip<C: FlagChoice + PartialEq + std::fmt::Debug>() {
            for choice in C::ALL {
                assert_eq!(choice.to_string().parse::<C>().ok(), Some(*choice));
            }
            assert!("none-of-these".parse::<C>().is_err());
        }
        round_trip::<GcMode>();
        round_trip::<AcceptMode>();
        round_trip::<StartStrategy>();
        round_trip::<FailurePolicy>();
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn fanout_is_no_accept_mode_off_macos() {
        let refused = "fanout".parse::<AcceptMode>().map_err(|e| e.to_string());
        assert_eq!(refused, Err("the fanout accept mode exists on macOS only".to_string()));
    }

    #[test]
    fn a_bool_is_one_of_the_eight_words_in_any_case() {
        assert_eq!(read::<bool>("On", FlagShape::Bool), Some(true));
        assert_eq!(read::<bool>("0", FlagShape::Bool), Some(false));
        assert_eq!(read::<bool>("warn", FlagShape::Bool), None);
        assert_eq!(read::<bool>("", FlagShape::Bool), None);
    }

    #[test]
    fn a_count_takes_zero_and_a_positive_starts_at_one() {
        assert_eq!(read::<u64>("0", FlagShape::Count), Some(0));
        assert_eq!(read::<u64>("-1", FlagShape::Count), None);
        assert_eq!(read::<NonZeroUsize>("0", FlagShape::Positive), None);
        assert_eq!(read::<NonZeroUsize>("1", FlagShape::Positive), NonZeroUsize::new(1));
    }

    #[test]
    fn a_port_is_zero_to_65535() {
        assert_eq!(read::<u16>("0", FlagShape::Port), Some(0));
        assert_eq!(read::<u16>("65535", FlagShape::Port), Some(65535));
        assert_eq!(read::<u16>("65536", FlagShape::Port), None);
        assert_eq!(read::<u16>("-1", FlagShape::Port), None);
    }

    #[test]
    fn ports_are_each_a_port() {
        let ports = |raw| read::<Vec<NonZeroU16>>(raw, FlagShape::Ports);
        assert_eq!(
            ports("8443, 9443"),
            Some(vec![NonZeroU16::new(8443).unwrap(), NonZeroU16::new(9443).unwrap()])
        );
        assert_eq!(ports("8443,0"), None);
        assert_eq!(ports("8443,"), Some(vec![NonZeroU16::new(8443).unwrap()]));
        assert_eq!(ports(""), None);
    }

    #[test]
    fn hosts_drop_blank_names_and_need_one() {
        let hosts = |raw| read::<Vec<String>>(raw, FlagShape::Hosts);
        assert_eq!(
            hosts("a.example, ,b.example"),
            Some(vec!["a.example".into(), "b.example".into()])
        );
        assert_eq!(hosts(" , "), None);
    }

    #[test]
    fn a_path_or_host_is_not_empty() {
        assert_eq!(read::<PathBuf>("", FlagShape::Path), None);
        assert_eq!(read::<PathBuf>("/tmp", FlagShape::Path), Some(PathBuf::from("/tmp")));
        assert_eq!(read::<String>("", FlagShape::Host), None);
    }

    #[test]
    fn a_slot_starts_at_the_lane_minimum() {
        let min = mipyre_process::MIN_SLOT_BYTES;
        assert!(read::<LaneSlotBytes>(&(min - 1).to_string(), FlagShape::SlotBytes).is_none());
        assert!(read::<LaneSlotBytes>(&min.to_string(), FlagShape::SlotBytes).is_some());
    }

    #[test]
    fn a_duration_reads_in_its_unit_and_a_positive_one_is_above_zero() {
        let seconds = FlagShape::PositiveDuration {
            unit: TimeUnit::Seconds,
        };
        assert_eq!(read::<Duration>("0.001", seconds), Some(Duration::from_millis(1)));
        assert_eq!(read::<Duration>("0.0009", seconds), None);
        assert_eq!(read::<Duration>("0", seconds), None);
        assert_eq!(read::<Duration>("-1", seconds), None);
        let micros = FlagShape::Duration { unit: TimeUnit::Micros };
        assert_eq!(read::<Duration>("0", micros), Some(Duration::ZERO));
        assert_eq!(read::<Duration>("1.5", micros), None);
        let millis = FlagShape::PositiveDuration { unit: TimeUnit::Millis };
        assert_eq!(read::<Duration>("100", millis), Some(Duration::from_millis(100)));
    }

    fn read<T: FlagType>(raw: &str, shape: FlagShape) -> Option<T> {
        T::read(raw, shape)
    }
}
