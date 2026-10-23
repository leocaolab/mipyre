//! Reads the deployment once (command line, environment, `pyproject.toml`) and lays it over the
//! values app code gives one server, into the typed configuration the server is built from.
//!
//! Nothing deeper in the run path reads the environment. A value that cannot be used is a
//! startup error naming its source and the text as written, never a silent default; every such
//! error is found before any is reported (b/335 D19).

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

use pyo3::prelude::*;

use crate::runtime::process::{FailurePolicy, LaneRecipe, StartStrategy};
use crate::runtime::BridgeConfig;
use crate::serve::AcceptMode;
use mipyre_subinterp::gc::{GcMode, GcPolicy};

use super::settings::{
    CodeCall, CodeKey, Deployment, EntryPoint, Flag, FlagType, PyprojectKey, ServerFlagValues, ServerFlags, FLAGS,
    RESERVED_PREFIXES,
};

/// Counts the requests a worker serves between `gc.collect()`s in count mode; on the baseline
/// test 5000 gave p99 = 2.0 ms, 100_000 gave about 300 µs, and never collecting gave 240 µs.
const DEFAULT_GC_THRESHOLD: u64 = 100_000;
/// Counts the requests after which idle mode collects even without a lull.
const DEFAULT_GC_OOM_FAILSAFE: u64 = 50_000;
const DEFAULT_GC_IDLE: std::time::Duration = std::time::Duration::from_millis(100);
/// Runs four main-interpreter bridge threads because handlers mix CPU and I/O, and an
/// I/O-bound handler releases the GIL for its peers.
const DEFAULT_BRIDGE_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
/// Gives each bridge thread this many queue slots when the capacity is not set.
const BRIDGE_QUEUE_PER_WORKER: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// Holds the sizes given to a server, each with where it was given; `None` takes the topology's
/// default.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ServerSizes {
    /// Sizes the server's threads: the TPC threads, or the Tokio IO threads of the
    /// multi-thread (`tpc` off) server.
    pub(crate) threads: Option<FlagValue<NonZeroUsize>>,
    /// Sizes the multi-thread server's sub-interpreter pool; a TPC server has one
    /// sub-interpreter per thread.
    pub(crate) subinterpreters: Option<FlagValue<NonZeroUsize>>,
}

/// Says which server the `tpc` flag selects; only [`Topology::resolve`] reads it.
///
/// [`Topology::resolve`]: super::topology::Topology::resolve
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServerKind {
    /// Runs thread-per-core, which is the default.
    Tpc(AcceptMode),
    /// Runs the channel pool, or multi-thread Tokio in GIL mode, for `MIPYRE_TPC=0`.
    MultiThread,
}

/// Holds the engine's own settings a server runs with (from its flags).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EngineSettings {
    pub(crate) gc: GcPolicy,
    pub(crate) bridge: BridgeConfig,
    /// Counts the requests the async worker pool queues before it answers 503; `None` keeps the
    /// pool's default.
    pub(crate) async_queue_capacity: Option<NonZeroUsize>,
    /// Turns on the hot-path counters and the RSS sampler.
    pub(crate) metrics: bool,
}

// ---- The deployment: what the command line, the environment and pyproject.toml give (b/335 C3). ----

/// Where a deployment value came from, as the user wrote that place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeploymentPlace {
    /// An option of mipyre's command line: `command` is `mipyre run` or `mipyre dev`.
    Cli {
        command: &'static str,
        option: &'static str,
    },
    /// An environment variable.
    Env(&'static str),
    /// A key of `pyproject.toml`, on `line` (from 1).
    File {
        path: PathBuf,
        line: usize,
        key: PyprojectKey,
    },
}

impl std::fmt::Display for DeploymentPlace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentPlace::Cli { command, option } => write!(f, "{command} {option}"),
            DeploymentPlace::Env(name) => f.write_str(name),
            DeploymentPlace::File { path, line, key } => write!(f, "{}:{line}: {key}", path.display()),
        }
    }
}

/// A value the deployment gave, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeploymentValue<T> {
    pub(crate) value: T,
    pub(crate) from: DeploymentPlace,
}

/// A value app code gave, and where (one of the flag's [`Flag::code`] keys).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodeValue<T> {
    pub(crate) value: T,
    pub(crate) at: &'static CodeKey,
}

/// A flag's value once the deployment is laid over the code (D7): the deployment's, with the
/// code's different value it overrode, or the code's. Nobody giving it is `None` around this,
/// and its owner works the default out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FlagValue<T> {
    Deployed {
        given: DeploymentValue<T>,
        replaced: Option<CodeValue<T>>,
    },
    Code(CodeValue<T>),
}

impl<T: PartialEq> FlagValue<T> {
    /// The deployment's value if it gave one (keeping the code's if it was different), else the
    /// code's; `None` when neither gave one.
    pub(crate) fn over(deployed: Option<DeploymentValue<T>>, code: Option<CodeValue<T>>) -> Option<Self> {
        match (deployed, code) {
            (Some(given), code) => {
                let replaced = code.filter(|code| code.value != given.value);
                Some(FlagValue::Deployed { given, replaced })
            }
            (None, Some(code)) => Some(FlagValue::Code(code)),
            (None, None) => None,
        }
    }
}

impl<T> FlagValue<T> {
    pub(crate) fn value(&self) -> &T {
        match self {
            FlagValue::Deployed { given, .. } => &given.value,
            FlagValue::Code(code) => &code.value,
        }
    }

    /// Where the value came from, as errors and summaries print it.
    pub(crate) fn source(&self) -> FlagSource {
        match self {
            FlagValue::Deployed { given, .. } => FlagSource::Deployed(given.from.clone()),
            FlagValue::Code(code) => FlagSource::Code(code.at),
        }
    }
}

/// Where a value came from, as summaries and errors print it; worked out from a [`FlagValue`],
/// never stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FlagSource {
    Deployed(DeploymentPlace),
    Code(&'static CodeKey),
    /// Nobody gave it: the flag's default.
    Default,
}

impl std::fmt::Display for FlagSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlagSource::Deployed(place) => place.fmt(f),
            FlagSource::Code(key) => key.fmt(f),
            FlagSource::Default => f.write_str("the default"),
        }
    }
}

/// Keeps a flag's deployment value, or the reason it cannot be used.
pub(crate) fn take<T>(read: Result<Option<T>, FlagError>, errors: &mut Vec<FlagError>) -> Option<T> {
    read.unwrap_or_else(|error| {
        errors.push(error);
        None
    })
}

/// Adds the summary line of `flag` when the deployment gave it.
pub(crate) fn given_line<T: FlagType>(lines: &mut Vec<String>, flag: &Flag, given: &Option<DeploymentValue<T>>) {
    if let Some(given) = given {
        lines.push(format!("{}={} ({})", flag.name, given.value.written(), given.from));
    }
}

/// Adds the summary line of server flag `flag` when somebody gave it: its value and source, and
/// the code value a deployment value replaced.
pub(crate) fn settled_line<T: FlagType>(lines: &mut Vec<String>, flag: &Flag, settled: &Option<FlagValue<T>>) {
    if let Some(value) = settled {
        let mut line = format!("{}={} ({})", flag.name, value.value().written(), value.source());
        if let FlagValue::Deployed {
            replaced: Some(code), ..
        } = value
        {
            line.push_str(&format!(" overrides {} ({})", code.value.written(), code.at));
        }
        lines.push(line);
    }
}

/// Reads the value app code gave a flag at `at`, as Python wrote it (`raw`).
pub(crate) fn code_value<T: FlagType>(
    flag: &'static Flag,
    at: &'static CodeKey,
    raw: &str,
) -> Result<CodeValue<T>, ServerFlagsError> {
    match T::read(raw, flag.shape) {
        Some(value) => Ok(CodeValue { value, at }),
        None => Err(ServerFlagsError::InvalidValue {
            at,
            raw: raw.to_string(),
            expected: flag.shape.expected(),
        }),
    }
}

/// Why app code's flag value cannot be used: a keyword that is no flag of its call, or a value
/// of the wrong shape.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ServerFlagsError {
    #[error("{keyword}= is not a flag {call} takes")]
    UnknownKeyword { call: CodeCall, keyword: String },
    #[error("{at}: {raw:?} is invalid: {expected}")]
    InvalidValue {
        at: &'static CodeKey,
        raw: String,
        expected: String,
    },
}

/// One reason the configuration cannot be used, with the source and the text as written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FlagError {
    #[error("{from}: {raw:?} is invalid for {flag}: {expected}")]
    InvalidValue {
        flag: &'static str,
        from: DeploymentPlace,
        raw: String,
        expected: String,
    },
    /// Carries the value with each invalid UTF-8 sequence replaced.
    #[error("{env}={lossy:?} is invalid for {flag}: it is not UTF-8")]
    NotUnicode {
        flag: &'static str,
        env: &'static str,
        lossy: String,
    },
    #[error("{}:{line}:{column}: pyproject.toml is not valid TOML: {message}", .path.display())]
    MalformedPyproject {
        path: PathBuf,
        line: usize,
        column: usize,
        message: String,
    },
    #[error("{}:{line}: [{table}] must be a table", .path.display())]
    NotATable {
        path: PathBuf,
        line: usize,
        table: &'static str,
    },
    #[error("{}: cannot be read: {error}", .path.display())]
    UnreadablePyproject { path: PathBuf, error: std::io::ErrorKind },
    /// A `MIPYRE_` variable no flag reads (D19): a misspelled flag or a removed one.
    #[error(
        "{name} is set, but it is not one of mipyre's settings (`mipyre run --help` lists them); \
         unset it, or rename it if it is your own ({reserved} are left to test and benchmark \
         tools)",
        reserved = RESERVED_PREFIXES.map(|prefix| format!("{prefix}*")).join(" and ")
    )]
    UnknownVariable { name: String },
    /// A key of `[tool.mipyre]` or `[tool.mipyre.process]` no flag reads (D19).
    #[error("{}:{line}: {key} in [{table}] is not one of mipyre's settings (`mipyre run --help` lists them)", .path.display())]
    UnknownKey {
        path: PathBuf,
        line: usize,
        table: &'static str,
        key: String,
    },
    /// A value given for a server (or an entry point) that does not use it (D19).
    #[error("{flag} is given ({given}), but {user} does not use it: it is used only by {needs}")]
    UnusedByTopology {
        flag: &'static str,
        given: FlagSource,
        user: String,
        needs: &'static str,
    },
    #[error("tls_cert ({cert}) is given without tls_key: give both, or neither")]
    TlsCertWithoutKey { cert: FlagSource },
    #[error("tls_key ({key}) is given without tls_cert: give both, or neither")]
    TlsKeyWithoutCert { key: FlagSource },
    #[error("port {port} is given twice: as {first} and as {second}")]
    DuplicatePort {
        port: u16,
        first: FlagSource,
        second: FlagSource,
    },
}

/// The configuration cannot be used: every error found, never none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigError {
    first: Box<FlagError>,
    rest: Vec<FlagError>,
}

impl ConfigError {
    /// Returns `errors` as one error, or `None` when there are none.
    pub(crate) fn of(errors: Vec<FlagError>) -> Option<Self> {
        let mut errors = errors.into_iter();
        let first = errors.next()?;
        Some(ConfigError {
            first: Box::new(first),
            rest: errors.collect(),
        })
    }

    pub(crate) fn errors(&self) -> impl Iterator<Item = &FlagError> {
        std::iter::once(&*self.first).chain(&self.rest)
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lines: Vec<String> = self.errors().map(ToString::to_string).collect();
        f.write_str(&lines.join("\n"))
    }
}

impl std::error::Error for ConfigError {}

/// The Python exception a [`ConfigError`] raises, `mipyre.engine.ConfigError`.
pub(crate) mod exception {
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;

    /// The configuration cannot be used. `errors` holds every reason, one line each, with the
    /// source it came from. The exception holds its reasons itself, so raising one needs no
    /// interpreter until Python sees it.
    #[pyclass(module = "mipyre.engine", extends = PyValueError, frozen, name = "ConfigError")]
    pub(crate) struct ConfigError {
        #[pyo3(get)]
        errors: Vec<String>,
    }

    #[pymethods]
    impl ConfigError {
        #[new]
        fn new(errors: Vec<String>) -> Self {
            ConfigError { errors }
        }

        fn __str__(&self) -> String {
            self.errors.join("\n")
        }
    }
}

/// Raises as `mipyre.engine.ConfigError`, one line per error.
impl From<ConfigError> for PyErr {
    fn from(e: ConfigError) -> Self {
        let errors: Vec<String> = e.errors().map(ToString::to_string).collect();
        PyErr::new::<exception::ConfigError, _>((errors,))
    }
}

impl From<ServerFlagsError> for PyErr {
    fn from(e: ServerFlagsError) -> Self {
        match e {
            ServerFlagsError::UnknownKeyword { .. } => pyo3::exceptions::PyTypeError::new_err(e.to_string()),
            ServerFlagsError::InvalidValue { .. } => PyErr::new::<exception::ConfigError, _>((vec![e.to_string()],)),
        }
    }
}

/// A key of `[tool.mipyre]` or `[tool.mipyre.process]` with its value as text and the line it is on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileEntry {
    raw: String,
    line: usize,
}

/// The three deployment sources, read: the command line's flags by name, one snapshot of the
/// environment, and `pyproject.toml`'s two tables.
pub(crate) struct DeploymentSources<'a> {
    entry: EntryPoint,
    flags: &'a [(&'static str, String)],
    env: &'a [(OsString, OsString)],
    file: Option<PyprojectTables<'a>>,
}

/// `pyproject.toml`'s `[tool.mipyre]` and `[tool.mipyre.process]`: every key with its value as text
/// and its line.
struct PyprojectTables<'a> {
    path: &'a Path,
    entries: BTreeMap<(Table, String), FileEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Table {
    Top,
    Process,
}

impl Table {
    /// The table as `pyproject.toml` spells it.
    fn written(self) -> &'static str {
        match self {
            Table::Top => "tool.mipyre",
            Table::Process => "tool.mipyre.process",
        }
    }

    fn of(key: PyprojectKey) -> (Table, &'static str) {
        match key {
            PyprojectKey::Top(key) => (Table::Top, key),
            PyprojectKey::Process(key) => (Table::Process, key),
        }
    }
}

/// Reads one flag's deployment value: the first of the command line, the environment and the
/// file that gives it (D7), read into `T`, or `None` when none does.
pub(crate) fn read_flag<T: FlagType>(
    flag: &'static Flag,
    sources: &DeploymentSources<'_>,
) -> Result<Option<DeploymentValue<T>>, FlagError> {
    debug_assert!(
        T::fits(flag.shape),
        "{}: flags! gave it a shape its type is not written in",
        flag.name
    );
    let (raw, from) = if let Some((_, raw)) = sources.flags.iter().find(|(name, _)| *name == flag.name) {
        let command = sources.entry.command().unwrap_or("mipyre run");
        (
            raw.clone(),
            DeploymentPlace::Cli {
                command,
                option: flag.cli,
            },
        )
    } else if let Some((_, raw)) = sources.env.iter().find(|(name, _)| name == flag.env) {
        let raw = raw.clone().into_string().map_err(|raw| FlagError::NotUnicode {
            flag: flag.name,
            env: flag.env,
            lossy: raw.to_string_lossy().into_owned(),
        })?;
        (raw, DeploymentPlace::Env(flag.env))
    } else if let Some((path, entry)) = sources.file.as_ref().and_then(|file| {
        let (table, key) = Table::of(flag.file);
        file.entries
            .get(&(table, key.to_string()))
            .map(|entry| (file.path, entry))
    }) {
        let from = DeploymentPlace::File {
            path: path.to_path_buf(),
            line: entry.line,
            key: flag.file,
        };
        (entry.raw.clone(), from)
    } else {
        return Ok(None);
    };
    match T::read(&raw, flag.shape) {
        Some(value) => Ok(Some(DeploymentValue { value, from })),
        None => Err(FlagError::InvalidValue {
            flag: flag.name,
            from,
            raw,
            expected: flag.shape.expected(),
        }),
    }
}

/// The line and column (from 1) of byte `offset` in `text`.
fn line_and_column(text: &str, offset: usize) -> (usize, usize) {
    let before = &text[..offset.min(text.len())];
    let line = before.matches('\n').count() + 1;
    let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
    (line, column)
}

/// Writes a TOML value as a flag's text: a string as itself, an array as its items joined by
/// commas, anything else as it is written in the file.
fn as_flag_text(value: &toml::de::DeValue<'_>, written: &str) -> String {
    match value {
        toml::de::DeValue::String(s) => s.to_string(),
        toml::de::DeValue::Array(items) => items
            .iter()
            .map(|item| as_flag_text(item.get_ref(), ""))
            .collect::<Vec<_>>()
            .join(","),
        toml::de::DeValue::Integer(i) => i.as_str().to_string(),
        toml::de::DeValue::Float(x) => x.as_str().to_string(),
        toml::de::DeValue::Boolean(b) => b.to_string(),
        toml::de::DeValue::Datetime(_) | toml::de::DeValue::Table(_) => written.to_string(),
    }
}

/// Reads `[tool.mipyre]` and `[tool.mipyre.process]` out of `text`, each key with its value as
/// text and its line.
fn pyproject_tables(path: &Path, text: &str) -> Result<BTreeMap<(Table, String), FileEntry>, FlagError> {
    let document = toml::de::DeTable::parse(text).map_err(|e| {
        let (line, column) = line_and_column(text, e.span().map_or(0, |span| span.start));
        FlagError::MalformedPyproject {
            path: path.to_path_buf(),
            line,
            column,
            message: e.message().to_string(),
        }
    })?;
    let mut entries = BTreeMap::new();
    let Some(tool) = document.get_ref().get("tool") else {
        return Ok(entries);
    };
    let toml::de::DeValue::Table(tool) = tool.get_ref() else {
        return Ok(entries);
    };
    let Some(ours) = tool.get("mipyre") else {
        return Ok(entries);
    };
    let not_a_table = |table, span: std::ops::Range<usize>| FlagError::NotATable {
        path: path.to_path_buf(),
        line: line_and_column(text, span.start).0,
        table,
    };
    let toml::de::DeValue::Table(tool_table) = ours.get_ref() else {
        return Err(not_a_table("tool.mipyre", ours.span()));
    };
    for (key, value) in tool_table {
        if key.get_ref() == "process" {
            let toml::de::DeValue::Table(process) = value.get_ref() else {
                return Err(not_a_table("tool.mipyre.process", value.span()));
            };
            for (key, value) in process {
                entries.insert((Table::Process, key.get_ref().to_string()), file_entry(text, value));
            }
        } else {
            entries.insert((Table::Top, key.get_ref().to_string()), file_entry(text, value));
        }
    }
    Ok(entries)
}

fn file_entry(text: &str, value: &toml::Spanned<toml::de::DeValue<'_>>) -> FileEntry {
    let span = value.span();
    let written = text.get(span.clone()).unwrap_or_default();
    FileEntry {
        raw: as_flag_text(value.get_ref(), written),
        line: line_and_column(text, span.start).0,
    }
}

/// Every `MIPYRE_` variable of `env` no flag reads, outside the reserved prefixes; a variable
/// whose name is not UTF-8 is named with its invalid sequences replaced.
fn unknown_variables(env: &[(OsString, OsString)]) -> Vec<FlagError> {
    env.iter()
        .map(|(name, _)| name.to_string_lossy())
        .filter(|name| name.starts_with("MIPYRE_"))
        .filter(|name| !RESERVED_PREFIXES.iter().any(|prefix| name.starts_with(prefix)))
        .filter(|name| !FLAGS.iter().any(|flag| flag.env == name.as_ref()))
        .map(|name| FlagError::UnknownVariable {
            name: name.into_owned(),
        })
        .collect()
}

/// Every key of the file's two tables no flag reads.
fn unknown_keys(file: &PyprojectTables<'_>) -> Vec<FlagError> {
    file.entries
        .iter()
        .filter(|((table, key), _)| !FLAGS.iter().any(|flag| Table::of(flag.file) == (*table, key.as_str())))
        .map(|((table, key), entry)| FlagError::UnknownKey {
            path: file.path.to_path_buf(),
            line: entry.line,
            table: table.written(),
            key: key.clone(),
        })
        .collect()
}

impl Deployment {
    /// Resolves the deployment from its three sources, given in full: `env` one snapshot of the
    /// environment, `flags` the command line's values by flag name as typed, `file` the path and
    /// text of `pyproject.toml`. It reads nothing else, and returns every error at once.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] with every value that cannot be used and the file if it is not TOML.
    pub(crate) fn resolve(
        env: &[(OsString, OsString)],
        flags: &[(&'static str, String)],
        file: Option<(&Path, &str)>,
        entry: EntryPoint,
    ) -> Result<Self, ConfigError> {
        let mut errors = Vec::new();
        let file = file.and_then(|(path, text)| match pyproject_tables(path, text) {
            Ok(entries) => Some(PyprojectTables { path, entries }),
            Err(error) => {
                errors.push(error);
                None
            }
        });
        errors.extend(unknown_variables(env));
        if let Some(file) = &file {
            errors.extend(unknown_keys(file));
        }
        let sources = DeploymentSources {
            entry,
            flags,
            env,
            file,
        };
        let deployment = Deployment::read_flags(entry, &sources, &mut errors);
        if let Some(reload) = deployment.reload.as_ref().filter(|_| !entry.reloads()) {
            errors.push(FlagError::UnusedByTopology {
                flag: "reload",
                given: FlagSource::Deployed(reload.from.clone()),
                user: entry.to_string(),
                needs: super::settings::FlagUse::ReloadingEntry.users(),
            });
        }
        match ConfigError::of(errors) {
            Some(error) => Err(error),
            None => Ok(deployment),
        }
    }

    /// Resolves the deployment of this process: its environment and the `pyproject.toml` in its
    /// working directory, read once here, with the command line's `flags`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] as [`Deployment::resolve`] does, and when the file exists but
    /// cannot be read.
    pub(crate) fn from_process(flags: &[(&'static str, String)], entry: EntryPoint) -> Result<Self, ConfigError> {
        let env: Vec<(OsString, OsString)> = std::env::vars_os().collect();
        let unreadable = |path: PathBuf, error: std::io::Error| ConfigError {
            first: Box::new(FlagError::UnreadablePyproject {
                path,
                error: error.kind(),
            }),
            rest: Vec::new(),
        };
        let path = std::env::current_dir()
            .map_err(|e| unreadable(PathBuf::from("pyproject.toml"), e))?
            .join("pyproject.toml");
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => Some(text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(unreadable(path, e)),
        };
        Self::resolve(&env, flags, text.as_deref().map(|text| (path.as_path(), text)), entry)
    }
}

/// One server's flags laid over its code and checked together: everything its run is built from
/// that a flag decides (b/335 C4).
#[derive(Debug, Clone)]
pub(crate) struct ServerRecipe {
    pub(crate) values: ServerFlagValues,
    pub(crate) server: ServerKind,
    pub(crate) tuning: EngineSettings,
    pub(crate) sizes: ServerSizes,
}

/// Returns the value of `settled`, else `default`.
fn or_default<T: Copy>(settled: &Option<FlagValue<T>>, default: T) -> T {
    settled.as_ref().map_or(default, |value| *value.value())
}

/// Every rule that needs two flags: TLS's certificate and key go together, and no port is
/// given twice (the main port, the extra TLS ones, the extra plain ones).
fn combination_errors(values: &ServerFlagValues) -> Vec<FlagError> {
    let mut errors = Vec::new();
    match (&values.tls_cert, &values.tls_key) {
        (Some(cert), None) => errors.push(FlagError::TlsCertWithoutKey { cert: cert.source() }),
        (None, Some(key)) => errors.push(FlagError::TlsKeyWithoutCert { key: key.source() }),
        _ => {}
    }
    let main = match &values.port {
        Some(port) => (*port.value(), port.source()),
        None => (DEFAULT_PORT, FlagSource::Default),
    };
    // Port 0 is the kernel's pick, a different port each time it binds.
    let mut ports: Vec<(u16, FlagSource)> = Vec::new();
    if main.0 != 0 {
        ports.push(main);
    }
    for extra in [&values.extra_tls_ports, &values.extra_plain_ports]
        .into_iter()
        .flatten()
    {
        for port in extra.value() {
            ports.push((port.get(), extra.source()));
        }
    }
    for (i, (port, second)) in ports.iter().enumerate() {
        if let Some((_, first)) = ports[..i].iter().find(|(earlier, _)| earlier == port) {
            errors.push(FlagError::DuplicatePort {
                port: *port,
                first: first.clone(),
                second: second.clone(),
            });
        }
    }
    errors
}

/// The port a server binds when no flag gives one.
pub(crate) const DEFAULT_PORT: u16 = 8000;
/// The address a server binds when no flag gives one.
pub(crate) const DEFAULT_HOST: &str = "127.0.0.1";

/// The process lanes' flags when none gives them: a 30 s handler timeout, 128 requests per
/// worker before 503, 256 KiB per request and answer, a 50 µs spin; workers default to the
/// physical cores.
const DEFAULT_LANE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const DEFAULT_LANE_QUEUE: NonZeroUsize = NonZeroUsize::new(128).unwrap();
const DEFAULT_LANE_SLOT_BYTES: usize = 256 * 1024;
const DEFAULT_LANE_SPIN: std::time::Duration = std::time::Duration::from_micros(50);

impl Deployment {
    /// The app's process lanes: this deployment laid over the code's `ProcessLanes` (in
    /// `code`), each flag the default where nobody gave it.
    pub(crate) fn lanes(&self, code: &ServerFlags, physical_cores: NonZeroUsize) -> LaneRecipe {
        let values = self.over(code);
        let slot_bytes = values
            .process_slot_bytes
            .as_ref()
            .map_or(DEFAULT_LANE_SLOT_BYTES, |v| v.value().get());
        LaneRecipe {
            start: or_default(&values.process_start, StartStrategy::Import),
            workers: or_default(&values.process_workers, physical_cores).get(),
            timeout_ms: u64::try_from(or_default(&values.process_timeout, DEFAULT_LANE_TIMEOUT).as_millis())
                .unwrap_or(u64::MAX),
            queue_per_worker: or_default(&values.process_queue, DEFAULT_LANE_QUEUE).get(),
            slot_bytes,
            worker_spin_us: u64::try_from(or_default(&values.process_spin_us, DEFAULT_LANE_SPIN).as_micros())
                .unwrap_or(u64::MAX),
            host_failure: or_default(&values.process_host_failure, FailurePolicy::Isolate),
        }
    }
}

/// The user's cache directory by the OS's convention: `~/Library/Caches` on macOS; elsewhere
/// `$XDG_CACHE_HOME` when it is an absolute path (the XDG spec ignores a relative one), else
/// `~/.cache`. `None` when this user has no home directory. Not a deployment setting: it is the
/// input of `isolate_dir`'s default, read once by the entry, as the CPUs are.
pub(crate) fn user_cache_dir() -> Option<PathBuf> {
    cache_dir_of(
        cfg!(target_os = "macos"),
        std::env::home_dir(),
        std::env::var_os("XDG_CACHE_HOME"),
    )
}

fn cache_dir_of(macos: bool, home: Option<PathBuf>, xdg_cache_home: Option<OsString>) -> Option<PathBuf> {
    if macos {
        return home.map(|home| home.join("Library").join("Caches"));
    }
    match xdg_cache_home.map(PathBuf::from) {
        Some(xdg) if xdg.is_absolute() => Some(xdg),
        _ => home.map(|home| home.join(".cache")),
    }
}

impl ServerRecipe {
    /// The directory each sub-interpreter worker keeps its copies of isolated extensions in:
    /// the `isolate_dir` flag, else `mipyre` in `cache_dir` (the user's cache directory, see
    /// [`user_cache_dir`]); `None` when neither is there.
    pub(crate) fn isolate_dir(&self, cache_dir: Option<&Path>) -> Option<PathBuf> {
        match &self.values.isolate_dir {
            Some(given) => Some(given.value().clone()),
            None => cache_dir.map(|dir| dir.join("mipyre")),
        }
    }

    /// Checks the rules that need two flags and turns the settled `values` into what the
    /// server is built from.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] with every rule the values break.
    pub(crate) fn of(values: ServerFlagValues) -> Result<Self, ConfigError> {
        if let Some(errors) = ConfigError::of(combination_errors(&values)) {
            return Err(errors);
        }
        let server = if or_default(&values.tpc, true) {
            ServerKind::Tpc(or_default(&values.tpc_accept, AcceptMode::PerThreadListener))
        } else {
            ServerKind::MultiThread
        };
        let threshold = NonZeroU64::new(or_default(&values.gc_threshold, DEFAULT_GC_THRESHOLD));
        let failsafe = NonZeroU64::new(or_default(&values.gc_oom_failsafe, DEFAULT_GC_OOM_FAILSAFE));
        let gc = match or_default(&values.gc_mode, GcMode::Count) {
            // A threshold of 0 never collects, so it is spelled as `off`.
            GcMode::Count => match threshold {
                Some(every) => GcPolicy::Count { every },
                None => GcPolicy::Off,
            },
            GcMode::Idle => GcPolicy::Idle {
                failsafe,
                tick: or_default(&values.gc_idle_ms, DEFAULT_GC_IDLE),
            },
            GcMode::Off => GcPolicy::Off,
        };
        let workers = or_default(&values.bridge_workers, DEFAULT_BRIDGE_WORKERS);
        let capacity = or_default(&values.bridge_capacity, workers.saturating_mul(BRIDGE_QUEUE_PER_WORKER));
        let tuning = EngineSettings {
            gc,
            bridge: BridgeConfig { workers, capacity },
            async_queue_capacity: values.async_queue_capacity.as_ref().map(|value| *value.value()),
            metrics: or_default(&values.metrics, false),
        };
        let sizes = ServerSizes {
            threads: values.threads.clone(),
            subinterpreters: values.subinterpreters.clone(),
        };
        Ok(ServerRecipe {
            values,
            server,
            tuning,
            sizes,
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    fn variables(names: &[&str]) -> Vec<(OsString, OsString)> {
        names
            .iter()
            .map(|name| (OsString::from(name), OsString::from("1")))
            .collect()
    }

    #[test]
    fn a_prefixed_variable_no_flag_reads_is_refused() {
        // Declared, a test tool's, a benchmark tool's, the reloader's protocol, not ours.
        let fine = [
            "MIPYRE_THREADS",
            "MIPYRE_TEST_PG_DSN",
            "MIPYRE_BENCH_PY",
            "_MIPYRE_RELOAD_CHILD",
            "PATH",
        ];
        assert!(Deployment::resolve(&variables(&fine), &[], None, EntryPoint::Script).is_ok());
        let e = Deployment::resolve(
            &variables(&["MIPYRE_RUNTIME", "MIPYRE_THREDS"]),
            &[],
            None,
            EntryPoint::Script,
        )
        .unwrap_err();
        let unknown: Vec<_> = e
            .errors()
            .map(|e| match e {
                FlagError::UnknownVariable { name } => name.as_str(),
                other => panic!("{other}"),
            })
            .collect();
        assert_eq!(unknown, ["MIPYRE_RUNTIME", "MIPYRE_THREDS"]);
        assert!(
            e.to_string()
                .contains("MIPYRE_RUNTIME is set, but it is not one of mipyre's settings"),
            "{e}"
        );
    }

    #[test]
    fn reload_is_used_only_by_an_entry_point_that_reloads() {
        let reload = variables(&["MIPYRE_RELOAD"]);
        for entry in [EntryPoint::Script, EntryPoint::CliRun] {
            assert!(Deployment::resolve(&reload, &[], None, entry).is_ok(), "{entry}");
        }
        for entry in [EntryPoint::CliDev, EntryPoint::Embedded] {
            let e = Deployment::resolve(&reload, &[], None, entry).unwrap_err();
            assert!(e.to_string().starts_with("reload is given (MIPYRE_RELOAD"), "{e}");
            assert!(e.to_string().contains(&format!("{entry} does not use it")), "{e}");
        }
        assert!(
            Deployment::resolve(&[], &[], None, EntryPoint::CliDev).is_ok(),
            "not given, nothing to say"
        );
    }

    #[test]
    fn a_pyproject_key_no_flag_reads_is_refused_with_its_line() {
        let path = Path::new("/app/pyproject.toml");
        let known = "[tool.mipyre]\nthreads = 2\n\n[tool.mipyre.process]\nworkers = 2\n";
        assert!(Deployment::resolve(&[], &[], Some((path, known)), EntryPoint::Script).is_ok());
        let unknown = "[tool.mipyre]\nthreads = 2\nruntime = \"main\"\n\n[tool.mipyre.process]\nworker = 2\n";
        let e = Deployment::resolve(&[], &[], Some((path, unknown)), EntryPoint::Script).unwrap_err();
        let lines: Vec<String> = e.errors().map(ToString::to_string).collect();
        assert_eq!(
            lines,
            [
                "/app/pyproject.toml:3: runtime in [tool.mipyre] is not one of mipyre's settings (`mipyre run --help` lists them)",
                "/app/pyproject.toml:6: worker in [tool.mipyre.process] is not one of mipyre's settings (`mipyre run --help` lists them)",
            ]
        );
    }

    #[test]
    fn the_cache_directory_follows_the_os_convention() {
        let home = Some(PathBuf::from("/home/u"));
        let at = |macos, xdg: Option<&str>| cache_dir_of(macos, home.clone(), xdg.map(OsString::from));
        assert_eq!(at(true, Some("/x")), Some(PathBuf::from("/home/u/Library/Caches")));
        assert_eq!(at(false, Some("/var/cache/u")), Some(PathBuf::from("/var/cache/u")));
        assert_eq!(at(false, Some("relative/cache")), Some(PathBuf::from("/home/u/.cache")));
        assert_eq!(at(false, None), Some(PathBuf::from("/home/u/.cache")));
        assert_eq!(cache_dir_of(false, None, None), None);
        assert_eq!(
            cache_dir_of(false, None, Some("/var/cache/u".into())),
            Some(PathBuf::from("/var/cache/u"))
        );
    }

    #[test]
    fn the_isolate_dir_is_the_flag_else_a_directory_in_the_cache_directory() {
        let given = crate::boot::ServerRecipe::of(
            Deployment::resolve(
                &[("MIPYRE_ISOLATE_DIR".into(), "/srv/iso".into())],
                &[],
                None,
                EntryPoint::Embedded,
            )
            .unwrap()
            .over(&ServerFlags::default()),
        )
        .unwrap();
        assert_eq!(
            given.isolate_dir(Some(Path::new("/c"))),
            Some(PathBuf::from("/srv/iso"))
        );
        let default = crate::boot::ServerRecipe::of(Deployment::default().over(&ServerFlags::default())).unwrap();
        assert_eq!(
            default.isolate_dir(Some(Path::new("/c"))),
            Some(PathBuf::from("/c/mipyre"))
        );
        assert_eq!(default.isolate_dir(None), None);
    }

    /// The recipe of a server whose whole environment is `vars`, and whose code gives nothing.
    pub(in crate::boot) fn recipe(vars: &[(&str, &str)]) -> Result<ServerRecipe, ConfigError> {
        let env: Vec<(OsString, OsString)> = vars
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v)))
            .collect();
        crate::boot::ServerRecipe::of(
            Deployment::resolve(&env, &[], None, EntryPoint::Embedded)?.over(&ServerFlags::default()),
        )
    }

    /// The one error a recipe from `vars` stops at: its source and the text written there.
    fn invalid(vars: &[(&str, &str)]) -> (String, String) {
        let e = recipe(vars).expect_err("expected a ConfigError");
        let errors: Vec<&FlagError> = e.errors().collect();
        match errors.as_slice() {
            [FlagError::InvalidValue { from, raw, .. }] => (from.to_string(), raw.clone()),
            other => panic!("expected one InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn nothing_set_is_the_documented_defaults() {
        let config = recipe(&[]).unwrap();
        assert_eq!(config.server, ServerKind::Tpc(AcceptMode::PerThreadListener));
        assert_eq!(
            config.tuning.gc,
            GcPolicy::Count {
                every: NonZeroU64::new(100_000).expect("Proof: 100_000 is not 0")
            }
        );
        assert_eq!(
            recipe(&[("MIPYRE_GC_MODE", "idle")]).unwrap().tuning.gc,
            GcPolicy::Idle {
                failsafe: NonZeroU64::new(50_000),
                tick: std::time::Duration::from_millis(100)
            }
        );
        assert_eq!(config.tuning.bridge.workers.get(), 4);
        assert_eq!(config.tuning.bridge.capacity.get(), 64);
        assert!(!config.tuning.metrics);
    }

    #[test]
    fn tpc_is_a_boolean() {
        let server = |raw| recipe(&[("MIPYRE_TPC", raw)]).unwrap().server;
        assert_eq!(server("0"), ServerKind::MultiThread);
        assert_eq!(server("off"), ServerKind::MultiThread);
        assert_eq!(server("1"), ServerKind::Tpc(AcceptMode::PerThreadListener));
        assert_eq!(
            invalid(&[("MIPYRE_TPC", "maybe")]),
            ("MIPYRE_TPC".into(), "maybe".into())
        );
    }

    #[test]
    fn gc_numbers_must_parse() {
        assert_eq!(
            recipe(&[("MIPYRE_GC_THRESHOLD", "0")]).unwrap().tuning.gc,
            GcPolicy::Off
        );
        let config = recipe(&[
            ("MIPYRE_GC_MODE", "idle"),
            ("MIPYRE_GC_OOM_FAILSAFE", "7"),
            ("MIPYRE_GC_IDLE_MS", "5"),
        ])
        .unwrap();
        assert_eq!(
            config.tuning.gc,
            GcPolicy::Idle {
                failsafe: NonZeroU64::new(7),
                tick: std::time::Duration::from_millis(5)
            }
        );
        for (var, raw) in [
            ("MIPYRE_GC_THRESHOLD", "5k"),
            ("MIPYRE_GC_THRESHOLD", "-1"),
            ("MIPYRE_GC_OOM_FAILSAFE", "lots"),
            ("MIPYRE_GC_IDLE_MS", "0"),
            ("MIPYRE_GC_IDLE_MS", "1.5"),
            ("MIPYRE_GC_MODE", "idel"),
        ] {
            assert_eq!(invalid(&[(var, raw)]), (var.into(), raw.into()));
        }
    }

    #[test]
    fn each_gc_mode_takes_its_own_numbers() {
        let policy = |mode| {
            recipe(&[
                ("MIPYRE_GC_THRESHOLD", "10"),
                ("MIPYRE_GC_OOM_FAILSAFE", "20"),
                ("MIPYRE_GC_MODE", mode),
            ])
            .unwrap()
            .tuning
            .gc
        };
        assert_eq!(
            policy("count"),
            GcPolicy::Count {
                every: NonZeroU64::new(10).expect("Proof: 10 is not 0")
            }
        );
        assert_eq!(
            policy("idle"),
            GcPolicy::Idle {
                failsafe: NonZeroU64::new(20),
                tick: std::time::Duration::from_millis(100)
            }
        );
        assert_eq!(policy("off"), GcPolicy::Off);
    }

    #[test]
    fn bridge_sizes_are_positive() {
        assert_eq!(
            recipe(&[("MIPYRE_GIL_BRIDGE_WORKERS", "2")])
                .unwrap()
                .tuning
                .bridge
                .capacity
                .get(),
            32
        );
        assert_eq!(
            recipe(&[("MIPYRE_GIL_BRIDGE_CAPACITY", "5")])
                .unwrap()
                .tuning
                .bridge
                .capacity
                .get(),
            5
        );
        for (var, raw) in [
            ("MIPYRE_GIL_BRIDGE_CAPACITY", "0"),
            ("MIPYRE_GIL_BRIDGE_CAPACITY", "big"),
            ("MIPYRE_GIL_BRIDGE_WORKERS", "0"),
            ("MIPYRE_GIL_BRIDGE_WORKERS", "four"),
        ] {
            assert_eq!(invalid(&[(var, raw)]), (var.into(), raw.into()));
        }
    }

    #[test]
    fn async_queue_capacity_is_positive_and_unset_by_default() {
        assert_eq!(recipe(&[]).unwrap().tuning.async_queue_capacity, None);
        let config = recipe(&[("MIPYRE_ASYNC_QUEUE_CAPACITY", "40000")]).unwrap();
        assert_eq!(config.tuning.async_queue_capacity.map(NonZeroUsize::get), Some(40000));
        for raw in ["0", "-1", "lots"] {
            assert_eq!(
                invalid(&[("MIPYRE_ASYNC_QUEUE_CAPACITY", raw)]),
                ("MIPYRE_ASYNC_QUEUE_CAPACITY".into(), raw.into())
            );
        }
    }

    #[test]
    fn metrics_and_topology_take_their_values_only() {
        assert!(recipe(&[("MIPYRE_METRICS", "1")]).unwrap().tuning.metrics);
        assert_eq!(invalid(&[("MIPYRE_METRICS", "most")]).0, "MIPYRE_METRICS");
        assert_eq!(invalid(&[("MIPYRE_TPC_DARWIN", "fan-out")]).0, "MIPYRE_TPC_DARWIN");
        let fanout = recipe(&[("MIPYRE_TPC_DARWIN", "fanout")]);
        #[cfg(target_os = "macos")]
        assert_eq!(fanout.unwrap().server, ServerKind::Tpc(AcceptMode::Fanout));
        #[cfg(not(target_os = "macos"))]
        assert!(fanout.is_err());
    }

    #[test]
    fn gc_mode_parses_the_three_modes_and_nothing_else() {
        assert_eq!("count".parse::<GcMode>().ok(), Some(GcMode::Count));
        assert_eq!("idle".parse::<GcMode>().ok(), Some(GcMode::Idle));
        assert_eq!("off".parse::<GcMode>().ok(), Some(GcMode::Off));
        for raw in ["idel", "", "IDLE", " idle"] {
            assert_eq!(raw.parse::<GcMode>().ok(), None);
        }
        let message = recipe(&[("MIPYRE_GC_MODE", "idel")])
            .expect_err("expected a ConfigError")
            .to_string();
        assert!(message.contains("MIPYRE_GC_MODE") && message.contains("\"idel\""));
    }

    mod overlay {
        use super::super::*;

        fn code(pairs: &[(&str, &str)]) -> ServerFlags {
            let mut code = ServerFlags::default();
            for (keyword, raw) in pairs {
                code.give(CodeCall::Run, keyword, raw).unwrap();
            }
            code
        }

        fn deployment(vars: &[(&str, &str)]) -> Deployment {
            let env: Vec<(OsString, OsString)> = vars
                .iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v)))
                .collect();
            Deployment::resolve(&env, &[], None, EntryPoint::Script).unwrap()
        }

        fn errors(e: ConfigError) -> Vec<String> {
            e.errors().map(ToString::to_string).collect()
        }

        #[test]
        fn the_deployment_overrides_the_code_and_says_what_it_replaced() {
            let r = crate::boot::ServerRecipe::of(
                deployment(&[("MIPYRE_PORT", "8001"), ("MIPYRE_THREADS", "16")]).over(&code(&[("port", "9000")])),
            )
            .unwrap();
            let summary = r.values.summary();
            assert!(
                summary.contains(&"port=8001 (MIPYRE_PORT) overrides 9000 (run(port=))".to_string()),
                "{summary:?}"
            );
            assert!(
                summary.contains(&"threads=16 (MIPYRE_THREADS)".to_string()),
                "{summary:?}"
            );
            assert_eq!(summary.len(), 2, "{summary:?}");
        }

        #[test]
        fn the_same_value_from_both_replaces_nothing() {
            let r =
                crate::boot::ServerRecipe::of(deployment(&[("MIPYRE_PORT", "9000")]).over(&code(&[("port", "9000")])))
                    .unwrap();
            assert!(matches!(
                r.values.port,
                Some(FlagValue::Deployed { replaced: None, .. })
            ));
        }

        #[test]
        fn the_code_wins_over_the_default() {
            let r = crate::boot::ServerRecipe::of(Deployment::default().over(&code(&[("threads", "2")]))).unwrap();
            let threads = r.sizes.threads.unwrap();
            assert_eq!(
                (threads.value().get(), threads.source().to_string()),
                (2, "run(threads=)".to_string())
            );
        }

        #[test]
        fn tls_needs_both_halves_from_any_source() {
            let half = crate::boot::ServerRecipe::of(Deployment::default().over(&code(&[("tls_cert", "/c.pem")])))
                .unwrap_err();
            assert_eq!(
                errors(half),
                ["tls_cert (run(tls_cert=)) is given without tls_key: give both, or neither"]
            );
            let other = crate::boot::ServerRecipe::of(
                deployment(&[("MIPYRE_TLS_KEY", "/k.pem")]).over(&ServerFlags::default()),
            )
            .unwrap_err();
            assert_eq!(
                errors(other),
                ["tls_key (MIPYRE_TLS_KEY) is given without tls_cert: give both, or neither"]
            );
            assert!(crate::boot::ServerRecipe::of(
                deployment(&[("MIPYRE_TLS_CERT", "/c.pem")]).over(&code(&[("tls_key", "/k.pem")]))
            )
            .is_ok());
        }

        #[test]
        fn a_port_given_twice_names_both_places() {
            let e = crate::boot::ServerRecipe::of(
                deployment(&[("MIPYRE_TLS_PORTS", "8443,9000")]).over(&code(&[("port", "9000")])),
            )
            .unwrap_err();
            assert_eq!(
                errors(e),
                ["port 9000 is given twice: as run(port=) and as MIPYRE_TLS_PORTS"]
            );
            let e = crate::boot::ServerRecipe::of(
                deployment(&[("MIPYRE_PLAIN_PORTS", "8000")]).over(&ServerFlags::default()),
            )
            .unwrap_err();
            assert_eq!(
                errors(e),
                ["port 8000 is given twice: as the default and as MIPYRE_PLAIN_PORTS"]
            );
            assert!(crate::boot::ServerRecipe::of(
                deployment(&[("MIPYRE_PORT", "0"), ("MIPYRE_PLAIN_PORTS", "8000")]).over(&ServerFlags::default())
            )
            .is_ok());
        }

        #[test]
        fn a_keyword_that_is_no_flag_of_its_call_is_refused() {
            let mut code = ServerFlags::default();
            assert!(code.give(CodeCall::Run, "gc_mode", "idle").is_err());
            assert!(code.give(CodeCall::ServerFlags, "gc_mode", "idle").is_ok());
            assert_eq!(
                code.give(CodeCall::Run, "threads", "0").unwrap_err().to_string(),
                "run(threads=): \"0\" is invalid: expected a positive whole number"
            );
        }
    }

    mod deployment {
        use std::ffi::OsString;
        use std::num::{NonZeroU16, NonZeroUsize};
        use std::path::Path;

        use super::super::*;

        fn env(vars: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
            vars.iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v)))
                .collect()
        }

        const PATH: &str = "/srv/app/pyproject.toml";

        fn resolve(
            vars: &[(&str, &str)],
            flags: &[(&'static str, &str)],
            file: Option<&str>,
        ) -> Result<Deployment, ConfigError> {
            let flags: Vec<(&'static str, String)> = flags.iter().map(|(n, v)| (*n, v.to_string())).collect();
            Deployment::resolve(
                &env(vars),
                &flags,
                file.map(|text| (Path::new(PATH), text)),
                EntryPoint::CliRun,
            )
        }

        fn errors(e: ConfigError) -> Vec<String> {
            e.errors().map(ToString::to_string).collect()
        }

        #[test]
        fn the_command_line_beats_the_environment_which_beats_the_file() {
            let file = "[tool.mipyre]\nthreads = 2\nport = 9000\nhost = \"0.0.0.0\"\n";
            let d = resolve(
                &[("MIPYRE_THREADS", "4"), ("MIPYRE_PORT", "9100")],
                &[("threads", "8")],
                Some(file),
            )
            .unwrap();
            let threads = d.threads.unwrap();
            assert_eq!(threads.value, NonZeroUsize::new(8).unwrap());
            assert_eq!(threads.from.to_string(), "mipyre run --threads");
            let port = d.port.unwrap();
            assert_eq!((port.value, port.from), (9100, DeploymentPlace::Env("MIPYRE_PORT")));
            assert_eq!(
                d.host.unwrap().from.to_string(),
                "/srv/app/pyproject.toml:4: [tool.mipyre] host"
            );
            assert!(d.subinterpreters.is_none());
        }

        /// `read_flag` asserts in a debug build that each flag's shape is one its type is written
        /// in, so reading every flag once checks the whole table.
        #[test]
        fn every_flag_is_read_with_a_shape_its_type_takes() {
            let every: Vec<(&'static str, String)> =
                crate::boot::FLAGS.iter().map(|f| (f.name, String::new())).collect();
            let e = Deployment::resolve(&[], &every, None, EntryPoint::CliRun).unwrap_err();
            assert!(e.errors().count() > 0);
        }

        #[test]
        fn nothing_given_is_nothing_deployed() {
            let d = resolve(&[("PATH", "/usr/bin")], &[], None).unwrap();
            assert!(d.summary().is_empty());
            assert_eq!(d.entry, EntryPoint::CliRun);
        }

        #[test]
        fn a_process_table_key_reads_its_flag_with_the_file_line() {
            let file = "[tool.mipyre.process]\nworkers = 3\ntimeout = 0.5\n";
            let d = resolve(&[], &[], Some(file)).unwrap();
            assert_eq!(d.process_workers.unwrap().value, NonZeroUsize::new(3).unwrap());
            let timeout = d.process_timeout.unwrap();
            assert_eq!(timeout.value, std::time::Duration::from_millis(500));
            assert_eq!(
                timeout.from.to_string(),
                "/srv/app/pyproject.toml:3: [tool.mipyre.process] timeout"
            );
        }

        #[test]
        fn a_list_in_the_file_is_the_flags_comma_list() {
            let d = resolve(&[], &[], Some("[tool.mipyre]\nextra_tls_ports = [8443, 9443]\n")).unwrap();
            assert_eq!(d.extra_tls_ports.unwrap().value.len(), 2);
        }

        #[test]
        fn every_error_is_returned_at_once_each_with_its_source() {
            let file = "[tool.mipyre]\n\nport = 70000\n";
            let e = resolve(&[("MIPYRE_GC_MODE", "sometimes")], &[("threads", "0")], Some(file)).unwrap_err();
            let all = errors(e);
            assert_eq!(all.len(), 3, "{all:?}");
            assert!(
                all.iter()
                    .any(|l| l.starts_with("mipyre run --threads: \"0\" is invalid for threads")),
                "{all:?}"
            );
            assert!(
                all.iter()
                    .any(|l| l.starts_with("MIPYRE_GC_MODE: \"sometimes\" is invalid for gc_mode: expected one of")),
                "{all:?}"
            );
            assert!(
                all.iter()
                    .any(|l| l.starts_with("/srv/app/pyproject.toml:3: [tool.mipyre] port: \"70000\" is invalid")),
                "{all:?}"
            );
        }

        #[cfg(unix)]
        #[test]
        fn a_variable_that_is_not_utf8_is_named() {
            use std::os::unix::ffi::OsStringExt;
            let vars = vec![(OsString::from("MIPYRE_HOST"), OsString::from_vec(vec![b'a', 0xff]))];
            let e = Deployment::resolve(&vars, &[], None, EntryPoint::Script).unwrap_err();
            assert_eq!(
                errors(e),
                ["MIPYRE_HOST=\"a\u{fffd}\" is invalid for host: it is not UTF-8"]
            );
        }

        #[test]
        fn a_file_that_is_not_toml_is_one_error_with_its_line_and_column() {
            let e = resolve(&[], &[], Some("[tool.mipyre]\nthreads = = 2\n")).unwrap_err();
            let all = errors(e);
            assert_eq!(all.len(), 1);
            assert!(all[0].starts_with("/srv/app/pyproject.toml:2:"), "{all:?}");
        }

        #[test]
        fn a_settings_table_that_is_not_a_table_is_an_error_on_its_line() {
            let e = resolve(&[], &[], Some("[tool]\nmipyre = 3\n")).unwrap_err();
            assert_eq!(errors(e), ["/srv/app/pyproject.toml:2: [tool.mipyre] must be a table"]);
        }

        #[test]
        fn a_process_table_that_is_not_a_table_is_an_error_on_its_line() {
            let e = resolve(&[], &[], Some("[tool.mipyre]\nprocess = \"fork\"\n")).unwrap_err();
            assert_eq!(
                errors(e),
                ["/srv/app/pyproject.toml:2: [tool.mipyre.process] must be a table"]
            );
        }

        #[test]
        fn a_file_without_the_table_gives_nothing() {
            let d = resolve(&[], &[], Some("[project]\nname = \"app\"\n")).unwrap();
            assert!(d.summary().is_empty());
        }

        #[test]
        fn the_summary_names_each_value_and_its_source() {
            let d = resolve(&[("MIPYRE_METRICS", "1")], &[("process.workers", "2")], None).unwrap();
            assert_eq!(
                d.summary(),
                [
                    "metrics=on (MIPYRE_METRICS)",
                    "process.workers=2 (mipyre run --process-workers)"
                ]
            );
        }

        #[test]
        fn a_bad_port_list_names_the_variable_and_the_value() {
            for var in ["MIPYRE_TLS_PORTS", "MIPYRE_PLAIN_PORTS"] {
                for raw in ["443x", "70000", "0"] {
                    let all = errors(resolve(&[(var, raw)], &[], None).unwrap_err());
                    assert!(
                        all.len() == 1 && all[0].starts_with(&format!("{var}: \"{raw}\" is invalid")),
                        "{all:?}"
                    );
                }
            }
        }

        #[test]
        fn port_lists_parse_and_a_trailing_comma_is_nothing() {
            let d = resolve(
                &[("MIPYRE_TLS_PORTS", "8443, 9443,"), ("MIPYRE_PLAIN_PORTS", "8082")],
                &[],
                None,
            )
            .unwrap();
            let ports = |given: Option<DeploymentValue<Vec<NonZeroU16>>>| {
                given.unwrap().value.iter().map(|p| p.get()).collect::<Vec<_>>()
            };
            assert_eq!(
                (ports(d.extra_tls_ports), ports(d.extra_plain_ports)),
                (vec![8443, 9443], vec![8082])
            );
            assert!(resolve(&[], &[], None).unwrap().extra_plain_ports.is_none());
        }

        #[test]
        fn a_port_that_is_not_a_number_names_the_variable() {
            assert_eq!(
                errors(resolve(&[("MIPYRE_PORT", "80a")], &[], None).unwrap_err()),
                ["MIPYRE_PORT: \"80a\" is invalid for port: expected a port from 0 to 65535 (0: the kernel picks one)"]
            );
        }

        #[cfg(not(target_os = "macos"))]
        #[test]
        fn fanout_off_macos_says_so() {
            let e = resolve(&[("MIPYRE_TPC_DARWIN", "fanout")], &[], None).unwrap_err();
            assert!(errors(e)[0].ends_with("(fanout exists on macOS only)"));
        }
    }
}
