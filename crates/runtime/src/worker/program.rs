//! Reads, on main, the program every worker of one server runs: the script, main's `sys.path`
//! and the run's command line.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBool;
use serde_json::{json, Map, Value};

/// The program every worker of one server runs, read on main when the workers are built.
#[derive(Clone)]
pub struct WorkerProgram {
    pub script_path: PathBuf,
    pub(crate) script: String,
    /// It is main's `sys.path`, because a new interpreter's own holds only what the process
    /// started with, and what main added since (the CLI's working directory, a test runner's
    /// root) would be missing.
    pub(crate) import_path: Vec<PathBuf>,
    pub(crate) command_line: CommandLine,
    /// The routers (`module:attr`) with sub-interpreter routes are the only ones a worker
    /// imports.
    pub(crate) routers: Vec<String>,
    /// It is the directory the worker keeps its copies of isolated extensions in when it loads
    /// the isolation adapter for `runtime="isolated"` routes, and `None` when it does not.
    pub(crate) isolation: Option<PathBuf>,
    /// Main's `sys.path` entries that are not paths are left out of `import_path`; whether
    /// that is worth a word is the caller's to decide.
    pub skipped_import_path: Vec<SkippedPathEntry>,
}

pyo3::create_exception!(
    mipyre.engine,
    MissingAppFileError,
    crate::DeclarationError,
    "The app has no source file, and what it was asked to do needs one: sub-interpreter \
     workers rebuild the app by executing it, and the reloader watches its directory. \
     `__main__` has no `__file__` (`python -c`, the interactive interpreter, a host \
     embedding Python) while no file was named. A DeclarationError."
);

/// Returns the app's source file as an absolute path: `named` when there is one, else
/// `__main__.__file__`; a relative one is taken from the working directory now.
///
/// # Errors
///
/// Returns `MissingAppFileError` when nothing is named and `__main__` has no `__file__`, and the
/// `OSError` when a relative path needs a working directory that cannot be read.
pub fn app_file(py: Python<'_>, named: Option<&Path>) -> PyResult<PathBuf> {
    let file = match named {
        Some(path) => path.to_path_buf(),
        None => main_file(py)?,
    };
    let file = if file.is_relative() {
        std::env::current_dir()?.join(file)
    } else {
        file
    };
    Ok(normal(&file))
}

/// Drops `.` components and lets `..` remove the one before it, as `os.path.abspath` does, so
/// the path compares equal to Python's spelling of the same file.
fn normal(path: &Path) -> PathBuf {
    path.components().fold(PathBuf::new(), |mut out, component| {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
        out
    })
}

fn main_file(py: Python<'_>) -> PyResult<PathBuf> {
    let main = py.import("__main__")?;
    match main.getattr_opt("__file__")? {
        Some(file) => file.extract(),
        None => {
            let argv0 = match py.import("sys")?.getattr_opt("argv")? {
                None => "absent: sys has no argv".to_owned(),
                Some(argv) => match argv.get_item(0) {
                    Ok(first) => first.repr()?.to_string(),
                    Err(e) if e.is_instance_of::<pyo3::exceptions::PyIndexError>(py) => {
                        "absent: sys.argv is empty".to_owned()
                    }
                    Err(e) => return Err(e),
                },
            };
            let main = main.repr()?;
            Err(MissingAppFileError::new_err(format!(
                "there is no app source file: sub-interpreter workers rebuild the app by executing \
                 its script, and reload watches the directory of the app's source file, but \
                 __main__ is {main}, which has no __file__ (sys.argv[0] is {argv0}). Python sets \
                 __main__.__file__ only when it runs a file (`python app.py`, `python -m pkg`); \
                 `python -c`, the interactive interpreter and hosts that embed Python (such as an \
                 execnet / pytest-xdist worker) leave it unset. Start the app from a file, or with \
                 `mipyre run module:app` (`mipyre dev module:app` to reload), or, without reload, \
                 create it with Mipyre(runtime='main') to serve every handler on the main \
                 interpreter."
            )))
        }
    }
}

/// A `sys.path` entry of main that a worker cannot take as a path.
#[derive(Clone)]
pub struct SkippedPathEntry {
    /// It is the entry's `repr()`, or the text of the error its `repr()` raised.
    pub entry: String,
    /// It is the `os.fsdecode` or conversion error, as raised.
    pub reason: String,
}

impl WorkerProgram {
    /// Reads `script_path` and main's `sys.path` now; an unreadable script is an error. The
    /// workers get `command_line`.
    pub fn read(
        py: Python<'_>,
        script_path: PathBuf,
        command_line: CommandLine,
        routers: Vec<String>,
        isolation: Option<PathBuf>,
    ) -> PyResult<Self> {
        let script = std::fs::read_to_string(&script_path)
            .map_err(|e| PyRuntimeError::new_err(format!("read script '{}': {e}", script_path.display())))?;
        let (import_path, skipped_import_path) = import_path(py)?;
        Ok(WorkerProgram {
            script_path,
            script,
            import_path,
            command_line,
            routers,
            isolation,
            skipped_import_path,
        })
    }
}

/// Splits main's `sys.path` into what `os.fsdecode` makes a path of, undecodable bytes included,
/// and the entries it cannot, which a worker cannot take because the objects belong to main.
fn import_path(py: Python<'_>) -> PyResult<(Vec<PathBuf>, Vec<SkippedPathEntry>)> {
    let fsdecode = py.import("os")?.getattr("fsdecode")?;
    let entries = py.import("sys")?.getattr("path")?;
    let mut path = Vec::new();
    let mut skipped = Vec::new();
    for entry in entries.try_iter()? {
        let entry = entry?;
        match fsdecode.call1((&entry,)).and_then(|p| p.extract::<PathBuf>()) {
            Ok(p) => path.push(p),
            Err(reason) => skipped.push(SkippedPathEntry {
                entry: match entry.repr() {
                    Ok(repr) => repr.to_string(),
                    Err(repr_error) => repr_error.to_string(),
                },
                reason: reason.to_string(),
            }),
        }
    }
    Ok((path, skipped))
}

/// The app's part of this process's command line: what every interpreter's `sys.argv` is, and
/// each flag the app declared with the value it got. An argument is OS text, as Python got it:
/// bytes that are not UTF-8 survive the copy to a worker.
///
/// `parsed` is false when no entry parsed a command line and the run gives a flag nothing parsed
/// no value (a script that never called `flags.parse()`): `argv` is then main's `sys.argv` and
/// `flags` holds only the values a test overrode. A test process's run is the empty command line
/// parsed: every flag its default or its override, `argv` main's.
#[pyclass(module = "mipyre.engine", frozen, eq, from_py_object)]
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(any(test, feature = "testkit"), derive(Default))]
pub struct CommandLine {
    argv: Vec<OsString>,
    flags: BTreeMap<String, AppFlag>,
    parsed: bool,
}

/// One flag the app declared: its kind and default (a worker's declaration is checked against
/// them) and its value. Both values fit the kind: only [`AppFlag::new`] makes one.
#[derive(Clone, Debug, PartialEq)]
struct AppFlag {
    kind: FlagKind,
    default: AppValue,
    value: AppValue,
}

#[derive(Clone, Debug, PartialEq)]
enum FlagKind {
    Str,
    Int,
    Float,
    Bool,
    /// The choices are never empty.
    Choice(Vec<String>),
    StrList,
}

#[derive(Clone, Debug, PartialEq)]
enum AppValue {
    None,
    Str(String),
    Int(i64),
    /// Always finite: JSON has no NaN or infinity.
    Float(f64),
    Bool(bool),
    StrList(Vec<String>),
}

/// Which of a flag's two values a [`CommandLineError::MisfitValue`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueRole {
    Default,
    Value,
}

impl std::fmt::Display for ValueRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ValueRole::Default => "default",
            ValueRole::Value => "value",
        })
    }
}

/// Why a command line could not be built or read back. A field's text is the JSON it held.
#[derive(Debug, thiserror::Error)]
enum CommandLineError {
    #[error("flag {flag}: unknown kind {kind:?} (the kinds are str, int, float, bool, choice, strs)")]
    UnknownKind { flag: String, kind: String },
    #[error("flag {flag}: kind {kind} takes choices only when it is choice, and then at least one; got {choices:?}")]
    Choices {
        flag: String,
        kind: String,
        choices: Option<Vec<String>>,
    },
    #[error("flag {flag}: its {role} {value:?} is no {kind} value")]
    MisfitValue {
        flag: String,
        role: ValueRole,
        kind: &'static str,
        value: String,
    },
    #[error("a command line that is not JSON ({source}): {text:?}")]
    NotJson { text: String, source: serde_json::Error },
    #[error("a command line whose {field} is {found}, not what CommandLine.encode() writes there")]
    Malformed { field: String, found: String },
}

fn malformed(field: impl Into<String>, found: Option<&Value>) -> CommandLineError {
    CommandLineError::Malformed {
        field: field.into(),
        found: found.map_or_else(|| "missing".to_owned(), Value::to_string),
    }
}

/// Returns `e` as the `ValueError` Python callers of `CommandLine` get.
fn value_error(e: CommandLineError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

impl FlagKind {
    fn new(flag: &str, word: &str, choices: Option<Vec<String>>) -> Result<FlagKind, CommandLineError> {
        let refused = |choices| CommandLineError::Choices {
            flag: flag.to_owned(),
            kind: word.to_owned(),
            choices,
        };
        let no_choices = |kind| match &choices {
            None => Ok(kind),
            Some(_) => Err(refused(choices.clone())),
        };
        match word {
            "str" => no_choices(FlagKind::Str),
            "int" => no_choices(FlagKind::Int),
            "float" => no_choices(FlagKind::Float),
            "bool" => no_choices(FlagKind::Bool),
            "strs" => no_choices(FlagKind::StrList),
            "choice" => match choices {
                Some(choices) if !choices.is_empty() => Ok(FlagKind::Choice(choices)),
                choices => Err(refused(choices)),
            },
            other => Err(CommandLineError::UnknownKind {
                flag: flag.to_owned(),
                kind: other.to_owned(),
            }),
        }
    }

    fn word(&self) -> &'static str {
        match self {
            FlagKind::Str => "str",
            FlagKind::Int => "int",
            FlagKind::Float => "float",
            FlagKind::Bool => "bool",
            FlagKind::Choice(_) => "choice",
            FlagKind::StrList => "strs",
        }
    }

    fn choices(&self) -> Option<&[String]> {
        match self {
            FlagKind::Choice(choices) => Some(choices),
            _ => None,
        }
    }

    /// Says whether `value` is one this kind takes; only a bool flag has no `None`.
    fn fits(&self, value: &AppValue) -> bool {
        match (self, value) {
            (FlagKind::Bool, AppValue::Bool(_)) => true,
            (FlagKind::Bool, _) => false,
            (_, AppValue::None) => true,
            (FlagKind::Str, AppValue::Str(_)) | (FlagKind::Int, AppValue::Int(_)) => true,
            (FlagKind::Float, AppValue::Float(f)) => f.is_finite(),
            (FlagKind::Choice(choices), AppValue::Str(s)) => choices.contains(s),
            (FlagKind::StrList, AppValue::StrList(_)) => true,
            _ => false,
        }
    }
}

impl AppFlag {
    fn new(flag: &str, kind: FlagKind, default: AppValue, value: AppValue) -> Result<AppFlag, CommandLineError> {
        for (role, v) in [(ValueRole::Default, &default), (ValueRole::Value, &value)] {
            if !kind.fits(v) {
                return Err(CommandLineError::MisfitValue {
                    flag: flag.to_owned(),
                    role,
                    kind: kind.word(),
                    value: format!("{v:?}"),
                });
            }
        }
        Ok(AppFlag { kind, default, value })
    }
}

impl AppValue {
    /// Reads `obj` as a value of `kind`; what does not fit is left for [`AppFlag::new`] to refuse.
    fn from_py(kind: &FlagKind, obj: &Bound<'_, PyAny>) -> PyResult<AppValue> {
        if obj.is_none() {
            return Ok(AppValue::None);
        }
        Ok(match kind {
            FlagKind::Str | FlagKind::Choice(_) => AppValue::Str(obj.extract()?),
            FlagKind::Int if obj.is_instance_of::<PyBool>() => {
                return Err(PyValueError::new_err(format!("an int flag's value is {obj}, a bool")))
            }
            FlagKind::Int => AppValue::Int(obj.extract()?),
            FlagKind::Float => AppValue::Float(obj.extract()?),
            FlagKind::Bool => AppValue::Bool(obj.extract()?),
            FlagKind::StrList => AppValue::StrList(obj.extract()?),
        })
    }

    fn to_py<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(match self {
            AppValue::None => py.None().into_bound(py),
            AppValue::Str(s) => s.into_pyobject(py)?.into_any(),
            AppValue::Int(i) => i.into_pyobject(py)?.into_any(),
            AppValue::Float(f) => f.into_pyobject(py)?.into_any(),
            AppValue::Bool(b) => b.into_pyobject(py)?.to_owned().into_any(),
            AppValue::StrList(l) => l.into_pyobject(py)?.into_any(),
        })
    }

    fn to_json(&self) -> Value {
        match self {
            AppValue::None => Value::Null,
            AppValue::Str(s) => json!(s),
            AppValue::Int(i) => json!(i),
            AppValue::Float(f) => json!(f),
            AppValue::Bool(b) => json!(b),
            AppValue::StrList(l) => json!(l),
        }
    }

    /// Reads a value `to_json` wrote for `kind`; `None` when it is no such value.
    fn from_json(kind: &FlagKind, v: &Value) -> Option<AppValue> {
        if v.is_null() {
            return Some(AppValue::None);
        }
        match kind {
            FlagKind::Str | FlagKind::Choice(_) => v.as_str().map(|s| AppValue::Str(s.to_owned())),
            FlagKind::Int => v.as_i64().map(AppValue::Int),
            FlagKind::Float => v.as_f64().map(AppValue::Float),
            FlagKind::Bool => v.as_bool().map(AppValue::Bool),
            FlagKind::StrList => strings(v).map(AppValue::StrList),
        }
    }
}

fn strings(v: &Value) -> Option<Vec<String>> {
    v.as_array()?.iter().map(|s| s.as_str().map(str::to_owned)).collect()
}

impl CommandLine {
    /// Returns the command line of a process whose entry parsed none: main's `sys.argv`, no flags.
    /// One that is missing or holds something other than `str` is an error naming it, caused by
    /// what reading it raised.
    pub fn of_main(py: Python<'_>) -> PyResult<Self> {
        let sys = py.import("sys")?;
        sys.getattr("argv")
            .and_then(|argv| argv.extract())
            .map(|argv| CommandLine {
                argv,
                flags: BTreeMap::new(),
                parsed: false,
            })
            .map_err(|raised| {
                let error = PyRuntimeError::new_err("every worker gets main's sys.argv, which must be a list of str");
                error.set_cause(py, Some(raised));
                error
            })
    }

    fn to_json(&self) -> Value {
        let argv: Vec<&[u8]> = self.argv.iter().map(|a| a.as_bytes()).collect();
        let flags: Map<String, Value> = self
            .flags
            .iter()
            .map(|(name, flag)| {
                let mut entry = Map::new();
                entry.insert("kind".into(), json!(flag.kind.word()));
                if let Some(choices) = flag.kind.choices() {
                    entry.insert("choices".into(), json!(choices));
                }
                entry.insert("default".into(), flag.default.to_json());
                entry.insert("value".into(), flag.value.to_json());
                (name.clone(), Value::Object(entry))
            })
            .collect();
        json!({ "argv": argv, "flags": flags, "parsed": self.parsed })
    }

    fn from_json(v: &Value) -> Result<CommandLine, CommandLineError> {
        let args = v
            .get("argv")
            .and_then(Value::as_array)
            .ok_or_else(|| malformed("argv", v.get("argv")))?;
        let argv = args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                arg.as_array()
                    .and_then(|bytes| {
                        bytes
                            .iter()
                            .map(|b| b.as_u64().and_then(|b| u8::try_from(b).ok()))
                            .collect()
                    })
                    .map(OsString::from_vec)
                    .ok_or_else(|| malformed(format!("argv[{i}]"), Some(arg)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let parsed = v
            .get("parsed")
            .and_then(Value::as_bool)
            .ok_or_else(|| malformed("parsed", v.get("parsed")))?;
        let entries = v
            .get("flags")
            .and_then(Value::as_object)
            .ok_or_else(|| malformed("flags", v.get("flags")))?;
        let mut flags = BTreeMap::new();
        for (name, entry) in entries {
            let field = |key: &str| (format!("flags.{name}.{key}"), entry.get(key));
            let word = match field("kind") {
                (_, Some(Value::String(word))) => word,
                (at, found) => return Err(malformed(at, found)),
            };
            let choices = match field("choices") {
                (_, None) => None,
                (at, Some(c)) => Some(strings(c).ok_or_else(|| malformed(at, Some(c)))?),
            };
            let kind = FlagKind::new(name, word, choices)?;
            let value_of = |key: &str| {
                let (at, found) = field(key);
                found
                    .and_then(|v| AppValue::from_json(&kind, v))
                    .ok_or_else(|| malformed(at, found))
            };
            let (default, value) = (value_of("default")?, value_of("value")?);
            flags.insert(name.clone(), AppFlag::new(name, kind, default, value)?);
        }
        Ok(CommandLine { argv, flags, parsed })
    }

    /// Returns the command line as JSON text, which [`CommandLine::decode_text`] reads back.
    pub fn encode_text(&self) -> String {
        self.to_json().to_string()
    }

    /// Reads back what [`CommandLine::encode_text`] wrote.
    ///
    /// # Errors
    ///
    /// Text that is not JSON is [`CommandLineError::NotJson`], quoting its first 200 characters;
    /// JSON that is no command line names the field and what it held.
    fn decode_text(text: &str) -> Result<CommandLine, CommandLineError> {
        let v = serde_json::from_str::<Value>(text).map_err(|source| CommandLineError::NotJson {
            text: text.chars().take(200).collect(),
            source,
        })?;
        CommandLine::from_json(&v)
    }
}

/// A flag as Python gives and reads it: `(kind, choices, default, value)`.
type PyFlag<'py> = (String, Option<Vec<String>>, Bound<'py, PyAny>, Bound<'py, PyAny>);

#[pymethods]
impl CommandLine {
    /// Builds a command line from `flags`, each `name: (kind, choices, default, value)`.
    #[new]
    fn new(argv: Vec<OsString>, flags: BTreeMap<String, PyFlag<'_>>, parsed: bool) -> PyResult<Self> {
        let flags = flags
            .into_iter()
            .map(|(name, (word, choices, default, value))| {
                let kind = FlagKind::new(&name, &word, choices).map_err(value_error)?;
                let default = AppValue::from_py(&kind, &default)?;
                let value = AppValue::from_py(&kind, &value)?;
                let flag = AppFlag::new(&name, kind, default, value).map_err(value_error)?;
                Ok((name, flag))
            })
            .collect::<PyResult<_>>()?;
        Ok(CommandLine { argv, flags, parsed })
    }

    #[getter]
    fn argv(&self) -> Vec<OsString> {
        self.argv.clone()
    }

    #[getter]
    fn parsed(&self) -> bool {
        self.parsed
    }

    /// Returns flag `name` as `(kind, choices, default, value)`, or `None` when it has none.
    fn flag<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Option<PyFlag<'py>>> {
        self.flags
            .get(name)
            .map(|f| {
                Ok((
                    f.kind.word().to_owned(),
                    f.kind.choices().map(<[String]>::to_vec),
                    f.default.to_py(py)?,
                    f.value.to_py(py)?,
                ))
            })
            .transpose()
    }

    /// Returns the command line as JSON text, which `decode` reads back.
    fn encode(&self) -> String {
        self.encode_text()
    }

    #[staticmethod]
    fn decode(text: &str) -> PyResult<CommandLine> {
        CommandLine::decode_text(text).map_err(value_error)
    }

    fn __repr__(&self) -> String {
        format!("CommandLine({})", self.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flag(kind: FlagKind, default: AppValue, value: AppValue) -> AppFlag {
        AppFlag::new("f", kind, default, value).unwrap()
    }

    #[test]
    fn a_normal_path_stays_as_it_is() {
        assert_eq!(normal(Path::new("/srv/app/main.py")), PathBuf::from("/srv/app/main.py"));
    }

    #[test]
    fn a_relative_file_joined_to_the_working_directory_loses_its_dots() {
        let file = Path::new("/home/me/project").join("./src/../app.py");
        assert_eq!(normal(&file), PathBuf::from("/home/me/project/app.py"));
    }

    #[test]
    fn a_dot_dot_at_the_root_stays_at_the_root() {
        assert_eq!(normal(Path::new("/../app.py")), PathBuf::from("/app.py"));
    }

    #[test]
    fn a_command_line_reads_back_as_it_was_encoded() {
        let line = CommandLine {
            argv: vec![
                OsString::from("/srv/app.py"),
                OsString::from_vec(b"postgres://db/\xff".to_vec()),
            ],
            flags: BTreeMap::from([
                (
                    "dsn".into(),
                    flag(FlagKind::Str, AppValue::None, AppValue::Str("x".into())),
                ),
                (
                    "n".into(),
                    flag(FlagKind::Int, AppValue::Int(-3), AppValue::Int(i64::MAX)),
                ),
                (
                    "ratio".into(),
                    flag(FlagKind::Float, AppValue::Float(1.0), AppValue::Float(0.25)),
                ),
                (
                    "on".into(),
                    flag(FlagKind::Bool, AppValue::Bool(false), AppValue::Bool(true)),
                ),
                (
                    "mode".into(),
                    flag(
                        FlagKind::Choice(vec!["a".into(), "b".into()]),
                        AppValue::Str("a".into()),
                        AppValue::None,
                    ),
                ),
                (
                    "tags".into(),
                    flag(
                        FlagKind::StrList,
                        AppValue::StrList(vec![]),
                        AppValue::StrList(vec!["t".into()]),
                    ),
                ),
            ]),
            parsed: true,
        };
        let text = line.encode_text();
        assert_eq!(CommandLine::decode_text(&text).unwrap(), line);
    }

    #[test]
    fn a_whole_float_stays_a_float() {
        let line = CommandLine {
            argv: vec![],
            flags: BTreeMap::from([("r".into(), flag(FlagKind::Float, AppValue::None, AppValue::Float(2.0)))]),
            parsed: false,
        };
        assert_eq!(CommandLine::decode_text(&line.encode_text()).unwrap(), line);
    }

    #[test]
    fn a_value_that_does_not_fit_its_kind_is_refused() {
        let choice = FlagKind::Choice(vec!["a".into()]);
        assert!(AppFlag::new(
            "m",
            choice.clone(),
            AppValue::Str("a".into()),
            AppValue::Str("a".into())
        )
        .is_ok());
        assert!(AppFlag::new("m", choice, AppValue::Str("a".into()), AppValue::Str("b".into())).is_err());
        assert!(AppFlag::new("b", FlagKind::Bool, AppValue::Bool(false), AppValue::None).is_err());
        assert!(AppFlag::new("r", FlagKind::Float, AppValue::None, AppValue::Float(f64::NAN)).is_err());
        assert!(AppFlag::new("r", FlagKind::Float, AppValue::None, AppValue::Float(f64::MAX)).is_ok());
    }

    #[test]
    fn a_choice_flag_without_choices_is_refused() {
        assert!(FlagKind::new("m", "choice", Some(vec![])).is_err());
        assert!(FlagKind::new("m", "choice", None).is_err());
        assert!(FlagKind::new("m", "str", Some(vec!["a".into()])).is_err());
        assert!(FlagKind::new("m", "text", None).is_err());
    }

    #[test]
    fn text_that_is_no_encoded_command_line_names_what_it_held() {
        assert!(matches!(
            CommandLine::decode_text("not json"),
            Err(CommandLineError::NotJson { text, .. }) if text == "not json"
        ));
        for (text, at, held) in [
            (r#"{"argv": ["a"], "flags": {}, "parsed": true}"#, "argv[0]", r#""a""#),
            (r#"{"argv": [], "flags": {}}"#, "parsed", "missing"),
            (
                r#"{"argv": [], "flags": {"n": {"kind": "int", "default": null, "value": "x"}}, "parsed": true}"#,
                "flags.n.value",
                r#""x""#,
            ),
        ] {
            match CommandLine::decode_text(text) {
                Err(CommandLineError::Malformed { field, found }) => {
                    assert_eq!((field.as_str(), found.as_str()), (at, held))
                }
                other => panic!("{text}: {other:?}"),
            }
        }
    }
}
