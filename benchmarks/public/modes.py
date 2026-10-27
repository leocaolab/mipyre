"""What is measured and how a run's raw output becomes numbers: no process, file or clock here."""

from __future__ import annotations

import dataclasses
import enum
import json
import re
import statistics
from collections.abc import Iterable, Mapping, Sequence

MAX_LOAD_1MIN = 1.5
BUILD_EXECUTABLES = frozenset({"cargo", "rustc", "maturin"})
CI_VENV = ".venv-gate/"


class Server(enum.Enum):
    TPC = "tpc"
    MULTI_THREAD = "multi-thread"

    def env(self) -> dict[str, str]:
        return {"MIPYRE_TPC": "0"} if self is Server.MULTI_THREAD else {}


@dataclasses.dataclass(frozen=True)
class Mode:
    runtime: str
    server: Server
    process_start: str | None = None

    @property
    def name(self) -> str:
        start = f" start={self.process_start}" if self.process_start else ""
        return f"{self.runtime}{start} / {self.server.value}"

    @property
    def slug(self) -> str:
        return "-".join(filter(None, (self.runtime, self.process_start, self.server.value)))

    def run_args(self) -> list[str]:
        start = ["--process-start", self.process_start] if self.process_start else []
        return ["--runtime", self.runtime, *start]


class Body(enum.Enum):
    TEXT = enum.auto()
    JSON = enum.auto()


@dataclasses.dataclass(frozen=True)
class Scenario:
    name: str
    path: str
    body: Body
    answer: object

    def holds(self, raw: bytes) -> bool:
        text = raw.decode("utf-8", errors="replace")
        if self.body is Body.TEXT:
            return text == self.answer
        try:
            return json.loads(text) == self.answer
        except json.JSONDecodeError:
            return False


PLAINTEXT = Scenario("plaintext", "/plaintext", Body.TEXT, "Hello, World!")
JSON_SMALL = Scenario("json", "/json", Body.JSON, {"message": "Hello, World!"})
CPU = Scenario("cpu", "/cpu", Body.JSON, {"result": 119999})
NUMPY = Scenario("numpy", "/numpy", Body.JSON, {"mean": 4999.5})

BASIC = (PLAINTEXT, JSON_SMALL, CPU)
WITH_NUMPY = (*BASIC, NUMPY)

CELLS: tuple[tuple[Mode, tuple[Scenario, ...]], ...] = (
    (Mode("subinterp", Server.TPC), BASIC),
    (Mode("subinterp", Server.MULTI_THREAD), BASIC),
    (Mode("isolated", Server.TPC), BASIC),
    (Mode("process", Server.TPC, "import"), WITH_NUMPY),
    (Mode("process", Server.TPC, "fork"), WITH_NUMPY),
    (Mode("process", Server.MULTI_THREAD, "fork"), WITH_NUMPY),
    (Mode("main", Server.TPC), WITH_NUMPY),
    (Mode("main", Server.MULTI_THREAD), BASIC),
)


def cell_name(mode: Mode, scenario: Scenario) -> str:
    return f"{mode.slug}/{scenario.name}"


CELL_NAMES = frozenset(cell_name(mode, scenario) for mode, scenarios in CELLS for scenario in scenarios)


@dataclasses.dataclass(frozen=True)
class WrkParams:
    threads: int
    connections: int
    duration_s: int

    def args(self) -> list[str]:
        return ["-t", str(self.threads), "-c", str(self.connections), "-d", f"{self.duration_s}s", "--latency"]


class WrkOutputUnreadable(ValueError):
    def __init__(self, missing: str, raw: str) -> None:
        super().__init__(f"wrk output has no {missing}:\n{raw}")
        self.raw = raw


@dataclasses.dataclass(frozen=True)
class WrkRun:
    requests_per_s: float
    p50_ms: float
    p99_ms: float
    requests: int
    socket_errors: int
    non_2xx: int


_UNIT_MS = {"us": 0.001, "ms": 1.0, "s": 1000.0}


def _search(pattern: str, raw: str, what: str) -> re.Match[str]:
    found = re.search(pattern, raw, re.MULTILINE)
    if found is None:
        raise WrkOutputUnreadable(what, raw)
    return found


def _percentile_ms(raw: str, pct: str) -> float:
    found = _search(rf"^\s+{pct}%\s+([\d.]+)(us|ms|s)\s*$", raw, f"{pct}% latency line")
    return float(found.group(1)) * _UNIT_MS[found.group(2)]


def parse_wrk(raw: str) -> WrkRun:
    """Read `wrk --latency` output; raise `WrkOutputUnreadable` carrying the output when a line is missing."""
    socket = re.search(r"Socket errors: connect (\d+), read (\d+), write (\d+), timeout (\d+)", raw)
    non_2xx = re.search(r"Non-2xx or 3xx responses: (\d+)", raw)
    return WrkRun(
        requests_per_s=float(_search(r"Requests/sec:\s+([\d.]+)", raw, "Requests/sec line").group(1)),
        p50_ms=_percentile_ms(raw, "50"),
        p99_ms=_percentile_ms(raw, "99"),
        requests=int(_search(r"(\d+) requests in", raw, "request count").group(1)),
        socket_errors=sum(int(n) for n in socket.groups()) if socket else 0,
        non_2xx=int(non_2xx.group(1)) if non_2xx else 0,
    )


@dataclasses.dataclass(frozen=True)
class ProcStat:
    pid: int
    ppid: int
    cpu_ticks: int


def parse_proc_stat(text: str) -> ProcStat:
    """Read `/proc/<pid>/stat`; the command name in parentheses may hold spaces, so fields count from its last `)`."""
    pid, _, rest = text.partition(" (")
    fields = rest.rpartition(") ")[2].split()
    return ProcStat(pid=int(pid), ppid=int(fields[1]), cpu_ticks=int(fields[11]) + int(fields[12]))


def tree_ticks(stats: Iterable[ProcStat], root: int) -> int:
    """Sum the CPU ticks of `root` and every process below it."""
    children: dict[int, list[ProcStat]] = {}
    by_pid: dict[int, ProcStat] = {}
    for stat in stats:
        children.setdefault(stat.ppid, []).append(stat)
        by_pid[stat.pid] = stat
    if root not in by_pid:
        return 0
    total, todo = 0, [by_pid[root]]
    while todo:
        stat = todo.pop()
        total += stat.cpu_ticks
        todo.extend(children.get(stat.pid, []))
    return total


def cores_used(ticks_before: int, ticks_after: int, ticks_per_s: int, wall_s: float) -> float:
    return (ticks_after - ticks_before) / ticks_per_s / wall_s


def _is_build_or_ci(cmdline: str) -> bool:
    executable = cmdline.split(" ", 1)[0].rsplit("/", 1)[-1]
    return executable in BUILD_EXECUTABLES or CI_VENV in cmdline


def foreign_work(cmdlines: Iterable[str]) -> str | None:
    """Name the builds and CI test runs in the process list, or return None when there are none."""
    busy = [line for line in cmdlines if _is_build_or_ci(line)]
    return "a build or CI test run is running: " + " | ".join(line[:160] for line in busy) if busy else None


def busy_reason(loadavg: str, cmdlines: Iterable[str]) -> str | None:
    """Say why the machine is too busy to measure on, or None when it is quiet enough."""
    load_1min = float(loadavg.split()[0])
    if load_1min >= MAX_LOAD_1MIN:
        return f"1-minute load {load_1min} >= {MAX_LOAD_1MIN} (/proc/loadavg: {loadavg.strip()})"
    return foreign_work(cmdlines)


@dataclasses.dataclass(frozen=True)
class Spread:
    median: float
    low: float
    high: float

    @classmethod
    def of(cls, values: Sequence[float]) -> Spread:
        return cls(statistics.median(values), min(values), max(values))


@dataclasses.dataclass(frozen=True)
class CellSummary:
    mode: str
    scenario: str
    reps: int
    requests_per_s: Spread
    p50_ms: Spread
    p99_ms: Spread
    server_cores: Spread
    client_cores: Spread
    errors: int


def summarize(records: Iterable[Mapping[str, object]]) -> list[CellSummary]:
    """Group the measured runs by mode and scenario, in the order they first appear."""
    groups: dict[tuple[str, str], list[Mapping[str, object]]] = {}
    for record in records:
        if record.get("refused"):
            continue
        groups.setdefault((str(record["mode"]), str(record["scenario"])), []).append(record)

    def spread(runs: list[Mapping[str, object]], key: str) -> Spread:
        return Spread.of([float(str(run[key])) for run in runs])

    return [
        CellSummary(
            mode=mode,
            scenario=scenario,
            reps=len(runs),
            requests_per_s=spread(runs, "requests_per_s"),
            p50_ms=spread(runs, "p50_ms"),
            p99_ms=spread(runs, "p99_ms"),
            server_cores=spread(runs, "server_cores"),
            client_cores=spread(runs, "client_cores"),
            errors=sum(int(str(run["socket_errors"])) + int(str(run["non_2xx"])) for run in runs),
        )
        for (mode, scenario), runs in groups.items()
    ]


def _cell(spread: Spread, digits: int) -> str:
    return f"{spread.median:,.{digits}f} ({spread.low:,.{digits}f}–{spread.high:,.{digits}f})"


def markdown_table(summaries: Iterable[CellSummary]) -> str:
    rows = [
        "| mode / server | scenario | requests/s | p50 ms | p99 ms | server cores used | client cores used | errors | reps |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    rows += [
        f"| {s.mode} | {s.scenario} | {_cell(s.requests_per_s, 0)} | {_cell(s.p50_ms, 2)} | {_cell(s.p99_ms, 2)} "
        f"| {_cell(s.server_cores, 1)} | {_cell(s.client_cores, 1)} | {s.errors} | {s.reps} |"
        for s in summaries
    ]
    return "\n".join(rows)
