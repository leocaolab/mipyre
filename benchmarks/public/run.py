"""Measure mipyre in each runtime mode with wrk on Linux, or print the table of a results file.

    python benchmarks/public/run.py measure --out results.jsonl [--reps 3] [--server-cpus 0-3,8-11] [--client-cpus 4-7,12-15]
        [--cell subinterp-tpc/plaintext ...]
    python benchmarks/public/run.py table results.jsonl
"""

from __future__ import annotations

import argparse
import dataclasses
import http.client
import json
import os
import platform
import re
import resource
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import IO

import modes

HERE = Path(__file__).resolve().parent
APP = f"{HERE / 'app.py'}:app"
LISTENING = re.compile(r"Listening on https?://[^\s:/]+:(\d+)")
START_TIMEOUT_S = 120
STOP_TIMEOUT_S = 20
QUIET_POLL_S = 15
WARMUP_S = 2
LOG_TAIL_CHARS = 4000
MAX_ATTEMPTS = 5


class ServerDidNotStart(RuntimeError):
    pass


class WrkFailed(RuntimeError):
    pass


class Server:
    """Start `mipyre run` for one mode on the server CPUs and find its port in its output."""

    def __init__(self, mode: modes.Mode, cpus: str, log_path: Path) -> None:
        self.log_path = log_path
        self.log: IO[str] = open(log_path, "w")
        env = {**os.environ, **mode.server.env()}
        self.proc = subprocess.Popen(
            ["taskset", "-c", cpus, sys.executable, "-m", "mipyre", "run", APP,
             "--host", "127.0.0.1", "--port", "0", *mode.run_args()],
            stdout=self.log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, start_new_session=True,
        )
        self.port = self._wait_for_port()

    def output(self) -> str:
        return self.log_path.read_text(errors="replace")

    def _wait_for_port(self) -> int:
        deadline = time.monotonic() + START_TIMEOUT_S
        while True:
            text = self.output()
            if found := LISTENING.search(text):
                return int(found.group(1))
            if self.proc.poll() is not None or time.monotonic() > deadline:
                self.stop()
                raise ServerDidNotStart(f"exit {self.proc.returncode}:\n{text[-LOG_TAIL_CHARS:]}")
            time.sleep(0.1)

    def stop(self) -> int | None:
        try:
            os.killpg(self.proc.pid, signal.SIGINT)
            self.proc.wait(timeout=STOP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
        except ProcessLookupError:
            self.proc.wait()
        self.log.close()
        return self.proc.returncode


def fetch(port: int, path: str) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        conn.request("GET", path)
        answer = conn.getresponse()
        return answer.status, answer.read()
    finally:
        conn.close()


def all_stats() -> list[modes.ProcStat]:
    stats = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                stats.append(modes.parse_proc_stat((entry / "stat").read_text()))
            except (FileNotFoundError, ProcessLookupError):
                continue
    return stats


def cmdlines() -> list[str]:
    lines = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit() and entry.name != str(os.getpid()):
            try:
                lines.append((entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace"))
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                continue
    return lines


def wait_until_quiet() -> str:
    """Wait until the machine is quiet enough to measure on, logging each reason to wait; return /proc/loadavg."""
    while True:
        loadavg = Path("/proc/loadavg").read_text()
        reason = modes.busy_reason(loadavg, cmdlines())
        if reason is None:
            return loadavg.strip()
        print(f"waiting: {reason}", flush=True)
        time.sleep(QUIET_POLL_S)


def wrk(port: int, path: str, params: modes.WrkParams, cpus: str) -> str:
    done = subprocess.run(
        ["taskset", "-c", cpus, "wrk", *params.args(), f"http://127.0.0.1:{port}{path}"],
        capture_output=True, text=True, check=False,
    )
    if done.returncode != 0:
        raise WrkFailed(f"wrk exited {done.returncode}:\n{done.stdout}{done.stderr}")
    return done.stdout


def children_cpu_s() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def measure_once(server: Server, scenario: modes.Scenario, params: modes.WrkParams, client_cpus: str) -> dict:
    wrk(server.port, scenario.path, dataclasses.replace(params, duration_s=WARMUP_S), client_cpus)
    ticks_before, client_before, wall_before = tree_ticks(server), children_cpu_s(), time.monotonic()
    raw = wrk(server.port, scenario.path, params, client_cpus)
    wall = time.monotonic() - wall_before
    ticks_after, client_after = tree_ticks(server), children_cpu_s()
    run = modes.parse_wrk(raw)
    return {
        **dataclasses.asdict(run),
        "server_cores": modes.cores_used(ticks_before, ticks_after, os.sysconf("SC_CLK_TCK"), wall),
        "client_cores": (client_after - client_before) / wall,
        "wrk_output": raw,
    }


def tree_ticks(server: Server) -> int:
    return modes.tree_ticks(all_stats(), server.proc.pid)


def check_answers(server: Server, scenarios: tuple[modes.Scenario, ...]) -> list[str]:
    """Return one line per scenario whose answer is not the expected one, quoting what came back."""
    wrong = []
    for scenario in scenarios:
        status, body = fetch(server.port, scenario.path)
        if status != 200 or not scenario.holds(body):
            wrong.append(f"{scenario.path}: status {status}, body {body[:300]!r}, expected {scenario.answer!r}")
    return wrong


def git_sha() -> str:
    return subprocess.run(["git", "-C", str(HERE), "rev-parse", "HEAD"], capture_output=True, text=True,
                          check=True).stdout.strip()


def machine() -> str:
    model = next(
        (line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
         if line.startswith("model name")),
        platform.processor(),
    )
    return f"{platform.node()}: {model}, Linux {platform.release()}, {os.cpu_count()} logical CPUs"


def measure(args: argparse.Namespace) -> None:
    params = modes.WrkParams(args.wrk_threads, args.connections, args.duration)
    context = {
        "sha": git_sha(), "machine": machine(), "python": platform.python_version(),
        "server_cpus": args.server_cpus, "client_cpus": args.client_cpus,
        "tool": "wrk " + " ".join(params.args()), "warmup_s": WARMUP_S,
    }
    logs = Path(args.out).with_suffix(".logs")
    logs.mkdir(exist_ok=True)
    with open(args.out, "a") as out:
        for rep in range(1, args.reps + 1):
            for mode, scenarios in modes.CELLS:
                for scenario in scenarios:
                    if args.cell and modes.cell_name(mode, scenario) not in args.cell:
                        continue
                    for attempt in range(1, MAX_ATTEMPTS + 1):
                        record = run_cell(mode, scenarios, scenario, rep, params, args, logs)
                        out.write(json.dumps({**context, **record, "attempt": attempt}) + "\n")
                        out.flush()
                        if not record.get("disturbed"):
                            break
    print("finished", flush=True)


def run_cell(mode: modes.Mode, scenarios: tuple[modes.Scenario, ...], scenario: modes.Scenario, rep: int,
             params: modes.WrkParams, args: argparse.Namespace, logs: Path) -> dict:
    cell = {"mode": mode.name, "scenario": scenario.name, "rep": rep, "load_at_start": wait_until_quiet()}
    print(f"{cell} measuring", flush=True)
    try:
        server = Server(mode, args.server_cpus, logs / f"{mode.slug}-{scenario.name}-{rep}.log")
    except ServerDidNotStart as e:
        return {**cell, "refused": f"server did not start: {e}"}
    try:
        result = measure_checked(server, scenarios, scenario, params, args.client_cpus)
    finally:
        exit_code = server.stop()
    return {**cell, **result, "server_exit": exit_code, "server_log_head": server.output()[:LOG_TAIL_CHARS]}


def measure_checked(server: Server, scenarios: tuple[modes.Scenario, ...], scenario: modes.Scenario,
                    params: modes.WrkParams, client_cpus: str) -> dict:
    wrong = check_answers(server, scenarios)
    if wrong:
        return {"refused": "wrong answers: " + "; ".join(wrong)}
    try:
        result = measure_once(server, scenario, params, client_cpus)
    except (WrkFailed, modes.WrkOutputUnreadable) as e:
        return {"refused": str(e)}
    if foreign := modes.foreign_work(cmdlines()):
        return {**result, "refused": f"disturbed during the run: {foreign}", "disturbed": True}
    return result


def table(args: argparse.Namespace) -> None:
    records = [json.loads(line) for line in Path(args.results).read_text().splitlines() if line]
    print(modes.markdown_table(modes.summarize(records)))
    for record in records:
        if record.get("refused"):
            print(f"\nnot measured: {record['mode']} {record['scenario']} rep {record['rep']}: {record['refused']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(required=True)
    m = sub.add_parser("measure")
    m.add_argument("--out", required=True)
    m.add_argument("--reps", type=int, default=3)
    m.add_argument("--server-cpus", default="0-3,8-11")
    m.add_argument("--client-cpus", default="4-7,12-15")
    m.add_argument("--wrk-threads", type=int, default=8)
    m.add_argument("--connections", type=int, default=64)
    m.add_argument("--duration", type=int, default=10)
    m.add_argument("--cell", action="append", choices=sorted(modes.CELL_NAMES),
                   help="measure only this cell; repeat for several (default: every cell)")
    m.set_defaults(run=measure)
    t = sub.add_parser("table")
    t.add_argument("results")
    t.set_defaults(run=table)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
