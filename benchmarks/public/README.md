# mipyre runtime modes benchmark

Throughput and latency of one small app served in each of mipyre's runtime modes, measured
with `wrk` on one Linux machine. It reports mipyre only: it is not a comparison with other
frameworks.

## What is measured

The app (`app.py`) takes its runtime from `BENCH_RUNTIME`, so every mode
serves exactly the same handlers.

| route | handler |
|---|---|
| `/plaintext` | returns the text `Hello, World!` |
| `/json` | returns `{"message": "Hello, World!"}` |
| `/cpu` | pure Python: `sum(i * i % 7 for i in range(60_000))`, a few ms of bytecode per request |
| `/numpy` | the mean of a 10 000-element float64 array (numpy imported inside the handler) |

`/cpu` shows whether a mode runs Python in parallel: a mode with one interpreter lock tops out
at one core's worth of requests, a parallel one scales with the cores it is given.

Modes (`modes.py`, `CELLS`):

| mode | how it is started | where handlers run |
|---|---|---|
| `subinterp / tpc` | `BENCH_RUNTIME=subinterp` (default) | one own-GIL sub-interpreter per thread of the thread-per-core server |
| `subinterp / multi-thread` | `BENCH_RUNTIME=subinterp`, `MIPYRE_TPC=0` | a pool of sub-interpreters behind a multi-thread Tokio server |
| `isolated / tpc` | `BENCH_RUNTIME=isolated` | the same sub-interpreters; extensions that cannot load in one get a private copy per worker. This app loads none, so it measures the mode's overhead only |
| `process start=import / tpc` | `BENCH_RUNTIME=process` | worker processes (process lanes); each worker imports the app |
| `process start=fork / tpc` | `BENCH_RUNTIME=process`, `MIPYRE_PROCESS_START=fork` | worker processes forked from a template that imported the app |
| `process start=fork / multi-thread` | `BENCH_RUNTIME=process`, `MIPYRE_PROCESS_START=fork`, `MIPYRE_TPC=0` | the same forked workers behind the multi-thread server |
| `main / tpc` | `BENCH_RUNTIME=main` | the server's main interpreter (one GIL) |
| `main / multi-thread` | `BENCH_RUNTIME=main`, `MIPYRE_TPC=0` | the main interpreter behind the multi-thread server |

`/numpy` is measured on `process` and `main` only: numpy does not load in a plain
sub-interpreter.

Every server's answers to all its routes are checked against the expected values before it
is measured; a server that answers wrongly is not measured, and what it answered is kept in
the results file under `refused`.

## How a run is made

For each repetition, for each mode, for each route:

1. wait until the machine is quiet: the 1-minute load average is below 1.5 and no build
   (`cargo`, `rustc`, `maturin`) or CI test run is in the process list;
2. start a fresh server with `BENCH_RUNTIME=… taskset -c <server CPUs> python -m mipyre run app.py:app`;
3. check every route's answer;
4. warm up with `wrk` for 2 s, then measure with `wrk` for 10 s, `wrk` pinned to the client CPUs;
5. record requests/s, p50 and p99 latency, socket errors and non-2xx answers, and the CPU
   time the server's whole process tree (server, process-lane template and workers) used
   during the measured run, divided by its wall time: the cores the server actually used.
   The same is recorded for `wrk` (client cores used), to show the client was not the limit.

A run during which a build or CI test run appeared is kept in the results file as
`disturbed` and made again, up to 5 attempts.

The repetition loop is outermost, so drift on the machine is spread over every cell instead of
landing on one.

## Running it

Linux only (it reads `/proc` and pins with `taskset`); needs `wrk`. From a checkout with
mipyre built (`maturin develop --release`) and numpy installed:

```sh
lscpu -e        # pick server and client CPUs; keep SMT siblings on the same side
nice -n 10 taskset -c 4-7,12-15 python benchmarks/public/run.py measure \
    --out results.jsonl --reps 3 \
    --server-cpus 0-3,8-11 --client-cpus 4-7,12-15 \
    --wrk-threads 8 --connections 64 --duration 10
python benchmarks/public/run.py table results.jsonl
```

`--cell <mode>/<route>` (repeatable, e.g. `--cell subinterp-tpc/plaintext`) measures only
those cells. `measure` appends one JSON line per run (with the raw `wrk` output and the start of the
server's log); `table` prints the median and min–max of every cell, and every run that was
not measured with the reason.

## Results

Measured 2026-11-12, 12:11–16:37 EST, for the 4.1.0 release.

- Code: mipyre at commit `3944ef6f`; Python 3.14.7, numpy 2.5.4.
- Machine: AMD Ryzen 7 7840HS, 8 cores / 16 threads (SMT siblings are CPU n and n+8),
  Linux 7.0.0-38-generic.
- Core split: server on CPUs 0-3,8-11 (4 physical cores with their SMT siblings), `wrk` on
  CPUs 4-7,12-15; everything under `nice -n 10`.
- Load: the 1-minute load average at the start of each run was 0.62–1.49, and every run but
  one waited for the 5-minute load average to be at or under 2 (`main / tpc` `/numpy`, third
  repetition, started at 6.85 after a burst during its wait). No run was disturbed by a build
  or CI test run.
- Tool: `wrk` 4.1.0, `-t 8 -c 64 -d 10s --latency`, after a 2 s warm-up, against a fresh
  server per run; default mipyre settings otherwise.
- Each cell: median (min–max) of 3 repetitions. Errors are the sum over the 3 runs of socket
  errors and non-2xx answers.

| mode / server | scenario | requests/s | p50 ms | p99 ms | server cores used | client cores used | errors | reps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| subinterp / tpc | plaintext | 464,500 (457,178–465,637) | 0.14 (0.13–0.14) | 0.21 (0.21–0.29) | 4.0 (4.0–4.0) | 5.5 (5.4–5.5) | 0 | 3 |
| subinterp / tpc | json | 451,048 (448,770–453,666) | 0.14 (0.13–0.15) | 0.26 (0.21–0.27) | 4.0 (4.0–4.0) | 5.4 (5.3–5.4) | 0 | 3 |
| subinterp / tpc | cpu | 1,681 (1,604–1,688) | 39.63 (35.62–42.28) | 46.67 (42.10–66.78) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| subinterp / multi-thread | plaintext | 371,425 (364,779–372,816) | 0.16 (0.16–0.16) | 0.42 (0.42–0.44) | 7.0 (7.0–7.1) | 4.5 (4.4–4.5) | 0 | 3 |
| subinterp / multi-thread | json | 349,362 (336,417–367,957) | 0.17 (0.16–0.17) | 0.51 (0.43–0.55) | 6.8 (6.6–7.0) | 4.3 (4.2–4.4) | 0 | 3 |
| subinterp / multi-thread | cpu | 1,932 (1,906–1,953) | 33.00 (32.97–33.07) | 34.99 (34.02–36.00) | 8.0 (8.0–8.0) | 0.1 (0.1–0.1) | 0 | 3 |
| isolated / tpc | plaintext | 459,030 (399,615–463,622) | 0.14 (0.14–0.15) | 0.24 (0.23–0.26) | 4.0 (4.0–4.0) | 5.4 (4.9–5.4) | 0 | 3 |
| isolated / tpc | json | 420,614 (420,526–452,641) | 0.13 (0.13–0.15) | 0.24 (0.24–0.27) | 4.0 (4.0–4.0) | 5.1 (5.1–5.4) | 0 | 3 |
| isolated / tpc | cpu | 1,639 (1,583–1,641) | 42.29 (37.37–42.52) | 62.83 (56.51–69.12) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | plaintext | 377,351 (371,823–378,260) | 0.15 (0.14–0.17) | 0.32 (0.27–0.34) | 6.6 (6.6–6.9) | 4.5 (4.4–4.5) | 0 | 3 |
| process start=import / tpc | json | 367,014 (362,292–368,941) | 0.16 (0.15–0.16) | 0.37 (0.31–0.52) | 6.7 (6.6–6.7) | 4.3 (4.3–4.4) | 0 | 3 |
| process start=import / tpc | cpu | 1,676 (1,650–1,681) | 41.00 (40.04–44.29) | 46.28 (46.15–53.38) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | numpy | 317,086 (312,631–318,170) | 0.19 (0.18–0.20) | 0.30 (0.28–0.50) | 7.7 (7.7–7.7) | 3.9 (3.8–3.9) | 0 | 3 |
| process start=fork / tpc | plaintext | 373,818 (357,954–379,877) | 0.16 (0.16–0.17) | 0.35 (0.26–0.62) | 6.6 (6.6–6.6) | 4.4 (4.3–4.5) | 0 | 3 |
| process start=fork / tpc | json | 365,517 (359,025–372,529) | 0.17 (0.16–0.18) | 0.35 (0.30–0.53) | 6.7 (6.6–6.7) | 4.4 (4.3–4.4) | 0 | 3 |
| process start=fork / tpc | cpu | 1,634 (1,347–1,665) | 37.04 (34.85–44.25) | 57.51 (55.12–69.97) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / tpc | numpy | 313,456 (286,560–328,672) | 0.18 (0.18–0.21) | 0.35 (0.33–1.15) | 7.6 (7.2–7.7) | 3.9 (3.6–4.0) | 0 | 3 |
| process start=fork / multi-thread | plaintext | 201,487 (132,360–216,691) | 0.30 (0.27–0.39) | 0.85 (0.85–3.61) | 5.7 (4.0–6.0) | 2.4 (1.7–2.7) | 0 | 3 |
| process start=fork / multi-thread | json | 234,258 (232,278–235,500) | 0.25 (0.25–0.25) | 0.75 (0.73–0.76) | 6.3 (6.3–6.3) | 2.7 (2.7–2.7) | 0 | 3 |
| process start=fork / multi-thread | cpu | 1,647 (1,646–1,666) | 35.61 (30.76–43.93) | 119.34 (67.81–139.38) | 4.2 (4.2–4.2) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / multi-thread | numpy | 192,267 (181,410–193,837) | 0.31 (0.31–0.33) | 0.92 (0.90–0.98) | 6.4 (6.2–6.4) | 2.3 (2.3–2.3) | 0 | 3 |
| main / tpc | plaintext | 310,327 (301,847–313,703) | 0.20 (0.20–0.20) | 0.38 (0.34–0.42) | 6.1 (6.0–6.1) | 3.8 (3.7–3.8) | 0 | 3 |
| main / tpc | json | 285,770 (280,598–292,494) | 0.21 (0.21–0.23) | 0.41 (0.31–0.42) | 6.2 (6.1–6.3) | 3.5 (3.4–3.5) | 0 | 3 |
| main / tpc | cpu | 413 (394–421) | 150.19 (147.10–157.98) | 218.29 (217.26–222.42) | 1.0 (1.0–1.0) | 0.0 (0.0–0.0) | 0 | 3 |
| main / tpc | numpy | 106,618 (103,091–107,446) | 0.57 (0.57–0.59) | 0.94 (0.91–1.00) | 3.6 (3.6–3.6) | 1.3 (1.3–1.3) | 0 | 3 |
| main / multi-thread | plaintext | 174,749 (160,316–176,951) | 0.35 (0.34–0.37) | 1.45 (1.42–1.58) | 5.7 (5.2–5.7) | 2.1 (2.0–2.1) | 0 | 3 |
| main / multi-thread | json | 171,925 (169,836–173,314) | 0.35 (0.35–0.36) | 1.52 (1.50–1.56) | 5.5 (5.5–5.6) | 2.0 (2.0–2.1) | 0 | 3 |
| main / multi-thread | cpu | 366 (364–368) | 134.24 (134.02–144.02) | 1,300.00 (1,200.00–1,340.00) | 1.1 (1.1–1.1) | 0.0 (0.0–0.0) | 14 | 3 |

Reading the table:

- `/cpu` is the parallelism test. On the sub-interpreter and process modes it runs on every
  core the server has: about 1,700 requests/s on 4 server cores, and about 1,900 on the
  multi-thread server's 8 sub-interpreters, which also use the SMT siblings. On `main` it
  is serial: about 400 requests/s on one core, whichever server is in front.
- The errors on `main / multi-thread` `/cpu` are all `wrk` timeouts (2 s): 64 connections queue
  behind one interpreter lock, so the slowest requests wait longer than that. No mode answered
  a request with a non-2xx status.
- In `process` mode every route, including `/plaintext`, crosses to a worker process, so the
  server uses more CPU per request than in `subinterp` mode; `start=import` and `start=fork`
  differ only in how a worker starts, which this benchmark does not measure, and serve
  requests at the same speed.
- `/numpy` runs in parallel in `process` mode (about 315,000 requests/s) and on one interpreter
  lock in `main` mode (about 107,000).
