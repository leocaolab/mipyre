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

Measured 2026-11-14, 01:00–03:22 EST, for the 4.2.0 release.

- Code: mipyre at commit `2e023796`; Python 3.14.7, numpy 2.5.4.
- Machine: AMD Ryzen 7 7840HS, 8 cores / 16 threads (SMT siblings are CPU n and n+8),
  Linux 7.0.0-38-generic.
- Core split: server on CPUs 0-3,8-11 (4 physical cores with their SMT siblings), `wrk` on
  CPUs 4-7,12-15; everything under `nice -n 10`.
- Load: the 1-minute load average at the start of each run was 0.53–1.49, and each cell
  waited for the 5-minute load average to be at or under 2 before its first run; at the start
  of the 84 runs it was 0.80–4.34, over 2 for 12 of them (the highest, 4.34, for
  `process start=fork / multi-thread` `/cpu`, third repetition). No run was disturbed by a
  build or CI test run.
- Tool: `wrk` 4.1.0, `-t 8 -c 64 -d 10s --latency`, after a 2 s warm-up, against a fresh
  server per run; default mipyre settings otherwise.
- Each cell: median (min–max) of 3 repetitions. Errors are the sum over the 3 runs of socket
  errors and non-2xx answers.

| mode / server | scenario | requests/s | p50 ms | p99 ms | server cores used | client cores used | errors | reps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| subinterp / tpc | plaintext | 463,173 (460,782–465,847) | 0.13 (0.13–0.13) | 0.22 (0.21–0.24) | 4.0 (4.0–4.0) | 5.4 (5.4–5.5) | 0 | 3 |
| subinterp / tpc | json | 457,099 (454,366–457,948) | 0.14 (0.14–0.14) | 0.22 (0.22–0.23) | 4.0 (4.0–4.0) | 5.4 (5.4–5.4) | 0 | 3 |
| subinterp / tpc | cpu | 1,707 (1,705–1,708) | 39.22 (36.81–42.92) | 44.31 (41.74–52.15) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| subinterp / multi-thread | plaintext | 372,647 (369,676–378,021) | 0.16 (0.16–0.16) | 0.45 (0.42–0.46) | 7.0 (6.9–7.0) | 4.4 (4.4–4.5) | 0 | 3 |
| subinterp / multi-thread | json | 370,625 (369,080–376,102) | 0.16 (0.16–0.16) | 0.44 (0.42–0.45) | 7.0 (7.0–7.1) | 4.4 (4.4–4.5) | 0 | 3 |
| subinterp / multi-thread | cpu | 1,980 (1,969–1,984) | 32.01 (32.01–32.04) | 33.97 (33.44–34.03) | 8.0 (8.0–8.0) | 0.1 (0.1–0.1) | 0 | 3 |
| isolated / tpc | plaintext | 438,156 (430,228–456,928) | 0.14 (0.14–0.15) | 0.23 (0.22–0.24) | 4.0 (4.0–4.0) | 5.2 (5.2–5.4) | 0 | 3 |
| isolated / tpc | json | 389,240 (386,762–447,721) | 0.15 (0.14–0.15) | 0.26 (0.23–0.28) | 4.0 (4.0–4.0) | 4.7 (4.7–5.3) | 0 | 3 |
| isolated / tpc | cpu | 1,687 (1,610–1,706) | 39.70 (36.91–41.99) | 51.60 (46.50–59.09) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | plaintext | 380,096 (379,242–381,651) | 0.16 (0.16–0.17) | 0.27 (0.27–0.29) | 6.6 (6.6–6.7) | 4.5 (4.4–4.5) | 0 | 3 |
| process start=import / tpc | json | 380,523 (376,665–384,884) | 0.16 (0.16–0.17) | 0.27 (0.26–0.29) | 6.7 (6.6–6.8) | 4.5 (4.5–4.5) | 0 | 3 |
| process start=import / tpc | cpu | 1,669 (1,649–1,678) | 34.41 (31.87–39.05) | 54.45 (49.27–56.42) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | numpy | 317,560 (316,507–318,522) | 0.20 (0.20–0.20) | 0.36 (0.32–0.40) | 7.6 (7.6–7.7) | 3.9 (3.8–3.9) | 0 | 3 |
| process start=fork / tpc | plaintext | 379,328 (378,681–380,524) | 0.17 (0.15–0.17) | 0.28 (0.27–0.29) | 6.6 (6.6–6.7) | 4.5 (4.5–4.5) | 0 | 3 |
| process start=fork / tpc | json | 377,449 (376,208–377,911) | 0.16 (0.16–0.17) | 0.25 (0.24–0.27) | 6.6 (6.6–6.6) | 4.4 (4.4–4.5) | 0 | 3 |
| process start=fork / tpc | cpu | 1,687 (1,677–1,688) | 36.41 (30.77–37.80) | 52.35 (43.23–55.20) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / tpc | numpy | 316,289 (315,530–317,489) | 0.20 (0.19–0.22) | 0.30 (0.29–0.34) | 7.7 (7.6–7.7) | 3.8 (3.8–3.9) | 0 | 3 |
| process start=fork / multi-thread | plaintext | 239,684 (238,695–240,746) | 0.25 (0.24–0.25) | 0.74 (0.72–0.75) | 6.3 (6.2–6.3) | 2.7 (2.7–2.8) | 0 | 3 |
| process start=fork / multi-thread | json | 229,019 (228,326–234,876) | 0.26 (0.25–0.26) | 0.76 (0.76–0.78) | 6.2 (6.2–6.3) | 2.7 (2.6–2.7) | 0 | 3 |
| process start=fork / multi-thread | cpu | 1,648 (1,625–1,673) | 40.68 (39.02–41.38) | 80.79 (66.08–128.30) | 4.2 (4.2–4.2) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / multi-thread | numpy | 194,338 (192,138–195,338) | 0.31 (0.31–0.31) | 0.91 (0.90–0.93) | 6.4 (6.4–6.4) | 2.3 (2.3–2.3) | 0 | 3 |
| main / tpc | plaintext | 309,914 (304,121–311,523) | 0.20 (0.18–0.20) | 0.39 (0.33–0.41) | 6.1 (5.9–6.1) | 3.8 (3.7–3.8) | 0 | 3 |
| main / tpc | json | 289,083 (287,380–292,751) | 0.22 (0.21–0.23) | 0.35 (0.34–0.35) | 6.3 (6.2–6.3) | 3.5 (3.5–3.5) | 0 | 3 |
| main / tpc | cpu | 418 (416–421) | 147.84 (147.38–148.50) | 211.34 (210.40–216.57) | 1.0 (1.0–1.0) | 0.0 (0.0–0.0) | 0 | 3 |
| main / tpc | numpy | 107,622 (105,986–107,898) | 0.57 (0.56–0.57) | 0.92 (0.92–0.94) | 3.6 (3.6–3.6) | 1.3 (1.3–1.3) | 0 | 3 |
| main / multi-thread | plaintext | 174,488 (174,381–177,352) | 0.34 (0.34–0.34) | 1.45 (1.41–1.45) | 5.6 (5.5–5.6) | 2.1 (2.1–2.2) | 0 | 3 |
| main / multi-thread | json | 170,618 (167,731–174,515) | 0.35 (0.35–0.36) | 1.52 (1.35–1.62) | 5.5 (5.3–5.5) | 2.1 (2.0–2.1) | 0 | 3 |
| main / multi-thread | cpu | 366 (365–366) | 134.35 (133.88–139.32) | 1,290.00 (1,280.00–1,390.00) | 1.1 (1.1–1.1) | 0.0 (0.0–0.0) | 13 | 3 |

Reading the table:

- `/cpu` is the parallelism test. On the sub-interpreter and process modes it runs on every
  core the server has: about 1,700 requests/s on 4 server cores, and about 2,000 on the
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
