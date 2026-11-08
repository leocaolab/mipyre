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

Measured 2026-11-08, 11:05–12:39 EST, for the 4.0.0 release.

- Code: mipyre at commit `87fc071e`; Python 3.14.7, numpy 2.5.4.
- Machine: AMD Ryzen 7 7840HS, 8 cores / 16 threads (SMT siblings are CPU n and n+8),
  Linux 7.0.0-38-generic.
- Core split: server on CPUs 0-3,8-11 (4 physical cores with their SMT siblings), `wrk` on
  CPUs 4-7,12-15; everything under `nice -n 10`.
- Load: the 1-minute load average at the start of each run was 0.53–1.49. No run was
  disturbed by a build or CI test run.
- Tool: `wrk` 4.1.0, `-t 8 -c 64 -d 10s --latency`, after a 2 s warm-up, against a fresh
  server per run; default mipyre settings otherwise.
- Each cell: median (min–max) of 3 repetitions. Errors are the sum over the 3 runs of socket
  errors and non-2xx answers.

| mode / server | scenario | requests/s | p50 ms | p99 ms | server cores used | client cores used | errors | reps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| subinterp / tpc | plaintext | 465,932 (461,960–472,078) | 0.13 (0.12–0.14) | 0.22 (0.21–0.24) | 4.0 (4.0–4.0) | 5.5 (5.4–5.5) | 0 | 3 |
| subinterp / tpc | json | 458,079 (455,279–460,845) | 0.13 (0.12–0.15) | 0.24 (0.23–0.25) | 4.0 (4.0–4.0) | 5.4 (5.4–5.5) | 0 | 3 |
| subinterp / tpc | cpu | 1,689 (1,688–1,692) | 36.35 (34.61–37.09) | 50.79 (45.53–51.31) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| subinterp / multi-thread | plaintext | 378,906 (376,188–383,981) | 0.16 (0.16–0.16) | 0.42 (0.38–0.43) | 7.1 (7.1–7.2) | 4.5 (4.5–4.6) | 0 | 3 |
| subinterp / multi-thread | json | 375,221 (374,991–376,905) | 0.16 (0.16–0.16) | 0.41 (0.41–0.42) | 7.1 (7.1–7.1) | 4.5 (4.5–4.5) | 0 | 3 |
| subinterp / multi-thread | cpu | 1,992 (1,971–1,993) | 32.00 (32.00–32.06) | 33.37 (33.15–34.02) | 8.0 (8.0–8.0) | 0.1 (0.1–0.1) | 0 | 3 |
| isolated / tpc | plaintext | 471,866 (471,006–472,740) | 0.14 (0.13–0.14) | 0.23 (0.21–0.24) | 4.0 (4.0–4.0) | 5.5 (5.4–5.5) | 0 | 3 |
| isolated / tpc | json | 458,496 (457,702–461,108) | 0.14 (0.12–0.14) | 0.22 (0.21–0.26) | 4.0 (4.0–4.0) | 5.4 (5.4–5.5) | 0 | 3 |
| isolated / tpc | cpu | 1,702 (1,701–1,709) | 33.37 (33.00–36.57) | 47.68 (45.29–54.95) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | plaintext | 381,047 (380,307–381,244) | 0.15 (0.15–0.16) | 0.29 (0.28–0.32) | 6.7 (6.6–6.7) | 4.5 (4.5–4.5) | 0 | 3 |
| process start=import / tpc | json | 376,182 (374,014–384,239) | 0.18 (0.16–0.18) | 0.31 (0.30–0.33) | 6.7 (6.7–6.9) | 4.4 (4.4–4.5) | 0 | 3 |
| process start=import / tpc | cpu | 1,690 (1,683–1,697) | 35.90 (34.60–37.85) | 48.01 (39.31–50.51) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | numpy | 318,369 (316,971–321,114) | 0.19 (0.18–0.19) | 0.32 (0.31–0.35) | 7.6 (7.6–7.7) | 3.9 (3.8–3.9) | 0 | 3 |
| process start=fork / tpc | plaintext | 381,343 (381,214–382,253) | 0.16 (0.14–0.16) | 0.30 (0.27–0.31) | 6.6 (6.6–6.7) | 4.5 (4.5–4.5) | 0 | 3 |
| process start=fork / tpc | json | 379,990 (376,123–381,564) | 0.16 (0.16–0.17) | 0.32 (0.25–0.32) | 6.6 (6.6–6.8) | 4.5 (4.4–4.5) | 0 | 3 |
| process start=fork / tpc | cpu | 1,678 (1,673–1,684) | 35.87 (35.65–42.04) | 48.13 (46.56–48.19) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / tpc | numpy | 321,831 (320,331–323,630) | 0.18 (0.18–0.20) | 0.35 (0.31–0.36) | 7.7 (7.7–7.8) | 3.9 (3.8–3.9) | 0 | 3 |
| process start=fork / multi-thread | plaintext | 238,415 (237,219–239,570) | 0.25 (0.24–0.25) | 0.75 (0.75–0.76) | 6.3 (6.2–6.3) | 2.7 (2.7–2.7) | 0 | 3 |
| process start=fork / multi-thread | json | 233,790 (233,131–234,365) | 0.25 (0.25–0.25) | 0.75 (0.74–0.76) | 6.3 (6.2–6.3) | 2.7 (2.7–2.7) | 0 | 3 |
| process start=fork / multi-thread | cpu | 1,675 (1,665–1,683) | 35.19 (31.04–36.96) | 109.02 (76.50–142.81) | 4.2 (4.2–4.2) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / multi-thread | numpy | 192,661 (192,355–195,365) | 0.31 (0.31–0.31) | 0.94 (0.89–0.95) | 6.4 (6.4–6.4) | 2.3 (2.2–2.3) | 0 | 3 |
| main / tpc | plaintext | 315,024 (314,529–316,115) | 0.20 (0.20–0.20) | 0.33 (0.33–0.37) | 6.1 (6.1–6.2) | 3.7 (3.7–3.8) | 0 | 3 |
| main / tpc | json | 292,869 (289,662–293,520) | 0.21 (0.21–0.21) | 0.33 (0.33–0.33) | 6.3 (6.3–6.3) | 3.5 (3.5–3.5) | 0 | 3 |
| main / tpc | cpu | 427 (426–428) | 144.79 (144.35–144.94) | 223.40 (222.57–226.62) | 1.0 (1.0–1.0) | 0.0 (0.0–0.0) | 0 | 3 |
| main / tpc | numpy | 107,473 (106,443–107,578) | 0.57 (0.57–0.57) | 0.92 (0.92–0.94) | 3.6 (3.6–3.6) | 1.3 (1.3–1.3) | 0 | 3 |
| main / multi-thread | plaintext | 173,511 (172,841–178,680) | 0.35 (0.34–0.35) | 1.48 (1.38–1.51) | 5.6 (5.6–5.7) | 2.1 (2.1–2.1) | 0 | 3 |
| main / multi-thread | json | 175,007 (174,298–176,605) | 0.35 (0.34–0.35) | 1.38 (1.37–1.40) | 5.4 (5.4–5.4) | 2.1 (2.1–2.1) | 0 | 3 |
| main / multi-thread | cpu | 362 (361–362) | 129.22 (128.82–134.45) | 1,350.00 (1,340.00–1,440.00) | 1.1 (1.1–1.1) | 0.0 (0.0–0.0) | 23 | 3 |

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
- `/numpy` runs in parallel in `process` mode (about 320,000 requests/s) and on one interpreter
  lock in `main` mode (about 107,000).
