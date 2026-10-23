# mipyre runtime modes benchmark

Throughput and latency of one small app served in each of mipyre's runtime modes, measured
with `wrk` on one Linux machine. It reports mipyre only: it is not a comparison with other
frameworks.

## What is measured

The app (`app.py`) declares no runtime; `mipyre run --runtime <mode>` picks it, so every mode
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
| `subinterp / tpc` | `--runtime subinterp` (default) | one own-GIL sub-interpreter per thread of the thread-per-core server |
| `subinterp / multi-thread` | `--runtime subinterp`, `MIPYRE_TPC=0` | a pool of sub-interpreters behind a multi-thread Tokio server |
| `isolated / tpc` | `--runtime isolated` | the same sub-interpreters; extensions that cannot load in one get a private copy per worker. This app loads none, so it measures the mode's overhead only |
| `process start=import / tpc` | `--runtime process` | worker processes (process lanes); each worker imports the app |
| `process start=fork / tpc` | `--runtime process --process-start fork` | worker processes forked from a template that imported the app |
| `process start=fork / multi-thread` | `--runtime process --process-start fork`, `MIPYRE_TPC=0` | the same forked workers behind the multi-thread server |
| `main / tpc` | `--runtime main` | the server's main interpreter (one GIL) |
| `main / multi-thread` | `--runtime main`, `MIPYRE_TPC=0` | the main interpreter behind the multi-thread server |

`/numpy` is measured on `process` and `main` only: numpy does not load in a plain
sub-interpreter.

Every server's answers to all its routes are checked against the expected values before it
is measured; a server that answers wrongly is not measured, and what it answered is kept in
the results file under `refused`.

## How a run is made

For each repetition, for each mode, for each route:

1. wait until the machine is quiet: the 1-minute load average is below 1.5 and no build
   (`cargo`, `rustc`, `maturin`) or CI test run is in the process list;
2. start a fresh server with `taskset -c <server CPUs> python -m mipyre run app.py:app --runtime …`;
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
    --wrk-threads 8 --connections 128 --duration 10
python benchmarks/public/run.py table results.jsonl
```

`measure` appends one JSON line per run (with the raw `wrk` output and the start of the
server's log); `table` prints the median and min–max of every cell, and every run that was
not measured with the reason.

## Results

Measured 2026-10-21, 00:34–03:56 EDT.

- Code: mipyre at commit `230abefd` (the benchmark ran from `6d2c51b0`, which adds only
  `benchmarks/public/` and its test on top of it); Python 3.14.7, numpy 2.5.4.
- Machine: AMD Ryzen 7 7840HS, 8 cores / 16 threads (SMT siblings are CPU n and n+8),
  Linux 7.0.0-38-generic.
- Core split: server on CPUs 0-3,8-11 (4 physical cores with their SMT siblings), `wrk` on
  CPUs 4-7,12-15; everything under `nice -n 10`.
- Load: the 1-minute load average at the start of each run was 1.16–1.49.
- Tool: `wrk` 4.1.0, `-t 8 -c 128 -d 10s --latency`, after a 2 s warm-up, against a fresh
  server per run; default mipyre settings otherwise.
- Each cell: median (min–max) of 3 repetitions. Errors are the sum over the 3 runs of socket
  errors and non-2xx answers.

| mode / server | scenario | requests/s | p50 ms | p99 ms | server cores used | client cores used | errors | reps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| subinterp / tpc | plaintext | 470,727 (467,268–475,335) | 0.26 (0.26–0.30) | 0.43 (0.39–0.47) | 4.0 (4.0–4.0) | 5.4 (5.3–5.4) | 0 | 3 |
| subinterp / tpc | json | 466,825 (462,188–468,631) | 0.27 (0.27–0.28) | 0.39 (0.38–0.41) | 4.0 (4.0–4.0) | 5.3 (5.3–5.4) | 0 | 3 |
| subinterp / tpc | cpu | 1,702 (1,700–1,705) | 71.88 (70.21–71.98) | 99.82 (94.59–103.72) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| subinterp / multi-thread | plaintext | 426,442 (421,938–428,242) | 0.29 (0.29–0.29) | 0.66 (0.65–0.66) | 7.6 (7.6–7.7) | 4.9 (4.9–5.0) | 0 | 3 |
| subinterp / multi-thread | json | 422,226 (421,182–427,118) | 0.29 (0.29–0.29) | 0.68 (0.67–0.68) | 7.6 (7.5–7.6) | 4.9 (4.8–4.9) | 0 | 3 |
| subinterp / multi-thread | cpu | 1,978 (1,972–1,989) | 64.74 (64.01–64.97) | 66.04 (65.96–66.13) | 8.0 (8.0–8.0) | 0.1 (0.1–0.1) | 0 | 3 |
| isolated / tpc | plaintext | 475,376 (470,812–477,535) | 0.28 (0.26–0.29) | 0.44 (0.38–0.54) | 4.0 (4.0–4.0) | 5.4 (5.4–5.4) | 0 | 3 |
| isolated / tpc | json | 463,833 (461,611–464,122) | 0.25 (0.25–0.27) | 0.48 (0.38–0.51) | 4.0 (4.0–4.0) | 5.3 (5.2–5.3) | 0 | 3 |
| isolated / tpc | cpu | 1,698 (1,650–1,705) | 69.48 (64.26–74.67) | 101.14 (80.86–109.47) | 4.0 (4.0–4.0) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | plaintext | 406,171 (405,401–408,797) | 0.31 (0.30–0.33) | 0.43 (0.43–0.44) | 5.8 (5.8–5.8) | 4.6 (4.6–4.6) | 0 | 3 |
| process start=import / tpc | json | 403,880 (401,323–404,159) | 0.32 (0.31–0.32) | 0.45 (0.43–0.49) | 5.8 (5.8–5.8) | 4.6 (4.6–4.6) | 0 | 3 |
| process start=import / tpc | cpu | 1,672 (1,657–1,675) | 79.67 (73.08–82.19) | 90.92 (88.45–93.59) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=import / tpc | numpy | 325,601 (324,261–326,361) | 0.39 (0.37–0.40) | 0.60 (0.56–0.81) | 7.2 (7.2–7.3) | 3.8 (3.8–3.8) | 0 | 3 |
| process start=fork / tpc | plaintext | 407,616 (404,342–408,622) | 0.31 (0.30–0.32) | 0.43 (0.43–0.51) | 5.8 (5.8–5.8) | 4.6 (4.5–4.7) | 0 | 3 |
| process start=fork / tpc | json | 403,105 (402,538–404,389) | 0.30 (0.29–0.30) | 0.45 (0.45–0.49) | 5.8 (5.8–5.8) | 4.6 (4.6–4.6) | 0 | 3 |
| process start=fork / tpc | cpu | 1,664 (1,657–1,674) | 80.81 (77.17–84.09) | 92.30 (87.84–102.89) | 4.1 (4.1–4.1) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / tpc | numpy | 325,662 (325,396–330,708) | 0.38 (0.38–0.39) | 0.59 (0.59–0.62) | 7.3 (7.3–7.3) | 3.8 (3.8–3.9) | 0 | 3 |
| process start=fork / multi-thread | plaintext | 288,316 (285,569–288,894) | 0.41 (0.41–0.41) | 1.14 (1.13–1.17) | 6.8 (6.7–6.8) | 3.2 (3.2–3.2) | 0 | 3 |
| process start=fork / multi-thread | json | 281,437 (277,026–282,259) | 0.43 (0.42–0.43) | 1.16 (1.15–1.20) | 6.7 (6.7–6.8) | 3.1 (3.1–3.1) | 0 | 3 |
| process start=fork / multi-thread | cpu | 1,661 (1,631–1,666) | 67.77 (64.48–80.05) | 153.85 (149.66–178.01) | 4.2 (4.2–4.2) | 0.1 (0.1–0.1) | 0 | 3 |
| process start=fork / multi-thread | numpy | 217,831 (214,347–217,883) | 0.56 (0.56–0.57) | 1.51 (1.49–1.54) | 6.7 (6.7–6.7) | 2.5 (2.4–2.5) | 0 | 3 |
| main / tpc | plaintext | 259,531 (255,944–262,748) | 0.48 (0.47–0.49) | 0.86 (0.85–0.91) | 4.0 (4.0–4.0) | 2.9 (2.8–2.9) | 0 | 3 |
| main / tpc | json | 240,431 (240,129–244,632) | 0.51 (0.50–0.53) | 1.09 (1.03–1.13) | 4.0 (4.0–4.0) | 2.7 (2.7–2.7) | 0 | 3 |
| main / tpc | cpu | 414 (412–415) | 271.12 (262.94–274.00) | 1,310.00 (1,260.00–1,360.00) | 1.1 (1.1–1.1) | 0.0 (0.0–0.0) | 11 | 3 |
| main / tpc | numpy | 77,185 (76,880–78,536) | 1.63 (1.60–1.63) | 7.23 (7.03–7.26) | 3.1 (3.0–3.1) | 0.9 (0.9–0.9) | 0 | 3 |
| main / multi-thread | plaintext | 168,434 (167,565–168,469) | 0.75 (0.74–0.75) | 3.55 (3.25–3.55) | 5.8 (5.6–5.8) | 1.9 (1.9–1.9) | 0 | 3 |
| main / multi-thread | json | 166,558 (165,955–166,657) | 0.75 (0.75–0.75) | 3.39 (3.37–3.46) | 5.5 (5.5–5.5) | 1.9 (1.9–1.9) | 0 | 3 |
| main / multi-thread | cpu | 359 (358–364) | 209.98 (143.73–230.21) | 1,680.00 (1,660.00–1,710.00) | 1.2 (1.2–1.2) | 0.0 (0.0–0.0) | 215 | 3 |

Reading the table:

- `/cpu` is the parallelism test. On the sub-interpreter and process modes it runs on every
  core the server has: about 1,700 requests/s on 4 server cores, and about 2,000 on the
  multi-thread server's 8 sub-interpreters, which also use the SMT siblings. On `main` it
  is serial: about 400 requests/s on 1.1 cores, whichever server is in front.
- The errors on `main` `/cpu` are all `wrk` timeouts (2 s): 128 connections queue behind one
  interpreter lock, so the slowest requests wait longer than that. No mode answered a request
  with a non-2xx status.
- In `process` mode every route, including `/plaintext`, crosses to a worker process, so the
  server uses more CPU per request than in `subinterp` mode; `start=import` and `start=fork`
  differ only in how a worker starts, which this benchmark does not measure, and serve
  requests at the same speed.
- `/numpy` runs in parallel in `process` mode (about 326,000 requests/s) and on one interpreter
  lock in `main` mode (about 77,000).
