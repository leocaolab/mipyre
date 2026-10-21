# Contributing to mipyre

Thanks for helping. Bug reports, feature requests and patches are all welcome.

## How this repository works

This repository is a release snapshot: each mipyre version is one commit, exported from the tree where development happens. A pull request is therefore not merged here directly. A maintainer carries the change into the development tree, where it goes through the same tests, performance checks and review as every other change, and it ships in the next release. The CHANGELOG entry for that release credits you by your GitHub handle.

## Reporting a bug

Open an issue with:

- the mipyre version (`python -c "import mipyre, importlib.metadata as m; print(m.version('mipyre'))"`), the Python version and the OS;
- the smallest app that shows the problem, and how you start it;
- what you expected, and what happened instead: the full traceback or log lines, not a summary.

## Reporting a security issue

Do not open a public issue. Use **Report a vulnerability** on this repository's Security tab, which reaches the maintainers privately.

## Sending a patch

1. Open an issue first for anything beyond a small fix, so the approach can be agreed before you write it.
2. Keep one change per pull request, with a test that fails before the change and passes after it.
3. Make sure the checks below pass.

By submitting a contribution you agree that it is licensed under the Apache License 2.0, as section 5 of the license states.

## Building from source

You need a stable Rust toolchain, Python 3.14 (the regular build with the GIL, not the free-threaded one) and [maturin](https://www.maturin.rs/).

```bash
python3.14 -m venv .venv
. .venv/bin/activate
pip install "maturin>=1.9.4,<2.0"
maturin develop --release -E test
```

## Running the checks

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
pytest tests -n auto
```

The database tests run only when `MIPYRE_TEST_PG_DSN` names a PostgreSQL database they may write to, for example `postgresql://postgres:postgres@127.0.0.1:5432/postgres`; without it they are skipped.
