"""A Rust panic in a worker API call must reach Python as a normal exception.

The async engine in a sub-interpreter worker pulls requests and sends responses
through `mipyre.engine._worker_recv` / `_worker_send` (`crates/runtime/src/worker_api.rs`),
PyO3 `#[pyfunction]`s. PyO3 would turn a panic
into `PanicException`, a `BaseException` the engine's fetcher thread (which catches
`Exception`) would die on, so each one maps a panic to `RuntimeError` instead.

Structural test: no raw `extern "C"` entry point is left in `crates/runtime/src/` or its
`exchange/`. That every worker API function goes through the mapping is not read from
worker_api.rs's text here: each is written through `worker_api_function!`
(`crates/runtime/src/worker_api/guard.rs`), which runs its body through `no_panic`.

The behavior is unit-tested beside the calls (`runtime::worker_api` tests): a panic in a call that answers no request comes out as the
engine's RuntimeError subclass `_EnginePanic`, never PanicException, carrying the request error
unlogged; a panic in a call that answers a request fails that request once, naming the call.
"""

import pathlib


_SRC = pathlib.Path("crates/runtime/src")
_EXCHANGE = pathlib.Path("crates/runtime/src/exchange")


def test_no_extern_c_entry_points_left():
    for path in [*_SRC.glob("*.rs"), *_EXCHANGE.glob("*.rs")]:
        code = "\n".join(line.split("//", 1)[0] for line in path.read_text().splitlines())
        assert 'extern "C" fn' not in code, (
            f"{path}: a raw extern \"C\" entry point is back; write it with worker_api_function! "
            "in worker_api.rs instead"
        )
