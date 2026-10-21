"""A Rust panic in a worker API call must reach Python as a normal exception.

The async engine in a sub-interpreter worker pulls requests and sends responses
through `mipyre.engine._worker_recv` / `_worker_send` (`src/runtime/worker_api.rs`).
They used to be `extern "C"` functions injected into worker globals, each wrapped in
`ffi_catch_unwind`, because a panic crossing `extern "C"` aborts the process (Rust
1.81+). Layer 2 (M4) replaced them with PyO3 `#[pyfunction]`s. PyO3 would turn a panic
into `PanicException`, a `BaseException` the engine's fetcher thread (which catches
`Exception`) would die on, so each one maps a panic to `RuntimeError` instead.

Structural test: no raw `extern "C"` entry point is left in `crates/pyext/src/runtime/` or
`exchange/` (`src/interp/` until b/229, `src/python/` until b/51 E8). (Rewritten for Layer 2 M4,
approved by the user on 2026-09-23.) That every worker API function goes through the mapping is no
longer read from worker_api.rs's text here (b/321): each is written through `worker_api_function!`
(`runtime/worker_api/guard.rs`), which runs its body through `no_panic`, and archcheck E-T10 refuses a
hand-written `#[pyfunction]` in worker_api.rs.

The behavior (b/149, the user's ruling of 2026-10-17) is unit-tested beside the calls
(`runtime::worker_api` tests, b/300): a panic in a call that answers no request comes out as the
engine's RuntimeError subclass `_EnginePanic`, never PanicException, carrying the request error
unlogged; a panic in a call that answers a request fails that request once, naming the call.
"""

import pathlib


_SRC = pathlib.Path("crates/pyext/src/runtime")
_EXCHANGE = pathlib.Path("crates/pyext/src/exchange")


def test_no_extern_c_entry_points_left():
    for path in [*_SRC.glob("*.rs"), *_EXCHANGE.glob("*.rs")]:
        code = "\n".join(line.split("//", 1)[0] for line in path.read_text().splitlines())
        assert 'extern "C" fn' not in code, (
            f"{path}: a raw extern \"C\" entry point is back; write it with worker_api_function! "
            "in worker_api.rs instead"
        )
