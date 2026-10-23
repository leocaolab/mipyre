# Compatibility: libraries, sub-interpreters and PyO3

mipyre runs your handlers in sub-interpreters by default, and an extension module can only be imported there if it says it supports that. This page says which libraries load, how to check one yourself, and why mipyre is built against a fork of PyO3.

## Which libraries load in a sub-interpreter

mipyre loads extensions in its sub-interpreters exactly as CPython does. An extension that declares multi-interpreter support (`Py_mod_multiple_interpreters` with a per-interpreter GIL) loads; any other fails with CPython's own `ImportError` (`module X does not support loading in subinterpreters`). When that happens at startup, the server's error names the library, the line that imported it, and the ways to serve those routes:

```text
numpy can't load in a sub-interpreter; imported at /srv/app/app.py:1  import numpy as np
Routes that use it need one of:
  1. runtime='process': they run in worker processes, where numpy loads as anywhere else. ...
  2. runtime='main': they run on the main interpreter (one GIL; for few routes).
  3. runtime='isolated': each sub-interpreter loads its own copy of numpy with CPython's multi-interpreter check off ...
```

Measured on 2026-10-15 with Python 3.14.7 on macOS arm64, by importing each library in a fresh `concurrent.interpreters` sub-interpreter:

| Library | In a sub-interpreter | Runtime to use |
|---|---|---|
| the standard library (`json`, `sqlite3`, `zlib`, `ssl`, `hashlib`, ...) | loads | `subinterp` |
| `httpx` (pure Python) | loads | `subinterp` |
| `msgpack` 1.x | loads | `subinterp` |
| numpy 2.5 | `ImportError` (`numpy._core._multiarray_umath does not support loading in subinterpreters`) | `process` |
| pandas 3.0, scipy | `ImportError` (through numpy) | `process` |
| pyarrow | `ImportError` (`pyarrow.lib`) | `process` |
| orjson | `ImportError` (`orjson.orjson`) | `process` |
| pydantic / pydantic_core | `ImportError` (`pydantic_core._pydantic_core`) | `process` or `main` |
| polars 2.0 (PyPI wheel) | the import succeeds with a warning ("Polars binary is missing!": polars catches its binary's `ImportError`), and the first use fails (`NameError: name 'PySeries' is not defined`) | `process` |

Not measured (they weren't installed): scikit-learn, lxml, pillow, cryptography, psycopg, PyYAML, ujson. Check them yourself as below.

mipyre's own engine and its JSON serializer, isojson, declare multi-interpreter support and load in every sub-interpreter. Pure-Python packages load as long as everything they import does.

### Check a library yourself

Run this with the Python 3.14 you serve with, in the environment your app uses:

```python
# check_subinterp.py
import sys
from concurrent import interpreters


def loads_in_a_subinterpreter(module: str) -> bool:
    interp = interpreters.create()
    try:
        interp.exec(f"import {module}")
        return True
    except interpreters.ExecutionFailed as e:
        lines = [line for line in str(e).splitlines() if line.strip()]
        reason = next((line for line in lines if "subinterpreters" in line), lines[-1])
        print(f"  {reason.strip()}")
        return False
    finally:
        interp.close()


for name in sys.argv[1:]:
    print(name, "loads" if loads_in_a_subinterpreter(name) else "does not load")
```

```bash
python check_subinterp.py json numpy
```

```text
json loads
  Original error was: module numpy._core._multiarray_umath does not support loading in subinterpreters
numpy does not load
```

A library that imports but then fails on first use (as the polars wheel does) passes this check, so also call something from it inside `interp.exec(...)`.

## The PyO3 fork

The mipyre engine is a Rust extension written with [PyO3](https://github.com/PyO3/pyo3). Upstream PyO3 keeps its type objects, module objects and caches in process-wide statics, so it refuses to load a module in a second interpreter that has its own GIL. That is why PyO3-based libraries such as pydantic-core and polars don't load in a sub-interpreter.

mipyre is built against [leocaolab/pyo3](https://github.com/leocaolab/pyo3), a fork that moves that state per interpreter: `#[pyclass]` and exception types, module objects, the `PyOnceLock` and `intern!` caches, and the deferred-decref pool. Its modules declare per-interpreter GIL support, and they load in strict sub-interpreters. mipyre's `Cargo.toml` pins the fork with `[patch.crates-io]` to a git tag, currently `subinterp-2026-10-16.5`; the crate version stays `0.29.2`, so the patch applies to every crate that depends on `pyo3 = "0.29"`.

The fork is experimental. Its design and measurements are posted upstream on [PyO3#3451](https://github.com/PyO3/pyo3/issues/3451). Each defect it fixed is listed in its [bug ledger](https://github.com/leocaolab/pyo3/blob/main/SUBINTERP-FIXES.md#bug-ledger) with the test that fails without the fix, and its CI runs them on every push.

What it means for you:

- **Your own PyO3 extension** loads in mipyre's sub-interpreters only if it is built against the fork. Add the same `[patch.crates-io]` section to your extension's `Cargo.toml`:

  ```toml
  [patch.crates-io]
  pyo3 = { git = "https://github.com/leocaolab/pyo3", tag = "subinterp-2026-10-22.1" }
  pyo3-ffi = { git = "https://github.com/leocaolab/pyo3", tag = "subinterp-2026-10-22.1" }
  pyo3-macros = { git = "https://github.com/leocaolab/pyo3", tag = "subinterp-2026-10-22.1" }
  pyo3-macros-backend = { git = "https://github.com/leocaolab/pyo3", tag = "subinterp-2026-10-22.1" }
  pyo3-build-config = { git = "https://github.com/leocaolab/pyo3", tag = "subinterp-2026-10-22.1" }
  ```

  Built against upstream PyO3, it can still run on `process` or `main`.
- **pydantic-core and polars from PyPI** are built against upstream PyO3, so they don't load in a sub-interpreter: use `process` (or `main` for pydantic). polars built from source against the fork does load in sub-interpreters.
