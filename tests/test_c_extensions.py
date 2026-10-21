"""A C extension that can't load in a sub-interpreter (numpy) stops the start with
CPython's own ImportError, the user's line that imported it, and the two fixes (design §8).
mipyre does not copy or patch extensions to force them into sub-interpreters: routes
that use them run in a process (runtime="process")."""

import os
import subprocess
import sys
import textwrap

import pytest


def test_numpy_at_the_top_of_a_sub_interpreter_app_is_a_start_error_saying_what_to_do(tmp_path):
    pytest.importorskip("numpy")
    app = tmp_path / "numpy_subinterp_app.py"
    app.write_text(textwrap.dedent("""\
        from mipyre import Mipyre
        import numpy as np

        app = Mipyre()


        @app.get("/mean")
        def mean(req):
            return {"v": float(np.mean([1.0, 2.0]))}
        """))
    env = dict(os.environ, MIPYRE_WORKERS="2")
    r = subprocess.run([sys.executable, "-m", "mipyre", "run", f"{app}:app", "--port", "0"],
                       capture_output=True, text=True, timeout=120, env=env, cwd=tmp_path)
    out = r.stdout + r.stderr
    assert r.returncode != 0, out[-3000:]
    assert "Listening on" not in out
    # CPython's (or numpy's) own words, kept.
    assert "ImportError" in out, out[-3000:]
    # Which library, where the user imported it, and the two fixes.
    assert "numpy can't load in a sub-interpreter" in out, out[-3000:]
    assert f"{app}:2  import numpy as np" in out, out[-3000:]
    assert "Router(runtime='process')" in out and "inside those routes' functions" in out
