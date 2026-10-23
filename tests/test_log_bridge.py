"""``mipyre._log.bridge``: a sink that fails is handed to ``logging``'s own ``handleError``,
and the code that logged goes on (b/393; the bridge once swallowed it with ``except: pass``);
the level gate is the handler's, and the user's root logger is only ever lowered."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import textwrap

import pytest

from mipyre._log.bridge import RustLogHandler, handler_level, root_level


def test_a_sink_that_raises_goes_to_handle_error_and_the_caller_goes_on(capsys, monkeypatch):
    def sink(levelno, name, message, pathname, lineno, worker_id, lane):
        raise RuntimeError("the sink is gone")

    # `handleError` reports only while `raiseExceptions` is on (logging's default).
    monkeypatch.setattr(logging, "raiseExceptions", True)
    logger = logging.getLogger("tests.log_bridge.raising_sink")
    handler = RustLogHandler(worker_id=None, emit=sink)
    logger.addHandler(handler)
    monkeypatch.setattr(logger, "propagate", False)
    monkeypatch.setattr(logger, "level", logging.INFO)
    went_on = False
    try:
        logger.info("request %s served", "r-1")
        went_on = True
    finally:
        logger.removeHandler(handler)

    assert went_on
    err = capsys.readouterr().err
    assert "--- Logging error ---" in err, err
    assert "RuntimeError: the sink is gone" in err, err
    # The record that could not be emitted, as it was logged.
    assert "Message: 'request %s served'" in err and "Arguments: ('r-1',)" in err, err


def test_the_handler_takes_what_the_engine_logs_and_everything_before_it_has_a_level():
    assert handler_level(logging.ERROR) == logging.ERROR
    assert handler_level(None) == logging.DEBUG


@pytest.mark.parametrize(
    "current,engine_level,expected",
    [
        # The engine logs less than the user wants: the user's level stays.
        (logging.INFO, logging.ERROR, logging.INFO),
        (logging.WARNING, logging.WARNING, logging.WARNING),
        # The engine logs more: lowered so its records reach the handler.
        (logging.WARNING, logging.INFO, logging.INFO),
        (logging.WARNING, None, logging.DEBUG),
        # NOTSET on the root lets every record through; it stays so.
        (logging.NOTSET, logging.ERROR, logging.NOTSET),
    ],
)
def test_the_root_level_is_lowered_never_raised(current, engine_level, expected):
    assert root_level(current, engine_level) == expected


def test_serving_does_not_raise_the_users_root_logger_level(tmp_path):
    """The user's root logger is at INFO with their own handler (Sentry, a file): after the
    server starts at the production level (ERROR), a warning from a handler still reaches it."""
    script = tmp_path / "app_script.py"
    script.write_text(textwrap.dedent("""
        import logging

        seen = []

        class Keep(logging.Handler):
            def emit(self, record):
                seen.append(record.getMessage())

        logging.getLogger().addHandler(Keep())
        logging.getLogger().setLevel(logging.INFO)

        from mipyre import Mipyre
        from mipyre.testing import TestClient

        app = Mipyre(runtime="main")

        @app.get("/x")
        def x(req):
            logging.getLogger("user").warning("user-warning")
            return "ok"

        with TestClient(app) as c:
            c.get("/x")
        print("ROOT", logging.getLevelName(logging.getLogger().level), "SEEN", seen)
    """))
    env = {k: v for k, v in os.environ.items() if not k.startswith("MIPYRE_")}
    proc = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, env=env, timeout=120, cwd=tmp_path
    )
    out = [line for line in proc.stdout.splitlines() if line.startswith("ROOT")]
    assert out, proc.stderr[-2000:]
    assert "user-warning" in out[0], out[0]
