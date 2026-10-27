"""The app's logging settings, resolved from ``debug=`` / ``log_config=`` and turned on by
``enable_access_log()`` or the launch (``MIPYRE_LOG=1``), without starting a server."""

import pytest

from mipyre import LogLevel, Mipyre
from mipyre._log.settings import _AccessLog, _LogSettings


def test_debug_and_production_defaults():
    assert _LogSettings.resolve(True, {}) == _LogSettings(LogLevel.Debug, True, "text")
    assert _LogSettings.resolve(False, {}) == _LogSettings(LogLevel.Warn, False, "json")


def test_the_user_config_overrides_the_mode_defaults_and_its_level_is_parsed():
    settings = _LogSettings.resolve(False, {"level": "warn", "access_log": True, "format": "text"})
    assert settings == _LogSettings(LogLevel.Warn, True, "text")
    with pytest.raises(ValueError, match="loud"):
        _LogSettings.resolve(True, {"level": "loud"})


def test_asked_by_neither_the_user_nor_the_launch_the_settings_are_as_configured():
    configured = _LogSettings(LogLevel.Error, False, "json")
    assert configured.with_access_log(None, False) == configured


@pytest.mark.parametrize("hidden", [LogLevel.Warn, LogLevel.Error, LogLevel.Off])
@pytest.mark.parametrize(
    "choice,launch_log", [(_AccessLog(None), False), (None, True)], ids=["user", "launch"]
)
def test_the_access_log_raises_a_level_that_would_hide_it_to_info(hidden, choice, launch_log):
    settings = _LogSettings(hidden, False, "json").with_access_log(choice, launch_log)
    assert (settings.level, settings.access_log) == (LogLevel.Info, True)


def test_the_access_log_keeps_a_level_that_shows_it():
    configured = _LogSettings(LogLevel.Info, False, "json")
    assert configured.with_access_log(_AccessLog(None), False).level == LogLevel.Info


@pytest.mark.parametrize("launch_log", [False, True])
def test_the_level_the_user_gave_wins_over_the_configured_one_and_the_launch(launch_log):
    settings = _LogSettings(LogLevel.Info, False, "json").with_access_log(
        _AccessLog(LogLevel.Error), launch_log
    )
    assert (settings.level, settings.access_log) == (LogLevel.Error, True)


def test_an_explicit_level_is_kept_by_a_later_call_without_one():
    app = Mipyre()
    app.enable_access_log(level="error")
    app.enable_access_log()
    assert app._access_log == _AccessLog(LogLevel.Error)


def test_a_bad_call_leaves_the_users_choice_as_it_was():
    app = Mipyre()
    app.enable_access_log(level="warn")
    with pytest.raises(ValueError):
        app.enable_access_log(level="error", sample=0)
    assert app._access_log == _AccessLog(LogLevel.Warn)
