"""The logging settings of an app: the ``LogConfig`` a user passes, and the values a server applies."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal, TypedDict

from mipyre.engine import LogLevel


# A level by name, in either case; `LogLevel.parse` reads it.
LogLevelName = Literal[
    "off", "error", "warn", "warning", "info", "debug", "trace",
    "OFF", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE",
]


class LogConfig(TypedDict, total=False):
    """Logging configuration dictionary.

    Keys:
        level: a ``LogLevel``, or its name ("OFF", "ERROR", "WARN", "INFO", "DEBUG",
            "TRACE"); an unknown name raises ``ValueError`` in ``Mipyre()``
        access_log: Whether to log every HTTP request (method, path, status, latency)
        format: "text" (human-readable) or "json" (structured, for ELK/Datadog)
    """
    level: LogLevel | LogLevelName
    access_log: bool
    format: Literal["text", "json"]


def _parse_level(level: LogLevel | LogLevelName) -> LogLevel:
    """Return ``level`` as a ``LogLevel``; an unknown name raises ``ValueError``."""
    return level if isinstance(level, LogLevel) else LogLevel.parse(level)


@dataclass(frozen=True)
class _AccessLog:
    """The access log the user turned on with ``enable_access_log()``, at the level they
    gave it (``None``: no level given, the configured one stands)."""

    level: LogLevel | None


@dataclass(frozen=True)
class _LogSettings:
    """Every key of a ``LogConfig`` set and its level parsed."""

    level: LogLevel
    access_log: bool
    format: Literal["text", "json"]

    @classmethod
    def resolve(cls, debug: bool, user: LogConfig) -> _LogSettings:
        """Return the user's settings over the defaults of the mode: debug logs text with
        the access log at DEBUG, production logs JSON at ERROR without it."""
        if debug:
            return cls(
                level=_parse_level(user.get("level", LogLevel.Debug)),
                access_log=user.get("access_log", True),
                format=user.get("format", "text"),
            )
        return cls(
            level=_parse_level(user.get("level", LogLevel.Error)),
            access_log=user.get("access_log", False),
            format=user.get("format", "json"),
        )

    def with_access_log(self, choice: _AccessLog | None, launch_log: bool) -> _LogSettings:
        """Return the settings a server applies: these, with the access log on when the
        user's ``choice`` or the launch (``MIPYRE_LOG=1``, ``mipyre dev``) asks for it, at
        the level the choice gave, else at the configured one raised to INFO when that
        would hide the access lines (ERROR, OFF). Asked by neither, these as they are."""
        if choice is None and not launch_log:
            return self
        if choice is not None and choice.level is not None:
            level = choice.level
        elif self.level in (LogLevel.Error, LogLevel.Off):
            level = LogLevel.Info
        else:
            level = self.level
        return replace(self, level=level, access_log=True)
