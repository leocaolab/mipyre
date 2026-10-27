"""Pydantic-settings base for Mipyre apps.

Thin wrapper that adds Mipyre-friendly defaults so user code stays short::

    from mipyre.config import Settings

    class Settings(Settings):
        database_url: str
        log_level: str = "INFO"

    settings = Settings()  # reads env + .env by default

Defaults:
- ``env_file = ".env"`` — loaded if present, ignored otherwise.
- ``env_file_encoding = "utf-8"``.
- ``extra = "ignore"`` — unknown env vars don't crash the app.
- ``case_sensitive = False`` — ``DATABASE_URL`` and ``database_url``
  both populate the ``database_url`` field.

Subclasses override any of these via the usual ``model_config``::

    class Settings(Settings):
        model_config = SettingsConfigDict(env_prefix="APP_")
        port: int = 8000

``pydantic-settings`` is an optional dependency: importing this module without it raises
``ImportError`` saying how to install it.
"""

from __future__ import annotations

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError as e:  # pragma: no cover — clear install guidance
    raise ImportError(
        "mipyre.config requires pydantic-settings. Install with:\n"
        "    pip install 'pydantic-settings>=2'"
    ) from e


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


__all__ = ["Settings"]
