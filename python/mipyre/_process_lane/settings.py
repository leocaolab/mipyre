"""The process lanes' settings as the app's code gives them (docs/process-lanes-declared-design.md)."""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class ProcessLanes:
    """Settings of the worker processes ``runtime="process"`` routes run on, as the app's code
    gives them. A deployment value wins over each (``mipyre run --process-workers``, else
    ``MIPYRE_PROCESS_WORKERS`` / ``_TIMEOUT`` / ``_QUEUE`` / ``_SLOT_BYTES``, else
    ``[tool.mipyre.process]``: ``workers``, ``timeout``, ``queue_per_worker``,
    ``slot_bytes``); one nobody gives is the default: one worker per physical core, a 30 s
    handler timeout (in seconds wherever it is written; the worker is killed, 504), 128
    requests per worker before 503, 256 KiB per request and per answer.

    ``host_failure``: what a failed host or lane does (``MIPYRE_LANE_HOST_FAILURE``,
    ``host_failure`` in the table). ``"isolate"`` (default): the failed part is taken out of
    service, the other hosts keep serving, and the server ends only once no lane is left.
    ``"exit"``: the server ends at the first one, as ``panic_on_oops`` does. Either way the
    server ends with the first cause, and the process exits non-zero (systemd restarts it).

    ``start``: how a worker gets the app (``MIPYRE_PROCESS_START``, ``start`` in the table).
    ``"import"`` (default): it starts without the app and imports it itself — safe whatever
    the app's libraries do at import. ``"fork"``: a template imports the app, then forks each
    worker — faster to replace and less memory, for apps that replace workers often; the
    template must be single-threaded when it forks, or the start fails naming the threads."""

    start: str | None = None
    workers: int | None = None
    timeout: float | None = None
    queue_per_worker: int | None = None
    slot_bytes: int | None = None
