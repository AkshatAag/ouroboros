"""Storage operation counts and phase timings in the existing event journal."""

import contextvars
import functools
import logging
import pathlib
import sys
import time
from contextlib import contextmanager

from ouroboros.utils import append_jsonl, utc_now_iso

_OBSERVATION = contextvars.ContextVar("storage_observation", default=None)


def storage_count(name, amount=1):
    row = _OBSERVATION.get()
    if row is not None:
        row["operations"][name] = row["operations"].get(name, 0) + amount


@contextmanager
def storage_phase(name):
    row, started = _OBSERVATION.get(), time.monotonic()
    try:
        yield
    finally:
        if row is not None:
            phase = row["phases"].setdefault(name, {"calls": 0, "seconds": 0.0})
            phase["calls"] += 1
            phase["seconds"] += time.monotonic() - started


def storage_observed(function):
    """Instrument every caller, including calls outside a model timing scope.

    Nested operations keep independent counters; telemetry cannot change the
    read's result or exception. No bodies, paths or task text enter the event.
    """
    @functools.wraps(function)
    def observed(*args, **kwargs):
        caller = sys._getframe(1)
        row = {"type": "storage_read_timing", "operation": function.__name__, "calls": 1,
               "caller": f"{caller.f_globals.get('__name__', '')}:{caller.f_code.co_name}",
               "phases": {}, "operations": {}, "failed": False}
        del caller
        token, started = _OBSERVATION.set(row), time.monotonic()
        try:
            return function(*args, **kwargs)
        except BaseException:
            row["failed"] = True
            raise
        finally:
            _OBSERVATION.reset(token)
            row.update(ts=utc_now_iso(), seconds=time.monotonic() - started)
            root = args[0] if args else kwargs.get("drive_root", kwargs.get("env"))
            root = getattr(root, "drive_root", root)
            if root is not None:
                try:
                    append_jsonl(pathlib.Path(root) / "logs" / "events.jsonl", row)
                except Exception:
                    logging.getLogger(__name__).debug("Storage timing unavailable", exc_info=True)
    return observed


def counted_result_paths(paths):
    paths = sorted(paths)
    storage_count("directory_scans")
    storage_count("entries", len(paths))
    for path in paths:
        storage_count("decode_attempts")
        yield path
