"""Bounded, supervised SQL reads; SQLite's heap limit belongs only to the child.

The store owners supply lifecycle admission and the Drive name locks. Their
transaction stays open while a separate mode=ro connection executes SQL. Money
defaults to VACUUM INTO a disposable snapshot, checked before any user query.
The parent owns the temporary directory and reaps the child even on timeout.
"""
from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading

from ouroboros import store

MAX_BYTES = 8 * 1024 * 1024
HEAP_BYTES = 134217728
PRAGMAS = {"table_info", "table_xinfo", "table_list", "index_list", "index_info"}


def packs_index_path(root: Path) -> Path:
    # Integration replaces this locator with the packing module's constant.
    return root / "observability" / "packs" / "index.sqlite"


def database_path(root: Path, db: str) -> Path:
    return {"current": store.store_path(root), "money": root / "state" / "usage.sqlite",
            "packs": packs_index_path(root)}[db]


@contextlib.contextmanager
def _source(root, db, seconds):
    if db == "current":
        with store.open_store(root, write=False, wait_sec=seconds) as txn:
            if txn.phase() != store.PUBLISHED:
                raise store.StoreRetired("current store is not published")
            yield txn.meta("lock_tier") or store.TIER_ENFORCED
    elif db == "money":
        from ouroboros import usage_store
        with usage_store.hold(root, write=False, migrate=False, timeout_sec=seconds):
            yield usage_store.store_tier(root, wait_sec=seconds, migrate=False)
    else:
        # The pack index shares the root's tier; serialize name-tier index reads.
        tier = store.probe(root)[1] or store.TIER_ENFORCED
        guard = (store.name_lock(root, "packs.lock", timeout_sec=seconds)
                 if tier == store.TIER_NAME else contextlib.nullcontext())
        with guard:
            yield tier


@contextlib.contextmanager
def _connection(path, tier, seconds):
    uri = path.as_uri() + "?mode=ro"
    if tier == store.TIER_NAME:
        uri += "&vfs=" + ("win32-none" if os.name == "nt" else "unix-none")
    conn = sqlite3.connect(uri, uri=True, timeout=seconds, isolation_level=None)
    timer = threading.Timer(seconds, conn.interrupt)
    try:
        conn.execute(f"PRAGMA hard_heap_limit={HEAP_BYTES}").close()
        timer.start()
        yield conn
    finally:
        timer.cancel()
        if timer.ident is not None:
            timer.join()
        conn.close()


def _authorizer(db):
    def authorize(action, first, second, database, origin):
        allowed = action in {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE}
        if action == sqlite3.SQLITE_READ:
            allowed = db != "current" or first in {"tasks", "sqlite_master", "sqlite_schema"}
        elif action == sqlite3.SQLITE_FUNCTION:
            allowed = str(second).lower() != "load_extension"
        elif action == sqlite3.SQLITE_PRAGMA:
            allowed = str(first).lower() in PRAGMAS
        return sqlite3.SQLITE_OK if allowed else sqlite3.SQLITE_DENY
    return authorize


def _fetch(conn, request):
    conn.execute("PRAGMA query_only=ON").close()
    conn.set_authorizer(_authorizer(request["db"]))
    cursor = conn.cursor()
    try:
        cursor.execute(request["query"])
        result = {"columns": [col[0] for col in cursor.description or ()],
                  "rows": [], "truncated": False}
        used = len(json.dumps(result, ensure_ascii=False).encode("utf-8")) + 32
        if used > request["max_bytes"]:
            raise ValueError("byte_limit: column names exceed max_bytes")
        for row in cursor:
            values = [{"hex": value.hex()} if isinstance(value, bytes) else value for value in row]
            size = len(json.dumps(values, ensure_ascii=False).encode("utf-8")) + 2
            if len(result["rows"]) >= request["max_rows"] or used + size > request["max_bytes"]:
                result["truncated"] = True
                break
            result["rows"].append(values)
            used += size
        return result
    finally:
        cursor.close()
        conn.set_authorizer(None)


def _execute(request):
    def fetch_query(conn):
        try:
            return _fetch(conn, request)
        except (sqlite3.Error, sqlite3.Warning) as exc:
            # User SQL must not cross admission as a store-health failure.
            raise ValueError(str(exc)) from exc

    root, db = Path(request["root"]), request["db"]
    path, seconds = database_path(root, db), request["timeout_sec"]
    snapshot = request["snapshot"]
    with _source(root, db, seconds) as tier:
        with _connection(path, tier, seconds) as conn:
            if not snapshot:
                return fetch_query(conn)
            target = Path(request["scratch"]) / "snapshot.sqlite"
            # Trusted SQL only: user SQL never runs on this connection.
            conn.execute("VACUUM INTO ?", (str(target),)).close()
    with _connection(target, store.TIER_ENFORCED, seconds) as conn:
        cursor = conn.execute("PRAGMA quick_check")
        try:
            if cursor.fetchall() != [("ok",)]:
                raise ValueError("snapshot quick_check failed")
        finally:
            cursor.close()
        return fetch_query(conn)


def run_sql(ctx, root, *, db, query, max_rows, max_bytes, snapshot, timeout_sec):
    """Only host-resolved sources cross the process boundary, never an SQL path."""
    from ouroboros.process_containment import CONTAINMENT_ENV_PREFIX
    from ouroboros.process_custody import current_custody_session_id, spawn_supervised

    repo = Path(__file__).resolve().parents[1]
    env = {key: value for key, value in os.environ.items()
           if key in {"PATH", "HOME", "USERPROFILE", "SystemRoot", "WINDIR", "TEMP", "TMP", "TMPDIR",
                      "LANG", "LC_ALL", "OUROBOROS_SETTINGS_PATH"}
           or key.startswith(CONTAINMENT_ENV_PREFIX)}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(repo),
               OUROBOROS_DATA_DIR=str(root), OUROBOROS_REPO_DIR=str(repo))
    with tempfile.TemporaryDirectory(prefix="query-data-") as scratch:
        request = dict(root=str(root), db=db, query=query, max_rows=max_rows, max_bytes=max_bytes,
                       snapshot=snapshot, timeout_sec=timeout_sec, scratch=scratch)
        proc = spawn_supervised(
            [sys.executable, "-m", "ouroboros.query_data_sql", current_custody_session_id()],
            drive_root=Path(ctx.drive_root), purpose="query_data", scope="task",
            owner_task_id=str(ctx.task_id or ""), cwd=str(repo), env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            raw, _ = proc.communicate(json.dumps(request).encode("utf-8"), timeout=timeout_sec + 5)
            if proc.returncode or len(raw) > max_bytes + 4096:
                return {"status": "error", "code": "executor_failed"}
            return json.loads(raw)
        except subprocess.TimeoutExpired:
            return {"status": "error", "code": "deadline"}
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            for stream in (proc.stdin, proc.stdout):
                stream.close()


def main():
    from ouroboros.process_custody import adopt_session_id, start_parent_lifeline
    adopt_session_id(sys.argv[1])
    start_parent_lifeline(label="query_data")
    try:
        result = {"status": "ok", "data": _execute(json.load(sys.stdin))}
    except MemoryError:
        result = {"status": "error", "code": "memory_limit", "exception_class": "MemoryError"}
    except (sqlite3.Error, OSError, ValueError, RuntimeError) as exc:
        result = {"status": "error", "code": "query_failed", "exception_class": type(exc).__name__,
                  "message": str(exc)}
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
