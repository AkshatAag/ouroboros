"""Supervised maintenance packer; scheduling and storage protocol: docs/PERSISTENCE.md, evidence pack row."""
from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import logging
import os
import pathlib
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ouroboros import evidence_packs as packs
from ouroboros.startup_historical_audit import _REPORT_BYTES, read_job_report, spawn_maintenance_job

log = logging.getLogger(__name__)



def task_stamps(root: pathlib.Path, task_id: str, *, obligation_rows: Optional[dict] = None, txn=None) -> str:
    """Evidence and settlement-source revisions, including owner-held duties and outbox sends."""
    from ouroboros.artifacts import task_artifact_dir_path
    from ouroboros.task_store import signature
    from ouroboros.usage_store import STORE_REL
    from ouroboros.usage_ledger import LEDGER_REL

    if obligation_rows is None:
        obligation_rows = packs.settlement_obligations(root, txn=txn)
    stamps = [("task", signature(root, task_id, txn=txn)), ("obligations", obligation_rows.get(task_id, {}))]
    paths = [root / "observability" / "calls" / task_id,
                 task_artifact_dir_path(root, task_id, create=False).joinpath(*packs.CHECKPOINTS_REL.parts),
                 root / "state/terminal_deliveries.json",
                 root / STORE_REL, root / (str(STORE_REL) + "-wal"), root / LEDGER_REL]
    for path in paths:
        try:
            stat = path.stat()
            stamps.append((str(path.relative_to(root)), stat.st_ino, stat.st_size, stat.st_mtime_ns))
        except OSError:
            stamps.append((str(path.relative_to(root)), 0))
    return json.dumps(stamps, sort_keys=True, separators=(",", ":"))


def _loose_task_ids(root: pathlib.Path) -> set:
    ids = set()
    with contextlib.suppress(OSError):
        ids.update(entry.name for entry in os.scandir(root / "observability" / "calls") if entry.is_dir(follow_symlinks=False))
    with contextlib.suppress(OSError):
        for entry in os.scandir(root / "task_results" / "artifacts"):
            if entry.is_dir(follow_symlinks=False) and (pathlib.Path(entry.path) / "source_handles" / "context_checkpoints").is_dir():
                ids.add(entry.name)
    return ids


def swept_stamps(root: pathlib.Path) -> Optional[Dict[str, str]]:
    """Per task, the stamps the last sweep recorded; ``None`` when the cache is unavailable."""
    import sqlite3

    from ouroboros import store

    if not packs.packs_dir(root).is_dir():
        return {}
    try:
        with packs.open_index(root) as conn:
            return {row["task_id"]: row["stamps"] for row in conn.execute("SELECT task_id, stamps FROM swept")}
    except (store.StoreError, sqlite3.Error, OSError):
        return None


def record_swept(root: pathlib.Path, task_id: str, stamps: str) -> None:
    import sqlite3

    from ouroboros import store

    with contextlib.suppress(store.StoreError, sqlite3.Error, OSError):
        with packs.open_index(root, write=True) as conn:
            conn.execute("INSERT OR REPLACE INTO swept VALUES (?,?)", (task_id, stamps))


def changed_tasks(root: pathlib.Path, *, obligation_rows: Optional[dict] = None) -> List[str]:
    """Tasks with loose evidence whose directories or result changed since a sweep recorded them."""
    root = pathlib.Path(root)
    ids = _loose_task_ids(root)
    if ids and obligation_rows is None:
        obligation_rows = packs.settlement_obligations(root)
    swept = swept_stamps(root) if ids else {}
    return sorted(task for task in ids if swept is None or swept.get(task) !=
                  task_stamps(root, task, obligation_rows=obligation_rows))


def archive_pending(root: pathlib.Path) -> bool:
    directory = pathlib.Path(root) / packs.USAGE_ARCHIVE_REL
    return directory.is_dir() and any(directory.glob("segment_ep*.jsonl"))


def ordered_candidates(root: pathlib.Path, settled: List[str]) -> List[Tuple[str, int]]:
    """Settled tasks with their loose raw size: the ones whose result changed after the newest
    published pack first (newest result first), then the backfill by descending raw size."""
    from ouroboros.task_store import signature

    def timestamp(value):
        try:
            return datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            return 0

    newest_pack = max((timestamp(sidecar.get("created_at")) for _, sidecar in packs.list_sidecars(root)), default=0)
    recent, backfill = [], []
    for task in settled:
        raw = sum(int(m["size"]) for m in packs.loose_members(root, task))
        revision = signature(root, task)
        touched = timestamp(revision[1]) if revision else 0
        (recent if newest_pack and touched > newest_pack else backfill).append((task, raw, touched))
    recent.sort(key=lambda item: (-item[2], item[0]))
    backfill.sort(key=lambda item: (-item[1], item[0]))
    return [(task, raw) for task, raw, _ in recent + backfill]


def live_manifest_shas(root: pathlib.Path, *, obligation_rows: Optional[dict] = None, txn=None) -> set:
    """Shas any NON-settled task's manifests reference: those loose blobs stay."""
    keep: set = set()
    if obligation_rows is None:
        obligation_rows = packs.settlement_obligations(root, txn=txn)
    for task in _loose_task_ids(root):
        if not packs.task_settled(root, task, obligation_rows=obligation_rows, txn=txn):
            keep.update(sha256 for _, sha256, _ in packs.manifest_blob_refs(root, task))
    return keep



def run_pass(root: pathlib.Path, *, stop=lambda: False) -> Dict[str, Any]:
    """Pack every settled candidate one task at a time, then the money archive and the unpacker."""
    root = pathlib.Path(root)
    report: Dict[str, Any] = {"status": "completed", "packed": 0, "candidates": 0, "raw": 0, "compressed": 0,
                              "unlinked": 0, "kept": 0, "busy": 0, "faults": 0, "archived": 0}
    obligation_cache: dict = {}
    duties = packs.settlement_obligations(root, cache=obligation_cache)
    revisions = {task: task_stamps(root, task, obligation_rows=duties)
                 for task in changed_tasks(root, obligation_rows=duties)}
    facts = {task: packs.task_settlement_facts(root, task, obligation_rows=duties) for task in revisions}
    settled = {task for task, fact in facts.items() if packs.facts_settled(fact)}
    for task, fact in facts.items():
        if task not in settled and fact["money_open"] is False and "unknowns" not in fact["obligations"]:
            record_swept(root, task, revisions[task])
    candidates = ordered_candidates(root, sorted(settled))
    report["candidates"] = len(candidates)
    listing = packs.list_sidecars(root)
    for task, raw in candidates:
        if stop():
            report["status"] = "unknown"
            break
        try:
            outcome = packs.pack_task(root, task, keep_shas=set(), listing=listing, revision=revisions[task],
                                      obligation_cache=obligation_cache,
                                      record=lambda **facts: packs.record_storage_pack(root, **facts))
        except Exception as exc:
            report["faults"] += 1
            packs.record_storage_pack(root, phase="fault", task_id=task, exception_class=type(exc).__name__)
            log.warning("Evidence pack of %s failed", task, exc_info=True)
            continue
        report["packed"] += 1
        report["raw"] += int(outcome.get("raw") or 0)
        report["compressed"] += int(outcome.get("packed") or 0)
        for key in ("unlinked", "kept", "busy"):
            report[key] += int(outcome.get(key) or 0)
        if not any(outcome.get(key) for key in ("busy", "kept", "deferred")):
            record_swept(root, task, revisions[task])
    try:
        report["archived"] = packs.archive_usage_segments(root)["archived"]
    except Exception as exc:
        report["faults"] += 1
        packs.record_storage_pack(root, phase="fault", task_id="", exception_class=type(exc).__name__)
    return report



class EvidencePacker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._armed = False
        self._process = None
        self._thread: Optional[threading.Thread] = None

    def tick(self, data_root: pathlib.Path, repo_dir: pathlib.Path) -> bool:
        """Called from the maintenance sweep; spawns at most one child, never on the first tick."""
        with self._lock:
            if self._stopped.is_set() or (self._thread is not None and self._thread.is_alive()):
                return False
            if not self._armed:
                self._armed = True  # the boot tick only arms the packer
                return False
            root = pathlib.Path(data_root)
            try:
                if not changed_tasks(root) and not archive_pending(root):
                    return False
            except Exception:
                log.debug("Evidence pack candidate check failed", exc_info=True)
                return False
            self._thread = threading.Thread(target=self._run, args=(root, pathlib.Path(repo_dir)),
                                            name="evidence-packer", daemon=True)
            self._thread.start()
        return True

    def stop(self) -> None:
        self._stopped.set()
        process = self._process
        if process is not None:
            self._signal(process)

    def live_children(self) -> list:
        """Exit census after Stop: include an unfinished spawn until its owner returns."""
        process, thread = self._process, self._thread
        if process is not None and process.poll() is None:
            return [process]
        return [thread] if thread is not None and thread.is_alive() else []

    @staticmethod
    def _signal(process) -> None:
        with contextlib.suppress(OSError):
            process.kill()  # its own process group: the signal reaches only our child

    def _run(self, data_root: pathlib.Path, repo_dir: pathlib.Path) -> None:
        started = time.monotonic()
        try:
            if self._stopped.is_set():
                return
            process = spawn_maintenance_job(data_root, repo_dir, "evidence_packer")
            with self._lock:
                self._process = process
            if self._stopped.is_set():
                self._signal(process)
            packs.record_storage_pack(data_root, phase="started", pid=process.pid)
            raw, exit_code = read_job_report(process, self._signal)
            fields = _report_fields(raw)
            phase = ("stopped" if self._stopped.is_set() else "unknown" if not fields
                     else "failed" if exit_code else fields.pop("status", "unknown"))
            fields.pop("status", None)
            packs.record_storage_pack(data_root, phase=phase, exit_code=exit_code,
                                      duration_seconds=time.monotonic() - started, **fields)
            if fields.get("packed"):
                log.info("Evidence packs: packed %d/%d tasks, %.2f GB -> %.2f GB", fields["packed"], fields["candidates"],
                         fields["raw"] / 1e9, fields["compressed"] / 1e9)
        except Exception as exc:
            packs.record_storage_pack(data_root, phase="failed", exception_class=type(exc).__name__,
                                      duration_seconds=time.monotonic() - started)
        finally:
            with self._lock:
                self._process = None


def _report_fields(raw: bytes) -> Dict[str, Any]:
    try:
        value = json.loads(raw) if len(raw) <= _REPORT_BYTES else None
        if not isinstance(value, dict) or value.get("status") not in {"completed", "unknown", "failed"}:
            return {}
        fields: Dict[str, Any] = {"status": value["status"]}
        for key in ("packed", "candidates", "raw", "compressed", "unlinked", "kept", "busy", "faults", "archived"):
            number = value.get(key)
            if type(number) is not int or number < 0:
                return {}
            fields[key] = number
        return fields
    except (ValueError, UnicodeError, TypeError):
        return {}


packer = EvidencePacker()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--custody-session", default="")
    args = parser.parse_args()
    os.environ["OUROBOROS_DATA_DIR"] = str(pathlib.Path(args.data_root).resolve())
    from ouroboros.process_custody import adopt_session_id, start_parent_lifeline

    adopt_session_id(args.custody_session)
    start_parent_lifeline(label="evidence_packer")
    try:
        report = run_pass(pathlib.Path(args.data_root))
    except Exception as exc:
        report = {"status": "failed", "exception_class": type(exc).__name__}
    print(json.dumps(report, separators=(",", ":")), flush=True)
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
