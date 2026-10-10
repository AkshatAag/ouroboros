"""The evidence packer: a supervised interpreter the maintenance tick spawns when candidates exist.

Never at boot. The supervisor's reconcile sweep asks ``packer.tick(root, repo_dir)``: the first
tick after start only arms it; a later tick spawns ``python -m ouroboros.evidence_packer`` the
way the historical audit is spawned (``spawn_supervised``, ``scope="session"``, its own process
group, durable custody, parent lifeline) when some task with loose evidence changed since the
last sweep recorded it, or an uncompressed money archive segment exists. The caller returns at
once; Stop latches and kills the child; one child at a time. The child packs one settled task
at a time (``evidence_packs.pack_task``: write, publish, re-read + verify, index, unlink), then
compresses the money archive, refreshes the data-root unpacker and prints one bounded JSON
report; the parent records ``storage_pack`` rows in ``logs/supervisor.jsonl`` and one health
line "packed N/M tasks, X GB -> Y GB".
"""
from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import pathlib
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ouroboros import evidence_packs as packs

log = logging.getLogger(__name__)
_REPORT_BYTES = 4096


# ---- candidates --------------------------------------------------------------

def task_stamps(root: pathlib.Path, task_id: str) -> str:
    """Evidence and settlement-source revisions, including owner-held duties and outbox sends."""
    from ouroboros.artifacts import task_artifact_dir_path
    from ouroboros.task_results import task_result_path
    from ouroboros.usage_store import STORE_REL
    from ouroboros.usage_ledger import LEDGER_REL

    stamps = []
    paths = [root / "observability" / "calls" / task_id,
                 task_artifact_dir_path(root, task_id, create=False).joinpath(*packs.CHECKPOINTS_REL.parts),
                 task_result_path(root, task_id, create=False), root / "state/terminal_deliveries.json",
                 root / "state/migrations.json", root / "state/obligations/rebuild.owed",
                 root / STORE_REL, root / (str(STORE_REL) + "-wal"), root / LEDGER_REL]
    paths.extend(sorted((root / "state/obligations").glob("*.json")))
    for path in paths:
        try:
            stat = path.stat()
            stamps.append((str(path.relative_to(root)), stat.st_ino, stat.st_size, stat.st_mtime_ns))
        except OSError:
            stamps.append((str(path.relative_to(root)), 0))
    return json.dumps(stamps, separators=(",", ":"))


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


def changed_tasks(root: pathlib.Path) -> List[str]:
    """Tasks with loose evidence whose directories or result changed since a sweep recorded them."""
    root = pathlib.Path(root)
    ids = _loose_task_ids(root)
    swept = swept_stamps(root) if ids else {}
    return sorted(task for task in ids if swept is None or swept.get(task) != task_stamps(root, task))


def archive_pending(root: pathlib.Path) -> bool:
    directory = pathlib.Path(root) / packs.USAGE_ARCHIVE_REL
    return directory.is_dir() and any(directory.glob("segment_ep*.jsonl"))


def ordered_candidates(root: pathlib.Path, settled: List[str]) -> List[Tuple[str, int]]:
    """Settled tasks with their loose raw size: the ones whose result changed after the newest
    published pack first (newest result first), then the backfill by descending raw size."""
    from ouroboros.task_results import task_result_path

    newest_pack = max((path.stat().st_mtime_ns for path, _ in packs.list_sidecars(root)), default=0)
    recent, backfill = [], []
    for task in settled:
        raw = sum(int(m["size"]) for m in packs.loose_members(root, task))
        try:
            touched = task_result_path(root, task, create=False).stat().st_mtime_ns
        except (OSError, ValueError):
            touched = 0
        (recent if newest_pack and touched > newest_pack else backfill).append((task, raw, touched))
    recent.sort(key=lambda item: (-item[2], item[0]))
    backfill.sort(key=lambda item: (-item[1], item[0]))
    return [(task, raw) for task, raw, _ in recent + backfill]


def live_manifest_shas(root: pathlib.Path) -> set:
    """Shas any NON-settled task's manifests reference: those loose blobs stay."""
    keep: set = set()
    for task in _loose_task_ids(root):
        if not packs.task_settled(root, task):
            keep.update(sha256 for _, sha256, _ in packs.manifest_blob_refs(root, task))
    return keep


# ---- one pass (the child) ----------------------------------------------------

def run_pass(root: pathlib.Path, *, stop=lambda: False) -> Dict[str, Any]:
    """Pack every settled candidate one task at a time, then the money archive and the unpacker."""
    root = pathlib.Path(root)
    report: Dict[str, Any] = {"status": "completed", "packed": 0, "candidates": 0, "raw": 0, "compressed": 0,
                              "unlinked": 0, "kept": 0, "busy": 0, "faults": 0, "archived": 0}
    revisions = {task: task_stamps(root, task) for task in changed_tasks(root)}
    facts = {task: packs.task_settlement_facts(root, task) for task in revisions}
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
            outcome = packs.pack_task(root, task, keep_shas=set(), listing=listing,
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
    with contextlib.suppress(OSError):
        packs.write_unpacker(root)
    return report


# ---- the supervisor side -----------------------------------------------------

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

    @staticmethod
    def _signal(process) -> None:
        with contextlib.suppress(OSError):
            process.kill()  # its own process group: the signal reaches only our child

    def _run(self, data_root: pathlib.Path, repo_dir: pathlib.Path) -> None:
        from ouroboros.config import SETTINGS_PATH
        from ouroboros.process_containment import CONTAINMENT_ENV_PREFIX
        from ouroboros.process_custody import current_custody_session_id, spawn_supervised

        started = time.monotonic()
        try:
            if self._stopped.is_set():
                return
            env = {key: os.environ[key] for key in (
                "PATH", "HOME", "USERPROFILE", "SystemRoot", "WINDIR", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL",
            ) if key in os.environ}
            env.update({key: value for key, value in os.environ.items() if key.startswith(CONTAINMENT_ENV_PREFIX)})
            env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(repo_dir), OUROBOROS_DATA_DIR=str(data_root),
                       OUROBOROS_REPO_DIR=str(repo_dir), OUROBOROS_SETTINGS_PATH=str(SETTINGS_PATH))
            process = spawn_supervised(
                [sys.executable, "-m", "ouroboros.evidence_packer", "--data-root", str(data_root),
                 "--custody-session", current_custody_session_id()],
                drive_root=data_root, purpose="evidence_packer", scope="session", cwd=str(repo_dir), env=env,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            )
            with self._lock:
                self._process = process
            if self._stopped.is_set():
                self._signal(process)
            packs.record_storage_pack(data_root, phase="started", pid=process.pid)
            try:
                raw = process.stdout.read(_REPORT_BYTES + 1)
                if len(raw) > _REPORT_BYTES:
                    self._signal(process)
                exit_code = process.wait()
            finally:
                process.stdout.close()
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
