"""Create, import, reconcile and export the current-facts store (dormant until the cutover).

``ensure_current_store(root)`` is the only function that creates or imports a store:
the sibling build with ``synchronous=FULL``, ``meta`` (provenance, counts, the freeze
instant, the per-file content baseline: size, mtime_ns and sha256 of every frozen
``task_results/*.json``) written before publication, publish by ``os.replace`` plus a
directory fsync, the generation stamp after; a published but unstamped store is
accepted by its own ``meta``; every child root from ``task_custody.own_child_drives``
gets its own store by the same code; inadmissible rows are quarantined at import
through the existing emitter; the cancel-latch migration runs as the first pass.
Reconciliation (a published store found at boot) is per row against the baseline: a
changed file replaces its row, a deleted file moves its row to quarantine, a new file is
imported, an untouched row keeps the DB version, a row changed on both sides lets the
file win and keeps the DB version in ``meta.conflicts``; a full rebuild happens ONLY
after a proven complete export. Timestamps alone never prove origin (a touched file
with the same bytes is untouched); edits to obligation set files alone are not detected.
``export_facts(root)`` is the inverse: every row to its byte-equivalent file, every
obligation set file in FULL, fsync.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import pathlib
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from ouroboros import obligations as o
from ouroboros import store
from ouroboros.store import (
    FLAG_COLUMNS, GENERATION, GENERATION_KEY, IMPORT_LOCK_NAME, LOCK_NAME, PUBLISHED, TIER_ENFORCED, TIER_NAME,
    StoreLost, Txn, encode_document,
)
from ouroboros.task_custody import HEADLESS_TASKS_DIR, TASK_DRIVES_DIR, own_child_drives
from ouroboros.task_result_schema import (
    TASK_RESULT_QUARANTINE_DIR, emit_quarantine_event, quarantine_task_result, task_result_schema_refusal,
)
from ouroboros.task_results import task_result_path, task_results_dir
from ouroboros.utils import append_jsonl, atomic_write_json, utc_now_iso, write_text_atomic

Record = Tuple[str, Dict[str, Any]]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def supervisor_row(root: pathlib.Path, kind: str, payload: Dict[str, Any]) -> None:
    """One best-effort ``logs/supervisor.jsonl`` row of ``kind`` (the import's and the switch's)."""
    try:
        append_jsonl(root / "logs" / "supervisor.jsonl", {"ts": utc_now_iso(), "type": kind, **payload})
    except Exception:
        import logging

        logging.getLogger(__name__).warning("%s row not recorded", kind, exc_info=True)


def child_roots(root: Any) -> List[pathlib.Path]:
    """Every own child drive of every task the root knows (result files and drive directories)."""
    root = pathlib.Path(root)
    ids = {path.stem for path in task_results_dir(root, create=False).glob("*.json")}
    for base in (root / HEADLESS_TASKS_DIR, root / TASK_DRIVES_DIR):
        if base.is_dir():
            ids.update(child.name for child in base.iterdir() if child.is_dir() and not child.is_symlink())
    drives: List[pathlib.Path] = []
    for task_id in sorted(ids):
        with contextlib.suppress(ValueError):
            drives.extend(own_child_drives(root, task_id))
    return drives


def ensure_current_store(root: Any, *, with_children: bool = True) -> Dict[str, Any]:
    """The one creator and importer; idempotent on every boot door (details: the module
    docstring). Returns the report it also writes to ``logs/supervisor.jsonl``."""
    root = pathlib.Path(root)
    path = store.store_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with store.process_lock(path), store.name_lock(root, IMPORT_LOCK_NAME, timeout_sec=600.0, stale_sec=600.0):
        for name in os.listdir(path.parent):
            if name.startswith(path.name + ".import-"):
                with contextlib.suppress(OSError):
                    (path.parent / name).unlink()  # an unfinished import never published: redone from scratch
        report, quarantined = _ensure(root, path)
    emit_quarantine_event(root, quarantined)
    if with_children:
        report["child_roots"] = {str(drive): ensure_current_store(drive, with_children=False)["status"]
                                 for drive in child_roots(root)}
    report["duration_seconds"] = round(time.monotonic() - started, 3)
    supervisor_row(root, "current_store_import", {"phase": report["status"],
                                                  **{key: value for key, value in report.items() if key != "child_roots"}})
    return report


def _ensure(root: pathlib.Path, path: pathlib.Path) -> Tuple[Dict[str, Any], list]:
    from ouroboros.startup_migrations import stamp

    state, _tier = store.probe(root)
    if state == "present":
        with store.open_store(root, write=True) as txn:
            phase = txn.phase()
            if phase == PUBLISHED:
                report = _reconcile(root, txn)
            export = txn.meta("export") or {}
        if phase == PUBLISHED:
            if not store.generation_stamped(root):
                stamp(root, **{GENERATION_KEY: GENERATION})  # the stamp a crash after publication never wrote
            return report
        if export.get("status") != "completed":
            return {"status": "switch_pending", "phase": phase}, []  # the seam resumes it; nothing moves here
        stamp(root, **{GENERATION_KEY: None})  # before the rename: absent + stamped is always a loss
        os.replace(path, path.with_name(f"{path.name}.exported-{utc_stamp()}"))  # a full rebuild after a proven complete export
    elif store.generation_stamped(root):
        raise StoreLost(f"current store missing beside its completed stamp: {path}")
    from ouroboros.platform_layer import kernel_file_locks_enforced

    tier = TIER_ENFORCED if kernel_file_locks_enforced(root / "state" / LOCK_NAME) else TIER_NAME
    return _build_and_publish(root, path, tier)


def _build_and_publish(root: pathlib.Path, path: pathlib.Path, tier: str) -> Tuple[Dict[str, Any], list]:
    from ouroboros.startup_migrations import migrate_cancel_latches, stamp

    migrate_cancel_latches(root)  # the first pass: the admissible latch rows are stamped before they are read
    records, baseline, quarantined, refused = _read_results(root)
    sets = _read_sets(root)
    provenance = {"status": "completed", "imported_at": utc_now_iso(), "lock_tier": tier,
                  "results": len(records), "quarantined": len(quarantined), "refused": refused,
                  "sets": {name: len(rows) for name, rows in sets.items()}}
    tmp = path.with_name(f"{path.name}.import-{os.getpid()}-{uuid.uuid4().hex[:8]}")
    try:
        _build(tmp, tier, root, records, sets, baseline, provenance)
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()
    store.fsync_directory(path.parent)
    stamp(root, **{GENERATION_KEY: GENERATION})  # after publication: a published, unstamped store is still ours
    return {"status": "completed", "lock_tier": tier, "results": len(records), "quarantined": len(quarantined),
            "refused": len(refused), "sets": len(sets)}, quarantined


def _admit(path: pathlib.Path, raw: bytes) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """``(document, reason, outcome)``: ``admitted``; ``quarantined`` (the file moved, bytes
    unchanged, through the existing quarantine); ``refused`` (left in place: a task-id
    mismatch, or a quarantine that could not move the file)."""
    try:
        data = json.loads(raw)
    except ValueError:
        data = None
    refusal = task_result_schema_refusal(data)
    if refusal:  # writers are quiescent: a row the quarantine finds admissible again stays refused here
        return None, refusal, "quarantined" if quarantine_task_result(path, refusal) == "moved" else "refused"
    if str(data.get("task_id") or "") != path.stem:
        return None, "task_id_mismatch", "refused"
    return data, "", "admitted"


def _baseline_entry(path: pathlib.Path, raw: bytes, document: Optional[Dict[str, Any]], reason: str = "") -> Dict[str, Any]:
    info = path.stat()
    entry = {"size": info.st_size, "mtime_ns": info.st_mtime_ns, "sha256": sha256_hex(raw)}
    if document is None:
        return {**entry, "refused": reason}  # remembered, so an untouched refused file stays untouched
    return {**entry, "body_sha256": sha256_hex(encode_document(document).encode("utf-8"))}


def _read_results(root: pathlib.Path):
    records: List[Record] = []
    baseline: Dict[str, Dict[str, Any]] = {}
    quarantined: List[Dict[str, str]] = []
    refused: Dict[str, str] = {}
    for path in sorted(task_results_dir(root, create=False).glob("*.json")):
        try:
            raw = path.read_bytes()
        except OSError as exc:
            refused[path.stem] = type(exc).__name__
            continue
        document, reason, outcome = _admit(path, raw)
        if outcome == "quarantined":
            quarantined.append({"task_id": path.stem, "reason": reason})
            continue
        if outcome == "refused":
            refused[path.stem] = reason
        else:
            records.append((path.stem, document))
        baseline[path.stem] = _baseline_entry(path, raw, document, reason)
    return records, baseline, quarantined, refused


def _read_sets(root: pathlib.Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Every readable obligation set file, as the old body or the exporter left it."""
    directory = o.path(root, "nonterminal").parent
    sets: Dict[str, Dict[str, Dict[str, Any]]] = {}
    with o.locked(root):
        for path in sorted(directory.glob("*.json")):
            with contextlib.suppress(o.ObligationsUnavailable):
                sets[path.stem] = o._read(root, path.stem)
    return sets


def _pending_drive(root: pathlib.Path, task_id: str, document: Dict[str, Any]) -> bool:
    """The boot classification's rule: nonterminal, or settled without its terminal files ready."""
    from ouroboros.headless import terminal_task_files_ready
    from ouroboros.task_status import SETTLED_STATUSES

    status = document.get("status")
    if status in {"running", "interrupted"}:
        return True
    if status in SETTLED_STATUSES:
        try:
            return not terminal_task_files_ready(root, {**document, "id": task_id}, document)
        except Exception:
            return True  # unknown readiness keeps the drive pending, as the boot classification does
    return False


def _build(tmp: pathlib.Path, tier: str, root: pathlib.Path, records: List[Record], sets: Dict[str, Any],
           baseline: Dict[str, Any], provenance: Dict[str, Any]) -> None:
    conn = store.connect(tmp, tier, create=True)
    try:
        conn.execute("PRAGMA synchronous = FULL")
        conn.executescript(store._SCHEMA)
        conn.execute(f"PRAGMA application_id = {store._APPLICATION_ID[tier]}")
        conn.execute("BEGIN IMMEDIATE")
        txn = Txn(conn)
        for task_id, document in records:
            txn.store_task(document, pending_drive=_pending_drive(root, task_id, document))
        for name, rows in sets.items():
            txn.replace_set(name, rows)
        for key, value in (("schema_version", store.SCHEMA_VERSION), ("lock_tier", tier), ("generation", GENERATION),
                           ("phase", PUBLISHED), ("import", provenance), ("freeze_utc", utc_now_iso()),
                           ("baseline", baseline)):
            txn.set_meta(key, value)
        store.commit(conn)
    finally:
        conn.close()


def _quarantine_copy(root: pathlib.Path, task_id: str, body: str) -> None:
    """The DB version of a row whose file an old body deleted: kept beside the quarantined bytes."""
    directory = task_results_dir(root) / TASK_RESULT_QUARANTINE_DIR
    directory.mkdir(parents=True, exist_ok=True)
    write_text_atomic(directory / f"{task_id}.deleted-{utc_stamp()}.json", body, fsync=True)


def _reconcile(root: pathlib.Path, txn: Txn) -> Tuple[Dict[str, Any], list]:
    """Per-row reconciliation of an out-of-band change against the content baseline (P6')."""
    baseline: Dict[str, Dict[str, Any]] = txn.meta("baseline") or {}
    present: Dict[str, pathlib.Path] = {}
    changed: List[str] = []
    for path in sorted(task_results_dir(root, create=False).glob("*.json")):
        try:
            info = path.stat()
        except FileNotFoundError:
            continue
        present[path.stem] = path
        entry = baseline.get(path.stem)
        if entry is None or (info.st_size, info.st_mtime_ns) == (entry["size"], entry["mtime_ns"]):
            continue  # new (imported below) or untouched
        if sha256_hex(path.read_bytes()) == entry["sha256"]:  # touched, same bytes: timestamps prove nothing
            baseline[path.stem] = {**entry, "size": info.st_size, "mtime_ns": info.st_mtime_ns}
            continue
        changed.append(path.stem)
    fresh = sorted(task_id for task_id in present if task_id not in baseline)
    deleted = sorted(task_id for task_id in baseline if task_id not in present)
    counts = {"changed": len(changed), "new": len(fresh), "deleted": len(deleted), "results": len(baseline)}
    if not (changed or fresh or deleted):
        txn.set_meta("baseline", baseline)
        return {"status": "unchanged", **counts}, []
    now = utc_now_iso()
    quarantined: List[Dict[str, str]] = []
    refused: Dict[str, str] = {}
    conflicts: Dict[str, Any] = txn.meta("conflicts") or {}
    for task_id in changed + fresh:  # the old body was the authority while it ran: its files win
        path = present[task_id]
        raw = path.read_bytes()
        document, reason, outcome = _admit(path, raw)
        if outcome == "quarantined":  # the file left the directory with its bytes; the row goes with it
            quarantined.append({"task_id": task_id, "reason": reason})
            txn.delete_task(task_id)
            baseline.pop(task_id, None)
            continue
        if outcome == "refused":
            refused[task_id] = reason
            baseline[task_id] = _baseline_entry(path, raw, None, reason)
            continue
        body = txn.body_text(task_id)
        if body is not None and sha256_hex(body.encode("utf-8")) != baseline.get(task_id, {}).get("body_sha256"):
            conflicts[task_id] = {"at": now, "db_body": body, "file_sha256": sha256_hex(raw)}
        txn.store_task(document, pending_drive=_pending_drive(root, task_id, document))
        baseline[task_id] = _baseline_entry(path, raw, document)
    for task_id in deleted:
        body = txn.body_text(task_id)
        if body is not None:
            _quarantine_copy(root, task_id, body)
            quarantined.append({"task_id": task_id, "reason": "deleted_by_old_body"})
        txn.delete_task(task_id)
        baseline.pop(task_id, None)
    sets = _read_sets(root)  # the old body maintained the set files too
    for name in sorted(set(txn.set_names()) | set(sets)):
        txn.replace_set(name, sets.get(name, {}))
    for key, value in (("baseline", baseline), ("freeze_utc", now), ("conflicts", conflicts),
                       ("reconciled", {"at": now, **counts, "quarantined": len(quarantined),
                                       "refused": refused, "conflicts": len(conflicts)})):
        txn.set_meta(key, value)
    return {"status": "reconciled", **counts, "quarantined": len(quarantined), "refused": len(refused),
            "conflicts": len(conflicts)}, quarantined


def export_facts(root: Any) -> Dict[str, Any]:
    """Every task row to ``task_results/<id>.json`` (tmp + rename, a byte-identical file left
    alone), every obligation set file in FULL (the stored rows plus the derived members from
    the flag and status columns), fsync of files and directories; the content baseline is
    refreshed to what was written, so a later boot sees these files as untouched."""
    root = pathlib.Path(root)
    written = total = exported = 0
    with store.open_store(root, write=True) as txn:
        baseline = {task_id: entry for task_id, entry in (txn.meta("baseline") or {}).items() if entry.get("refused")}
        for task_id in txn.task_ids():
            exported += 1
            document = txn.load(task_id)
            text = encode_document(document)
            data = text.encode("utf-8")
            path = task_result_path(root, task_id)
            try:
                current = path.read_bytes()
            except OSError:
                current = None
            if current != data:
                write_text_atomic(path, text, fsync=True)
                written += 1
            total += len(data)
            baseline[task_id] = _baseline_entry(path, data, document)
        names = sorted(set(txn.set_names()) | set(o.BOOT_SETS))  # an older body expects every set file
        for name in names:
            rows = dict(txn.set_rows(name))
            if name in FLAG_COLUMNS:
                derived = txn.flagged(name)
            elif name == "nonterminal":
                derived = [record["task_id"] for record in txn.conn.execute(
                    "SELECT task_id FROM tasks WHERE status IN ('running', 'interrupted') ORDER BY task_id")]
            else:
                derived = []  # owner-held sets: rows only
            for task_id in derived:
                rows.setdefault(task_id, o.result_facts(txn.load(task_id)))
            atomic_write_json(o.path(root, name), rows, fsync=True)
        txn.set_meta("baseline", baseline)
    store.fsync_directory(task_results_dir(root, create=False))
    store.fsync_directory(o.path(root, "nonterminal").parent)
    return {"results": exported, "written": written, "bytes": total, "sets": len(names)}
