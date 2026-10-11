"""Create, import, reconcile and export the current-facts store (dormant until the cutover).

``ensure_current_store(root)`` is the only function that creates or imports a store:
the sibling build with ``synchronous=FULL``, ``meta`` (provenance, counts, the freeze
instant, the per-file content baseline: size, mtime_ns and sha256 of every frozen
``task_results/*.json``) written before publication, publish by ``os.replace`` plus a
directory fsync, the generation stamp after; a published but unstamped store is
accepted by its own ``meta``; the lock tier is re-probed on every boot and governs an
EXISTING store too: a header naming the other tier is re-tiered in place (application id
and ``meta.lock_tier``) under the import lock before any writer opens it, so the open
mode follows what the kernel enforces today; every own child drive down the tree (``descendant_drives``,
over ``task_custody.own_child_drives``) gets its own store by the same code; inadmissible rows are quarantined at import
through the existing emitter; the cancel-latch migration runs as the first pass.
Reconciliation (a published store found at boot) is per row against the baseline: a
changed file replaces its row, a deleted file moves its row to quarantine, a new file is
imported, an untouched row keeps the DB version, a row changed on both sides lets the
file win and keeps the DB version in ``meta.conflicts``; a full rebuild happens ONLY
after a proven complete export. A file whose size and mtime_ns equal its baseline is
untouched without being read (one listing plus one stat per boot; a rewrite that preserves
both is the disclosed residual); a stat-changed file is compared by sha256, so a touch with
the same bytes stays untouched; edits to obligation set files alone are not detected.
``export_facts(root)`` is the inverse: every row to its byte-equivalent file, every
obligation set file in FULL, fsync.
"""

from __future__ import annotations

import contextlib
import errno
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
    GENERATION, GENERATION_KEY, IMPORT_LOCK_NAME, LOCK_NAME, PUBLISHED, TIER_ENFORCED, TIER_NAME,
    StoreLost, Txn, document_text, encode_document,
)
from ouroboros.task_custody import HEADLESS_TASKS_DIR, TASK_DRIVES_DIR, own_child_drives
from ouroboros.task_result_schema import (
    TASK_RESULT_QUARANTINE_DIR, emit_quarantine_event, quarantine_task_result, stamp_task_result_schema,
    task_result_schema_refusal,
)
from ouroboros.task_results import task_result_path, task_results_dir
from ouroboros.utils import append_jsonl, atomic_write_json, machine_json, utc_now_iso, write_text_atomic

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


def _own_children(root: pathlib.Path) -> List[pathlib.Path]:
    """The own child drives of every task ONE root knows (result files and drive directories)."""
    ids = {path.stem for path in task_results_dir(root, create=False).glob("*.json")}
    for base in (root / HEADLESS_TASKS_DIR, root / TASK_DRIVES_DIR):
        if base.is_dir():
            ids.update(child.name for child in base.iterdir() if child.is_dir() and not child.is_symlink())
    drives: List[pathlib.Path] = []
    for task_id in sorted(ids):
        with contextlib.suppress(ValueError):
            drives.extend(own_child_drives(root, task_id))
    return drives


def descendant_drives(root: Any) -> List[pathlib.Path]:
    """Every own child drive down the tree (a child's drive names its own children the same
    way), parents before children: the one walk the import, the switch, the export, the
    retirement and the recovery all use."""
    found: List[pathlib.Path] = []
    pending = [pathlib.Path(root)]
    while pending:
        children = _own_children(pending.pop(0))
        found.extend(children)
        pending.extend(children)
    return found


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
        report["drives"] = {str(drive): ensure_current_store(drive, with_children=False)["status"]
                            for drive in descendant_drives(root)}
    report["duration_seconds"] = round(time.monotonic() - started, 3)
    supervisor_row(root, "current_store_import", {"phase": report["status"],
                                                  **{key: value for key, value in report.items() if key != "drives"}})
    return report


def _ensure(root: pathlib.Path, path: pathlib.Path) -> Tuple[Dict[str, Any], list]:
    from ouroboros.platform_layer import kernel_file_locks_enforced
    from ouroboros.startup_migrations import stamp

    state, header = store.probe(root)
    tier = TIER_ENFORCED if kernel_file_locks_enforced(root / "state" / LOCK_NAME) else TIER_NAME
    if state == "present":
        retiered = {"retiered_from": header} if header != tier else {}
        with store.open_store(root, write=True, service=True) as txn:  # opened under the header's protocol, left under today's
            if retiered:
                txn.conn.execute(f"PRAGMA application_id = {store._APPLICATION_ID[tier]}")
                txn.set_meta("lock_tier", tier)
            phase = txn.phase()
            if phase == PUBLISHED:
                report, quarantined = _reconcile(root, txn)
            export = txn.meta("export") or {}
        if phase == PUBLISHED:
            if not store.generation_stamped(root):
                stamp(root, **{GENERATION_KEY: GENERATION})  # the stamp a crash after publication never wrote
            _latches_into_intents(root, report.pop("latched", []))
            return {**report, "lock_tier": tier, **retiered}, quarantined
        if export.get("status") != "completed":
            return {"status": "switch_pending", "phase": phase, "lock_tier": tier, **retiered}, []  # the seam resumes it
        stamp(root, **{GENERATION_KEY: None})  # before the rename: absent + stamped is always a loss
        os.replace(path, path.with_name(f"{path.name}.exported-{utc_stamp()}"))  # a full rebuild after a proven complete export
    elif store.generation_stamped(root):
        raise StoreLost(f"current store missing beside its completed stamp: {path}")
    return _build_and_publish(root, path, tier)


def _build_and_publish(root: pathlib.Path, path: pathlib.Path, tier: str) -> Tuple[Dict[str, Any], list]:
    from ouroboros.startup_migrations import boot_admissions, migrate_cancel_latches, stamp

    records, baseline, quarantined, refused = _read_results(root)
    sets = _read_sets(root)
    for name, rows in boot_admissions(root).items():  # under the set files: an interrupted publication stays a candidate
        sets[name] = {**rows, **sets.get(name, {})}
    folded, custody = _custody_rows(root, records)
    for name, rows in custody.items():
        sets[name] = {**sets.get(name, {}), **rows} if name == "custody_open" else _merge_receipts(sets.get(name, {}), rows)
    provenance = {"status": "completed", "imported_at": utc_now_iso(), "lock_tier": tier,
                  "results": len(records), "quarantined": len(quarantined), "refused": refused,
                  "sets": {name: len(rows) for name, rows in sets.items()},
                  "custody_runs": len(folded.get("runs", {}))}
    tmp = path.with_name(f"{path.name}.import-{os.getpid()}-{uuid.uuid4().hex[:8]}")
    try:
        _build(tmp, tier, root, records, sets, baseline, provenance, folded)
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()
    store.fsync_directory(path.parent)
    stamp(root, **{GENERATION_KEY: GENERATION})  # after publication: a published, unstamped store is still ours
    _latches_into_intents(root, [task_id for task_id, entry in baseline.items() if entry.get("latch_stamped")])
    migrate_cancel_latches(root, records=[(task_id, document, "") for task_id, document in records])
    return {"status": "completed", "lock_tier": tier, "results": len(records), "quarantined": len(quarantined),
            "refused": len(refused), "sets": len(sets)}, quarantined


def _latches_into_intents(root: pathlib.Path, latched: List[str]) -> None:
    """The legacy latch rows this import stamped: one typed events row per import, and each
    becomes a cancel intent (the generation watermark does not gate rows an old body wrote later)."""
    if not latched:
        return
    from ouroboros.cancel_intents import migrate_legacy_cancel_latches

    append_jsonl(root / "logs/events.jsonl", {"ts": utc_now_iso(), "type": "task_result_cancel_latch_admitted",
                                              "count": len(latched), "task_ids": latched, "reason": "unstamped_pre_7_0"})
    with store.open_store(root, write=False) as txn:
        records = [(task_id, txn.load(task_id), "") for task_id in latched]
    migrate_legacy_cancel_latches(root, records=[record for record in records if record[1]])


def _custody_rows(root: pathlib.Path, records: List[Record]) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Dict[str, Any]]]]:
    """The custody half of the import, folded ONCE from the retained event chain: every run's
    row (``custody_runs``), the pending invocations (``custody_open``), the definite refusals
    and the task marks, plus the closing receipts still owed to a result's terminal refresh
    (``delegated_runs``). A chain that exists but cannot be read in full (EMFILE, EIO, an
    unopenable segment) ABORTS the import typed — nothing is built or stamped and the next
    boot retries — never a published store with no custody row; a missing chain is empty."""
    from ouroboros import delegate_custody as c
    from ouroboros.delegate_custody_current import fold

    chain = c.event_log_path(root)
    try:
        if c.custody_chain_unreadable(root):
            raise OSError(errno.EIO, "custody chain exists but cannot be opened")
        rows = tuple(c._iter_rows(chain, strict=True))
    except OSError as exc:
        raise _unreadable(exc, chain, "custody chain") from exc
    folded = fold(rows)
    owed = {task_id for task_id, document in records if document.get("delegated_runs_unreconciled")}
    receipts: Dict[str, Dict[str, Any]] = {}
    for rid, (facts, _hold) in folded["runs"].items():
        task_id = str(facts.get("task_id") or "")
        if facts.get("settled") and task_id in owed:
            receipts.setdefault(task_id, {"task_id": task_id, "closed_runs": {}})["closed_runs"][rid] = facts
    return folded, {"custody_open": folded["open"], "delegated_runs": receipts}


def _merge_receipts(held: Dict[str, Dict[str, Any]], folded: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    merged = {task_id: dict(facts) for task_id, facts in held.items()}
    for task_id, facts in folded.items():
        entry = merged.setdefault(task_id, {"task_id": task_id})
        entry["closed_runs"] = {**facts.get("closed_runs", {}), **entry.get("closed_runs", {})}
    return merged


def _admit(path: pathlib.Path, raw: bytes) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """``(document, reason, outcome)``: ``admitted``; ``quarantined`` (the file moved, bytes
    unchanged, through the existing quarantine); ``refused`` (left in place: a task-id
    mismatch, or a quarantine that could not move the file)."""
    try:
        data = json.loads(raw)
    except ValueError:
        data = None
    refusal = task_result_schema_refusal(data)
    if (refusal == "unstamped_pre_7_0" and isinstance(data, dict) and data.get("status") == "cancel_requested"
            and data.get("task_id") == path.stem):
        # The one legacy carve-out: an admissible pre-7 cancel latch is stamped on import (same
        # status, same fields) so custody can drive it to its terminal; every other unstamped row quarantines.
        return stamp_task_result_schema(data), "latch_stamped", "admitted"
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
    if reason == "latch_stamped":
        entry["latch_stamped"] = True
    return {**entry, "body_sha256": sha256_hex(encode_document(document).encode("utf-8"))}


def _read_result(path: pathlib.Path) -> Optional[bytes]:
    """The file's bytes, ``None`` when it is gone; a resource or I/O error is typed and ABORTS
    the import (an unreadable file is never a refused row, and nothing gets stamped)."""
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _unreadable(exc, path) from exc


def _stat_result(path: pathlib.Path) -> Optional[Tuple[int, int]]:
    """``(size, mtime_ns)``, ``None`` when the file is gone; other errors typed as for a read."""
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _unreadable(exc, path) from exc
    return info.st_size, info.st_mtime_ns


def _unreadable(exc: OSError, path: pathlib.Path, what: str = "task result") -> store.StoreError:
    name = errno.errorcode.get(exc.errno, str(exc.errno))
    kind = store.StoreBusy if exc.errno in (errno.EMFILE, errno.ENFILE) else store.StoreError
    return kind(f"{what} not readable ({name}), import aborted: {path}")


def _read_results(root: pathlib.Path):
    records: List[Record] = []
    baseline: Dict[str, Dict[str, Any]] = {}
    quarantined: List[Dict[str, str]] = []
    refused: Dict[str, str] = {}
    for path in sorted(task_results_dir(root, create=False).glob("*.json")):
        raw = _read_result(path)
        if raw is None:
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


def set_file_path(root: pathlib.Path, name: str) -> pathlib.Path:
    """The old body's set file ``state/obligations/<name>.json``: the importer's input, the exporter's output."""
    return pathlib.Path(root) / "state" / "obligations" / f"{name}.json"


def _read_sets(root: pathlib.Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Every readable obligation set file, as the old body or the exporter left it."""
    sets: Dict[str, Dict[str, Dict[str, Any]]] = {}
    try:
        paths = sorted(set_file_path(root, "nonterminal").parent.glob("*.json"))
    except OSError:
        return sets
    for path in paths:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue  # an unreadable set file imports nothing: the derived half comes from the documents
        if isinstance(value, dict) and all(isinstance(row, dict) for row in value.values()):
            sets[path.stem] = value
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
           baseline: Dict[str, Any], provenance: Dict[str, Any], folded: Optional[Dict[str, Any]] = None) -> None:
    conn = store.connect(tmp, tier, create=True)
    try:
        conn.execute("PRAGMA synchronous = FULL")
        conn.executescript(store._SCHEMA)
        conn.execute(f"PRAGMA application_id = {store._APPLICATION_ID[tier]}")
        conn.execute("BEGIN IMMEDIATE")
        txn = Txn(conn, root)
        for task_id, document in records:
            txn.store_task(document, pending_drive=_pending_drive(root, task_id, document))
        for name, rows in sets.items():
            txn.replace_set(name, rows)
        if folded:
            for rid, (facts, hold) in folded["runs"].items():
                txn.put_custody(rid, facts, state=hold)
            for name in ("refused", "marks"):
                for identity, facts in folded[name].items():
                    txn.put_row(f"custody_{name}", identity, facts)
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
    """Per-row reconciliation of an out-of-band change against the content baseline (P6').

    The budget is one listing plus one stat per file: a file whose ``(size, mtime_ns)``
    equals its baseline is untouched WITHOUT being read (no supported actor rewrites a
    result while preserving both — the one disclosed residual). Only a stat-changed file is
    read: the same sha256 is a touch (the stored stat is refreshed, nothing else), a different
    one is a change."""
    baseline: Dict[str, Dict[str, Any]] = txn.meta("baseline") or {}
    present: Dict[str, Tuple[pathlib.Path, Optional[bytes]]] = {}
    changed: List[str] = []
    for path in sorted(task_results_dir(root, create=False).glob("*.json")):
        entry = baseline.get(path.stem)
        stat = _stat_result(path)
        if stat is None:
            continue
        if entry is not None and stat == (entry["size"], entry["mtime_ns"]):
            present[path.stem] = (path, None)  # untouched: not read
            continue
        raw = _read_result(path)
        if raw is None:
            continue
        present[path.stem] = (path, raw)
        if entry is None:
            continue  # new: imported below
        if sha256_hex(raw) != entry["sha256"]:
            changed.append(path.stem)
        else:
            baseline[path.stem] = {**entry, "size": stat[0], "mtime_ns": stat[1]}  # touched, same bytes
    fresh = sorted(task_id for task_id in present if task_id not in baseline)
    deleted = sorted(task_id for task_id in baseline if task_id not in present)
    counts = {"changed": len(changed), "new": len(fresh), "deleted": len(deleted), "results": len(baseline)}
    if not (changed or fresh or deleted):
        txn.set_meta("baseline", baseline)
        return {"status": "unchanged", **counts}, []
    now = utc_now_iso()
    quarantined: List[Dict[str, str]] = []
    refused: Dict[str, str] = {}
    latched: List[str] = []
    conflicts: Dict[str, Any] = txn.meta("conflicts") or {}
    for task_id in changed + fresh:  # the old body was the authority while it ran: its files win
        path, raw = present[task_id]
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
        if reason == "latch_stamped":
            latched.append(task_id)
        body = txn.body_text(task_id)
        if body is not None and sha256_hex(body.encode("utf-8")) != baseline.get(task_id, {}).get("body_sha256"):
            conflicts[task_id] = {"at": now, "db_body": body, "file_sha256": sha256_hex(raw)}
        txn.store_task(document, pending_drive=_pending_drive(root, task_id, document))
        baseline[task_id] = _baseline_entry(path, raw, document, reason)
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
            "conflicts": len(conflicts), "latched": latched}, quarantined


def _set_rows_in_full(txn: Txn, name: str) -> Dict[str, Any]:
    """An obligation set file's content: the stored rows plus the members the flag and
    status columns derive (owner-held sets are rows only)."""
    return o.members_in(txn, name)


def _identity(text: str) -> Dict[str, Any]:
    data = text.encode("utf-8")
    return {"size": len(data), "sha256": sha256_hex(data)}


def export_facts(root: Any) -> Dict[str, Any]:
    """The exact inverse of the import, in two transactions around the writes.

    T1 commits ``meta.export_manifest``: the name, size and sha256 of every file the export
    will write (every row's whole ``load`` document to ``task_results/<id>.json``, every
    obligation set file in FULL) and the stale result files it removes (admissible files
    whose row the store no longer holds; refused files and the quarantine stay as evidence).
    Then the files: tmp + rename, a byte-identical file left alone, fsync of files and
    directories. T2 refreshes the content baseline to what was written and completes the
    manifest. A death in between leaves a manifest ``absorb_own_export`` reads at
    abandonment, so the store's own partial writes are never mistaken for an old body's."""
    root = pathlib.Path(root)
    names: List[str] = []
    with store.open_store(root, write=True, service=True) as txn:  # T1: the plan, durable before the first write
        baseline = txn.meta("baseline") or {}
        task_ids = txn.task_ids()
        names = sorted(set(txn.set_names()) | set(o.BOOT_SETS))  # an older body expects every set file
        stale = sorted(path.stem for path in task_results_dir(root, create=False).glob("*.json")
                       if path.stem not in task_ids and not (baseline.get(path.stem) or {}).get("refused"))
        manifest = {"status": "writing", "started_at": utc_now_iso(),
                    "results": {task_id: _identity(document_text(txn.load(task_id))) for task_id in task_ids},
                    "sets": {name: _identity(machine_json(_set_rows_in_full(txn, name)))  # what atomic_write_json writes
                             for name in names},
                    "removed": stale}
        txn.set_meta("export_manifest", manifest)
    written = total = 0
    entries: Dict[str, Dict[str, Any]] = {}
    with store.open_store(root, write=False) as txn:  # the frozen rows, streamed to their files
        for task_id in task_ids:
            document = txn.load(task_id)
            text = document_text(document)
            data = text.encode("utf-8")
            path = task_result_path(root, task_id)
            if _read_result(path) != data:  # a read error aborts the export typed, never «rewrite blindly»
                write_text_atomic(path, text, fsync=True)
                written += 1
            total += len(data)
            entries[task_id] = _baseline_entry(path, data, document)
        for name in names:
            atomic_write_json(set_file_path(root, name), _set_rows_in_full(txn, name), fsync=True)
    for task_id in stale:
        with contextlib.suppress(FileNotFoundError):
            task_result_path(root, task_id).unlink()
    store.fsync_directory(task_results_dir(root, create=False))
    store.fsync_directory(set_file_path(root, "nonterminal").parent)
    with store.open_store(root, write=True, service=True) as txn:  # T2: the baseline follows what was written
        refused = {task_id: entry for task_id, entry in (txn.meta("baseline") or {}).items() if entry.get("refused")}
        txn.set_meta("baseline", {**refused, **entries})
        txn.set_meta("export_manifest", {**manifest, "status": "completed", "completed_at": utc_now_iso()})
    return {"results": len(task_ids), "written": written, "removed": len(stale), "bytes": total, "sets": len(names)}


def absorb_own_export(root: Any, txn: Txn) -> Dict[str, Any]:
    """At the abandonment of a partial export (inside the transaction that republishes the
    store): every file on disk whose size and sha256 match the manifest is this store's own
    write and joins the baseline; a removed stale file leaves it. The next boot then sees
    nothing out of band and keeps every row, the owner-held sets included, instead of
    re-importing the store's own partial export as an old body's change."""
    root = pathlib.Path(root)
    manifest = txn.meta("export_manifest") or {}
    if manifest.get("status") != "writing":
        return {}
    baseline = txn.meta("baseline") or {}
    adopted = 0
    for task_id, identity in manifest.get("results", {}).items():
        path = task_result_path(root, task_id)
        raw = _read_result(path)
        if raw is not None and len(raw) == identity["size"] and sha256_hex(raw) == identity["sha256"]:
            baseline[task_id] = _baseline_entry(path, raw, txn.load(task_id))
            adopted += 1
    for task_id in manifest.get("removed", []):
        if _read_result(task_result_path(root, task_id)) is None:
            baseline.pop(task_id, None)
    txn.set_meta("baseline", baseline)
    txn.set_meta("export_manifest", {**manifest, "status": "abandoned", "adopted": adopted, "at": utc_now_iso()})
    return {"adopted": adopted, "planned": len(manifest.get("results", {}))}
