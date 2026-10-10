"""The authority switch of the current-facts store (dormant until the cutover wires the doors).

A door that moves the checkout to a tree WITHOUT the store (UI Rollback, the automatic
boot-smoke rollback, an Update replace to an older official ref, the agent's rollback
through the UI door) records an intent (``record_switch_intent``) instead of switching
at once; the restart exit, after every worker is dead, runs ``run_switch_seam``:

preflight (nothing moves on failure; a refusal is a normal restart on the current code
plus a reason for the owner) -> phase ``exporting`` in every store -> facts
(``store_import.export_facts`` per root) -> money (``usage_store.export_journal`` when the
target lacks ``usage_store.py``), idempotent through the switch receipt -> handles (an
interface the packing stage fills; «no packs» now) -> fsync -> ``export.status=completed``
+ receipt -> the intent is CONSUMED -> the stores are retired -> the SAME switching owner
the door used to call runs (``switching_owner``) -> the caller exits 42 or re-execs. A
death after the consume and before the switch leaves the new code with a completed export:
the next boot finds no intent and a retired store, re-imports from the complete export and
tells the owner the switch did not happen. An unconfirmed stop of a writer refuses the
switch, never hangs. The switch receipt (``meta`` and ``state/switch_receipt.json``)
records every completed step and the files it wrote or retired; abandonment after a
partial export restores BOTH authorities.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from ouroboros import store, store_import, usage_store
from ouroboros.store import EXPORTING, GENERATION_KEY, PUBLISHED, RETIRED
from ouroboros.store_import import sha256_hex as _sha, supervisor_row, utc_stamp as _stamp
from ouroboros.usage_ledger import LEDGER_REL, UsageAccountingError
from ouroboros.utils import atomic_write_json, read_json_dict, utc_now_iso

INTENT_REL = pathlib.Path("state/switch_intent.json")
RECEIPT_REL = pathlib.Path("state/switch_receipt.json")
PROGRESS_REL = pathlib.Path("state/switch_progress.json")
DOORS = ("ui_rollback", "vcs_rollback", "boot_smoke_rollback", "update_replace")
SPACE_MARGIN = 1.1  # free space needed over the bytes the export writes
Owner = Callable[[Dict[str, Any]], Tuple[bool, str]]


def _progress(root: pathlib.Path, phase: str, **facts: Any) -> None:
    atomic_write_json(root / PROGRESS_REL, {"phase": phase, "at": utc_now_iso(), **facts})
    supervisor_row(root, "storage_switch", {"phase": phase, **facts})


# ---- intent and receipt -------------------------------------------------------------

def record_switch_intent(root: Any, *, target_sha: str, target_ref: str, reason: str, door: str) -> Dict[str, Any]:
    """The door's record of the switch it wants; write-once until consumed or refused."""
    root = pathlib.Path(root)
    if door not in DOORS:
        raise ValueError(f"unknown switch door: {door}")
    if (root / INTENT_REL).exists():
        raise RuntimeError(f"a switch intent is already recorded: {root / INTENT_REL}")
    intent = {"target_sha": str(target_sha), "target_ref": str(target_ref), "reason": str(reason), "door": door,
              "requested_at": utc_now_iso()}
    atomic_write_json(root / INTENT_REL, intent, fsync=True)
    return intent


def read_switch_intent(root: Any) -> Optional[Dict[str, Any]]:
    return read_json_dict(pathlib.Path(root) / INTENT_REL)


def _receipt(root: pathlib.Path, intent: Dict[str, Any]) -> Dict[str, Any]:
    """The receipt of THIS intent (by target and request time); a fresh one otherwise."""
    kept = read_json_dict(root / RECEIPT_REL) or {}
    same = {key: kept.get("intent", {}).get(key) for key in ("target_sha", "requested_at")} == {
        key: intent.get(key) for key in ("target_sha", "requested_at")}
    return kept if same else {"intent": intent, "attempts": 0, "steps": {}, "export": {}}


def _write_receipt(root: pathlib.Path, receipt: Dict[str, Any]) -> None:
    atomic_write_json(root / RECEIPT_REL, receipt, fsync=True)
    try:
        with store.open_store(root, write=True) as txn:
            txn.set_meta("switch_receipt", receipt)
    except store.StoreError:
        pass  # a retired or absent store: the file is the receipt


def target_file_bytes(repo_dir: Any, sha: str, rel: str) -> Optional[bytes]:
    """The bytes of ``rel`` in the target commit, ``None`` when the target lacks it."""
    result = subprocess.run(["git", "cat-file", "-p", f"{sha}:{rel}"], cwd=str(repo_dir), capture_output=True,
                            env={**os.environ, "LC_ALL": "C", "LANG": "C"})
    return result.stdout if result.returncode == 0 else None


def switch_needs(repo_dir: Any, target_sha: str) -> Dict[str, bool]:
    """What the target tree cannot read, computed from the target before anything moves."""
    return {"facts": target_file_bytes(repo_dir, target_sha, "ouroboros/store.py") is None,
            "money": target_file_bytes(repo_dir, target_sha, "ouroboros/usage_store.py") is None,
            "handles": target_file_bytes(repo_dir, target_sha, "ouroboros/evidence_packs.py") is None}


def next_engine_pin(repo_dir: Any, target_sha: str):
    """The engine pin the next generation selects, read from the target as a bound candidate."""
    from ouroboros.claudexor_runtime import _PIN_FILENAME, load_runtime_pin

    raw = target_file_bytes(repo_dir, target_sha, "ouroboros/" + _PIN_FILENAME)
    return None if raw is None else load_runtime_pin(data=raw)


def switching_owner(door: str) -> Owner:
    """Exactly the call the door makes today, so every rescue, verification and handoff stays."""
    if door in {"ui_rollback", "vcs_rollback"}:
        from supervisor.update_recovery import rollback_to_version

        return lambda intent: rollback_to_version(intent["target_sha"], reason=intent["door"])
    if door == "boot_smoke_rollback":
        from supervisor.update_merge import rollback_managed_update

        return lambda intent: rollback_managed_update(intent["reason"], reopen_writer_admission=False)
    if door == "update_replace":
        from supervisor.git_ops import BRANCH_DEV
        from supervisor.git_ops_reset import checkout_and_reset

        return lambda intent: checkout_and_reset(BRANCH_DEV, reason="ui_update_apply", unsynced_policy="rescue_and_reset")
    raise ValueError(f"unknown switch door: {door}")


# ---- preflight, money, abandonment -----------------------------------------------------

def _roots(root: pathlib.Path) -> Tuple[list, str]:
    """The drives whose store is present, and the refusal when one is LOST (absent beside its
    completed stamp) or not inspectable: a retired drive has nothing left to export, a lost
    one is never «switched» past."""
    present = []
    for drive in (root, *store_import.descendant_drives(root)):
        try:
            state = store.probe(drive)[0]
        except store.StoreError as exc:
            return present, f"{type(exc).__name__}: {exc}"
        if state == "present":
            present.append(drive)
        elif store.generation_stamped(drive):
            return present, f"current store lost at {drive}: missing beside its completed stamp"
    return present, ""


def _export_bytes(roots: Sequence[pathlib.Path]) -> int:
    total = 0
    for drive in roots:
        with store.open_store(drive, write=False) as txn:
            total += int(txn.conn.execute("SELECT COALESCE(SUM(LENGTH(CAST(body AS BLOB))), 0) FROM task_bodies").fetchone()[0])
            total += int(txn.conn.execute("SELECT COALESCE(SUM(LENGTH(CAST(facts AS BLOB))), 0) FROM obligations").fetchone()[0])
    return total


def _money_refusal(root: pathlib.Path, receipt: Dict[str, Any]) -> str:
    """The money exporter's own refusals, checked before anything moves (``usage_store.export_journal``)."""
    if (receipt["steps"].get("money") or {}).get("status") == "completed":
        return ""
    kept, path = root / LEDGER_REL, root / usage_store.STORE_REL
    if not kept.exists():
        return ""
    if not path.exists():
        started = (receipt["steps"].get("money") or {}).get("status") == "started"
        if started and any(name.startswith(path.name + ".exported-") for name in os.listdir(path.parent)):
            return ""  # this export's own rename landed; the money step completes the receipt
        return f"a journal exists without a store: {kept}"
    with usage_store.hold(root, migrate=False, write=False) as txn:
        imported = ((txn.meta("import") or {}).get("source") or {}).get("size")
    own = (receipt["steps"].get("money") or {}).get("status") == "started" and kept.stat().st_size != imported
    if kept.stat().st_size != imported and not own:
        return f"the money journal changed after the import; move it aside to export: {kept}"
    return ""


def _preflight(root: pathlib.Path, needs: Dict[str, bool], receipt: Dict[str, Any], roots: Sequence[pathlib.Path]) -> str:
    required = 0
    if needs["facts"]:
        required += _export_bytes(roots)
    if needs["money"]:
        refusal = _money_refusal(root, receipt)
        if refusal:
            return refusal
        money = root / usage_store.STORE_REL
        required += money.stat().st_size if money.exists() else 0
    free = shutil.disk_usage(root).free
    if free < required * SPACE_MARGIN:
        return f"free space {free} below {int(required * SPACE_MARGIN)} bytes the export needs"
    return ""


def _export_money(root: pathlib.Path, receipt: Dict[str, Any]) -> Dict[str, Any]:
    """``usage_store.export_journal`` made idempotent by the receipt: a completed step with its
    journal intact is skipped; an interrupted step recognises its own journal; a crash after
    the store rename is completed from the facts on disk."""
    step = receipt["steps"].get("money") or {}
    kept, path = root / LEDGER_REL, root / usage_store.STORE_REL
    if step.get("status") == "completed" and kept.exists() and _sha(kept.read_bytes()) == step["journal"]["sha256"]:
        return step
    own = None
    if step.get("status") == "started" and kept.exists():
        if not path.exists() and any(name.startswith(path.name + ".exported-") for name in os.listdir(path.parent)):
            retired = sorted(name for name in os.listdir(path.parent) if name.startswith(path.name + ".exported-"))[-1]
            return {"status": "completed", "journal": {"path": str(kept), "size": kept.stat().st_size,
                                                       "sha256": _sha(kept.read_bytes())}, "store_retired_as": retired,
                    "completed_at": utc_now_iso()}
        with usage_store.hold(root, migrate=False, write=False) as txn:
            imported = ((txn.meta("import") or {}).get("source") or {}).get("size")
        if kept.stat().st_size != imported:
            own = _sha(kept.read_bytes())  # our own journal from the interrupted attempt
    receipt["steps"]["money"] = {"status": "started", "started_at": utc_now_iso()}
    _write_receipt(root, receipt)
    pre_export = {name for name in os.listdir(kept.parent) if name.startswith(kept.name + ".pre-export-")}
    result = usage_store.export_journal(root, own_journal_sha256=own)
    set_aside = sorted({name for name in os.listdir(kept.parent) if name.startswith(kept.name + ".pre-export-")} - pre_export)
    journal = result["journal"]
    return {"status": "completed", "completed_at": utc_now_iso(), "store_retired_as": result["store_retired_as"],
            "pre_export_as": set_aside[-1] if set_aside else None, "marker": result["marker"],
            "journal": {"path": journal["path"], "size": journal["size"], "sha256": journal["sha256"]}}


def _abandon(root: pathlib.Path, receipt: Dict[str, Any]) -> Dict[str, Any]:
    """Restore both authorities after a partial export: the money store from its retired name
    when the journal is byte-identical to the receipt, the facts stores back to published."""
    restored: Dict[str, Any] = {}
    money = receipt["steps"].get("money") or {}
    kept, path = root / LEDGER_REL, root / usage_store.STORE_REL
    if money.get("status") == "completed" and money.get("store_retired_as") and not path.exists():
        retired = path.with_name(money["store_retired_as"])
        if retired.exists() and kept.exists() and _sha(kept.read_bytes()) == money["journal"]["sha256"]:
            kept.unlink()
            if money.get("pre_export_as") and kept.with_name(money["pre_export_as"]).exists():
                os.replace(kept.with_name(money["pre_export_as"]), kept)
            os.replace(retired, path)
            store.fsync_directory(path.parent)
            usage_store.forget(root)
            restored["money"] = "republished"
    for drive in _roots(root)[0]:
        with store.open_store(drive, write=True) as txn:
            if txn.phase() != PUBLISHED:
                absorbed = store_import.absorb_own_export(drive, txn)  # its own partial writes join the baseline
                txn.set_meta("phase", PUBLISHED)
                txn.set_meta("export", {"status": "abandoned", "at": utc_now_iso(), **absorbed})
                restored.setdefault("facts", []).append(str(drive))
    receipt["abandoned"] = {"at": utc_now_iso(), **restored}
    _write_receipt(root, receipt)
    return restored


def _refuse(root: pathlib.Path, intent: Dict[str, Any], receipt: Dict[str, Any], reason: str, **facts: Any) -> Dict[str, Any]:
    """Nothing switches: the intent is set aside, both authorities stay (restored if a previous
    attempt moved them), and the owner gets the reason at the next boot."""
    restored = _abandon(root, receipt)
    refused = root / INTENT_REL.with_name(f"{INTENT_REL.stem}.refused-{_stamp()}.json")
    os.replace(root / INTENT_REL, refused)
    _progress(root, "refused", reason=reason, restored=restored, intent_moved_to=refused.name, **facts)
    return {"status": "refused", "reason": reason, "restored": restored, **facts}


# ---- the seam ------------------------------------------------------------------------------

def run_switch_seam(root: Any, *, repo_dir: Any, owner: Optional[Owner] = None, blockers: Sequence[str] = (),
                    restore_handles: Optional[Callable[[pathlib.Path], Dict[str, Any]]] = None) -> Dict[str, Any]:
    """The export-then-switch at the restart exit (order and guarantees: the module docstring).

    ``blockers`` are the names of writers whose stop the caller could not confirm (the
    census of killed workers, the packer through process custody); any name refuses the
    switch. ``owner`` defaults to the door's own switching owner; ``restore_handles`` is the
    packing stage's A+ restore and reports no packs until it exists."""
    root = pathlib.Path(root)
    intent = read_switch_intent(root)
    if intent is None:
        return {"status": "no_intent"}
    receipt = _receipt(root, intent)
    receipt["attempts"] = int(receipt.get("attempts") or 0) + 1
    _write_receipt(root, receipt)
    if receipt["attempts"] > 2:
        return _refuse(root, intent, receipt, "second_failure", attempts=receipt["attempts"])
    if blockers:
        return _refuse(root, intent, receipt, "quiescence_unconfirmed", blockers=list(blockers))
    needs = switch_needs(repo_dir, intent["target_sha"])
    roots, refusal = _roots(root) if needs["facts"] else ([], "")
    _progress(root, "preflight", needs=needs, attempt=receipt["attempts"])
    refusal = refusal or _preflight(root, needs, receipt, roots)
    if refusal:
        return _refuse(root, intent, receipt, refusal)
    try:
        steps = receipt["steps"]
        if needs["facts"]:
            for drive in roots:
                with store.open_store(drive, write=True) as txn:
                    txn.set_meta("phase", EXPORTING)
                    txn.set_meta("export", {"status": "exporting", "started_at": utc_now_iso()})
            _progress(root, "exporting", roots=[str(drive) for drive in roots])
            steps["facts"] = {"status": "completed", "roots": {str(drive): store_import.export_facts(drive) for drive in roots},
                              "completed_at": utc_now_iso()}
            _write_receipt(root, receipt)
        if needs["money"]:
            _progress(root, "money")
            steps["money"] = _export_money(root, receipt)
            _write_receipt(root, receipt)
        if needs["handles"]:
            _progress(root, "handles")
            steps["handles"] = restore_handles(root) if restore_handles else {"status": "no_packs"}
            _write_receipt(root, receipt)
    except (OSError, store.StoreError, UsageAccountingError) as exc:
        receipt["export"] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}", "at": utc_now_iso()}
        _write_receipt(root, receipt)
        _progress(root, "failed", error=receipt["export"]["error"], attempt=receipt["attempts"])
        return {"status": "failed", "error": receipt["export"]["error"], "attempt": receipt["attempts"]}
    receipt["export"] = {"status": "completed", "completed_at": utc_now_iso(), "needs": needs}
    for drive in roots:
        with store.open_store(drive, write=True) as txn:
            txn.set_meta("export", receipt["export"])
    _write_receipt(root, receipt)
    os.unlink(root / INTENT_REL)  # consumed BEFORE the switch: the old body never sees an intent
    store.fsync_directory(root / "state")
    receipt["retired"] = {str(drive): _retire(drive) for drive in roots}
    _write_receipt(root, receipt)
    _progress(root, "switching", door=intent["door"], target_sha=intent["target_sha"])
    ok, message = (owner or switching_owner(intent["door"]))(intent)
    receipt["switched"] = {"ok": bool(ok), "message": str(message), "at": utc_now_iso()}
    _write_receipt(root, receipt)
    _progress(root, "switched" if ok else "switch_failed", message=str(message))
    return {"status": "switched" if ok else "switch_failed", "message": str(message), "needs": needs, "exit": 42}


def _retire(drive: pathlib.Path) -> str:
    """The store LAST: phase retired inside, the stamp cleared, THEN renamed aside (a death
    between the two leaves a present, unstamped, retired store the next import rebuilds from;
    absent beside the stamp therefore always means LOST). Returns the retired name."""
    from ouroboros.startup_migrations import stamp

    with store.open_store(drive, write=True) as txn:
        txn.set_meta("phase", RETIRED)
    path = store.store_path(drive)
    retired = path.with_name(f"{path.name}.exported-{_stamp()}")
    stamp(drive, **{GENERATION_KEY: None})
    os.replace(path, retired)
    store.fsync_directory(path.parent)
    return retired.name


def resume_pending_switch(root: Any, *, repo_dir: Any, owner: Optional[Owner] = None, blockers: Sequence[str] = (),
                          restore_handles: Optional[Callable[[pathlib.Path], Dict[str, Any]]] = None) -> Dict[str, Any]:
    """The boot door: a pending intent resumes the seam; a completed export whose switch never
    ran is reported (the owner presses the door again), never replayed."""
    root = pathlib.Path(root)
    if read_switch_intent(root) is not None:
        return run_switch_seam(root, repo_dir=repo_dir, owner=owner, blockers=blockers, restore_handles=restore_handles)
    receipt = read_json_dict(root / RECEIPT_REL) or {}
    if (receipt.get("export") or {}).get("status") == "completed" and not receipt.get("switched"):
        receipt["switched"] = {"ok": False, "message": "the switch never ran after the export", "at": utc_now_iso()}
        atomic_write_json(root / RECEIPT_REL, receipt, fsync=True)
        _progress(root, "switch_not_performed", target_sha=receipt.get("intent", {}).get("target_sha"))
        return {"status": "switch_not_performed", "message": "the export completed but the switch did not run; "
                                                             "the current code stays, press the door again"}
    return {"status": "nothing_pending"}
