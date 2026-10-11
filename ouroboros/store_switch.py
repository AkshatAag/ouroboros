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
the door used to call runs (``switching_owner``, bound to the intent's target and reason;
HEAD is verified against the target after it returns) -> the caller exits 42 or re-execs. A
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
    """Exactly the call the door makes today (every rescue, verification and handoff stays),
    bound to the intent: the rollback doors carry the intent's reason; the update doors first
    check that the managed update record they act on names the intent's target and refuse
    without moving anything otherwise. The seam verifies HEAD after any owner returns."""
    if door in {"ui_rollback", "vcs_rollback"}:
        from supervisor.update_recovery import rollback_to_version

        return lambda intent: rollback_to_version(intent["target_sha"], reason=intent["reason"])
    if door == "boot_smoke_rollback":
        from supervisor.update_merge import read_update_tx_strict, rollback_managed_update

        def rollback(intent: Dict[str, Any]) -> Tuple[bool, str]:
            recorded = str(read_update_tx_strict()[1].get("pre_update_sha") or "")
            return _unbound(recorded, intent, "the managed update transaction rolls back to") or rollback_managed_update(
                intent["reason"], reopen_writer_admission=False)

        return rollback
    if door == "update_replace":
        from supervisor import git_ops
        from supervisor.git_ops_reset import checkout_and_reset

        def replace(intent: Dict[str, Any]) -> Tuple[bool, str]:
            recorded = str((git_ops._read_update_intent() or {}).get("target_sha") or "")
            return _unbound(recorded, intent, "the managed update intent targets") or checkout_and_reset(
                git_ops.BRANCH_DEV, reason="ui_update_apply", unsynced_policy="rescue_and_reset")

        return replace
    raise ValueError(f"unknown switch door: {door}")


def _unbound(recorded: str, intent: Dict[str, Any], what: str) -> Optional[Tuple[bool, str]]:
    """The refusal when the owner's own record would move HEAD elsewhere than the intent."""
    if recorded.strip() == intent["target_sha"]:
        return None
    return False, (f"{what} {recorded.strip()[:12] or 'nothing'}, not the switch intent's {intent['target_sha'][:12]}; "
                   "checkout left unchanged")


def rev_parse(repo_dir: Any, ref: str) -> str:
    """The commit ``ref`` names in ``repo_dir``, ``""`` when it names none."""
    result = subprocess.run(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], cwd=str(repo_dir), capture_output=True,
                            text=True, env={**os.environ, "LC_ALL": "C", "LANG": "C"})
    return result.stdout.strip() if result.returncode == 0 else ""


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


def _largest_result_bytes(roots: Sequence[pathlib.Path]) -> int:
    """The largest single result file the facts export writes: the atomic rewrite of an existing
    result needs one temp file at a time, so the export's peak is one file, not the corpus."""
    largest = 0
    for drive in roots:
        with store.open_store(drive, write=False) as txn:
            largest = max(largest, int(txn.conn.execute(
                "SELECT COALESCE(MAX(LENGTH(CAST(body AS BLOB))), 0) FROM task_bodies").fetchone()[0]))
    return largest


def _handle_restore_bytes(root: pathlib.Path) -> int:
    return 0  # the packing stage's `evidence_packs.pending_restore_bytes(root)`: the handles' raw bytes the A+ restore writes


def _money_refusal(root: pathlib.Path, receipt: Dict[str, Any]) -> str:
    """The money exporter's own refusals, checked before anything moves (``usage_store.export_refusal``):
    a completed step whose journal is intact needs nothing; an interrupted one resumes."""
    step = receipt["steps"].get("money") or {}
    if step.get("status") == "completed" and _sha256_file(root / LEDGER_REL) == step["journal"]["sha256"]:
        return ""
    if _nothing_to_export(root):
        return ""
    return usage_store.export_refusal(root, resume=step.get("status") in {"started", "completed"})


def _nothing_to_export(root: pathlib.Path) -> bool:
    """An install that never had money: neither a store nor a journal."""
    return not (root / usage_store.STORE_REL).exists() and not (root / LEDGER_REL).exists()


def _sha256_file(path: pathlib.Path) -> Optional[str]:
    try:
        return _sha(path.read_bytes())
    except FileNotFoundError:
        return None


def _preflight(root: pathlib.Path, needs: Dict[str, bool], receipt: Dict[str, Any], roots: Sequence[pathlib.Path]) -> str:
    """The exporters' own refusals, then the free space the export needs: the handles' raw bytes
    the restore writes, the money journal (bounded by the store file) and the largest single
    result file (one atomic temp file at a time). A real ENOSPC still arrives as ``OSError``."""
    required = {"handles": _handle_restore_bytes(root) if needs["handles"] else 0,
                "largest_result": _largest_result_bytes(roots) if needs["facts"] else 0, "money": 0}
    if needs["facts"]:
        # The custody chain the old body will read must hold every event the store reduced: the
        # outbox is appended and verified empty before any store is set aside.
        from ouroboros.delegate_custody_current import flush_outbox, outbox_depth

        for drive in roots:
            flush_outbox(drive)
            depth = outbox_depth(drive)
            if depth:
                return f"custody outbox under {drive} still holds {depth} unappended event(s)"
    if needs["money"]:
        refusal = _money_refusal(root, receipt)
        if refusal:
            return refusal
        money = root / usage_store.STORE_REL
        required["money"] = money.stat().st_size if money.exists() else 0
    free = shutil.disk_usage(root).free
    if free < sum(required.values()):
        return f"free space {free} below the {sum(required.values())} bytes the export needs ({required})"
    return ""


def _export_money(root: pathlib.Path, receipt: Dict[str, Any]) -> Dict[str, Any]:
    """``usage_store.export_journal`` under the receipt: a completed step with its journal intact
    is skipped; an interrupted step resumes through the store's own export mark (its journal
    recognised by the mark's digest, a landed rename completed from the retired copy)."""
    step = receipt["steps"].get("money") or {}
    if step.get("status") == "completed" and _sha256_file(root / LEDGER_REL) == step["journal"]["sha256"]:
        return step
    if _nothing_to_export(root):
        return {"status": "nothing_to_export", "completed_at": utc_now_iso()}
    receipt["steps"]["money"] = {"status": "started", "started_at": step.get("started_at") or utc_now_iso()}
    _write_receipt(root, receipt)
    result = usage_store.export_journal(root, resume=True)
    return {"status": "completed", "completed_at": utc_now_iso(), "store_retired_as": result["store_retired_as"],
            "pre_export_as": result["pre_export_as"], "marker": result["marker"],
            "journal": {"path": str(root / LEDGER_REL), "size": result["journal"]["size"],
                        "sha256": result["journal"]["sha256"]}}


def _abandon(root: pathlib.Path, receipt: Dict[str, Any]) -> Dict[str, Any]:
    """Restore both authorities after a partial export, resumably: the receipt records the
    abandonment ``started`` first (a death inside resumes it at the next boot), the money
    store comes back through ``usage_store.abandon_export`` (store, journal, marker, mark),
    the facts stores back to published with their own partial writes absorbed."""
    receipt["abandonment"] = {"status": "started", "at": utc_now_iso()}
    _write_receipt(root, receipt)
    restored: Dict[str, Any] = {}
    money = receipt["steps"].get("money") or {}
    if money.get("status") in {"started", "completed"}:
        moved = usage_store.abandon_export(root, retired_as=money.get("store_retired_as"))
        if moved:
            restored["money"] = moved
    for drive in _roots(root)[0]:
        with store.open_store(drive, write=True) as txn:
            if txn.phase() != PUBLISHED:
                absorbed = store_import.absorb_own_export(drive, txn)  # its own partial writes join the baseline
                txn.set_meta("phase", PUBLISHED)
                txn.set_meta("export", {"status": "abandoned", "at": utc_now_iso(), **absorbed})
                restored.setdefault("facts", []).append(str(drive))
    receipt["abandonment"] = {"status": "completed", "at": utc_now_iso(), **restored}
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
    if (receipt.get("abandonment") or {}).get("status") == "started":  # a death inside the abandonment: finish it
        return _refuse(root, intent, receipt, "abandonment_resumed")
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
    head = rev_parse(repo_dir, "HEAD")
    if ok and head != (rev_parse(repo_dir, intent["target_sha"]) or intent["target_sha"]):  # the owner's word is not HEAD
        ok, message = False, f"the switching owner returned but HEAD is {head[:12] or 'unreadable'}, not the target " \
                             f"{intent['target_sha'][:12]}: {message}"
    receipt["switched"] = {"ok": bool(ok), "message": str(message), "head": head, "at": utc_now_iso()}
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
