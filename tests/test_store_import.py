"""The importer and exporter of the current-facts store (``ouroboros/store_import.py``):
publication and its crash points, child roots, quarantine at import, the byte-identical
export round trip, and the per-row reconciliation of an out-of-band change."""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sqlite3

import pytest

from ouroboros import obligations as o
from ouroboros import store, store_import
from ouroboros.store import GENERATION, GENERATION_KEY, StoreLost
from ouroboros.store_import import ensure_current_store, export_facts
from ouroboros.task_results import task_result_path
from ouroboros.utils import atomic_write_json


def result(task_id: str, status: str = "completed", **fields) -> dict:
    return {"task_id": task_id, "status": status, "ts": "2026-10-10T00:00:00+00:00",
            "updated_at": "2026-10-10T00:00:01+00:00", "_schema_version": 1, **fields}


def write_result(root: pathlib.Path, row: dict) -> pathlib.Path:
    path = task_result_path(root, row["task_id"])
    atomic_write_json(path, row)
    return path


def fixture_root(tmp_path: pathlib.Path) -> pathlib.Path:
    """Three results (one running with a child drive holding a retry row), two set files,
    one unstamped file (quarantined at import) and one id-mismatched file (refused)."""
    root = tmp_path / "data"
    write_result(root, result("root1", "running", parent_task_id=None, root_task_id="root1"))
    write_result(root, result("child1", parent_task_id="root1", root_task_id="root1", delegation_role="child",
                              nested={"k": [1, 2.5, "ü"]}, pause_notices=[{"episode": 1}]))
    write_result(root, result("done1", "failed", superseded_by="done1-retry"))
    atomic_write_json(task_result_path(root, "legacy"), {"task_id": "legacy", "status": "completed"})  # unstamped
    atomic_write_json(task_result_path(root, "mismatch"), result("other"))
    o.add(root, "nonterminal", "root1", {"task_id": "root1"})
    o.add(root, "pending_drives", "root1", {"task_id": "root1"})
    o.add(root, "custody_open", "run:r1", {"task_id": "root1", "custody": "{}"})
    o.add(root, "custody_open", "invocation:i1", {"task_id": "root1", "request": {"x": 1}})
    o.add(root, "pause_notices", "child1", {"task_id": "child1"})
    for name in o.BOOT_SETS:  # every boot classification leaves every set file behind
        if not o.path(root, name).exists():
            atomic_write_json(o.path(root, name), {})
    drive = root / "state" / "headless_tasks" / "root1" / "data"
    write_result(drive, result("root1", "completed", retry_task_id="root1-r2"))
    write_result(drive, result("root1-r2", "running"))
    return root


def snapshot(root: pathlib.Path) -> dict:
    files = {}
    for path in sorted((root / "task_results").glob("*.json")) + sorted((root / "state" / "obligations").glob("*.json")):
        files[str(path.relative_to(root))] = path.read_bytes()
    return files


def meta(root: pathlib.Path, key: str):
    with store.open_store(root, write=False) as txn:
        return txn.meta(key)


def load(root: pathlib.Path, task_id: str):
    with store.open_store(root, write=False) as txn:
        return txn.load(task_id)


def rows(path: pathlib.Path, kind: str) -> list:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if json.loads(line).get("type") == kind]


# ---- publication ------------------------------------------------------------------

def test_the_import_publishes_stamps_quarantines_and_covers_child_roots(tmp_path):
    root = fixture_root(tmp_path)
    before = snapshot(root)
    report = ensure_current_store(root)
    assert report["status"] == "completed" and report["results"] == 3
    assert report["quarantined"] == 1 and report["refused"] == 1
    assert store.probe(root) == ("present", store.TIER_ENFORCED)
    assert store.generation_stamped(root)
    provenance = meta(root, "import")
    assert provenance["refused"] == {"mismatch": "task_id_mismatch"} and provenance["lock_tier"] == "enforced"
    baseline = meta(root, "baseline")
    assert set(baseline) == {"root1", "child1", "done1", "mismatch"}
    assert baseline["mismatch"] == {**baseline["mismatch"], "refused": "task_id_mismatch"}
    for task_id, entry in baseline.items():
        raw = task_result_path(root, task_id).read_bytes()
        assert entry["sha256"] == hashlib.sha256(raw).hexdigest() and entry["size"] == len(raw)
    assert load(root, "child1")["nested"] == {"k": [1, 2.5, "ü"]}
    with store.open_store(root, write=False) as txn:
        assert txn.set_rows("custody_open") == {"run:r1": {"task_id": "root1", "custody": "{}"},
                                                "invocation:i1": {"task_id": "root1", "request": {"x": 1}}}
        row = txn.conn.execute("SELECT * FROM tasks WHERE task_id='root1'").fetchone()
        assert (row["status"], row["pending_drive"], row["root_task_id"]) == ("running", 1, "root1")
        assert txn.conn.execute("SELECT has_pause_notices FROM tasks WHERE task_id='child1'").fetchone()[0] == 1
    # the unstamped file moved to quarantine, once, with one durable event; the mismatch stayed in place
    assert not task_result_path(root, "legacy", create=False).exists()
    assert (root / "task_results" / "quarantine" / "legacy.json").exists()
    assert task_result_path(root, "mismatch", create=False).exists()
    events = rows(root / "logs" / "events.jsonl", "task_results_quarantined")
    assert len(events) == 1 and events[0]["count"] == 1
    # child roots: their own store by the same code, retry rows included
    drive = root / "state" / "headless_tasks" / "root1" / "data"
    assert report["child_roots"] == {str(drive): "completed"}
    with store.open_store(drive, write=False) as txn:
        assert txn.task_ids() == ["root1", "root1-r2"]
    assert [row["phase"] for row in rows(root / "logs" / "supervisor.jsonl", "current_store_import")] == ["completed"]
    # the frozen files are untouched by the import
    after = snapshot(root)
    assert {k: v for k, v in before.items() if "legacy" not in k} == after
    # idempotent: a second boot finds the store and nothing to reconcile
    assert ensure_current_store(root)["status"] == "unchanged"


def test_a_crash_before_publication_is_redone_from_scratch(tmp_path, monkeypatch):
    root = fixture_root(tmp_path)
    build = store_import._build

    def crash(tmp, *args, **kwargs):
        build(tmp, *args, **kwargs)
        raise KeyboardInterrupt("the process died before the rename")

    monkeypatch.setattr(store_import, "_build", crash)
    with pytest.raises(KeyboardInterrupt):
        ensure_current_store(root, with_children=False)
    assert store.probe(root) == ("absent", None) and not store.generation_stamped(root)
    stale = root / "state" / "current.sqlite.import-99999-deadbeef"
    stale.write_bytes(b"a sibling a crashed process left behind")
    monkeypatch.setattr(store_import, "_build", build)
    assert ensure_current_store(root, with_children=False)["status"] == "completed"
    assert not stale.exists()


def test_a_crash_after_publication_before_the_stamp_is_closed_by_the_next_boot(tmp_path, monkeypatch):
    from ouroboros import startup_migrations

    root = fixture_root(tmp_path)
    real_stamp = startup_migrations.stamp

    def no_generation_stamp(target, **facts):
        if facts.get(GENERATION_KEY) == GENERATION:
            raise KeyboardInterrupt("died after publishing, before the stamp")
        return real_stamp(target, **facts)

    monkeypatch.setattr(startup_migrations, "stamp", no_generation_stamp)
    with pytest.raises(KeyboardInterrupt):
        ensure_current_store(root, with_children=False)
    assert store.probe(root)[0] == "present" and not store.generation_stamped(root)
    monkeypatch.setattr(startup_migrations, "stamp", real_stamp)
    assert ensure_current_store(root, with_children=False)["status"] == "unchanged"  # accepted by its own meta
    assert store.generation_stamped(root)


def test_a_missing_store_beside_its_stamp_is_lost_and_only_the_explicit_rebuild_recovers(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    store.store_path(root).unlink()
    with pytest.raises(StoreLost):
        ensure_current_store(root, with_children=False)
    assert ensure_current_store(root, with_children=False, rebuild=True)["status"] == "completed"
    assert store.probe(root)[0] == "present"


def test_the_explicit_rebuild_sets_a_published_store_aside(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    assert ensure_current_store(root, with_children=False, rebuild=True)["status"] == "completed"
    assert [p.name for p in (root / "state").glob("current.sqlite.stale-*")]


def test_the_cancel_latch_migration_runs_first_so_a_latched_row_is_imported_not_quarantined(tmp_path):
    root = fixture_root(tmp_path)
    atomic_write_json(task_result_path(root, "wedged"), {"task_id": "wedged", "status": "cancel_requested"})
    report = ensure_current_store(root, with_children=False)
    assert report["quarantined"] == 1  # still only the legacy row
    assert load(root, "wedged")["status"] == "cancel_requested"
    assert "wedged" in (root / "state" / "cancel_intents.json").read_text(encoding="utf-8")


def test_the_name_tier_is_recorded_from_the_boot_probe(tmp_path, monkeypatch):
    from ouroboros import platform_layer

    monkeypatch.setattr(platform_layer, "kernel_file_locks_enforced", lambda path: False)
    root = fixture_root(tmp_path)  # built under the same tier the probe reports (one tier per directory)
    assert ensure_current_store(root, with_children=False)["lock_tier"] == store.TIER_NAME
    assert store.probe(root) == ("present", store.TIER_NAME)
    assert load(root, "done1")["status"] == "failed"  # readable through the name lock


# ---- export -------------------------------------------------------------------------

def test_the_export_reproduces_every_file_byte_for_byte(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root)
    drive = root / "state" / "headless_tasks" / "root1" / "data"
    expected, expected_child = snapshot(root), snapshot(drive)
    for path in list((root / "task_results").glob("*.json")) + list((root / "state" / "obligations").glob("*.json")):
        if path.stem != "mismatch":
            path.unlink()
    report = export_facts(root)
    child = export_facts(drive)
    assert (report["results"], report["written"]) == (3, 3) and child["results"] == 2
    assert snapshot(root) == expected
    # a child drive never booted a classification, so it gains the full set of files; its rows are exact
    assert {k: v for k, v in snapshot(drive).items() if k.startswith("task_results")} == expected_child
    nonterminal = json.loads((drive / "state" / "obligations" / "nonterminal.json").read_text(encoding="utf-8"))
    assert list(nonterminal) == ["root1-r2"]
    assert export_facts(root)["written"] == 0  # identical bytes are left alone
    assert ensure_current_store(root)["status"] == "unchanged"  # the baseline followed the export


def test_the_export_writes_derived_sets_in_full_from_the_columns(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    store.update_task(root, "done1", lambda current: {**current, "status": "running"})
    (root / "state" / "obligations" / "nonterminal.json").unlink()
    export_facts(root)
    nonterminal = json.loads((root / "state" / "obligations" / "nonterminal.json").read_text(encoding="utf-8"))
    assert list(nonterminal) == ["root1", "done1"]  # the stored row first, then the derived member
    pending = json.loads((root / "state" / "obligations" / "pending_drives.json").read_text(encoding="utf-8"))
    assert set(pending) == {"root1", "done1"}


# ---- per-row reconciliation (P6') -------------------------------------------------------

def test_a_changed_file_replaces_its_row_and_a_touched_file_does_not(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    path = task_result_path(root, "done1")
    os.utime(path, ns=(1, 1))  # timestamps alone prove nothing
    assert ensure_current_store(root, with_children=False)["status"] == "unchanged"
    atomic_write_json(path, result("done1", "completed", note="an old body wrote this"))
    report = ensure_current_store(root, with_children=False)
    assert report["status"] == "reconciled" and report["changed"] == 1 and report["conflicts"] == 0
    assert load(root, "done1")["note"] == "an old body wrote this"
    assert meta(root, "baseline")["done1"]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert ensure_current_store(root, with_children=False)["status"] == "unchanged"


def test_a_new_file_is_imported_and_a_deleted_file_moves_its_row_to_quarantine(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    write_result(root, result("fresh1"))
    task_result_path(root, "child1").unlink()
    report = ensure_current_store(root, with_children=False)
    assert (report["new"], report["deleted"]) == (1, 1)
    assert load(root, "fresh1")["status"] == "completed" and load(root, "child1") is None
    copies = list((root / "task_results" / "quarantine").glob("child1.deleted-*.json"))
    assert len(copies) == 1 and json.loads(copies[0].read_text(encoding="utf-8"))["nested"] == {"k": [1, 2.5, "ü"]}
    events = rows(root / "logs" / "events.jsonl", "task_results_quarantined")
    assert events[-1]["reasons"] == {"deleted_by_old_body": 1}
    assert "child1" not in meta(root, "baseline")


def test_a_row_changed_on_both_sides_lets_the_file_win_and_keeps_the_db_version(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    store.update_task(root, "done1", lambda current: {**current, "db_side": True})
    atomic_write_json(task_result_path(root, "done1"), result("done1", "completed", file_side=True))
    report = ensure_current_store(root, with_children=False)
    assert report["conflicts"] == 1
    assert load(root, "done1") == result("done1", "completed", file_side=True)
    conflict = meta(root, "conflicts")["done1"]
    assert json.loads(conflict["db_body"])["db_side"] is True
    # a db-only change with an untouched file is kept (no conflict, no replacement)
    store.update_task(root, "child1", lambda current: {**current, "db_only": True})
    assert ensure_current_store(root, with_children=False)["status"] == "unchanged"
    assert load(root, "child1")["db_only"] is True


def test_set_file_edits_alone_are_not_detected_but_follow_a_result_change(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    o.add(root, "custody_open", "run:r2", {"task_id": "done1"})
    assert ensure_current_store(root, with_children=False)["status"] == "unchanged"  # the named limit
    with store.open_store(root, write=False) as txn:
        assert "run:r2" not in txn.set_rows("custody_open")
    write_result(root, result("fresh2"))
    ensure_current_store(root, with_children=False)
    with store.open_store(root, write=False) as txn:
        assert "run:r2" in txn.set_rows("custody_open")


def test_an_inadmissible_changed_file_is_quarantined_with_its_row(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    path = task_result_path(root, "done1")
    path.write_text("{not json", encoding="utf-8")
    report = ensure_current_store(root, with_children=False)
    assert report["quarantined"] == 1 and load(root, "done1") is None
    assert not path.exists() and (root / "task_results" / "quarantine" / "done1.json").exists()


# ---- a full rebuild only after a proven complete export ---------------------------------

def test_an_unfinished_export_keeps_the_store_as_authority(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    with store.open_store(root, write=True) as txn:
        txn.set_meta("phase", store.EXPORTING)
        txn.set_meta("export", {"status": "exporting"})
    write_result(root, result("fresh3"))  # a change an old body could never have made here
    report = ensure_current_store(root, with_children=False)
    assert (report["status"], report["phase"]) == ("switch_pending", "exporting")
    assert store.probe(root)[0] == "present" and load(root, "fresh3") is None


def test_a_completed_export_whose_retirement_never_finished_is_set_aside_and_rebuilt(tmp_path):
    root = fixture_root(tmp_path)
    ensure_current_store(root, with_children=False)
    with store.open_store(root, write=True) as txn:
        txn.set_meta("phase", store.RETIRED)
        txn.set_meta("export", {"status": "completed"})
    write_result(root, result("fresh4"))
    report = ensure_current_store(root, with_children=False)
    assert report["status"] == "completed" and report["results"] == 4
    assert load(root, "fresh4")["status"] == "completed"
    aside = list((root / "state").glob("current.sqlite.exported-*"))
    assert len(aside) == 1
    with sqlite3.connect(aside[0]) as conn:
        assert json.loads(conn.execute("SELECT value FROM meta WHERE key='phase'").fetchone()[0]) == "retired"
