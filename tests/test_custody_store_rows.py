"""Delegated custody on the store (DELTA §3): the reducer's rows and the outbox around every
death point, the pending invocation's atomic transition, the open set, the export seam's
outbox verification and the importer's one fold of a legacy chain."""
from __future__ import annotations

import json
from contextlib import contextmanager

import pytest

from ouroboros import delegate_custody as c
from ouroboros import delegate_custody_current as current
from ouroboros import obligations as o
from ouroboros import store
from ouroboros.utils import append_jsonl


def _chain(root):
    path = c.event_log_path(root)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _writes(monkeypatch):
    """Count the store's write transactions (the reducing transaction is exactly one)."""
    opened = []
    real = store.open_store

    @contextmanager
    def tracked(root, **kwargs):
        with real(root, **kwargs) as txn:
            opened.append(bool(kwargs.get("write")))
            yield txn

    monkeypatch.setattr(store, "open_store", tracked)
    return opened


def test_a_death_between_the_append_and_the_outbox_delete_replays_the_event_once(tmp_path, monkeypatch):
    """P8 death point: the append landed, the process died before the outbox row was deleted.
    The next flush appends the same ``event_id`` again; the forensic fold and the importer
    dedup it, so the chain folds to exactly the rows."""
    assert c.emit(tmp_path, c.STARTED, {"run_id": "run", "task_id": "task"})

    @contextmanager
    def dies_after_append(root, **kwargs):
        if kwargs.get("write"):
            raise store.StoreBusy("process died before the outbox delete")
        with real(root, **kwargs) as txn:
            yield txn

    real = store.open_store
    with monkeypatch.context() as patched:
        settled = {"ts": "2026-01-01T00:00:00+00:00", "type": c.SETTLED, "event_id": "e" * 32,
                   "run_id": "run", "task_id": "task", "state": "succeeded"}
        with real(tmp_path, write=True) as txn:  # the reducing transaction committed...
            current._reduce(txn, settled, current._loaded(txn, settled), {})
            txn.outbox_put(settled)
        patched.setattr(store, "open_store", dies_after_append)
        assert current.flush_outbox(tmp_path) == 0  # ...the append landed, the delete did not
    assert [row["type"] for row in _chain(tmp_path)] == [c.STARTED, c.SETTLED]
    assert current.outbox_depth(tmp_path) == 1
    assert current.flush_outbox(tmp_path) == 1  # the next flush replays the parked event
    assert current.outbox_depth(tmp_path) == 0
    duplicated = _chain(tmp_path)
    assert [row["event_id"] for row in duplicated][1:] == ["e" * 32, "e" * 32]
    forensic = c.custody_rows(tmp_path)
    assert [row["type"] for row in forensic] == [c.STARTED, c.SETTLED], "one row per event id"
    assert vars(c.replay(tmp_path, rows=list(forensic))["run"]) == vars(c.replay(tmp_path)["run"])
    assert current.fold(duplicated)["runs"]["run"][0]["settled"] is True


def test_a_pending_invocation_becomes_its_run_or_its_refusal_in_one_transaction(tmp_path, monkeypatch):
    request = {"invocation_id": "inv", "task_id": "task", "route": "codex", "surface": "delegate",
               "request": {"prompt": "work"}, "billing_group": {"billing_group_id": "group-1"}}
    assert c.emit(tmp_path, c.START_REQUESTED, request)
    assert [row["invocation_id"] for row in c.pending_invocations(tmp_path)] == ["inv"]
    assert c.invocation_record(tmp_path, "inv")["state"] == "pending"

    writes, seen = _writes(monkeypatch), []

    def append(path, row):  # the first observation after the reducing transaction committed
        with store.open_store(tmp_path, write=False) as txn:
            seen.append((txn.get_row("custody_open", "invocation:inv"),
                         txn.custody_rows("invocation_id=?", ("inv",))["run"]["request_event"]["request"]))
        return real_append(path, row)

    real_append = current.append_jsonl
    monkeypatch.setattr(current, "append_jsonl", append)
    assert c.emit(tmp_path, c.STARTED, {"run_id": "run", "task_id": "task", "invocation_id": "inv"})
    assert writes[0] is True and seen == [(None, {"prompt": "work"})], \
        "the pending row leaves and the run row lands in the ONE reducing transaction"
    record = c.invocation_record(tmp_path, "inv")
    assert record["state"] == "started" and record["run_id"] == "run" and record["request"] == {"prompt": "work"}
    assert c.replay(tmp_path)["run"].billing_group == {"billing_group_id": "group-1"}
    assert current.tree_billing_group(tmp_path, "task") is None  # the run names no tree root

    assert c.emit(tmp_path, c.START_REQUESTED, {**request, "invocation_id": "inv-2"})
    assert c.emit(tmp_path, c.START_FAILED, {"invocation_id": "inv-2", "task_id": "task", "error": "transient"})
    assert c.invocation_record(tmp_path, "inv-2")["state"] == "pending", "a transient failure keeps the request"
    assert c.emit(tmp_path, c.START_FAILED, {"invocation_id": "inv-2", "task_id": "task", "definite": True,
                                             "error": "refused"})
    assert c.pending_invocations(tmp_path) == []
    refused = c.invocation_record(tmp_path, "inv-2")
    assert refused["state"] == "failed_definite" and refused["request"] == {"prompt": "work"}
    assert "inv-2" in o.members(tmp_path, "custody_refused")


def test_the_open_set_is_the_held_runs_and_the_pending_invocations(tmp_path):
    assert c.emit(tmp_path, c.STARTED, {"run_id": "live", "task_id": "a"})
    assert c.emit(tmp_path, c.STARTED, {"run_id": "owner", "task_id": "b", "project_id": "p", "project_owned": True})
    assert c.emit(tmp_path, c.SETTLED, {"run_id": "owner", "task_id": "b", "state": "succeeded"})
    assert c.emit(tmp_path, c.STARTED, {"run_id": "done", "task_id": "c"})
    assert c.emit(tmp_path, c.SETTLED, {"run_id": "done", "task_id": "c", "state": "succeeded"})
    assert c.emit(tmp_path, c.START_REQUESTED, {"invocation_id": "inv", "task_id": "d", "request": {"prompt": "x"}})

    view = current.held_snapshot(tmp_path)
    assert set(view["runs"]) == {"live", "owner"}, "unsettled, or settled with an owned registration"
    assert [row["invocation_id"] for row in view["pending"]] == ["inv"]
    assert set(c.replay(tmp_path)) == {"live", "owner", "done"}
    with store.open_store(tmp_path, write=False) as txn:
        assert {rid: facts["settled"] for rid, facts in txn.custody_rows("state=?", (current.CLOSED,)).items()} == {"done": True}
    assert current.task_snapshot(tmp_path, "d") == {"runs": {}, "pending": view["pending"]}
    assert set(current.task_snapshot(tmp_path, "c")["runs"]) == {"done"}


def test_the_export_preflight_flushes_the_outbox_and_refuses_a_stuck_one(tmp_path, monkeypatch):
    from ouroboros import store_switch
    from ouroboros.startup_migrations import prepare_startup_state

    prepare_startup_state(tmp_path)
    needs = {"facts": True, "money": False, "handles": False}
    with monkeypatch.context() as patched:
        patched.setattr(current, "append_jsonl", lambda *a, **kw: False)
        assert c.emit(tmp_path, c.STARTED, {"run_id": "run", "task_id": "task"})
        assert current.outbox_depth(tmp_path) == 1 and _chain(tmp_path) == []
        refusal = store_switch._preflight(tmp_path, needs, {"steps": {}}, [tmp_path])
        assert "custody outbox" in refusal and "1 unappended" in refusal
        assert current.outbox_depth(tmp_path) == 1, "a refused preflight moves nothing"
    assert store_switch._preflight(tmp_path, needs, {"steps": {}}, [tmp_path]) == ""
    assert current.outbox_depth(tmp_path) == 0
    assert [row["type"] for row in _chain(tmp_path)] == [c.STARTED]
    # a money-only export does not touch the chain
    with monkeypatch.context() as patched:
        patched.setattr(current, "flush_outbox", lambda root: pytest.fail("flushed on a chainless export"))
        assert store_switch._preflight(tmp_path, {"facts": False, "money": False, "handles": False}, {"steps": {}}, []) == ""


def test_the_import_folds_a_legacy_chain_into_the_custody_tables_once(tmp_path, monkeypatch):
    """A tree an old body wrote: no store, a retained chain. The first touch folds the chain
    once into ``custody_runs``, the pending invocations, the refusals and the task marks."""
    from ouroboros.store_import import ensure_current_store

    path = c.event_log_path(tmp_path)
    for row in (
        {"type": c.START_REQUESTED, "invocation_id": "inv-a", "task_id": "t1", "request": {"prompt": "a"}},
        {"type": c.STARTED, "run_id": "run-a", "task_id": "t1", "invocation_id": "inv-a", "root_task_id": "root"},
        {"type": c.SETTLED, "run_id": "run-a", "task_id": "t1", "state": "succeeded", "spendUsd": 0.5},
        {"type": c.START_REQUESTED, "invocation_id": "inv-b", "task_id": "t2", "request": {"prompt": "b"}},
        {"type": c.START_REQUESTED, "invocation_id": "inv-c", "task_id": "t3", "request": {"prompt": "c"}},
        {"type": c.START_FAILED, "invocation_id": "inv-c", "task_id": "t3", "definite": True},
        {"type": c.STARTED, "run_id": "run-d", "task_id": "t4", "root_task_id": "root"},
        {"type": c.CONTAINMENT_FAULT, "run_id": "run-x", "task_id": "t5", "reason": "cancel_unverified"},
        {"type": "delegate_run_patch_verdict", "task_id": "t4", "run_id": "run-d", "verdict": "accepted"},
    ):
        append_jsonl(path, {"ts": "2026-01-01T00:00:00+00:00", **row})
    folds = []
    real_fold = current.fold
    monkeypatch.setattr(current, "fold", lambda rows: folds.append(len(rows)) or real_fold(rows))
    ensure_current_store(tmp_path)
    assert folds == [9], "the chain is folded exactly once, by the importer"

    runs = c.replay(tmp_path)
    assert set(runs) == {"run-a", "run-d", "run-x"}
    assert runs["run-a"].settled and runs["run-a"].request_event["request"] == {"prompt": "a"}
    assert not runs["run-d"].settled and runs["run-x"].containment_fault["reason"] == "cancel_unverified"
    assert sorted(run.run_id for run in c.open_runs(tmp_path)) == ["run-d", "run-x"]
    assert [row["invocation_id"] for row in c.pending_invocations(tmp_path)] == ["inv-b"]
    assert c.invocation_record(tmp_path, "inv-c")["state"] == "failed_definite"
    assert current.marks(tmp_path, "t4")["patch_verdicts"][0]["verdict"] == "accepted"
    assert sorted(current.tree_task_ids(tmp_path, "root")) == ["t1", "t4"]
    with store.open_store(tmp_path, write=False) as txn:
        assert txn.meta("import")["custody_runs"] == 3
    # the chain is left as it was: the forensic read and the rows agree
    assert {row["run_id"] for row in c.custody_rows(tmp_path) if row.get("run_id")} == {"run-a", "run-d", "run-x"}
    assert vars(c.replay(tmp_path, rows=list(c.custody_rows(tmp_path)))["run-a"]) == vars(runs["run-a"])
