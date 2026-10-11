"""Task-local wakeups and operation counts on real evidence packs, never wall time."""
import json
from unittest.mock import patch

import pytest

from ouroboros import evidence_packer as packer, evidence_packs as packs, usage_store
from supervisor import terminal_delivery
from tests.test_evidence_packs import REPO, _result, _task
from tests.test_evidence_packs import root as root


def _reserve(root, task):
    with usage_store.hold(root, write=True) as txn:
        txn.write({"attempt_id": f"attempt-{task}", "kind": "attempt", "state": "reserved",
                   "task_id": task, "root_task_id": task, "model": "offline", "provider": "local",
                   "reservation_upper_bound_usd": 0.01, "pricing_known": True})


def _settle_money(root, task):
    with usage_store.hold(root, write=True) as txn:
        previous = txn.attempt(f"attempt-{task}")
        txn.write({**previous, "state": "released", "reason": "not_dispatched"}, previous)


@pytest.mark.parametrize("change", ["money", "delivery", "row", "evidence"])
def test_unrelated_changes_do_not_wake_swept_task(root, monkeypatch, change):
    _task(root, "packed", blobs=1)
    assert packer.run_pass(root)["unlinked"] == 2
    packer.run_pass(root)  # record the directory revision after our own unlink
    before = packer.task_stamps(root, "packed")
    unit = packer.EvidencePacker()
    assert not unit.tick(root, REPO)
    if change == "money":
        _reserve(root, "unrelated")
    elif change == "delivery":
        assert terminal_delivery.register_pending_delivery(
            root, {"delivery_id": "owed-unrelated", "task_id": "unrelated", "chat_id": 1})
    elif change == "row":
        _result(root, "unrelated", "running")
    else:
        _task(root, "unrelated", status="running", blobs=1)
        packer.record_swept(root, "unrelated", packer.task_stamps(root, "unrelated"))
    monkeypatch.setattr(packer, "spawn_maintenance_job", lambda *a: pytest.fail("unrelated change spawned a packer"))
    assert packer.task_stamps(root, "packed") == before
    assert packer.changed_tasks(root) == []
    assert not unit.tick(root, REPO)


@pytest.mark.parametrize("blocker", ["money", "delivery"])
def test_own_settlement_change_wakes_a_swept_task(root, blocker):
    _task(root, "later", blobs=1)
    if blocker == "money":
        _reserve(root, "later")
    else:
        assert terminal_delivery.register_pending_delivery(
            root, {"delivery_id": "owed-later", "task_id": "later", "chat_id": 1})
    assert packer.run_pass(root)["packed"] == 0
    assert packer.changed_tasks(root) == []
    if blocker == "money":
        _settle_money(root, "later")
    else:
        assert terminal_delivery.register_delivery(root, "owed-later")
    assert packer.changed_tasks(root) == ["later"]
    assert packer.run_pass(root)["unlinked"] == 2


@pytest.mark.parametrize("operation", ["changed_tasks", "run_pass", "tick"])
def test_money_and_deliveries_read_once_for_all_tasks(root, monkeypatch, operation):
    for number in range(12):
        _task(root, f"live-{number}", status="running", blobs=1)
    _reserve(root, "unrelated")
    statements, registry_reads = [], []
    original_connect = usage_store._connect
    from ouroboros import utils
    original_read = utils.read_json_dict

    def connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    def read(path, *args, **kwargs):
        if path.name == "terminal_deliveries.json":
            registry_reads.append(path)
        return original_read(path, *args, **kwargs)

    assert terminal_delivery.register_pending_delivery(
        root, {"delivery_id": "owed-live", "task_id": "live-0", "chat_id": 1})
    unit = packer.EvidencePacker()
    unit.tick(root, REPO)
    if operation == "tick":
        packer.run_pass(root)
    monkeypatch.setattr(usage_store, "_connect", connect)
    monkeypatch.setattr(utils, "read_json_dict", read)
    if operation == "tick":
        assert not unit.tick(root, REPO)
    else:
        getattr(packer, operation)(root)
    queries = [sql for sql in statements if "FROM attempts" in sql and sql.startswith("SELECT")]
    assert len(queries) == 1, queries
    assert len(registry_reads) == 1


def _measure(root, label):
    counts = {"case": label, "settlements": 0, "under_writer": 0, "verifies": 0}
    facts, verify = packs.task_settlement_facts, packs.verify_pack

    def observed_facts(*args, **kwargs):
        counts["settlements"] += 1
        counts["under_writer"] += int(kwargs.get("txn") is not None)
        return facts(*args, **kwargs)

    def observed_verify(*args, **kwargs):
        counts["verifies"] += 1
        return verify(*args, **kwargs)

    with patch.object(packs, "task_settlement_facts", observed_facts), patch.object(packs, "verify_pack", observed_verify):
        counts["report"] = packer.run_pass(root)
    print(json.dumps(counts, sort_keys=True), flush=True)
    assert counts["report"]["faults"] == 0
    return counts


@pytest.mark.parametrize("n", [12, 24, 48])
def test_measure_backfill_and_unrelated_money(root, n):
    usage_store.migrate_from_journal(root)
    for number in range(n):
        _task(root, f"settled-{number}", blobs=1)
    backfill = _measure(root, f"backfill-{n}")
    assert backfill["report"]["unlinked"] == 2 * n
    _measure(root, f"after-unlink-{n}")
    assert _measure(root, f"stable-{n}")["settlements"] == 0
    _reserve(root, "unrelated")
    print(json.dumps({"case": f"changed-after-money-{n}", "candidates": len(packer.changed_tasks(root))}), flush=True)
    _measure(root, f"unrelated-money-{n}")
