"""Packing consumes current store facts and task-local obligation revisions."""
from ouroboros import evidence_packer as packer, evidence_packs as packs, obligations, store, task_store
from tests.test_evidence_packs import _task, _result
from tests.test_evidence_packs import root as root


def test_store_only_task_settles_and_retries_after_a_row_write(root):
    _task(root, "retry", blobs=1, status="running", updated_at="2026-01-01T00:00:00Z")
    assert packer.run_pass(root)["packed"] == 0
    assert "retry" not in packer.changed_tasks(root)
    _result(root, "retry", updated_at="2026-01-01T00:00:00Z")
    assert not (root / "task_results/retry.json").exists()
    assert "retry" in packer.changed_tasks(root)
    assert packs.task_settled(root, "retry")
    assert packer.run_pass(root)["packed"] == 1


def test_owner_row_revision_is_task_local_and_ignores_receipts(root):
    _task(root, "a", blobs=1)
    _task(root, "b", blobs=1)
    initial = packer.task_stamps(root, "a")
    result_revision = task_store.signature(root, "a")
    obligations.add(root, "custom_duty", "run:1", {"request": {"task_id": "a"}, "phase": 1})
    first = packer.task_stamps(root, "a")
    assert first != initial and task_store.signature(root, "a") == result_revision
    assert not packs.task_settled(root, "a") and packs.task_settled(root, "b")
    obligations.add(root, "custom_duty", "run:1", {"request": {"task_id": "a"}, "phase": 2})
    second = packer.task_stamps(root, "a")
    assert second != first
    obligations.add(root, "custom_duty", "run:2", {"task_id": "b"})
    for name in obligations.RECEIPT_SETS:
        obligations.add(root, name, "a", {})
    assert packer.task_stamps(root, "a") == second
    obligations.remove(root, "custom_duty", "run:1")
    assert packer.task_stamps(root, "a") == initial and packs.task_settled(root, "a")


def test_candidate_census_reads_each_set_once_for_all_tasks(root, monkeypatch):
    for number in range(24):
        _task(root, f"t{number}", blobs=1, status="running")
        obligations.add(root, "custom_duty", f"run:{number}", {"task_id": f"t{number}"})
    reads = []
    original = obligations.members_in

    def read(txn, name):
        reads.append(name)
        return original(txn, name)

    monkeypatch.setattr(obligations, "members_in", read)
    assert packer.run_pass(root)["packed"] == 0
    assert reads and len(reads) == len(set(reads))


def test_store_read_failure_is_unknown_and_recovers(root, monkeypatch):
    _task(root, "a", blobs=1)
    with monkeypatch.context() as patch:
        def unavailable(*args, **kwargs):
            raise store.StoreBusy("simulated unavailable facts")
        patch.setattr(obligations, "members_in", unavailable)
        assert "unknowns" in packs.task_settlement_facts(root, "a")["obligations"]
        assert not packs.task_settled(root, "a")
    assert packs.task_settled(root, "a")
