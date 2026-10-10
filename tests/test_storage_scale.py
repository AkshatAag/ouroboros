"""Operation baselines for the facts move; no wall-clock performance gates."""

import json
from types import SimpleNamespace

import pytest

from ouroboros.task_finalization import build_swarm_efficiency
from ouroboros.task_results import list_task_results, write_task_result
from ouroboros.task_status import find_child_tasks
from ouroboros.utils import append_jsonl

pytestmark = pytest.mark.scale


def observations(root, operation):
    rows = [json.loads(line) for line in (root / "logs/events.jsonl").read_text(encoding="utf-8").splitlines()]
    return [row for row in rows if row.get("type") == "storage_read_timing" and row["operation"] == operation]


@pytest.mark.parametrize("count", [1, 64, 256])
def test_listing_counts_every_decode_even_when_status_filter_discards(tmp_path, count):
    for index in range(count):
        write_task_result(tmp_path, f"task-{index}", "completed")
    assert list_task_results(tmp_path, statuses=["running"]) == []
    first = observations(tmp_path, "list_task_results")[-1]
    assert first["operations"] == {"directory_scans": 1, "entries": count, "decode_attempts": count}
    assert first["calls"] == 1 and first["seconds"] >= 0 and not first["failed"]
    assert first["caller"].endswith(":test_listing_counts_every_decode_even_when_status_filter_discards")
    assert len(list_task_results(tmp_path)) == count
    assert len(observations(tmp_path, "list_task_results")) == 2


def test_child_lookup_measures_cold_and_warm_selection_phases(tmp_path):
    for index in range(32):
        write_task_result(tmp_path, f"task-{index}", "completed", delegation_role="subagent",
                          parent_task_id="parent" if index == 0 else "other", root_task_id="parent" if index == 0 else "other")
    for _ in range(2):
        assert [row["task_id"] for row in find_child_tasks(tmp_path, parent_task_id="parent", materialize_artifacts=False)] == ["task-0"]
    cold, warm = observations(tmp_path, "find_child_tasks")
    assert cold["operations"]["fact_decodes"] == 32
    assert warm["operations"].get("fact_decodes", 0) == 0
    for row in (cold, warm):
        assert row["operations"]["fact_entries"] == 32
        assert row["operations"]["selected_decodes"] == 1
        assert set(row["phases"]) == {"select", "effective", "queue_overlay"}
        assert all(phase["calls"] == 1 and phase["seconds"] >= 0 for phase in row["phases"].values())


def test_swarm_scan_and_depth_are_separate_observations(tmp_path):
    append_jsonl(tmp_path / "logs/events.jsonl", {"type": "swarm_fanout", "parent_task_id": "parent", "task_ids": ["child"]})
    rollup = build_swarm_efficiency(SimpleNamespace(drive_root=tmp_path), {"id": "parent"})
    assert rollup["subagent_count"] == 1
    row = observations(tmp_path, "build_swarm_efficiency")[-1]
    assert row["operations"]["event_rows"] == 1
    assert set(row["phases"]) == {"fanout_scan", "depth"}
    assert observations(tmp_path, "find_child_tasks")[-1]["calls"] == 1


def test_failure_is_counted_without_changing_exception_and_next_call_is_clean(tmp_path, monkeypatch):
    from ouroboros import task_results

    write_task_result(tmp_path, "task", "completed")
    with monkeypatch.context() as broken:
        def refuse(*args, **kwargs):
            raise OSError("fixture unreadable")
        broken.setattr(task_results, "read_json_dict", refuse)
        with pytest.raises(OSError, match="fixture unreadable"):
            list_task_results(tmp_path)
    assert observations(tmp_path, "list_task_results")[-1]["failed"]
    assert len(list_task_results(tmp_path)) == 1
    assert not observations(tmp_path, "list_task_results")[-1]["failed"]


def test_journal_failure_cannot_change_listing(tmp_path, monkeypatch):
    from ouroboros import storage_observation

    write_task_result(tmp_path, "task", "completed")
    monkeypatch.setattr(storage_observation, "append_jsonl", lambda *args: (_ for _ in ()).throw(OSError("fixture full")))
    assert len(list_task_results(tmp_path)) == 1
