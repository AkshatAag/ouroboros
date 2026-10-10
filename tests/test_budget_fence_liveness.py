"""The root budget fence reads the dispatch document of the last successful publication.

A send needs the dispatch document's generation certified by ``queue.alive`` — never a
fresh stamp: a stalled supervisor tick leaves the certificate in place and must not
stop a single call. A publication in flight (the publisher unlinks the stamp first) is
re-read briefly; a certificate that stays missing or mismatched (a failed publication)
refuses typed, and an active fence still refuses whatever the stamp's age.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from ouroboros import queue_observation
from ouroboros import usage_accounting as ua

ROOT, CHILD = "root-1", "child-1"
OLD_TS = "2020-01-01T00:00:00+00:00"


def _write(root, name, payload):
    path = pathlib.Path(root) / "state" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _publish(root, *, fences=(), alive_generation="g1"):
    _write(root, "queue_dispatch.json", {"generation": "g1", "ts": OLD_TS, "budget_root_fences": list(fences), "members": {}})
    if alive_generation is not None:
        _write(root, "queue.alive", {"generation": alive_generation, "ts": OLD_TS, "heartbeats": {}})


def _reserve(root, task_id):
    with ua.usage_scope(ua.UsageScope(drive_root=root, task_id=task_id, root_task_id=ROOT)):
        return ua.reserve_attempt(ua.AttemptRequest(model="m", provider="p", task_id=task_id))


@pytest.fixture(autouse=True)
def _tree(tmp_path, monkeypatch):
    from ouroboros.task_results import STATUS_RUNNING, write_task_result

    monkeypatch.setattr(ua, "_reservation_cost", lambda _request: 0.01)
    monkeypatch.setattr(queue_observation, "_sleep", lambda _sec: None)
    write_task_result(tmp_path, ROOT, STATUS_RUNNING, chat_id=0, root_task_id=ROOT)
    write_task_result(tmp_path, CHILD, STATUS_RUNNING, chat_id=0, root_task_id=ROOT, parent_task_id=ROOT)


def test_an_old_but_certified_publication_admits_an_unfenced_send(tmp_path):
    _publish(tmp_path)  # both stamps are years old: age never gates a send
    assert _reserve(tmp_path, CHILD).attempt_id


def test_an_active_fence_refuses_whatever_the_stamp_age(tmp_path):
    _publish(tmp_path, fences=[{"root_task_id": ROOT, "status": "active", "fence_id": "F1"}])
    with pytest.raises(ua.BudgetExceeded, match="paused pending explicit resume"):
        _reserve(tmp_path, CHILD)


@pytest.mark.parametrize("alive_generation", [None, "g0"])
def test_a_failed_publication_refuses_typed(tmp_path, alive_generation):
    _publish(tmp_path, alive_generation=alive_generation)  # unlinked stamp, or one of an older publication
    with pytest.raises(ua.UsageAccountingError, match="root budget fence authority unavailable"):
        _reserve(tmp_path, CHILD)


def test_a_publication_in_flight_is_waited_out(tmp_path, monkeypatch):
    _publish(tmp_path, alive_generation=None)
    waits = []

    def publisher_finishes(_sec):
        waits.append(_sec)
        _write(tmp_path, "queue.alive", {"generation": "g1", "ts": OLD_TS, "heartbeats": {}})

    monkeypatch.setattr(queue_observation, "_sleep", publisher_finishes)
    assert _reserve(tmp_path, CHILD).attempt_id and waits


def test_an_unreadable_dispatch_document_refuses_typed(tmp_path):
    _publish(tmp_path)
    (tmp_path / "state" / "queue_dispatch.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(ua.UsageAccountingError, match="root budget fence authority unavailable"):
        _reserve(tmp_path, CHILD)
