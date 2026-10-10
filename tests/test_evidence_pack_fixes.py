"""Regressions for the dispositioned S5 pack and reader findings."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import pathlib
import sqlite3

import pytest

from ouroboros import evidence_packer as packer
from ouroboros import evidence_packs as packs
from ouroboros import observability as obs
from ouroboros.artifacts import read_actor_source_bytes, store_actor_source_bytes
from tests.test_evidence_packs import _blob, _handle_path, _loose, _result, _task
from tests.test_evidence_packs import root as root


def test_windows_busy_retry_reverifies_disk_before_unlink(root, monkeypatch):
    refs, handle = _task(root, "busy", blobs=1)
    loose_paths = {pathlib.Path(refs[0]["path"]), _handle_path(root, "busy", handle)}
    original_unlink = os.unlink

    def busy(path, *args, **kwargs):
        if pathlib.Path(path) in loose_paths:
            raise PermissionError("simulated Windows sharing violation")
        return original_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", busy)
        assert packer.run_pass(root)["busy"] == 2
    packed = packs.packs_dir(root) / "busy.xz"
    intact = packed.read_bytes()
    packed.write_bytes(b"damaged pack")
    failed = packer.run_pass(root)
    assert failed["faults"] == 1 and failed["unlinked"] == 0
    assert _loose(refs[0]) and _handle_path(root, "busy", handle).exists()

    packed.write_bytes(intact)
    verified = []
    original_verify = packs.verify_pack

    def verify(path, sidecar):
        original_verify(path, sidecar)
        verified.append(path)

    monkeypatch.setattr(packs, "verify_pack", verify)
    passed = packer.run_pass(root)
    assert passed["faults"] == 0 and passed["unlinked"] == 2
    assert verified == [packed]
    assert not any(path.exists() for path in loose_paths)


@pytest.mark.parametrize("damage", ["{", '{"format_version": 1}', "[]"])
def test_bad_sidecar_refuses_restore_and_packing_without_hiding_neighbour(root, damage):
    refs, handle = _task(root, "broken", blobs=1)
    other, _ = _task(root, "healthy", blobs=1)
    packer.run_pass(root)
    directory = packs.packs_dir(root)
    original = (directory / "broken.xz").read_bytes()
    sidecar = directory / "broken.members.json"
    intact = sidecar.read_bytes()
    sidecar.write_text(damage, encoding="utf-8")
    for operation in (packs.pending_restore_bytes, packs.restore_checkpoint_handles):
        with pytest.raises(OSError, match="evidence_corrupt"):
            operation(root)
    _task(root, "broken", blobs=1, payloads=[b"new evidence"])
    with pytest.raises(OSError, match="evidence_corrupt"):
        packs.pack_task(root, "broken", keep_shas=set())
    assert (directory / "broken.xz").read_bytes() == original
    (directory / packs.INDEX_NAME).unlink()
    assert packs.blob_bytes(root, other[0]).startswith(b'{"message"')
    with pytest.raises(OSError, match="evidence_corrupt"):
        packs.blob_bytes(root, refs[0])

    sidecar.write_bytes(intact)
    assert packs.pending_restore_bytes(root) > 0
    assert packs.restore_checkpoint_handles(root)["handles"] >= 1
    assert _handle_path(root, "broken", handle).exists()


@pytest.mark.parametrize("occupied", ["pack", "sidecar"])
def test_generation_is_never_reused_while_either_publication_file_exists(root, occupied):
    _task(root, "orphan", blobs=1)
    directory = packs.packs_dir(root)
    directory.mkdir(parents=True)
    path = directory / ("orphan.xz" if occupied == "pack" else "orphan.members.json")
    path.write_bytes(b"occupied generation")
    before = path.read_bytes()
    if occupied == "pack":
        report = packs.pack_task(root, "orphan", keep_shas=set())
        assert report["generation"] == 2
    else:
        with pytest.raises(OSError, match="evidence_corrupt"):
            packs.pack_task(root, "orphan", keep_shas=set())
    assert path.read_bytes() == before
    with pytest.raises(FileExistsError):
        packs.write_pack(root, "orphan", 1, packs.loose_members(root, "orphan"))
    assert path.read_bytes() == before


def test_presence_reentry_projection_reads_loose_and_packed_source(root, monkeypatch):
    from ouroboros import artifacts
    from ouroboros.presence_continuation import reentry_source_projection

    _task(root, "presence", blobs=1)
    raw = json.dumps({"kind": "presence_reentry", "task_id": "presence", "conversation_key": "room",
                      "history": {"status": "ok", "start": 0, "end": 1}}).encode()
    ref = store_actor_source_bytes(root, "presence", category="context_checkpoints",
                                  source_id="presence-reentry", data=raw, extension="json")
    before = reentry_source_projection(root, "presence", ref["sha256"])
    assert before["complete_sha256"] == ref["sha256"]
    assert packer.run_pass(root)["faults"] == 0

    assert not _handle_path(root, "presence", ref).exists()
    assert reentry_source_projection(root, "presence", ref["sha256"]) == before
    assert reentry_source_projection(root, "presence", "0" * 64)["reason"] == "source_unavailable"
    monkeypatch.setattr(artifacts, "read_actor_source_bytes", lambda *a, **k: (_ for _ in ()).throw(OSError("read failed")))
    assert reentry_source_projection(root, "presence", ref["sha256"])["reason"] == "source_unreadable"


@pytest.mark.parametrize("blocker", ["delivery", "obligation"])
def test_outbox_and_owner_changes_wake_skipped_tasks(root, blocker):
    from ouroboros import obligations
    from supervisor import terminal_delivery

    refs, _ = _task(root, "wake", blobs=1)
    if blocker == "delivery":
        assert terminal_delivery.register_pending_delivery(root, {"delivery_id": "owed", "task_id": "wake", "chat_id": 1})
    else:
        obligations.add(root, "pending_drives", "wake")
    assert packer.run_pass(root)["packed"] == 0
    assert packer.changed_tasks(root) == [] and _loose(refs[0])
    if blocker == "delivery":
        assert terminal_delivery.register_delivery(root, "owed")
    else:
        obligations.remove(root, "pending_drives", "wake")
    assert packer.changed_tasks(root) == ["wake"]
    assert packer.run_pass(root)["packed"] == 1 and not _loose(refs[0])


def test_shared_blob_cleanup_retries_after_last_live_reference_settles(root):
    raw = b'{"shared":true}'
    refs, _ = _task(root, "settled", blobs=1, payloads=[raw])
    _task(root, "live", status="running", blobs=1, payloads=[raw])
    for _ in range(3):
        assert packer.run_pass(root)["kept"] >= 1
        assert _loose(refs[0])
    _result(root, "live")
    assert packer.run_pass(root)["faults"] == 0
    assert not _loose(refs[0])
    packer.run_pass(root)
    assert packer.changed_tasks(root) == []


def test_identical_checkpoint_and_blob_bytes_keep_both_index_kinds(root):
    raw = b'{"same": true}'
    refs, handle = _task(root, "same", blobs=1, payloads=[raw], handle=raw)
    other = _blob(root, raw, kind="bin")
    (root / "observability" / "calls" / "same" / "other.json").write_text(json.dumps(other), encoding="utf-8")
    assert packer.run_pass(root)["faults"] == 0
    assert not _loose(refs[0]) and not _handle_path(root, "same", handle).exists()
    assert read_actor_source_bytes(root, "same", handle) == raw
    assert packs.blob_bytes(root, refs[0]) == packs.blob_bytes(root, other) == raw
    with packs.open_index(root) as conn:
        assert {row[0] for row in conn.execute("SELECT kind FROM members")} == {"json", "bin", "checkpoint"}


def test_old_index_schema_rebuilds_without_rewriting_sidecars(root):
    refs, handle = _task(root, "indexed", blobs=1)
    packer.run_pass(root)
    directory = packs.packs_dir(root)
    sidecar = (directory / "indexed.members.json").read_bytes()
    with sqlite3.connect(directory / packs.INDEX_NAME) as conn:
        conn.execute("DROP TABLE members")
        conn.execute("CREATE TABLE members (sha256 TEXT, task_id TEXT, generation INTEGER, offset INTEGER, "
                     "size INTEGER, kind TEXT, PRIMARY KEY (sha256, task_id, generation))")
        conn.execute("DELETE FROM meta WHERE key='schema_version'")
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('format_version', '1')")
    packs._VALIDATED.clear()
    with packs.open_index(root) as conn:
        key = {row["name"] for row in conn.execute("PRAGMA table_info(members)") if row["pk"]}
        assert key == {"sha256", "kind", "task_id", "generation"}
        assert conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == str(packs.INDEX_SCHEMA_VERSION)
    assert (directory / "indexed.members.json").read_bytes() == sidecar
    assert json.loads(sidecar)["format_version"] == 1
    assert read_actor_source_bytes(root, "indexed", handle).startswith(b"# focus")
    assert packs.blob_bytes(root, refs[0]).startswith(b'{"message"')


def test_blob_resolver_tries_all_local_placements_then_retained_root(root):
    raw = b'{"shared": "exact bytes"}'
    refs, _ = _task(root, "a", blobs=1, payloads=[raw])
    _task(root, "b", blobs=1, payloads=[raw])
    packer.run_pass(root)
    (packs.packs_dir(root) / "a.xz").write_bytes(b"bad a")
    assert packs.blob_bytes(root, refs[0]) == raw
    child = root / "task_drives" / "a"
    child.mkdir(parents=True)
    retained, _ = _task(child, "a", blobs=1, payloads=[raw])
    packer.run_pass(child)
    (packs.packs_dir(root) / "b.xz").write_bytes(b"bad b")
    assert packs.blob_bytes(root, retained[0]) == raw
    (packs.packs_dir(child) / "a.xz").write_bytes(b"bad child")
    with pytest.raises(packs.EvidenceCorrupt, match="evidence_corrupt"):
        packs.blob_bytes(root, retained[0])


def test_blob_byte_cap_bounds_loose_reads_and_refuses_oversize_pack_before_decode(root, monkeypatch):
    raw = b'{"body": "' + b"x" * 10000 + b'"}'
    refs, _ = _task(root, "bounded", blobs=1, payloads=[raw])
    no_size = {key: value for key, value in refs[0].items() if key != "size"}
    sizes = []
    original_read = gzip.GzipFile.read

    def read(stream, size=-1):
        sizes.append(size)
        return original_read(stream, size)

    with monkeypatch.context() as patch:
        patch.setattr(gzip.GzipFile, "read", read)
        with pytest.raises(ValueError, match="byte limit"):
            obs.read_blob_ref(root, no_size, max_bytes=100, raw_bytes=True)
    assert sizes and all(0 <= size <= 101 for size in sizes)
    assert obs.read_blob_ref(root, no_size, max_bytes=len(raw), raw_bytes=True) == raw
    with pytest.raises(ValueError, match="size"):
        obs.read_blob_ref(root, no_size)
    packer.run_pass(root)
    with monkeypatch.context() as patch:
        patch.setattr(packs, "member_bytes", lambda *a: pytest.fail("oversize member decoded"))
        with pytest.raises(ValueError, match="byte limit"):
            obs.read_blob_ref(root, no_size, max_bytes=100, raw_bytes=True)
    assert obs.read_blob_ref(root, no_size, max_bytes=len(raw), raw_bytes=True) == raw


@pytest.mark.parametrize("reader", ["handle", "actor"])
def test_corrupt_loose_handle_falls_back_to_verified_pack(root, reader):
    raw = b"the retained checkpoint"
    _, handle = _task(root, "handle", blobs=1, handle=raw)
    packer.run_pass(root)
    loose = _handle_path(root, "handle", handle)
    loose.write_bytes(b"damaged loose")

    def read():
        return (packs.handle_bytes(root, "handle", handle["path"]) if reader == "handle"
                else read_actor_source_bytes(root, "handle", handle))

    assert read() == raw
    (packs.packs_dir(root) / "handle.xz").write_bytes(b"damaged pack")
    with pytest.raises(packs.EvidenceCorrupt, match="evidence_corrupt"):
        read()
    loose.write_bytes(raw)
    assert read() == raw


@pytest.mark.parametrize("name,entry", [
    ("pending_drives", {}), ("synthesis", {}), ("unknowns", {"task_id": "owed"}),
    ("custody_open", {"custody": {"task_id": "owed"}}),
    ("custody_open", {"request": {"task_id": "owed"}}),
    ("local_custody_witnesses", {"task_id": "owed"}),
])
def test_settlement_waits_for_owner_held_obligation(root, name, entry):
    from ouroboros import obligations

    refs, handle = _task(root, "owed", blobs=1)
    identity = "run:child" if entry else "owed"
    obligations.add(root, name, identity, entry)
    assert not packs.task_settled(root, "owed")
    assert name in packs.task_settlement_facts(root, "owed")["obligations"]
    assert packer.run_pass(root)["packed"] == 0
    assert _loose(refs[0]) and _handle_path(root, "owed", handle).exists()
    obligations.remove(root, name, identity)
    assert packs.task_settled(root, "owed")


def test_acceptance_debt_blocks_without_delivery_and_late_notice_requires_receipt(root):
    from supervisor import terminal_delivery

    _task(root, "debt", blobs=1, acceptance_debt={"debt_id": "debt"})
    assert not packs.task_settled(root, "debt")
    wanted = "task_acceptance:" + hashlib.sha256(b"debt").hexdigest()
    panel = {"panel_id": "p", "late_settlement": {"settled_at": "revision-1", "reviewed_subject": {"retry_key": wanted}}}
    projection = {"panels": [panel], "late_notice_receipts": {"acceptance-late:" + wanted: {
        "panel_id": "p", "settled_at": "revision-1", "custody": "publication"}}}
    _result(root, "debt", acceptance_debt={"debt_id": "debt"}, review_projection=projection)
    assert not packs.task_settled(root, "debt")
    receipt = projection["late_notice_receipts"]["acceptance-late:" + wanted]
    receipt["custody"] = "terminal_outbox"
    _result(root, "debt", acceptance_debt={"debt_id": "debt"}, review_projection=projection)
    event = {"delivery_id": "acceptance-late:" + wanted, "task_id": "debt", "chat_id": 1}
    assert terminal_delivery.register_pending_delivery(root, event)
    assert not packs.task_settled(root, "debt")
    assert terminal_delivery.register_delivery(root, event["delivery_id"])
    assert packs.task_settled(root, "debt")
    panel["actors"] = [{"operation_state": "in_flight"}]
    _result(root, "debt", acceptance_debt={"debt_id": "debt"}, review_projection=projection)
    assert not packs.task_settled(root, "debt")


@pytest.mark.parametrize("source", ["obligations", "terminal_deliveries"])
def test_unknown_settlement_sources_block_until_repaired(root, source):
    _task(root, "unknown", blobs=1)
    path = root / "state" / ("obligations/pending_drives.json" if source == "obligations" else "terminal_deliveries.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{", encoding="utf-8")
    assert not packs.task_settled(root, "unknown")
    path.write_text("{}", encoding="utf-8")
    assert packs.task_settled(root, "unknown")


@pytest.mark.parametrize("change", ["resume", "new_reference"])
def test_unlink_rereads_facts_and_references_after_compression(root, monkeypatch, change):
    refs, handle = _task(root, "packing", blobs=1)
    _task(root, "other", status="running", blobs=1)
    write = packs.write_pack

    def publish(*args, **kwargs):
        result = write(*args, **kwargs)
        if change == "resume":
            _result(root, "packing", "running")
        else:
            path = root / "observability/calls/other/new.json"
            path.write_text(json.dumps(refs[0]), encoding="utf-8")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(packs, "write_pack", publish)
        report = packer.run_pass(root)
    assert report["faults"] == 0 and _loose(refs[0])
    if change == "resume":
        assert _handle_path(root, "packing", handle).exists()
        assert "packing" in packer.changed_tasks(root)
    _result(root, "packing")
    _result(root, "other")
    assert packer.run_pass(root)["faults"] == 0
    assert not _loose(refs[0]) and not _handle_path(root, "packing", handle).exists()


def test_compression_is_outside_result_and_publication_locks(root, monkeypatch):
    from ouroboros import task_custody, utils
    from ouroboros.task_results import task_result_path

    _task(root, "unlocked", blobs=1)
    write = packs.write_pack

    def compress(*args, **kwargs):
        with task_custody.task_custody_lock(root, "unlocked", timeout_sec=0) as acquired:
            assert acquired
            utils.update_json_locked(task_result_path(root, "unlocked"), lambda current: None, timeout_sec=0)
        return write(*args, **kwargs)

    monkeypatch.setattr(packs, "write_pack", compress)
    assert packer.run_pass(root)["faults"] == 0


def test_stop_latches_and_signals_without_waiting_for_candidate_walk(root, monkeypatch):
    import threading
    from tests.test_evidence_packs import REPO, _FakeChild
    from ouroboros import process_custody

    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
    unit, child = packer.EvidencePacker(), _FakeChild(b"")
    unit._process = child
    unit.tick(root, REPO)

    def walk(_):
        entered.set()
        assert release.wait(5)
        return ["candidate"]

    monkeypatch.setattr(packer, "changed_tasks", walk)
    monkeypatch.setattr(process_custody, "spawn_supervised", lambda *a, **k: pytest.fail("spawned after Stop"))
    tick = threading.Thread(target=unit.tick, args=(root, REPO))
    stop = threading.Thread(target=lambda: (unit.stop(), stopped.set()))
    tick.start()
    try:
        assert entered.wait(2)
        stop.start()
        assert stopped.wait(0.3), "Stop waited for the mutex held by the disk walk"
        assert child.killed.is_set() and unit._stopped.is_set()
    finally:
        release.set()
        tick.join(3)
        stop.join(3)
        if unit._thread is not None:
            unit._thread.join(3)
    assert not tick.is_alive() and not stop.is_alive()


def test_restore_and_preflight_cover_all_descendants_but_not_symlink_drives(root):
    outer = root / "task_drives/outer"
    inner = outer / "state/headless_tasks/inner/data"
    raw = b"nested retained checkpoint"
    _, handle = _task(inner, "inner", blobs=1, handle=raw)
    assert packer.run_pass(inner)["faults"] == 0
    loose = _handle_path(inner, "inner", handle)
    assert not loose.exists()
    outside = root.parent / "outside"
    _, outside_handle = _task(outside, "outside", blobs=1, handle=b"outside bytes")
    packer.run_pass(outside)
    (root / "task_drives/linked").symlink_to(outside, target_is_directory=True)
    assert packs.pending_restore_bytes(root) == len(raw)
    assert packs.restore_checkpoint_handles(root)["handles"] == 1
    assert loose.read_bytes() == raw
    assert not _handle_path(outside, "outside", outside_handle).exists()
    assert packs.pending_restore_bytes(root) == 0
    assert packs.restore_checkpoint_handles(root)["handles"] == 0


def test_first_backfill_by_size_then_new_settlements_before_backlog(root):
    from ouroboros.task_results import task_result_path

    _task(root, "large", blobs=1, payloads=[b"x" * 10000])
    _task(root, "small", blobs=1, payloads=[b"small"])
    os.utime(task_result_path(root, "large"), ns=(10, 10))
    os.utime(task_result_path(root, "small"), ns=(20, 20))
    assert [task for task, _ in packer.ordered_candidates(root, ["small", "large"])] == ["large", "small"]
    _task(root, "published", blobs=1)
    packs.pack_task(root, "published", keep_shas=set())
    _task(root, "new", blobs=1, payloads=[b"new"])
    assert [task for task, _ in packer.ordered_candidates(root, ["large", "small", "new"])] == ["new", "large", "small"]


def test_manifest_members_follow_first_appearance_with_name_ties(root):
    _task(root, "ordered", blobs=0)
    directory = root / "observability/calls/ordered"
    first, second, third = [_blob(root, value) for value in (b"first", b"second", b"third")]
    for name, ref, created in [("z", first, "2026-01-01T00:00:01Z"), ("b", third, "2026-01-01T00:00:02Z"),
                               ("a", second, "2026-01-01T00:00:02Z")]:
        path = directory / (name + ".json")
        path.write_text(json.dumps({"created_at": created, "ref": ref}), encoding="utf-8")
    expected = [ref["sha256"] for ref in (first, second, third)]
    assert [sha for _, sha, _ in packs.manifest_blob_refs(root, "ordered")] == expected
    # A later atomic rewrite preserves the original created_at order.
    obs.write_call_manifest(root, task_id="ordered", call_id="z", manifest={"created_at": "2026-01-01T00:00:01Z", "ref": first})
    assert [sha for _, sha, _ in packs.manifest_blob_refs(root, "ordered")] == expected


@pytest.mark.parametrize("kind", ["task", "archive"])
def test_unpacker_is_published_before_unlink_and_write_failure_keeps_loose(root, monkeypatch, kind):
    if kind == "task":
        refs, handle = _task(root, "manual", blobs=1)
        loose = {pathlib.Path(refs[0]["path"]), _handle_path(root, "manual", handle)}
    else:
        source = root / "archive/usage_ledger/segment_ep1.jsonl"
        source.parent.mkdir(parents=True)
        source.write_text('{"attempt_id":"example"}\n', encoding="utf-8")
        loose = {source}
    original_write, original_unlink = packs.write_bytes_atomic, os.unlink

    def fail_readme(path, *args, **kwargs):
        if pathlib.Path(path).name == packs.README_NAME:
            raise PermissionError("read-only unpacker destination")
        return original_write(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(packs, "write_bytes_atomic", fail_readme)
        report = packer.run_pass(root)
    assert report["faults"] >= 1 and all(path.exists() for path in loose)

    def unlink(path, *args, **kwargs):
        if pathlib.Path(path) in loose:
            assert (packs.packs_dir(root) / packs.UNPACKER_NAME).read_text() == packs._UNPACKER
            assert (packs.packs_dir(root) / packs.README_NAME).read_text() == packs._README
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", unlink)
    assert packer.run_pass(root)["faults"] == 0
    assert not any(path.exists() for path in loose)
