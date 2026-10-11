"""Import failures retain source facts and recover through the next boot."""
import errno
import pathlib

import pytest

from ouroboros import store, store_import
from ouroboros.utils import atomic_write_json
from tests.test_store_import import add_set_member, load, meta, result, write_result


def set_rows(root):
    with store.open_store(root, write=False) as txn:
        return txn.set_rows("local_custody_witnesses")


@pytest.mark.parametrize("published", [False, True], ids=["initial", "p6"])
@pytest.mark.parametrize("source", ["file", "directory"])
@pytest.mark.parametrize("error", [errno.EMFILE, errno.ENFILE, errno.EACCES, errno.EIO])
def test_set_read_error_aborts_and_retry_keeps_facts(tmp_path, monkeypatch, published, source, error):
    root = tmp_path / "data"
    write_result(root, result("task1"))
    facts = {"task_id": "task1", "producer": "model_wait"}
    add_set_member(root, "local_custody_witnesses", "held", facts)
    if published:
        store_import.ensure_current_store(root)
        before = meta(root, "sets_baseline")
    # P6' is triggered by a result change; set-only edits remain outside its contract.
    write_result(root, result("task1", "failed"))
    add_set_member(root, "local_custody_witnesses", "new", facts)
    path = store_import.set_file_path(root, "local_custody_witnesses")
    original = path.read_bytes()
    real_read = pathlib.Path.read_text
    real_scan = store_import.os.scandir
    failed = []

    def read(target, *args, **kwargs):
        if target == path and source == "file" and not failed:
            failed.append(target)
            raise OSError(error, "injected set read failure")
        return real_read(target, *args, **kwargs)

    def scan(target):
        if pathlib.Path(target) == path.parent and source == "directory" and not failed:
            failed.append(target)
            raise OSError(error, "injected set listing failure")
        return real_scan(target)

    monkeypatch.setattr(pathlib.Path, "read_text", read)
    monkeypatch.setattr(store_import.os, "scandir", scan)
    kind = store.StoreBusy if error in (errno.EMFILE, errno.ENFILE) else store.StoreError
    with pytest.raises(kind, match="obligation sets? not readable"):
        store_import.ensure_current_store(root)
    assert failed and path.read_bytes() == original
    if published:
        assert set_rows(root) == {"held": facts}
        assert load(root, "task1")["status"] == "completed"
        assert meta(root, "sets_baseline") == before
    else:
        assert store.probe(root) == ("absent", None)
        assert not store.generation_stamped(root)
        assert not list((root / "state").glob("current.sqlite.import-*"))
    assert store_import.ensure_current_store(root)["status"] == ("reconciled" if published else "completed")
    assert set_rows(root) == {"held": facts, "new": facts}
    assert load(root, "task1")["status"] == "failed"


def test_initial_set_rows_and_baseline_use_one_snapshot(tmp_path, monkeypatch):
    root = tmp_path / "data"
    facts = {"task_id": "task1"}
    add_set_member(root, "local_custody_witnesses", "held", facts)
    path = store_import.set_file_path(root, "local_custody_witnesses")
    real_read = pathlib.Path.read_text
    reads = []

    def read(target, *args, **kwargs):
        value = real_read(target, *args, **kwargs)
        if target == path:
            reads.append(target)
            return value if len(reads) == 1 else "{}"
        return value

    monkeypatch.setattr(pathlib.Path, "read_text", read)
    store_import.ensure_current_store(root)
    assert reads == [path]
    assert set_rows(root) == {"held": facts}
    assert meta(root, "sets_baseline") == store_import._sets_baseline({"local_custody_witnesses": {"held": facts}})


@pytest.mark.parametrize("published", [False, True], ids=["initial", "p6"])
@pytest.mark.parametrize("malformed", ["{", "[]", '{"held": null}', "\ufffd"])
def test_malformed_set_refused_in_place_without_losing_rows(tmp_path, published, malformed):
    root = tmp_path / "data"
    write_result(root, result("task1"))
    facts = {"task_id": "task1"}
    add_set_member(root, "local_custody_witnesses", "held", facts)
    if published:
        store_import.ensure_current_store(root)
    path = store_import.set_file_path(root, "local_custody_witnesses")
    path.write_text(malformed, encoding="utf-8")
    write_result(root, result("task1", "failed"))
    with pytest.raises(store.StoreError, match="obligation set malformed"):
        store_import.ensure_current_store(root)
    assert path.read_text(encoding="utf-8") == malformed
    if published:
        assert set_rows(root) == {"held": facts}
        assert load(root, "task1")["status"] == "completed"
    else:
        assert store.probe(root) == ("absent", None)
        assert not store.generation_stamped(root)
    atomic_write_json(path, {"held": facts})
    store_import.ensure_current_store(root)
    assert set_rows(root) == {"held": facts}
    assert load(root, "task1")["status"] == "failed"
    # A real deletion still retires the file-held row when the old body changes a result.
    path.unlink()
    write_result(root, result("task1", "completed"))
    store_import.ensure_current_store(root)
    assert set_rows(root) == {}
