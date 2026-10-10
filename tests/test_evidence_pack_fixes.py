"""Regressions for the dispositioned S5 pack and reader findings."""
from __future__ import annotations

import os
import pathlib

import pytest

from ouroboros import evidence_packer as packer
from ouroboros import evidence_packs as packs
from tests.test_evidence_packs import _handle_path, _loose, _task
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
