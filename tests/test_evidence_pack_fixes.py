"""Regressions for the dispositioned S5 pack and reader findings."""
from __future__ import annotations

import os
import pathlib

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
