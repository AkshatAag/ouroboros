"""The authority switch (``ouroboros/store_switch.py``): intent and receipt, the needs read
from the target tree, preflight refusals that move nothing, the seam's order (export,
receipt, intent consumed, store retired, THEN the door's own switching owner), the
idempotent money step around every crash point, abandonment restoring both authorities,
and the chain new -> old -> new with failures around the consume and the switch."""
from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess

import pytest

from ouroboros import store, store_switch, usage_store
from ouroboros.store_import import ensure_current_store
from ouroboros.task_results import task_result_path
from ouroboros.usage_ledger import LEDGER_REL
from ouroboros.utils import atomic_write_json
from tests._usage_store_testing import seed
from tests.test_store_import import fixture_root, load, meta, result, rows, snapshot

REPO = pathlib.Path(__file__).resolve().parents[1]


def git(repo: pathlib.Path, *args: str) -> str:
    env = {**os.environ, "LC_ALL": "C", "LANG": "C", "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@x"}
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True, env=env).stdout.strip()


@pytest.fixture
def target_repo(tmp_path):
    """``old``: a tree without the store modules (a pin file included); ``new``: with them."""
    repo = tmp_path / "repo"
    (repo / "ouroboros").mkdir(parents=True)
    git(repo, "init", "-q")
    shutil.copy(REPO / "ouroboros" / "claudexor_runtime_pin.json", repo / "ouroboros" / "claudexor_runtime_pin.json")
    (repo / "ouroboros" / "task_results.py").write_text("# old\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "old")
    old = git(repo, "rev-parse", "HEAD")
    for name in ("store.py", "usage_store.py"):
        (repo / "ouroboros" / name).write_text("# new\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "new")
    return repo, old, git(repo, "rev-parse", "HEAD")


@pytest.fixture
def switch_root(tmp_path, monkeypatch):
    """A root with a published facts store (child drive included) and a money store
    imported from a two-attempt journal kept in place."""
    monkeypatch.setenv("OUROBOROS_DATA_DIR", str(tmp_path / "data"))
    root = fixture_root(tmp_path)
    ensure_current_store(root)
    seed(root, 2)
    usage_store.migrate_from_journal(root)
    yield root
    usage_store.forget(root)


def intent(root: pathlib.Path, target_sha: str, door: str = "ui_rollback") -> dict:
    return store_switch.record_switch_intent(root, target_sha=target_sha, target_ref="v7.6.0", reason="test", door=door)


class Owner:
    def __init__(self, ok=True):
        self.calls, self.ok = [], ok

    def __call__(self, intent):
        self.calls.append(intent)
        return self.ok, f"switched to {intent['target_sha'][:8]}"


def state(root: pathlib.Path) -> dict:
    return {"intent": (root / store_switch.INTENT_REL).exists(), "facts_store": store.probe(root)[0],
            "money_store": (root / usage_store.STORE_REL).exists(), "journal": (root / LEDGER_REL).exists(),
            "retired_facts": sorted(p.name for p in (root / "state").glob("current.sqlite.exported-*")),
            "retired_money": sorted(p.name for p in (root / "state").glob("usage.sqlite.exported-*"))}


def receipt(root: pathlib.Path) -> dict:
    return json.loads((root / store_switch.RECEIPT_REL).read_text(encoding="utf-8"))


# ---- intent, needs, pin --------------------------------------------------------------

def test_the_intent_is_write_once_and_typed(tmp_path):
    recorded = intent(tmp_path, "a" * 40)
    assert store_switch.read_switch_intent(tmp_path) == recorded and recorded["door"] == "ui_rollback"
    with pytest.raises(RuntimeError):
        intent(tmp_path, "b" * 40)
    with pytest.raises(ValueError):
        store_switch.record_switch_intent(tmp_path / "other", target_sha="c", target_ref="r", reason="x", door="front")
    assert store_switch.resume_pending_switch(tmp_path / "other", repo_dir=tmp_path) == {"status": "nothing_pending"}


def test_needs_and_the_engine_pin_come_from_the_target_tree(target_repo):
    repo, old, new = target_repo
    assert store_switch.switch_needs(repo, old) == {"facts": True, "money": True, "handles": True}
    assert store_switch.switch_needs(repo, new) == {"facts": False, "money": False, "handles": True}
    pin = store_switch.next_engine_pin(repo, old)
    assert pin is not None and pin.version == json.loads(
        (REPO / "ouroboros" / "claudexor_runtime_pin.json").read_text(encoding="utf-8"))["release"]["version"]
    git(repo, "rm", "-q", "ouroboros/claudexor_runtime_pin.json")
    git(repo, "commit", "-q", "-m", "no pin")
    assert store_switch.next_engine_pin(repo, git(repo, "rev-parse", "HEAD")) is None


def test_every_door_resolves_to_its_own_switching_owner():
    for door in store_switch.DOORS:
        assert callable(store_switch.switching_owner(door))
    with pytest.raises(ValueError):
        store_switch.switching_owner("window")


# ---- the seam -----------------------------------------------------------------------------

def test_the_seam_exports_consumes_the_intent_retires_the_store_and_only_then_switches(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    expected = snapshot(root)
    intent(root, old)
    seen = {}

    def owner(recorded):
        seen.update(state(root), receipt=receipt(root))
        return True, "rolled back"

    report = store_switch.run_switch_seam(root, repo_dir=repo, owner=owner)
    assert report["status"] == "switched" and report["exit"] == 42
    # at the owner call: intent consumed, export completed, both stores retired, journal written
    assert seen["intent"] is False and seen["facts_store"] == "absent" and seen["money_store"] is False
    assert seen["journal"] is True and seen["receipt"]["export"]["status"] == "completed"
    assert seen["receipt"]["steps"]["money"]["status"] == "completed"
    assert seen["receipt"]["steps"]["handles"] == {"status": "no_packs"}
    assert len(seen["retired_facts"]) == 1 and len(seen["retired_money"]) == 1
    assert snapshot(root) == expected  # the files the old body reads are the ones the store imported
    assert receipt(root)["switched"]["ok"] is True
    phases = [row["phase"] for row in rows(root / "logs" / "supervisor.jsonl", "storage_switch")]
    assert phases == ["preflight", "exporting", "money", "handles", "switching", "switched"]
    assert json.loads((root / store_switch.PROGRESS_REL).read_text(encoding="utf-8"))["phase"] == "switched"
    assert store_switch.resume_pending_switch(root, repo_dir=repo) == {"status": "nothing_pending"}
    # the child drive's store was retired with the canonical one
    drive = root / "state" / "headless_tasks" / "root1" / "data"
    assert store.probe(drive)[0] == "absent" and list((drive / "state").glob("current.sqlite.exported-*"))


def test_a_target_with_the_store_switches_without_an_export(switch_root, target_repo):
    root, (repo, _old, new) = switch_root, target_repo
    intent(root, new)
    owner = Owner()
    report = store_switch.run_switch_seam(root, repo_dir=repo, owner=owner)
    assert report["status"] == "switched" and len(owner.calls) == 1
    assert state(root) == {"intent": False, "facts_store": "present", "money_store": True, "journal": True,
                           "retired_facts": [], "retired_money": []}
    assert meta(root, "phase") == store.PUBLISHED
    assert receipt(root)["steps"].get("handles") == {"status": "no_packs"}


def test_an_unconfirmed_stop_refuses_before_anything_moves(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)
    before = state(root)
    owner = Owner()
    report = store_switch.run_switch_seam(root, repo_dir=repo, owner=owner, blockers=["worker:4242"])
    assert report["status"] == "refused" and report["reason"] == "quiescence_unconfirmed" and not owner.calls
    assert state(root) == {**before, "intent": False}
    assert meta(root, "phase") == store.PUBLISHED
    assert list((root / "state").glob("switch_intent.refused-*.json"))
    refused = [row for row in rows(root / "logs" / "supervisor.jsonl", "storage_switch") if row["phase"] == "refused"]
    assert refused[0]["reason"] == "quiescence_unconfirmed" and refused[0]["blockers"] == ["worker:4242"]
    # the quiet direction: no blockers, the same intent re-recorded, the switch runs
    intent(root, old)
    assert store_switch.run_switch_seam(root, repo_dir=repo, owner=owner)["status"] == "switched"


def test_preflight_refusals_move_nothing_and_repeat_honestly(switch_root, target_repo, monkeypatch):
    from collections import namedtuple

    root, (repo, old, _new) = switch_root, target_repo
    before = state(root)
    with monkeypatch.context() as patched:
        patched.setattr(shutil, "disk_usage", lambda path: namedtuple("usage", "total used free")(10, 0, 10))
        intent(root, old)
        report = store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())
    assert report["status"] == "refused" and "free space" in report["reason"]
    # the money exporter's own refusal: a journal that changed after the import
    with (root / LEDGER_REL).open("ab") as handle:
        handle.write(b'{"appended": "by an older body"}\n')
    for _ in range(2):  # a repeated refusal leaves both authorities exactly as they were
        intent(root, old)
        report = store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())
        assert report["status"] == "refused" and "changed after the import" in report["reason"]
        assert state(root) == {**before, "intent": False} and meta(root, "phase") == store.PUBLISHED


# ---- the money step around every crash point --------------------------------------------

def test_a_crash_before_the_journal_is_written_resumes_cleanly(switch_root, target_repo, monkeypatch):
    from ouroboros import usage_journal

    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)
    real = usage_journal.write_journal
    monkeypatch.setattr(usage_journal, "write_journal", lambda *a, **k: (_ for _ in ()).throw(OSError("disk went away")))
    report = store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())
    assert report["status"] == "failed" and report["attempt"] == 1
    assert state(root)["money_store"] is True and (root / store_switch.INTENT_REL).exists()
    assert meta(root, "phase") == store.EXPORTING and receipt(root)["steps"]["money"]["status"] == "started"
    monkeypatch.setattr(usage_journal, "write_journal", real)
    owner = Owner()
    report = store_switch.resume_pending_switch(root, repo_dir=repo, owner=owner)
    assert report["status"] == "switched" and len(owner.calls) == 1
    assert state(root)["money_store"] is False and state(root)["journal"] is True


def test_a_crash_after_the_journal_before_the_store_rename_recognises_its_own_journal(switch_root, target_repo, monkeypatch):
    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)
    real_replace = os.replace

    def refuse_store_rename(src, dst, *args, **kwargs):
        if "usage.sqlite.exported-" in str(dst):
            raise OSError("the rename never happened")
        return real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, "replace", refuse_store_rename)
    assert store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())["status"] == "failed"
    assert state(root)["money_store"] is True and state(root)["journal"] is True  # our own journal, store still here
    monkeypatch.setattr(os, "replace", real_replace)
    report = store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner())
    assert report["status"] == "switched"
    assert state(root)["money_store"] is False and len(state(root)["retired_money"]) == 1
    assert receipt(root)["steps"]["money"]["journal"]["size"] == (root / LEDGER_REL).stat().st_size


def test_a_crash_after_the_store_rename_before_the_receipt_completes_from_the_facts(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)
    assert store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())["status"] == "switched"
    # the receipt the crash never wrote: money started, export open, intent still recorded
    kept = receipt(root)
    kept["steps"]["money"] = {"status": "started"}
    kept["export"] = {}
    kept.pop("switched")
    atomic_write_json(root / store_switch.RECEIPT_REL, kept)
    atomic_write_json(root / store_switch.INTENT_REL, kept["intent"])
    owner = Owner()
    report = store_switch.run_switch_seam(root, repo_dir=repo, owner=owner)
    assert report["status"] == "switched" and len(owner.calls) == 1
    money = receipt(root)["steps"]["money"]
    assert money["status"] == "completed" and money["store_retired_as"].startswith("usage.sqlite.exported-")


def test_abandonment_after_a_partial_export_restores_both_authorities(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    before = state(root)
    intent(root, old)

    def handles_fail(_root):
        raise OSError("the handle restore died")

    report = store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner(), restore_handles=handles_fail)
    assert report["status"] == "failed"
    assert state(root)["money_store"] is False and meta(root, "phase") == store.EXPORTING  # money exported, facts open
    report = store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner(), blockers=["packer:7"])
    assert report["status"] == "refused" and report["restored"]["money"] == "republished"
    assert state(root) == {**before, "intent": False}
    assert meta(root, "phase") == store.PUBLISHED and (root / LEDGER_REL).stat().st_size == before_journal_size(root)
    with usage_store.read(root) as txn:  # the republished money store serves again
        assert len(txn.attempts()) == 2
    assert store.update_task(root, "done1", lambda current: {**current, "after": "abandonment"})["after"] == "abandonment"


def before_journal_size(root: pathlib.Path) -> int:
    rows_ = (root / LEDGER_REL).read_text(encoding="utf-8").splitlines()
    return sum(len(line.encode("utf-8")) + 1 for line in rows_)


def test_a_second_failure_of_the_same_intent_is_a_refusal_never_a_loop(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)

    def handles_fail(_root):
        raise OSError("still failing")

    for attempt in (1, 2):
        report = store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner(), restore_handles=handles_fail)
        assert report == {"status": "failed", "error": "OSError: still failing", "attempt": attempt}
    report = store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner(), restore_handles=handles_fail)
    assert report["status"] == "refused" and report["reason"] == "second_failure"
    assert not (root / store_switch.INTENT_REL).exists() and meta(root, "phase") == store.PUBLISHED
    assert state(root)["money_store"] is True and store.probe(root)[0] == "present"


# ---- retirement -----------------------------------------------------------------------------

def test_the_stamp_is_cleared_before_the_store_is_renamed_aside(switch_root, target_repo, monkeypatch):
    """A death right after the rename must leave «absent, unstamped» (the next import rebuilds
    from the complete export), never «absent beside its completed stamp» (LOST)."""
    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)
    real_replace = os.replace

    def die_after_the_rename(src, dst, *args, **kwargs):
        real_replace(src, dst, *args, **kwargs)
        if pathlib.Path(dst).parent == root / "state" and "current.sqlite.exported-" in str(dst):
            raise KeyboardInterrupt("died right after renaming the canonical store aside")

    monkeypatch.setattr(os, "replace", die_after_the_rename)
    with pytest.raises(KeyboardInterrupt):
        store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())
    monkeypatch.setattr(os, "replace", real_replace)
    assert store.probe(root)[0] == "absent" and not store.generation_stamped(root)
    with pytest.raises(store.StoreRetired):  # typed for a straggler until the next import
        store.update_task(root, "done1", lambda current: current)
    assert store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner())["status"] == "switch_not_performed"
    report = ensure_current_store(root)
    assert report["status"] == "completed" and report["results"] == 3
    assert report["child_roots"] == {str(root / "state" / "headless_tasks" / "root1" / "data"): "completed"}


def test_a_lost_drive_refuses_the_switch_instead_of_exporting_past_it(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    drive = root / "state" / "headless_tasks" / "root1" / "data"
    store.store_path(root).unlink()  # the canonical store lost beside its completed stamp
    intent(root, old)
    owner = Owner()
    report = store_switch.run_switch_seam(root, repo_dir=repo, owner=owner)
    assert report["status"] == "refused" and "lost" in report["reason"] and not owner.calls
    assert store.probe(drive)[0] == "present" and meta(drive, "phase") == store.PUBLISHED  # never exported nor retired
    assert state(root)["money_store"] is True and state(root)["journal"] is True
    assert not list((drive / "state").glob("current.sqlite.exported-*"))


# ---- the chain new -> old -> new ---------------------------------------------------------------

def test_a_death_after_the_consume_and_before_the_switch_keeps_the_new_code_and_reimports(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)

    def dies(_intent):
        raise KeyboardInterrupt("the process died between the consume and the switch")

    with pytest.raises(KeyboardInterrupt):
        store_switch.run_switch_seam(root, repo_dir=repo, owner=dies)
    assert not (root / store_switch.INTENT_REL).exists() and store.probe(root)[0] == "absent"
    # the next boot: no intent, a retired store, a complete export -> re-import, nothing lost, the owner told
    report = store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner())
    assert report["status"] == "switch_not_performed"
    assert ensure_current_store(root)["status"] == "completed"
    assert sorted(load(root, tid)["task_id"] for tid in ("root1", "child1", "done1")) == ["child1", "done1", "root1"]
    usage_store.migrate_from_journal(root)  # money re-imports its own export
    with usage_store.read(root) as txn:
        assert len(txn.attempts()) == 2
    assert store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner()) == {"status": "nothing_pending"}
    # pressing the door again runs the whole chain
    usage_store.forget(root)
    intent(root, old)
    assert store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())["status"] == "switched"


def test_the_old_body_runs_and_the_new_body_rebuilds_from_the_complete_export(switch_root, target_repo):
    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)
    assert store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())["status"] == "switched"
    # the old body: a new task, a deleted one, a changed one, money appended to the journal
    atomic_write_json(task_result_path(root, "by-old"), result("by-old"))
    task_result_path(root, "done1").unlink()
    atomic_write_json(task_result_path(root, "child1"), result("child1", "completed", old_body=True))
    assert not store.generation_stamped(root)
    # the new body boots: a full rebuild from the files the old body left (no reconciliation needed)
    report = ensure_current_store(root)
    assert report["status"] == "completed" and report["results"] == 3
    assert load(root, "by-old")["status"] == "completed" and load(root, "done1") is None
    assert load(root, "child1")["old_body"] is True
    assert store_switch.resume_pending_switch(root, repo_dir=repo, owner=Owner()) == {"status": "nothing_pending"}


def test_a_death_inside_the_facts_export_resumes_from_the_intent(switch_root, target_repo, monkeypatch):
    from ouroboros import store_import

    root, (repo, old, _new) = switch_root, target_repo
    intent(root, old)
    real = store_import.export_facts
    calls = []

    def dies_once(drive):
        calls.append(drive)
        if len(calls) == 1:
            raise OSError("died while writing the files")
        return real(drive)

    monkeypatch.setattr(store_import, "export_facts", dies_once)
    assert store_switch.run_switch_seam(root, repo_dir=repo, owner=Owner())["status"] == "failed"
    assert (root / store_switch.INTENT_REL).exists() and meta(root, "phase") == store.EXPORTING
    assert ensure_current_store(root, with_children=False)["status"] == "switch_pending"  # the importer leaves it
    owner = Owner()
    assert store_switch.resume_pending_switch(root, repo_dir=repo, owner=owner)["status"] == "switched"
    assert len(owner.calls) == 1 and receipt(root)["attempts"] == 2
