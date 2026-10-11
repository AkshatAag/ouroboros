"""The archived joi-lab mirrors refresh (scripts/sync_legacy_mirrors.py).

Owner decisions 2026-10-11: №1=B refresh the archives on every stable release,
№2=A keep and mark the outdated desktop releases.
"""

from __future__ import annotations

import json
import subprocess

from scripts import sync_legacy_mirrors as mirrors


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _commit(repo, files, message):
    for name, text in files.items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", message)
    return _git(repo, "rev-parse", "HEAD")


def test_mirrors_are_only_the_two_archived_joi_lab_repositories():
    assert [m.repo for m in mirrors.MIRRORS] == ["joi-lab/ouroboros", "joi-lab/ouroboros-desktop"]
    # The desktop branch old apps update from, and the fork's two update branches.
    assert {m.repo: m.branches for m in mirrors.MIRRORS} == {
        "joi-lab/ouroboros": ("ouroboros", "ouroboros-stable"),
        "joi-lab/ouroboros-desktop": ("ouroboros",),
    }


def test_readme_and_pointer_send_visitors_to_the_official_repository():
    fork, desktop = mirrors.MIRRORS
    readme = mirrors.render_readme(desktop, "7.6.0", "4b37f96223a1")
    assert mirrors.OFFICIAL_URL in readme and mirrors.LATEST_URL in readme
    assert "v7.6.0 (official commit 4b37f9622)" in readme
    assert mirrors.LATEST_URL in mirrors.render_pointer(fork, "7.6.0")
    assert "outdated Ouroboros Desktop builds" in mirrors.render_pointer(desktop, "7.6.0")
    assert "outdated Ouroboros Desktop builds" not in mirrors.render_pointer(fork, "7.6.0")


def test_outdated_notice_is_prepended_once_and_keeps_the_notes():
    once = mirrors.mark_outdated("Original notes")
    assert once.startswith(mirrors.OUTDATED_MARK) and once.endswith("Original notes")
    assert mirrors.mark_outdated(once) == once
    assert mirrors.mark_outdated(None).startswith(mirrors.OUTDATED_MARK)


def test_colab_stub_is_a_notebook_that_points_at_the_official_quickstart():
    notebook = json.loads(mirrors.colab_stub())
    assert notebook["nbformat"] == 4
    text = json.dumps(notebook)
    assert "razzant/ouroboros/main/notebooks/colab_quickstart.py" in text
    assert "joi-lab" not in text


def test_main_commit_keeps_both_histories_and_is_idempotent(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    base = _commit(repo, {"VERSION": "6.7.3\n", "README.md": "old\n"}, "base")
    mirror_main = _commit(repo, {"README.md": "moved\n"}, "tombstone")
    _git(repo, "checkout", "-q", base)
    source = _commit(repo, {"VERSION": "7.6.0\n", "README.md": "official\n", "app.py": "x = 1\n"}, "stable")
    monkeypatch.chdir(repo)
    fork = mirrors.MIRRORS[0]

    new = mirrors.build_main_commit(fork, source, mirror_main, "7.6.0")

    assert _git(repo, "rev-parse", f"{new}^1") == mirror_main  # no rewrite: a fast-forward of the mirror
    assert _git(repo, "rev-parse", f"{new}^2") == source
    assert _git(repo, "show", f"{new}:app.py") == "x = 1"
    assert _git(repo, "show", f"{new}:README.md").startswith("# Ouroboros moved to razzant/ouroboros")
    assert json.loads(_git(repo, "show", f"{new}:notebooks/quickstart.ipynb"))["nbformat"] == 4
    assert mirrors.build_main_commit(fork, source, new, "7.6.0") is None
    desktop = mirrors.build_main_commit(mirrors.MIRRORS[1], source, mirror_main, "7.6.0")
    assert "notebooks/quickstart.ipynb" not in _git(repo, "ls-tree", "-r", "--name-only", desktop)
