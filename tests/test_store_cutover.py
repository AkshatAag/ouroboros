"""The cutover's two structural guards.

1. No production code reads, patches, unlinks or stats ``task_results/*.json`` outside the
   importer, the exporter, the quarantine and the sidecar locks (two-sided: the guard must
   catch a planted violation).
2. Every named store query is answered from an index: ``EXPLAIN QUERY PLAN`` over a store of
   3,000 rows shows no table scan of ``tasks`` or ``task_bodies`` (two-sided: a scan-shaped
   query is caught).
"""
from __future__ import annotations

import pathlib
import re
import sqlite3
import subprocess
import sys

import pytest

from ouroboros import store, task_store

REPO = pathlib.Path(__file__).resolve().parents[1]

# The only production modules that may touch a result FILE: the importer/exporter, the schema
# owner's quarantine, the result module's own path helpers (sidecar locks, quarantine, the
# importer's directory) and the two sidecar-lock holders.
RESULT_FILE_OWNERS = {
    "ouroboros/store_import.py", "ouroboros/task_result_schema.py", "ouroboros/task_results.py",
    "ouroboros/terminal_projection.py", "ouroboros/owner_pause.py", "ouroboros/task_custody.py",
    "ouroboros/store_switch.py", "ouroboros/presence_runner.py",
}
# Directory-layout mentions that are not result files: the artifact tree under task_results/,
# data-root layout lists, lineage labels.
_ALLOWED_LINE = re.compile(
    r"task_results[\"'/)]*\s*/\s*[\"']?(artifacts|_ARTIFACT|versions)|ARTIFACTS_DIR|"
    r"lineage=\"task_results\"|\"task_results\",\s*\"uploads\"|\(\"memory\", \"logs\", \"state\", \"task_results\"\)|"
    r"_EVIDENCE_DIRS|\"task_results\",$|skill_payload_policy|task_results/artifacts|task_results\" / _ARTIFACT"
)
_VIOLATION = re.compile(
    r"task_result_path\(|task_results_dir\(|[\"']task_results[\"']\s*/\s*f?[\"'][^\"']*\.json|"
    r"/\s*[\"']task_results[\"']\s*/\s*f[\"']\{|task_results\.json|\"task_results\"\s*\)\s*\.(glob|iterdir)"
)


def _production_files():
    for folder in ("ouroboros", "supervisor", "skills"):
        yield from sorted((REPO / folder).rglob("*.py"))
    yield REPO / "server.py"


def result_file_consumers(files, base) -> list:
    hits = []
    for path in files:
        rel = path.relative_to(base).as_posix()
        if rel in RESULT_FILE_OWNERS:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _VIOLATION.search(line) and not _ALLOWED_LINE.search(line) and not line.lstrip().startswith("#"):
                hits.append(f"{rel}:{number}: {line.strip()}")
    return hits


def test_no_production_code_touches_a_result_file_outside_its_owners():
    assert result_file_consumers(_production_files(), REPO) == []


def test_the_consumer_guard_catches_a_planted_file_read(tmp_path):
    planted = tmp_path / "ouroboros" / "planted.py"
    planted.parent.mkdir(parents=True)
    planted.write_text(
        'import json\n'
        'def read(root, tid):\n'
        '    return json.loads((root / "task_results" / f"{tid}.json").read_text(encoding="utf-8"))\n',
        encoding="utf-8")
    hits = result_file_consumers([planted], tmp_path)
    assert hits and hits[0].startswith("ouroboros/planted.py:3")


@pytest.fixture(scope="module")
def populated(tmp_path_factory):
    root = tmp_path_factory.mktemp("store")
    with store.open_store(root, write=True) as txn:
        for i in range(3000):
            lineage = ({"delegation_role": "subagent", "parent_task_id": f"t{i - 1}", "root_task_id": "t0"}
                       if i % 7 else {"project_id": f"p{i % 5}"})
            txn.store_task({"task_id": f"t{i}", "status": "completed" if i % 3 else "running", "_schema_version": 1,
                            "ts": f"2026-01-01T00:{i // 60 % 60:02d}:{i % 60:02d}Z", **lineage})
    return root


def _plan(root, sql, params=()):
    conn = sqlite3.connect(store.store_path(root))
    try:
        return [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()]
    finally:
        conn.close()


def _scans(plan):
    return [step for step in plan if re.match(r"SCAN (tasks|task_bodies|t|b)\b", step)
            and "USING COVERING INDEX tasks_ts" not in step and "USING INDEX tasks_updated" not in step]


_BODY_JOIN = "SELECT b.body, t.* FROM tasks AS t JOIN task_bodies AS b ON b.task_id = t.task_id WHERE {where} ORDER BY t.task_id"


def named_queries():
    """``(name, sql, params)`` for every store read the runtime names (the bounded ordered
    scans ``newest_ids``/``inventory`` read an index in order and are allowed by ``_scans``)."""
    lineage_where, lineage_params = task_store.lineage_where("t5", "t0", direct_only=False)
    direct_where, direct_params = task_store.lineage_where("t5", "", direct_only=True)
    ids = ",".join("?" * 20)
    return [
        ("find_child_tasks subtree", _BODY_JOIN.format(where=lineage_where), lineage_params),
        ("find_child_tasks direct", _BODY_JOIN.format(where=direct_where), direct_params),
        ("list_task_results(statuses)", _BODY_JOIN.format(where="t.status IN (?,?)"), ("running", "interrupted")),
        ("documents_by_ids", _BODY_JOIN.format(where=f"t.task_id IN ({ids})"), tuple(f"t{i}" for i in range(20))),
        ("nonterminal members", "SELECT task_id FROM tasks WHERE status IN ('running','interrupted')", ()),
        *[(f"{name} members", f"SELECT task_id FROM tasks WHERE {column}=1", ())
          for name, column in store.FLAG_COLUMNS.items()],
        ("task_facts(lineage)", f"SELECT {','.join(task_store.FACT_KEYS)} FROM tasks WHERE delegation_role='subagent' AND "
                                "(parent_task_id=? OR root_task_id=?) ORDER BY task_id", ("t5", "t0")),
        ("task_facts(tree census)", f"SELECT {','.join(task_store.FACT_KEYS)} FROM tasks WHERE root_task_id=? OR root_task_id IS NULL "
                                    "ORDER BY task_id", ("t0",)),
        ("task_facts(project)", f"SELECT {','.join(task_store.FACT_KEYS)} FROM tasks WHERE project_id=? ORDER BY task_id", ("p1",)),
        ("newest_ids(project roots)", f"SELECT task_id FROM tasks WHERE project_id=? AND {task_store.ROOT_RESULT_WHERE} "
                                      "ORDER BY ts DESC, task_id DESC", ("p1",)),
        ("newest_ids(limit)", "SELECT task_id FROM tasks ORDER BY ts DESC, task_id DESC LIMIT 50", ()),
        ("inventory", "SELECT task_id, write_seq, updated_at FROM tasks ORDER BY updated_at DESC, task_id DESC", ()),
        ("read_document", "SELECT body FROM task_bodies WHERE task_id=?", ("t5",)),
        ("signature", "SELECT write_seq, updated_at FROM tasks WHERE task_id=?", ("t5",)),
        ("owner-held members", "SELECT id, facts FROM obligations WHERE set_name=?", ("custody_open",)),
    ]


@pytest.mark.parametrize("name,sql,params", named_queries(), ids=[row[0] for row in named_queries()])
def test_named_queries_read_an_index_never_the_table(populated, name, sql, params):
    plan = _plan(populated, sql, params)
    assert not _scans(plan), f"{name}: {plan}"


def test_the_plan_guard_catches_a_table_scan(populated):
    plan = _plan(populated, "SELECT task_id FROM tasks WHERE child_drive_root=?", ("/x",))
    assert _scans(plan)


def test_the_body_marker_sweep_is_the_one_declared_scan(populated):
    """``ids_with_fields`` (the acceptance settlement sweep) reads every body once with LIKE;
    it replaced a directory listing plus a full read of every file and stays a declared scan."""
    plan = _plan(populated, "SELECT task_id FROM task_bodies WHERE body LIKE ? ORDER BY task_id", ('%"x":%',))
    assert _scans(plan)


def test_select_lineage_matches_the_python_prefilter(populated):
    """The WHERE and the retired Python prefilter select the same rows."""
    def may_match(item, parent, root_id, direct_only):
        if str(item.get("superseded_by") or item.get("retry_task_id") or "").strip():
            return True
        if str(item.get("delegation_role") or "") != "subagent":
            return False
        if parent and str(item.get("parent_task_id") or "") == parent:
            return True
        return bool(not direct_only and root_id and str(item.get("root_task_id") or "") == root_id)

    everything = task_store.list_documents(populated)
    for parent, root_id, direct_only in (("t5", "t0", False), ("t5", "", True), ("", "t0", False), ("t9", "t0", True)):
        expected = sorted((row["task_id"] for row in everything if may_match(row, parent, root_id, direct_only)))
        selected = [row["task_id"] for row in task_store.select_lineage(populated, parent, root_id, direct_only=direct_only)]
        assert selected == expected, (parent, root_id, direct_only)


def test_the_store_opens_in_a_fresh_interpreter_without_the_runtime(tmp_path):
    """``task_store`` depends on ``store`` alone (an importer/exporter and the P8 lane run it bare)."""
    code = ("import pathlib, sys; from ouroboros import task_store; "
            "print(task_store.exists(pathlib.Path(sys.argv[1]), 'none'))")
    out = subprocess.run([sys.executable, "-c", code, str(tmp_path)], capture_output=True, text=True, cwd=REPO,
                         env={"PYTHONPATH": str(REPO), "PATH": "/usr/bin:/bin", "OUROBOROS_DATA_DIR": str(tmp_path)})
    assert out.returncode == 0 and out.stdout.strip() == "False", out.stderr
