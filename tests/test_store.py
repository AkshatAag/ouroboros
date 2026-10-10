"""The current-facts store core (``ouroboros/store.py``): the SQLite core money imports
back, the tri-state open, the lock tiers, the phase inside the write seam, the document
round trip and the SQLite 3.24 floor the CI floor job proves on a 3.37 system engine."""
from __future__ import annotations

import errno
import json
import os
import pathlib
import re
import sqlite3

import pytest

from ouroboros import platform_layer, store, usage_store
from ouroboros.store import StoreBusy, StoreLost, StoreMigrating, StoreRetired

REPO = pathlib.Path(__file__).resolve().parents[1]


def publish(root: pathlib.Path, *documents: dict, tier: str = store.TIER_ENFORCED,
            phase: str = store.PUBLISHED) -> pathlib.Path:
    """A published store with ``documents``, built as the importer builds one."""
    path = store.store_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = store.connect(path, tier, create=True)
    try:
        conn.execute("PRAGMA synchronous = FULL")
        conn.executescript(store._SCHEMA)
        conn.execute(f"PRAGMA application_id = {store._APPLICATION_ID[tier]}")
        conn.execute("BEGIN IMMEDIATE")
        txn = store.Txn(conn)
        for document in documents:
            txn.store_task(document)
        txn.set_meta("schema_version", store.SCHEMA_VERSION)
        txn.set_meta("lock_tier", tier)
        txn.set_meta("phase", phase)
        store.commit(conn)
    finally:
        conn.close()
    return path


def document(task_id: str, status: str = "completed", **fields) -> dict:
    return {"task_id": task_id, "status": status, "ts": "2026-10-10T00:00:00+00:00",
            "_schema_version": 1, **fields}


# ---- the core money imports back ----------------------------------------------

def test_money_runs_on_the_shared_core():
    assert usage_store.TIER_NAME is store.TIER_NAME and usage_store.TIER_ENFORCED is store.TIER_ENFORCED
    assert usage_store._identity is store.file_identity
    assert usage_store._process_lock is store.process_lock
    assert usage_store._fsync_directory is store.fsync_directory
    assert usage_store._busy is store.busy


def test_the_two_stores_name_their_own_lock_protocol_in_the_header(tmp_path):
    facts = publish(tmp_path)
    usage_store.migrate_from_journal(tmp_path)  # an empty money store on the same root
    money = tmp_path / usage_store.STORE_REL
    assert store.header_tier(facts, store._APPLICATION_ID) == store.TIER_ENFORCED
    assert store.header_tier(money, store._APPLICATION_ID) is None  # money's header is not ours
    assert usage_store._header_tier(money) == store.TIER_ENFORCED
    assert usage_store._header_tier(facts) is None
    assert store.header_tier(tmp_path / "missing.sqlite", store._APPLICATION_ID) is None


def test_money_errors_keep_their_vocabulary_over_the_shared_core(tmp_path):
    from ouroboros.usage_ledger import UsageAccountingError

    with pytest.raises(UsageAccountingError, match="usage store unavailable"):
        usage_store._connect(tmp_path / "state" / "absent.sqlite", store.TIER_ENFORCED)


# ---- the tri-state open ---------------------------------------------------------

def test_a_missing_store_is_migrating_until_published_and_lost_beside_its_stamp(tmp_path):
    from ouroboros.startup_migrations import stamp

    assert store.probe(tmp_path) == ("absent", None)
    with pytest.raises(StoreMigrating):
        with store.open_store(tmp_path, write=False):
            pass
    assert not store.store_path(tmp_path).exists()  # mode=rw never creates
    stamp(tmp_path, **{store.GENERATION_KEY: store.GENERATION})
    assert store.generation_stamped(tmp_path)
    with pytest.raises(StoreLost):
        with store.open_store(tmp_path, write=True):
            pass
    assert not store.store_path(tmp_path).exists()


@pytest.mark.parametrize("code", [errno.EMFILE, errno.ENFILE])
def test_an_exhausted_descriptor_table_is_not_absence(tmp_path, monkeypatch, code):
    """The #1307 rule: the process that cannot open files must not conclude the store is
    missing (and then create or rebuild one); the same stat failing with ENOENT is absence."""
    path = store.store_path(tmp_path)
    real_stat = os.stat

    def failing(target, *args, **kwargs):
        if pathlib.Path(target) == path:
            raise OSError(code, os.strerror(code))
        return real_stat(target, *args, **kwargs)

    monkeypatch.setattr(os, "stat", failing)
    with pytest.raises(StoreBusy, match=errno.errorcode[code]):
        store.probe(tmp_path)
    with pytest.raises(StoreBusy):
        with store.open_store(tmp_path, write=True):
            pass
    monkeypatch.setattr(os, "stat", real_stat)
    assert store.probe(tmp_path) == ("absent", None)  # the quiet direction: ENOENT is absence
    assert not path.exists()


def test_a_present_foreign_file_is_lost_never_replaced(tmp_path):
    path = store.store_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not a database at all, but someone's bytes" * 4)
    with pytest.raises(StoreLost):
        store.probe(tmp_path)
    with pytest.raises(StoreLost):
        with store.open_store(tmp_path, write=True):
            pass
    assert path.read_bytes().startswith(b"not a database")


def test_a_published_but_unstamped_store_is_accepted_by_its_own_meta(tmp_path):
    publish(tmp_path, document("t1"))
    assert not store.generation_stamped(tmp_path)
    assert store.probe(tmp_path) == ("present", store.TIER_ENFORCED)
    with store.open_store(tmp_path, write=False) as txn:
        assert txn.phase() == store.PUBLISHED
        assert txn.task_ids() == ["t1"]


# ---- lock tiers -----------------------------------------------------------------

def test_the_name_tier_opens_without_sqlite_locks_and_takes_the_name_lock_on_every_access(tmp_path, monkeypatch):
    publish(tmp_path, document("t1"), tier=store.TIER_NAME)
    assert store.probe(tmp_path) == ("present", store.TIER_NAME)
    uris, locks = [], []
    real_connect, real_acquire = sqlite3.connect, platform_layer.acquire_exclusive_file_lock

    def spy_connect(database, *args, **kwargs):
        uris.append(str(database))
        return real_connect(database, *args, **kwargs)

    def spy_acquire(path, **kwargs):
        locks.append(pathlib.Path(path).name)
        return real_acquire(path, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", spy_connect)
    monkeypatch.setattr(platform_layer, "acquire_exclusive_file_lock", spy_acquire)
    with store.open_store(tmp_path, write=False) as txn:
        assert txn.load("t1")["task_id"] == "t1"
    store.update_task(tmp_path, "t1", lambda current: {**current, "status": "running"})
    assert locks == [store.LOCK_NAME, store.LOCK_NAME]
    assert all("vfs=unix-none" in uri or "vfs=win32-none" in uri for uri in uris)


def test_the_enforced_tier_uses_sqlite_locks_only(tmp_path, monkeypatch):
    publish(tmp_path, document("t1"))
    locks = []
    real_acquire = platform_layer.acquire_exclusive_file_lock
    monkeypatch.setattr(platform_layer, "acquire_exclusive_file_lock",
                        lambda path, **kwargs: locks.append(pathlib.Path(path).name) or real_acquire(path, **kwargs))
    with store.open_store(tmp_path, write=True) as txn:
        txn.set_meta("probe", 1)
    assert store.LOCK_NAME not in locks


def test_a_held_name_lock_is_busy_never_a_hang(tmp_path):
    with store.name_lock(tmp_path, store.LOCK_NAME, timeout_sec=0.0):
        with pytest.raises(StoreBusy):
            with store.name_lock(tmp_path, store.LOCK_NAME, timeout_sec=0.0):
                pass
    with store.name_lock(tmp_path, store.LOCK_NAME, timeout_sec=0.0):
        pass  # released: the quiet direction


def test_contention_is_busy_typed(tmp_path):
    publish(tmp_path, document("t1"))
    holder = store.connect(store.store_path(tmp_path), store.TIER_ENFORCED)
    try:
        holder.execute("BEGIN IMMEDIATE")
        with pytest.raises(StoreBusy):
            with store.open_store(tmp_path, write=True, wait_sec=0.0):
                pass
    finally:
        holder.execute("ROLLBACK")
        holder.close()
    with store.open_store(tmp_path, write=True, wait_sec=0.0) as txn:  # free again: quiet
        txn.set_meta("probe", 2)


# ---- the phase inside the write seam ---------------------------------------------

def test_update_task_refuses_once_the_export_began_and_writes_nothing(tmp_path):
    publish(tmp_path, document("t1"))
    assert store.update_task(tmp_path, "t1", lambda current: {**current, "note": "before"})["note"] == "before"
    with store.open_store(tmp_path, write=True) as txn:
        txn.set_meta("phase", store.EXPORTING)
    with pytest.raises(StoreRetired):
        store.update_task(tmp_path, "t1", lambda current: {**current, "note": "after"})
    with store.open_store(tmp_path, write=False) as txn:
        assert txn.load("t1")["note"] == "before"


def test_a_straggler_after_the_retirement_is_told_retired_not_migrating(tmp_path):
    """The seam commits ``phase=retired`` into the store, clears the stamp and renames it aside.
    A writer that opened the file earlier reads that phase inside its own transaction; a writer
    that opens by path now (``update_task``, a reader) is told ``StoreRetired`` by the sibling,
    never «not published yet»."""
    from ouroboros.startup_migrations import stamp

    path = publish(tmp_path, document("t1"))
    straggler = store.connect(path, store.TIER_ENFORCED)  # opened before the seam ran
    with store.open_store(tmp_path, write=True) as txn:
        txn.set_meta("phase", store.RETIRED)
    stamp(tmp_path, **{store.GENERATION_KEY: None})
    retired = path.with_name(path.name + ".exported-test")
    os.replace(path, retired)
    with store.transaction(straggler, write=True):
        assert store.Txn(straggler).phase() == store.RETIRED  # the retired inode, read inside the transaction
    straggler.close()
    with pytest.raises(StoreRetired):
        store.update_task(tmp_path, "t1", lambda current: {**current, "note": "late"})
    with pytest.raises(StoreRetired):
        with store.open_store(tmp_path, write=False):
            pass
    with sqlite3.connect(retired) as conn:
        assert "late" not in conn.execute("SELECT body FROM task_bodies WHERE task_id='t1'").fetchone()[0]
    retired.unlink()  # no sibling, no stamp: the plain pre-publication answer
    with pytest.raises(StoreMigrating):
        with store.open_store(tmp_path, write=False):
            pass


# ---- documents, typed columns, obligation rows ------------------------------------

def test_update_task_round_trips_the_document_and_projects_the_typed_columns(tmp_path):
    publish(tmp_path, document("t1", parent_task_id="root", root_task_id="root", delegation_role="child"))
    assert store.update_task(tmp_path, "t1", lambda current: None) is None  # no write
    updated = store.update_task(tmp_path, "t1", lambda current: {**current, "status": "running", "extra": {"k": [1, 2.5]}})
    with store.open_store(tmp_path, write=False) as txn:
        assert txn.load("t1") == updated
        assert txn.load("absent") is None
        row = txn.conn.execute("SELECT * FROM tasks WHERE task_id='t1'").fetchone()
        assert (row["status"], row["parent_task_id"], row["root_task_id"], row["delegation_role"]) == (
            "running", "root", "root", "child")
        assert row["pending_drive"] == 1 and row["owes_synthesis"] == 0
        assert txn.body_text("t1") == json.dumps(updated, ensure_ascii=False, indent=2)
    store.update_task(tmp_path, "t1", lambda current: {**current, "status": "completed", "pause_notices": [{"n": 1}]},
                      pending_drive=True)
    with store.open_store(tmp_path, write=False) as txn:
        row = txn.conn.execute("SELECT * FROM tasks WHERE task_id='t1'").fetchone()
        assert (row["pending_drive"], row["has_pause_notices"]) == (1, 1)
        assert txn.flagged("pause_notices") == ["t1"] and txn.flagged("synthesis") == []


def test_riders_share_the_write_transaction(tmp_path):
    publish(tmp_path, document("t1"))

    def rider(txn, current, updated):
        txn.conn.execute("INSERT INTO custody_runs (run_id, task_id, state, facts) VALUES (?, ?, ?, ?)",
                         (f"run{updated['n']}", updated["task_id"], "started", "{}"))
        if updated.get("boom"):
            raise RuntimeError("the rider refused")

    store.update_task(tmp_path, "t1", lambda current: {**current, "n": 1}, riders=(rider,))
    with pytest.raises(RuntimeError, match="the rider refused"):  # the rider's own exception, not a constraint's
        store.update_task(tmp_path, "t1", lambda current: {**current, "n": 2, "boom": True}, riders=(rider,))
    with store.open_store(tmp_path, write=False) as txn:
        assert txn.load("t1")["n"] == 1  # the refused transaction rolled back whole
        assert [r["run_id"] for r in txn.conn.execute("SELECT run_id FROM custody_runs")] == ["run1"]  # run2 with it


def test_obligation_rows_keep_their_file_order(tmp_path):
    publish(tmp_path)
    rows = {"z": {"task_id": "z"}, "a": {"task_id": "a", "closed_runs": {"r": "x"}}, "m": {}}
    with store.open_store(tmp_path, write=True) as txn:
        txn.replace_set("custody_open", rows)
        txn.replace_set("unknowns", {"task:q": {"reason": "malformed"}})
    with store.open_store(tmp_path, write=False) as txn:
        assert txn.set_names() == ["custody_open", "unknowns"]
        assert list(txn.set_rows("custody_open").items()) == list(rows.items())


def test_encode_document_matches_the_result_writer_bytes(tmp_path):
    from ouroboros.utils import atomic_write_json

    row = document("t9", nested={"a": [1, {"b": "ü"}]}, cost_usd=0.000125)
    target = tmp_path / "t9.json"
    atomic_write_json(target, row)
    assert store.encode_document(row).encode("utf-8") == target.read_bytes()


# ---- the SQLite floor -----------------------------------------------------------------
_ABOVE_3_24 = re.compile(
    r"\bSTRICT\b|->>?|\bRETURNING\b|\bGENERATED\s+ALWAYS\b|\bjsonb?\(|\bjson_[a-z]+\(|\bIIF\(|\bVACUUM\s+INTO\b"
    r"|\bRENAME\s+COLUMN\b|\bDROP\s+COLUMN\b|\bOVER\s*\(|\bFILTER\s*\(|\bNULLS\s+(FIRST|LAST)\b", re.IGNORECASE)
STORE_MODULES = ("ouroboros/store.py", "ouroboros/store_import.py", "ouroboros/store_switch.py")


def _string_literals(source: str):
    """Every string literal except docstrings (prose, not SQL)."""
    import ast

    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                docstrings.add(id(first.value))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
            yield node.value


def _offending_sql(source: str):
    return [s for s in _string_literals(source) if _ABOVE_3_24.search(s)]


def test_nothing_above_sqlite_3_24_in_the_store_modules():
    """A file written by the bundled engine must open on a 3.37 system engine (the CI floor
    job runs this module there); the modules' SQL uses no syntax the floor lacks."""
    sources = {rel: (REPO / rel).read_text(encoding="utf-8") for rel in STORE_MODULES}  # all three exist
    for rel, source in sources.items():
        assert not _offending_sql(source), (rel, _offending_sql(source))
        # the self-check: a newer construct inserted into THIS module's real source is caught
        assert _offending_sql(source + "\nq = 'SELECT body ->> \"$.x\" FROM t'\n"), rel
        assert _offending_sql(source + "\nq = 'CREATE TABLE t (a) STRICT'\n"), rel
    assert not _offending_sql('"""a -> b docstring"""\ndef f() -> str:\n    """c -> d"""\n')  # prose, annotations
    assert sqlite3.sqlite_version_info >= (3, 24, 0)


def test_the_schema_builds_on_this_engine(tmp_path):
    path = publish(tmp_path, document("t1"))
    with sqlite3.connect(path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables == {"tasks", "task_bodies", "obligations", "task_handoffs", "custody_runs",
                          "custody_outbox", "meta"}
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0  # the version lives in meta
