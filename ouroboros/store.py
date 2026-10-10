"""The current-facts store ``state/current.sqlite`` and the SQLite core it shares with money.

Dormant until the cutover activates it: nothing in production reads, writes or
switches through this module yet (``task_results/*.json`` and the obligation set
files stay the authority). Here already: the connection, lock-tier, identity,
commit and transaction core ``usage_store`` imports back (one protocol for both
files: rollback journal, ``synchronous=FULL``, no WAL, one connection per
transaction, ``BEGIN IMMEDIATE`` for writes, every name-tier access inside one
name lock, nothing above SQLite 3.24 so the bundled engine's file opens on a
3.37 system engine); the schema (``tasks`` typed facts and obligation flags,
``task_bodies`` the original document joined only by ``load``, ``obligations``
owner-held sets, ``task_handoffs``/``custody_runs``/``custody_outbox`` filled by
the cutover, ``meta``); the tri-state open (``probe``/``open_store``); ``update_task``
(the phase read inside the transaction); ``load`` (body plus the row-backed fields
``ROW_BACKED_FIELDS`` names once the cutover decides them).
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import pathlib
import sqlite3
import threading
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

STORE_REL = pathlib.Path("state/current.sqlite")
LOCK_NAME = "current.lock"  # the name-tier lock around every access
IMPORT_LOCK_NAME = "current_import.lock"
GENERATION = 8  # the lifecycle generation that owns the store
GENERATION_KEY = "current_store_generation"  # in state/migrations.json
SCHEMA_VERSION = 1
STORE_WAIT_SEC = 10.0
TIER_ENFORCED, TIER_NAME = "enforced", "name"
PUBLISHED, EXPORTING, RETIRED = "published", "exporting", "retired"
# The header's application id names the lock protocol before the file is opened ('OUCE'/'OUCN').
_APPLICATION_ID = {TIER_ENFORCED: 0x4F554345, TIER_NAME: 0x4F55434E}
TYPED_COLUMNS = ("status", "parent_task_id", "root_task_id", "delegation_role", "superseded_by",
                 "retry_task_id", "project_id", "ts", "updated_at", "child_drive_root",
                 "headless_child_drive_root", "presence_binding_id")
# set name -> flag column; the writer decides ``pending_drive`` (nonterminal, or terminal files not ready)
FLAG_COLUMNS = {"synthesis": "owes_synthesis", "terminal_projection": "owes_terminal_projection",
                "promotions": "has_pending_promotion", "pause_notices": "has_pause_notices",
                "delegated_runs": "delegated_runs_unreconciled", "pending_drives": "pending_drive"}
ROW_BACKED_FIELDS: Tuple[str, ...] = ()  # document fields that live only in rows (the cutover names them)
_SCHEMA = """
CREATE TABLE tasks (
  task_id TEXT PRIMARY KEY, status TEXT, parent_task_id TEXT, root_task_id TEXT, delegation_role TEXT,
  superseded_by TEXT, retry_task_id TEXT, project_id TEXT, ts TEXT, updated_at TEXT,
  child_drive_root TEXT, headless_child_drive_root TEXT, presence_binding_id TEXT,
  presence_authority_recorded INTEGER NOT NULL DEFAULT 0,
  owes_synthesis INTEGER NOT NULL DEFAULT 0, owes_terminal_projection INTEGER NOT NULL DEFAULT 0,
  has_pending_promotion INTEGER NOT NULL DEFAULT 0, has_pause_notices INTEGER NOT NULL DEFAULT 0,
  delegated_runs_unreconciled INTEGER NOT NULL DEFAULT 0, pending_drive INTEGER NOT NULL DEFAULT 0);
CREATE INDEX tasks_parent ON tasks(parent_task_id); CREATE INDEX tasks_root ON tasks(root_task_id);
CREATE INDEX tasks_status ON tasks(status);
CREATE INDEX tasks_lineage ON tasks(task_id) WHERE superseded_by IS NOT NULL OR retry_task_id IS NOT NULL;
CREATE TABLE task_bodies (task_id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE obligations (set_name TEXT NOT NULL, id TEXT NOT NULL, facts TEXT NOT NULL, PRIMARY KEY (set_name, id));
CREATE TABLE task_handoffs (task_id TEXT NOT NULL, op_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT,
  claimed_at TEXT, returned_at TEXT, owner TEXT, facts TEXT NOT NULL, PRIMARY KEY (task_id, op_id));
CREATE TABLE custody_runs (run_id TEXT PRIMARY KEY, task_id TEXT, state TEXT, facts TEXT NOT NULL, updated_at TEXT);
CREATE INDEX custody_task ON custody_runs(task_id); CREATE INDEX custody_state ON custody_runs(state);
CREATE TABLE custody_outbox (event_id TEXT PRIMARY KEY, run_id TEXT, task_id TEXT, event TEXT NOT NULL, created_at TEXT);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
""" + "".join(f"CREATE INDEX tasks_{column} ON tasks(task_id) WHERE {column}=1;\n"
              for column in FLAG_COLUMNS.values())


class StoreError(RuntimeError):
    """The current-facts store cannot serve; the subclass says why."""


class StoreMigrating(StoreError):
    """Not published yet: the importer runs, or never ran on this root."""


class StoreRetired(StoreError):
    """The export began or completed: the files are the authority now."""


class StoreLost(StoreError):
    """The store is missing beside its completed stamp, or unreadable; never replaced silently."""


class StoreBusy(StoreError):
    """Contention or exhausted process resources: retry; never a new store."""


# ---- the core both SQLite stores share --------------------------------------
_PROCESS_LOCKS: Dict[str, threading.RLock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()


def process_lock(path: pathlib.Path) -> threading.RLock:
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(str(path), threading.RLock())


def file_identity(path: pathlib.Path) -> Optional[tuple]:
    """``(inode, device)`` of a present file; ``None`` only for a missing one."""
    try:
        info = os.stat(path)
    except FileNotFoundError:
        return None
    return info.st_ino, info.st_dev


def header_tier(path: pathlib.Path, application_ids: Dict[str, int]) -> Optional[str]:
    """The lock tier the file's SQLite header names; ``None`` for a missing or foreign file."""
    try:
        with open(path, "rb") as handle:
            header = handle.read(100)
    except FileNotFoundError:
        return None
    if len(header) < 100 or not header.startswith(b"SQLite format 3\x00"):
        return None
    application_id = int.from_bytes(header[68:72], "big")
    return next((tier for tier, value in application_ids.items() if value == application_id), None)


def connect(path: pathlib.Path, tier: str, *, create: bool = False, wait_sec: float = 0.0) -> sqlite3.Connection:
    """A connection with the tier's lock protocol (``sqlite3.Error`` is the caller's to type);
    ``wait_sec`` is SQLite's busy wait on the enforced tier (the name tier's one lock is the
    name protocol the caller holds); no statement runs here, the first may wait on a commit."""
    query = "mode=rwc" if create else "mode=rw"
    if tier == TIER_NAME:
        query += "&vfs=" + ("win32-none" if os.name == "nt" else "unix-none")
    conn = sqlite3.connect(f"{path.as_uri()}?{query}", uri=True, isolation_level=None,
                           timeout=max(0.0, wait_sec) if tier == TIER_ENFORCED else 0.0)
    conn.row_factory = sqlite3.Row
    return conn


def busy(exc: BaseException) -> bool:
    code = getattr(exc, "sqlite_errorcode", None)
    return code in {5, 6} or "locked" in str(exc).lower()


def commit(conn: sqlite3.Connection) -> None:
    """COMMIT, rolling back on failure; the ``sqlite3.OperationalError`` is the caller's to type."""
    try:
        conn.execute("COMMIT")
    except sqlite3.OperationalError:
        with contextlib.suppress(sqlite3.Error):
            conn.execute("ROLLBACK")
        raise


def fsync_directory(directory: pathlib.Path) -> None:
    if os.name == "nt":
        return
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection, *, write: bool, commit_wait_sec: Optional[float] = None,
                finish: Optional[Callable[[], None]] = None) -> Iterator[sqlite3.Connection]:
    """One transaction: a write takes the lock at ``BEGIN IMMEDIATE``, a read's only wait is
    here; any exception rolls back and propagates; ``finish`` (default ``commit``) runs at the
    end; ``commit_wait_sec`` is the busy wait a COMMIT spends on readers draining."""
    try:
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        if not write:  # take the read lock now
            conn.execute("SELECT 1 FROM meta LIMIT 1").fetchone()
        if commit_wait_sec is not None:
            conn.execute(f"PRAGMA busy_timeout = {int(commit_wait_sec * 1000)}")
        yield conn
    except BaseException:
        with contextlib.suppress(sqlite3.Error):
            conn.execute("ROLLBACK")
        raise
    (finish or (lambda: commit(conn)))()


# ---- the current-facts store ------------------------------------------------

def store_path(root: Any) -> pathlib.Path:
    return pathlib.Path(root) / STORE_REL


def generation_stamped(root: Any) -> bool:
    """Whether ``state/migrations.json`` records a completed store generation (a torn
    file proves nothing and reads as unstamped)."""
    from ouroboros.startup_migrations import watermarks

    try:
        return watermarks(root).get(GENERATION_KEY) == GENERATION
    except ValueError:
        return False


def probe(root: Any) -> Tuple[str, Optional[str]]:
    """``("absent", None)`` or ``("present", tier)``; the third answer is typed: ``StoreBusy`` when
    the file cannot be inspected (an exhausted descriptor table is not absence), ``StoreLost`` for a foreign file."""
    path = store_path(root)
    try:
        os.stat(path)
        tier = header_tier(path, _APPLICATION_ID)
    except FileNotFoundError:
        return "absent", None
    except OSError as exc:
        name = errno.errorcode.get(exc.errno, str(exc.errno))
        raise StoreBusy(f"current store not inspectable ({name}): {path}") from exc
    if tier is None:
        raise StoreLost(f"current store unreadable or foreign, never replaced: {path}")
    return "present", tier


def _typed(exc: sqlite3.Error) -> StoreError:
    if isinstance(exc, sqlite3.OperationalError) and busy(exc):
        return StoreBusy(f"current store busy: {exc}")
    if isinstance(exc, sqlite3.DatabaseError) and not isinstance(
            exc, (sqlite3.OperationalError, sqlite3.IntegrityError, sqlite3.ProgrammingError)):
        return StoreLost(f"current store unreadable: {exc}")
    return StoreError(f"current store unavailable: {exc}")


@contextlib.contextmanager
def name_lock(root: Any, filename: str, *, timeout_sec: float, stale_sec: float = 90.0) -> Iterator[None]:
    """The owner-aware name lock under ``state/`` (the name tier's one lock, the import lock)."""
    from ouroboros.platform_layer import acquire_exclusive_file_lock, release_exclusive_file_lock

    path = pathlib.Path(root) / "state" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = acquire_exclusive_file_lock(path, timeout_sec=timeout_sec, stale_sec=stale_sec, owner_aware_stale=True)
    if fd is None:
        raise StoreBusy(f"current store lock unavailable: {path}")
    try:
        yield
    finally:
        release_exclusive_file_lock(path, fd)


@contextlib.contextmanager
def open_store(root: Any, *, write: bool, wait_sec: float = STORE_WAIT_SEC) -> Iterator["Txn"]:
    """One transaction on the published store: ``StoreMigrating`` before publication, ``StoreLost`` for a
    missing store beside its completed stamp, ``StoreBusy`` on contention; ``mode=rw``, never creating."""
    root = pathlib.Path(root)
    state, tier = probe(root)
    if state == "absent":
        if generation_stamped(root):
            raise StoreLost(f"current store missing beside its completed stamp: {store_path(root)}")
        raise StoreMigrating(f"current store not published yet: {store_path(root)}")
    with contextlib.ExitStack() as stack:
        if tier == TIER_NAME:
            stack.enter_context(name_lock(root, LOCK_NAME, timeout_sec=wait_sec))
        try:
            conn = connect(store_path(root), tier, wait_sec=wait_sec)
        except sqlite3.Error as exc:
            raise _typed(exc) from exc
        stack.callback(conn.close)
        try:
            with transaction(conn, write=write, commit_wait_sec=wait_sec if tier == TIER_ENFORCED else None):
                yield Txn(conn)
        except sqlite3.Error as exc:
            raise _typed(exc) from exc


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))  # key order kept: facts round-trip


def encode_document(document: Dict[str, Any]) -> str:
    """The body text: the document minus its row-backed fields, serialized exactly as the
    result writer serializes a file (so an export reproduces the imported bytes)."""
    body = {key: value for key, value in document.items() if key not in ROW_BACKED_FIELDS}
    return json.dumps(body, ensure_ascii=False, indent=2)


def typed_columns(document: Dict[str, Any], *, pending_drive: Optional[bool] = None) -> Dict[str, Any]:
    """The ``tasks`` columns of a document: top-level typed facts and the obligation
    flags from the existing membership predicates."""
    from ouroboros.obligations import result_memberships

    names = result_memberships(document)
    columns: Dict[str, Any] = {}
    for column in TYPED_COLUMNS:
        value = document.get(column)
        columns[column] = None if value is None else str(value)
    columns["presence_authority_recorded"] = int(bool(document.get("presence_authority_recorded")))
    for name, column in FLAG_COLUMNS.items():
        columns[column] = int(name in names)
    columns["pending_drive"] = int("nonterminal" in names if pending_drive is None else pending_drive)
    return columns


class Txn:
    """One store transaction: documents, obligation rows, meta; riders of the cutover
    (handoffs, custody, outbox) share the same connection."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def meta(self, key: str) -> Any:
        record = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return None if record is None else json.loads(record["value"])

    def set_meta(self, key: str, value: Any) -> None:
        self.conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, _json(value)))

    def phase(self) -> str:
        return str(self.meta("phase") or PUBLISHED)

    def task_ids(self) -> List[str]:
        return [record["task_id"] for record in self.conn.execute("SELECT task_id FROM tasks ORDER BY task_id")]

    def body_text(self, task_id: str) -> Optional[str]:
        record = self.conn.execute("SELECT body FROM task_bodies WHERE task_id=?", (task_id,)).fetchone()
        return None if record is None else record["body"]

    def load(self, task_id: str) -> Optional[Dict[str, Any]]:
        """The document: its body plus the row-backed fields read from ``tasks``."""
        record = self.conn.execute(
            "SELECT b.body, t.* FROM task_bodies AS b JOIN tasks AS t ON t.task_id = b.task_id WHERE b.task_id=?",
            (task_id,)).fetchone()
        if record is None:
            return None
        document = json.loads(record["body"])
        for field in ROW_BACKED_FIELDS:
            document[field] = record[field]
        return document

    def store_task(self, document: Dict[str, Any], *, pending_drive: Optional[bool] = None) -> None:
        """Upsert the body and the typed columns of one document."""
        task_id = str(document["task_id"])
        columns = typed_columns(document, pending_drive=pending_drive)
        self.conn.execute("INSERT OR REPLACE INTO task_bodies (task_id, body) VALUES (?, ?)",
                          (task_id, encode_document(document)))
        names = ("task_id", *columns)
        self.conn.execute(f"INSERT OR REPLACE INTO tasks ({','.join(names)}) VALUES ({','.join('?' * len(names))})",
                          (task_id, *columns.values()))

    def delete_task(self, task_id: str) -> None:
        self.conn.execute("DELETE FROM task_bodies WHERE task_id=?", (task_id,))
        self.conn.execute("DELETE FROM tasks WHERE task_id=?", (task_id,))

    def set_names(self) -> List[str]:
        return [record["set_name"] for record in
                self.conn.execute("SELECT DISTINCT set_name FROM obligations ORDER BY set_name")]

    def set_rows(self, set_name: str) -> Dict[str, Dict[str, Any]]:
        """One obligation set, in insertion order (the order its file had)."""
        return {record["id"]: json.loads(record["facts"]) for record in self.conn.execute(
            "SELECT id, facts FROM obligations WHERE set_name=? ORDER BY rowid", (set_name,))}

    def replace_set(self, set_name: str, rows: Dict[str, Dict[str, Any]]) -> None:
        self.conn.execute("DELETE FROM obligations WHERE set_name=?", (set_name,))
        self.conn.executemany("INSERT INTO obligations (set_name, id, facts) VALUES (?, ?, ?)",
                              [(set_name, str(identity), _json(facts)) for identity, facts in rows.items()])

    def flagged(self, set_name: str) -> List[str]:
        """Task ids whose flag column says they belong to a derived set."""
        column = FLAG_COLUMNS[set_name]
        return [record["task_id"] for record in
                self.conn.execute(f"SELECT task_id FROM tasks WHERE {column}=1 ORDER BY task_id")]


def update_task(root: Any, task_id: str, mutator: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]], *,
                riders: Tuple[Callable[[Txn, Dict[str, Any], Dict[str, Any]], None], ...] = (),
                pending_drive: Optional[bool] = None) -> Optional[Dict[str, Any]]:
    """The write seam: ``BEGIN IMMEDIATE``; the phase read inside (``StoreRetired`` once an export
    began, which a straggler's older connection sees in the retired inode); the mutator over the
    current document (``None`` = no write: the read-only transaction commits empty); the
    cutover's riders on the same connection; COMMIT."""
    with open_store(root, write=True) as txn:
        phase = txn.phase()
        if phase != PUBLISHED:
            raise StoreRetired(f"current store phase is {phase}: the files are the authority")
        current = txn.load(task_id) or {}
        updated = mutator(json.loads(json.dumps(current)))
        if updated is None:
            return None
        txn.store_task({**updated, "task_id": task_id}, pending_drive=pending_drive)
        for rider in riders:
            rider(txn, current, updated)
        return updated
