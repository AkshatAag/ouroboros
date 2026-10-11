"""Current delegated custody for bookkeeping; evidence replay stays explicit.

Open runs, pending starts, undisposed patches and owned registrations are the
owner-held rows of ``custody_open``. Closing a run also owes an addressed
terminal-result refresh; its receipt travels in ``delegated_runs`` until that
refresh lands. The existing reducer defines every run field. The boot/periodic
sweep binds current reads so nested custody helpers cannot accidentally
cold-fold the historical chain. Every publication is one store transaction.
"""
from __future__ import annotations

import contextvars
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

from ouroboros import obligations as o
from ouroboros import store

_CURRENT = contextvars.ContextVar("current_custody_root", default=None)


def active(root) -> bool:
    return _CURRENT.get() == str(Path(root).resolve())


@contextmanager
def current_reads(root):
    token = _CURRENT.set(str(Path(root).resolve()))
    try:
        yield
    finally:
        _CURRENT.reset(token)


def _decode(raw):
    from ouroboros.delegate_custody import RunCustody
    from dataclasses import fields
    raw = deepcopy(raw)  # Reducer mutations must not also mutate the comparison snapshot.
    entry = RunCustody(**{f.name: raw[f.name] for f in fields(RunCustody) if f.name in raw})
    entry.verified_source_ranges = [tuple(pair) for pair in entry.verified_source_ranges]
    entry.output_reader_receipts = tuple(tuple(pair) for pair in entry.output_reader_receipts)
    for key, value in raw.items():
        if key.startswith("_"):
            setattr(entry, key, value)
    return entry


def _held(entry):
    return (not entry.settled or entry.project_owned
            or ((entry.snapshot_id or (entry.resource_ref.get("workspace_kind") == "directory"
                                      and entry.resource_ref.get("strategy") == "copy"))
                and not entry.patch_disposed))


def _runs(rows):
    return {key[4:]: _decode(facts["custody"]) for key, facts in rows.items() if key.startswith("run:")}


def state(root):
    return _runs(o.members(root, "custody_open"))


def pending(root):
    from ouroboros.delegate_pending import pending_invocations
    rows = [facts["request"] for key, facts in o.members(root, "custody_open").items()
            if key.startswith("invocation:")]
    return pending_invocations(root, rows=rows)


def snapshot(root):
    runs = {}
    with store.open_store(root, write=False) as txn:
        debts, open_rows = txn.set_rows("delegated_runs"), txn.set_rows("custody_open")
    for facts in debts.values():
        runs.update({rid: _decode(raw) for rid, raw in facts.get("closed_runs", {}).items()})
    runs.update(_runs(open_rows))
    return {"state": runs, "pending": pending(root), "current": True}


def _write_rows(txn, name, before, rows):
    for key in set(before) - set(rows):
        txn.delete_row(name, key)
    for key, facts in rows.items():
        if before.get(key) != facts:
            txn.put_row(name, key, facts)


@contextmanager
def publication(root, event):
    """Publish before append, then merge discharge with current state after it.

    Both halves are one store transaction each; the append in between is the event
    log's own write. A failure of either half is logged and leaves the row's fate to
    the append: the next start's import folds the retained chain into what the rows lack.
    """
    from ouroboros import delegate_custody as c

    rid, iid = str(event.get("run_id") or ""), str(event.get("invocation_id") or "")
    kind = event["type"]

    def publish():
        with store.open_store(root, write=True) as txn:
            rows = txn.set_rows("custody_open")
            before = dict(rows)
            # Work is addressable before its source event can be lost.
            if kind == c.START_REQUESTED and iid:
                rows.setdefault(f"invocation:{iid}", {"request": event})
            runs = _runs(rows)
            c._apply(runs, event)
            if kind == c.STARTED and rid in runs:
                rows[f"run:{rid}"] = {"custody": vars(runs[rid])}
            _write_rows(txn, "custody_open", before, rows)

    def discharge():
        with store.open_store(root, write=True) as txn:
            rows = txn.set_rows("custody_open")
            before = dict(rows)
            runs = _runs(rows)
            # Even with no STARTED in the set, retained closing receipts can accept
            # later output/patch facts without reading the event chain.
            debts = txn.set_rows("delegated_runs")
            prior_debts = {key: deepcopy(value) for key, value in debts.items()}
            if rid and rid not in runs:
                for facts in debts.values():
                    raw = facts.get("closed_runs", {}).get(rid)
                    if raw:
                        runs[rid] = _decode(raw)
                        break
            c._apply(runs, event)
            for run_id, entry in runs.items():
                if entry.settled and entry.task_id:
                    facts = dict(debts.get(entry.task_id, {}))
                    receipts = dict(facts.get("closed_runs", {}))
                    receipts[run_id] = vars(entry)
                    debts[entry.task_id] = {**facts, "task_id": entry.task_id, "closed_runs": receipts}
                if _held(entry):
                    rows[f"run:{run_id}"] = {"custody": vars(entry)}
                else:
                    rows.pop(f"run:{run_id}", None)
            if iid and (kind == c.STARTED or (kind == c.START_FAILED and event.get("definite") is True)):
                rows.pop(f"invocation:{iid}", None)
            # The closing receipt and the open-custody retirement commit together.
            _write_rows(txn, "delegated_runs", prior_debts, debts)
            _write_rows(txn, "custody_open", before, rows)

    _attempt(root, "custody", publish)
    landed = [False]
    yield landed
    if landed[0]:
        _attempt(root, "custody discharge", discharge)


def _attempt(root, what, fn):
    try:
        fn()
    except store.StoreError as exc:
        import logging
        logging.getLogger(__name__).warning("Custody %s not recorded under %s (%s); the retained chain keeps it",
                                            what, root, type(exc).__name__)


def rebuild(root, *, replace=False):
    """Explicit migration/repair only: fold the retained chain once."""
    from ouroboros import delegate_custody as c
    if c.custody_log_unreadable(root):
        raise o.ObligationsUnavailable("custody event chain is unreadable")
    rows = tuple(c._iter_rows(c.event_log_path(root)))
    runs = c._fold_rows(rows)
    result = {f"run:{rid}": {"custody": vars(entry)} for rid, entry in runs.items() if _held(entry)}
    for entry in c.pending_invocations(root, rows=rows):
        result[f"invocation:{entry['invocation_id']}"] = {"request": {"type": c.START_REQUESTED, **entry}}
    with store.open_store(root, write=True) as txn:
        # Repair runs before producers. On an interrupted import, any
        # already-published obligations remain extra candidates.
        before = txn.set_rows("custody_open")
        if not replace:
            result.update(before)
        _write_rows(txn, "custody_open", before, result)
        debts = txn.set_rows("delegated_runs")
        prior = {key: deepcopy(value) for key, value in debts.items()}
        for tid, facts in debts.items():
            facts["closed_runs"] = {**facts.get("closed_runs", {}), **{
                rid: vars(entry) for rid, entry in runs.items() if entry.task_id == tid and entry.settled}}
        _write_rows(txn, "delegated_runs", prior, debts)
    return len(result)
