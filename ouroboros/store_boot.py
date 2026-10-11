"""The store's boot and restart doors of the server process (ARCHITECTURE: Startup, Shutdown).

Boot (``start``, every door, before the lifespan yields): a pending authority switch is
resumed first (``store_switch.resume_pending_switch``: a switched checkout restarts at once
and nothing is imported into its retired store); then the import owner —
``startup_migrations.prepare_startup_state`` under ``store.importing`` — runs as a
background thread, so the HTTP surface comes up while the store is built or reconciled
(P6'). While it runs every write door answers ``migrating`` (``store.migrating``), the
supervisor and the startup recovery wait on ``wait_ready``, and ``when_ready`` runs the
providerless door's recovery and readiness after publication. Restart
(``switch_at_restart`` from ``server._check_restart``): the seam runs after the workers are
dead, with the same kill census ``body_adoption.arm`` reads as its quiescence evidence.
"""

from __future__ import annotations

import logging
import pathlib
import threading
from typing import Any, Callable, Dict, List, Optional

from ouroboros import store, store_switch

log = logging.getLogger(__name__)
_GUARD = threading.Lock()
_BOOTS: Dict[str, "_Boot"] = {}


class _Boot:
    def __init__(self) -> None:
        self.done = threading.Event()
        self.outcome: Dict[str, Any] = {"status": "importing"}
        self.steps: List[Callable[[], Any]] = []


def start(root: Any, *, repo_dir: Any, request_restart: Callable[[], Any],
          owner: Optional[store_switch.Owner] = None) -> Dict[str, Any]:
    """Resume a pending switch, then own the import off the ready path. Returns the switch
    outcome when one ran, else ``{"status": "importing"}``."""
    root = pathlib.Path(root)
    try:
        resumed = store_switch.resume_pending_switch(root, repo_dir=repo_dir, owner=owner, restore_handles=None)
    except Exception as exc:
        log.critical("Pending authority switch could not be resumed at boot; the intent waits", exc_info=True)
        resumed = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    if resumed["status"] != "nothing_pending":
        log.warning("Authority switch at boot: %s", resumed)
    if resumed["status"] == "switched":
        request_restart()  # the switched tree's own code runs next; nothing is imported into a retired store
        return resumed
    boot = _Boot()
    with _GUARD:
        _BOOTS[str(root)] = boot
    threading.Thread(target=_run, args=(root, repo_dir, boot), daemon=True, name="current-store-import").start()
    return {"status": "importing"}


def _run(root: pathlib.Path, repo_dir: Any, boot: _Boot) -> None:
    from ouroboros.delegate_custody_current import flush_outbox
    from ouroboros.startup_migrations import prepare_startup_state

    outcome: Dict[str, Any] = {"status": "failed"}
    try:
        with store.importing(root):
            report = prepare_startup_state(root, repo_dir=repo_dir, strict=False)
        outcome = {"status": "published" if store.store_ready(root) else str(report.get("status") or "unavailable"),
                   "report": report}
        flush_outbox(root)  # a custody event committed before a death reaches its chain at the next boot
    except Exception as exc:
        log.critical("Boot import of the current-facts store raised; every write door answers typed", exc_info=True)
        outcome = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
    with _GUARD:
        boot.outcome = outcome
        boot.done.set()
    while True:
        with _GUARD:
            if not boot.steps:
                return
            step = boot.steps.pop(0)
        _step(step)


def _step(step: Callable[[], Any]) -> None:
    try:
        step()
    except Exception:
        log.critical("A step waiting on the store's publication failed", exc_info=True)


def wait_ready(root: Any, timeout: Optional[float] = None) -> Dict[str, Any]:
    """Join the boot import of ROOT (at once when none was started here); its outcome."""
    with _GUARD:
        boot = _BOOTS.get(str(pathlib.Path(root)))
    if boot is None:
        return {"status": "not_started"}
    boot.done.wait(timeout)
    return dict(boot.outcome)


def when_ready(root: Any, *steps: Callable[[], Any]) -> None:
    """Run ``steps`` in order once ROOT's boot import has published (on its thread), or now
    when none runs: the providerless door's recovery and readiness."""
    with _GUARD:
        boot = _BOOTS.get(str(pathlib.Path(root)))
        if boot is not None and not boot.done.is_set():
            boot.steps.extend(steps)
            return
    for step in steps:
        _step(step)


def switch_at_restart(root: Any, repo_dir: Any, cleanup: Any) -> Dict[str, Any]:
    """The seam at the restart exit. ``cleanup`` is ``server._emergency_process_cleanup``'s
    census: ``worker_exits`` the pool's kill census (``supervisor.workers.last_worker_exit_census``),
    ``live_children`` the handles still alive after it, ``owned_stop`` ``stop_owned_work``'s
    outcome; an unconfirmed stop in any of them (a missing census included) is a blocker the
    seam refuses on, never a wait. Never raises."""
    root = pathlib.Path(root)
    if store_switch.read_switch_intent(root) is None:
        return {"status": "no_intent"}
    wait_ready(root)  # a boot-smoke rollback can arrive while this process still imports: the export reads a published store
    facts = cleanup if isinstance(cleanup, dict) else {}
    stop = facts.get("owned_stop") if isinstance(facts.get("owned_stop"), dict) else {}
    census = facts.get("worker_exits") if isinstance(facts.get("worker_exits"), dict) else None
    blockers = [name for name, blocked in (
        ("worker_exits_unconfirmed", census is None or bool(census.get("unconfirmed"))),
        ("child_processes_alive", bool(facts.get("live_children"))),
        ("owned_work_unconfirmed", stop.get("state") != "completed" or bool(stop.get("unconfirmed"))),
    ) if blocked]
    try:
        outcome = store_switch.run_switch_seam(root, repo_dir=repo_dir, blockers=blockers, restore_handles=None)
    except Exception as exc:
        log.critical("Authority switch at the restart exit raised; the intent waits for the next boot", exc_info=True)
        outcome = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    log.warning("Authority switch at the restart exit: %s", outcome)
    return outcome
