"""The store's boot and restart doors of the server process (ARCHITECTURE: Startup, Shutdown).

Boot (``start``, every door, before the lifespan yields): the import window opens on the
calling thread (``store.begin_import``), then one background thread — the import owner of
the root — resumes a pending authority switch first (``store_switch.resume_pending_switch``:
a switched checkout requests the restart and nothing is imported into its retired store)
and runs ``startup_migrations.prepare_startup_state`` in service mode, so the HTTP surface
comes up while the store is built or reconciled (P6'). The window closes only when the
store is published (``store.published``): a failed import, or a switch, leaves it open for
the life of the process, so every write door keeps answering ``migrating``
(``store.migrating``), the reads 503, and the next request never re-imports. The
supervisor's startup phase and the startup recovery join on ``require_published``;
``when_ready`` runs the providerless door's recovery and readiness after publication and
drops them otherwise. Restart (``switch_at_restart`` from ``server._check_restart``): the
seam runs after the workers are dead, with the same kill census ``body_adoption.arm``
reads as its quiescence evidence.
"""

from __future__ import annotations

import logging
import pathlib
import threading
from typing import Any, Callable, Dict, List, Optional

from ouroboros import evidence_packs, store, store_switch

log = logging.getLogger(__name__)
_GUARD = threading.Lock()
_BOOTS: Dict[str, "_Boot"] = {}


class _Boot:
    def __init__(self) -> None:
        self.done = threading.Event()
        self.outcome: Dict[str, Any] = {"status": "importing"}
        self.steps: List[Callable[[], Any]] = []

    @property
    def published(self) -> bool:
        return self.outcome.get("status") == "published"


def start(root: Any, *, repo_dir: Any, request_restart: Callable[[], Any],
          owner: Optional[store_switch.Owner] = None) -> Dict[str, Any]:
    """Open the import window and hand ROOT to its one import owner (a thread): the pending
    switch, then the import, off the ready path. Returns ``{"status": "importing"}``; the
    outcome is ``wait_ready``'s."""
    root = pathlib.Path(root)
    boot = _Boot()
    store.begin_import(root)  # before the thread exists: no door finds the root idle in between
    with _GUARD:
        _BOOTS[str(root)] = boot
    threading.Thread(target=_run, args=(root, repo_dir, boot, request_restart, owner),
                     daemon=True, name="current-store-import").start()
    return {"status": "importing"}


def _run(root: pathlib.Path, repo_dir: Any, boot: _Boot, request_restart: Callable[[], Any],
         owner: Optional[store_switch.Owner]) -> None:
    from ouroboros.delegate_custody_current import flush_outbox
    from ouroboros.startup_migrations import prepare_startup_state

    outcome: Dict[str, Any] = {"status": "failed"}
    try:
        with store.service_thread():  # the owner's own writes pass the gate; every other thread's wait
            resumed = _resume(root, repo_dir, owner)
            if resumed["status"] == "switched":
                outcome = resumed  # the window stays open: nothing is imported into a retired store
            else:
                report = prepare_startup_state(root, repo_dir=repo_dir, strict=False)
                outcome = {"status": "published" if store.published(root) else str(report.get("status") or "unavailable"),
                           "report": report}
                if outcome["status"] == "published":
                    flush_outbox(root)  # a custody event committed before a death reaches its chain at the next boot
                    store.end_import(root)  # publication: the doors open, the waiting steps run
    except Exception as exc:
        log.critical("Boot import of the current-facts store raised; the doors answer migrating until the next boot",
                     exc_info=True)
        outcome = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
    with _GUARD:
        boot.outcome = outcome
        boot.done.set()
        steps, boot.steps = boot.steps, []
    if resumed_switched := outcome["status"] == "switched":
        request_restart()  # the switched tree's own code runs next
    if not boot.published:
        log.critical("The current-facts store is not published after boot (%s); %d waiting step(s) dropped%s",
                     outcome["status"], len(steps), ": the restart was requested" if resumed_switched else "")
        return
    while True:
        for step in steps:
            _step(step)
        with _GUARD:
            steps, boot.steps = boot.steps, []
            if not steps:
                return


def _resume(root: pathlib.Path, repo_dir: Any, owner: Optional[store_switch.Owner]) -> Dict[str, Any]:
    try:
        resumed = store_switch.resume_pending_switch(
            root, repo_dir=repo_dir, owner=owner, restore_handles=evidence_packs.restore_checkpoint_handles)
    except Exception as exc:
        log.critical("Pending authority switch could not be resumed at boot; the intent waits", exc_info=True)
        resumed = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    if resumed["status"] != "nothing_pending":
        log.warning("Authority switch at boot: %s", resumed)
    return resumed


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


def require_published(root: Any) -> Dict[str, Any]:
    """Join the boot import of ROOT and raise ``StoreMigrating`` unless it published (a root no
    boot imported here passes): the supervisor's startup phase and the startup recovery."""
    outcome = wait_ready(root)
    if outcome.get("status") not in ("published", "not_started"):
        raise store.StoreMigrating(f"current store not published at boot: {outcome.get('status')}")
    return outcome


def when_ready(root: Any, *steps: Callable[[], Any]) -> None:
    """Run ``steps`` in order once ROOT's boot import has published (on its thread), or now
    when none runs here: the providerless door's recovery and readiness. A boot that did not
    publish drops them (the process stays not ready)."""
    with _GUARD:
        boot = _BOOTS.get(str(pathlib.Path(root)))
        if boot is not None and not boot.done.is_set():
            boot.steps.extend(steps)
            return
    if boot is not None and not boot.published:
        log.critical("%d step(s) dropped: the current-facts store is not published (%s)", len(steps), boot.outcome.get("status"))
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
        outcome = store_switch.run_switch_seam(
            root, repo_dir=repo_dir, blockers=blockers, restore_handles=evidence_packs.restore_checkpoint_handles)
    except Exception as exc:
        log.critical("Authority switch at the restart exit raised; the intent waits for the next boot", exc_info=True)
        outcome = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    log.warning("Authority switch at the restart exit: %s", outcome)
    return outcome
