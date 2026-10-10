"""Read queue projections against the last successfully published generation."""

import pathlib
import time

from ouroboros.utils import read_json_dict


def queue_stamp(document: dict) -> dict:
    stamp = document.get("_queue_alive") or {}
    return stamp if document.get("generation") and document["generation"] == stamp.get("generation") else {}


def load_queue_document(drive_root, name="queue_snapshot.json") -> dict:
    path = pathlib.Path(drive_root) / "state" / name
    document = read_json_dict(path)
    if document is None:
        return {"_snapshot_invalid" if path.exists() else "_snapshot_missing": True}
    document["_queue_alive"] = read_json_dict(path.parent / "queue.alive") or {}
    if name == "queue_snapshot.json":
        from ouroboros.model_wait import execution_elapsed_seconds, quota_waited_seconds

        heartbeats = queue_stamp(document).get("heartbeats") or {}
        now = time.time()
        for row in document.get("running") or []:
            if not isinstance(row, dict):
                continue
            started = row.get("started_at") or 0
            started = started if isinstance(started, (int, float)) else 0
            heartbeat = heartbeats.get(str(row.get("id") or ""))
            row.update(runtime_sec=round(max(0.0, now - started), 2) if started > 0 else 0.0,
                       quota_wait_sec=quota_waited_seconds(row, now), execution_sec=execution_elapsed_seconds(row, now),
                       heartbeat_lag_sec=round(max(0.0, now - heartbeat), 2) if isinstance(heartbeat, (int, float)) and heartbeat > 0 else None)
    return document
