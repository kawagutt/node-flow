"""Session id allocation for dev-process node executions."""

from __future__ import annotations

import hashlib


def new_session_id(*, run_id: str, node_name: str, index: int) -> str:
    raw = f"{run_id}:{node_name}:{index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
