"""Session id allocation for dev-process jobs."""

from __future__ import annotations

import hashlib


def new_session_id(*, run_id: str, job_key: str, index: int) -> str:
    raw = f"{run_id}:{job_key}:{index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
