"""Stale artifact marking when upstream stages are revised."""

from __future__ import annotations

from typing import Any, Dict

from nodeflow.workflows.dev_process.constants import STALE_DOWNSTREAM


def mark_stale(body: Dict[str, Any], *, upstream: str) -> None:
    stale = body.setdefault("stale", {})
    if not isinstance(stale, dict):
        stale = {}
        body["stale"] = stale
    for key in STALE_DOWNSTREAM.get(upstream, ()):
        stale[key] = True
        st = body.setdefault("stages", {}).get(key)
        if isinstance(st, dict):
            st["stale"] = True


def clear_stage_stale(body: Dict[str, Any], stage: str) -> None:
    stale = body.get("stale")
    if isinstance(stale, dict):
        stale.pop(stage, None)
        if not stale:
            body.pop("stale", None)
    stages = body.get("stages")
    if isinstance(stages, dict):
        st = stages.get(stage)
        if isinstance(st, dict):
            st.pop("stale", None)


def any_stale_remaining(body: Dict[str, Any]) -> bool:
    stale = body.get("stale")
    if isinstance(stale, dict) and any(stale.values()):
        return True
    stages = body.get("stages") or {}
    for st in stages.values():
        if isinstance(st, dict) and st.get("stale"):
            return True
    return False


def is_stage_stale(body: Dict[str, Any], stage: str) -> bool:
    stale = body.get("stale")
    if isinstance(stale, dict) and stale.get(stage):
        return True
    st = (body.get("stages") or {}).get(stage)
    return isinstance(st, dict) and bool(st.get("stale"))
