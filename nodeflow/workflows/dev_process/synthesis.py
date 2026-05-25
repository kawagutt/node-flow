"""Review synthesis helpers: owner assignment and routing (P11)."""

from __future__ import annotations

from typing import Any, Dict, List

from nodeflow.workflows.dev_process.constants import (
    STATE_AWAITING_IMPLEMENTATION_REWORK,
    STATE_AWAITING_PLAN_REVISION,
    STATE_AWAITING_SPEC_REVISION,
    STATE_AWAITING_TEST_REWORK,
)


def assign_owners_to_findings(findings: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in findings:
        if not isinstance(raw, dict):
            continue
        f = dict(raw)
        area = str(f.get("area") or "").lower()
        if "spec" in area and "revision" in area:
            f["owner"] = "spec"
        elif area in ("spec", "spec_conformance", "spec_revision"):
            f["owner"] = "spec"
        elif area == "plan":
            f["owner"] = "plan"
        elif area in ("test", "tests"):
            f["owner"] = "test"
        else:
            f.setdefault("owner", "implementation")
        out.append(f)
    return out


def route_owner_to_state(findings: List[Dict[str, Any]]) -> str:
    priority = ("spec", "plan", "test", "implementation")
    owners = {str(f.get("owner") or "implementation") for f in findings}
    for p in priority:
        if p in owners:
            return p
    return "implementation"


def owner_to_checkpoint_state(owner: str) -> str:
    mapping = {
        "spec": STATE_AWAITING_SPEC_REVISION,
        "plan": STATE_AWAITING_PLAN_REVISION,
        "implementation": STATE_AWAITING_IMPLEMENTATION_REWORK,
        "test": STATE_AWAITING_TEST_REWORK,
    }
    return mapping.get(owner, STATE_AWAITING_IMPLEMENTATION_REWORK)
