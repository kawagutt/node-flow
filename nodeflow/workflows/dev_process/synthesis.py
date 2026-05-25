"""Review synthesis helpers: owner assignment and routing (P11).

Owner routing determines which upstream node chain to re-run:

    spec → write_spec → review_spec → human_spec_gate → write_plan → …
    plan → write_plan → review_plan → implementation chain
    implementation → write_implementation → write_tests → run_tests → review
    test → write_tests → run_tests → review  (skips write_implementation)
"""

from __future__ import annotations

from typing import Any, Dict, List


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
    """Return the highest-priority owner from blocking findings."""
    priority = ("spec", "plan", "test", "implementation")
    owners = {str(f.get("owner") or "implementation") for f in findings}
    for p in priority:
        if p in owners:
            return p
    return "implementation"
