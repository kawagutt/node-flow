"""Final review routing: parse synthesis output and route to next action.

v1 implementation note
----------------------
The actual final synthesis is NOT produced by an LLM.  ``_build_final_synthesis``
in ``flow_actions`` derives owner routing from per-finding ``owner`` tags.
For impl/test owners, ``target_phase`` is left to human input via
``--target-phase`` on the CLI ``rework`` command.

``parse_final_synthesis`` and ``route_final_synthesis`` below define the
*schema* that a future LLM-based synthesis must conform to.  They are
tested and used in unit tests today, and will become the runtime path
once LLM synthesis is integrated.

v1 rewind behaviour for owner=test
-----------------------------------
In v1, ``rewind_to_phase`` always sets ``skip_implementation=False``.
Even when the owner is ``test``, the phase is rewound to
``phase_start_git_ref`` and **implementation is re-run** from that point.
Test-only rework (skipping implementation and re-running only tests)
requires saving a ``post_implementation_git_ref`` which is future work.
"""

from __future__ import annotations

from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    STATE_AWAITING_FINAL,
    STATE_AWAITING_IMPLEMENTATION,
    STATE_AWAITING_PLAN_REVISION,
    STATE_AWAITING_SPEC_REVISION,
)

VALID_OWNERS = frozenset({"implementation", "test", "plan", "spec"})


def parse_final_synthesis(output: Dict[str, Any]) -> Dict[str, Any]:
    """Parse and validate final_synthesis output.

    Expected schema:
        owner: "implementation" | "test" | "plan" | "spec"
        target_phase: "phase_002"  (required if owner is impl/test)
        findings: [...]

    Returns validated dict with owner, target_phase, findings.
    """
    owner = str(output.get("owner") or "").strip()
    if not owner:
        raise NodeExecutionFailure("final_synthesis output missing 'owner' field")
    if owner not in VALID_OWNERS:
        raise NodeExecutionFailure(
            f"final_synthesis owner {owner!r} not valid; must be one of {sorted(VALID_OWNERS)}"
        )

    target_phase = output.get("target_phase")
    if isinstance(target_phase, str):
        target_phase = target_phase.strip() or None

    if "findings" not in output:
        raise NodeExecutionFailure("final_synthesis output missing 'findings' field")
    findings = output["findings"]
    if not isinstance(findings, list):
        raise NodeExecutionFailure("final_synthesis 'findings' must be a list")

    if findings and owner in ("implementation", "test") and not target_phase:
        raise NodeExecutionFailure(
            "final_synthesis target_phase is required when owner is implementation/test"
        )

    return {
        "owner": owner,
        "target_phase": target_phase,
        "findings": findings,
    }


def route_final_synthesis(
    synthesis: Dict[str, Any],
    *,
    rewind_implemented: bool = False,
) -> Dict[str, Any]:
    """Route final synthesis result to the next state/action.

    Returns dict with:
        - decision: "ok" | "rework"
        - next_state: state to transition to
        - owner: routing owner
        - target_phase: phase to rewind to (if applicable)
    """
    owner = synthesis["owner"]

    if not synthesis.get("findings"):
        return {
            "decision": "ok",
            "next_state": STATE_AWAITING_FINAL,
            "owner": None,
            "target_phase": None,
        }

    if owner == "plan":
        return {
            "decision": "rework",
            "next_state": STATE_AWAITING_PLAN_REVISION,
            "owner": "plan",
            "target_phase": None,
        }

    if owner == "spec":
        return {
            "decision": "rework",
            "next_state": STATE_AWAITING_SPEC_REVISION,
            "owner": "spec",
            "target_phase": None,
        }

    target = synthesis.get("target_phase")

    if not rewind_implemented:
        raise NodeExecutionFailure(
            f"final_review requires rework on {target}; " "final_review rewind not yet implemented"
        )

    return {
        "decision": "rework",
        "next_state": STATE_AWAITING_IMPLEMENTATION,
        "owner": owner,
        "target_phase": target,
    }
