"""Legal action/state transitions for dev-process v2."""

from __future__ import annotations

from typing import List, Optional, Set

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_APPROVE_SPEC,
    ACTION_CONTINUE_IMPLEMENTATION,
    ACTION_MERGE,
    ACTION_REJECT_FINAL,
    ACTION_REJECT_SPEC,
    ACTION_REQUEST_SPEC_REVISION,
    ACTION_REVISE_PLAN,
    ACTION_REVISE_SPEC,
    ACTION_REWORK,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_IMPLEMENTATION,
    STATE_AWAITING_IMPLEMENTATION_REWORK,
    STATE_AWAITING_MERGE,
    STATE_AWAITING_PLAN_REVISION,
    STATE_AWAITING_REWORK_DECISION,
    STATE_AWAITING_SPEC_HUMAN_GATE,
    STATE_AWAITING_SPEC_REVISION,
    STATE_AWAITING_TEST_REWORK,
    STATE_FAILED,
    STATE_INITIALIZED,
    STATE_MERGED,
    TERMINAL_STATES,
)

_ALLOWED: dict[str, Set[str]] = {
    STATE_INITIALIZED: set(),
    STATE_AWAITING_SPEC_REVISION: {ACTION_REVISE_SPEC},
    STATE_AWAITING_SPEC_HUMAN_GATE: {
        ACTION_APPROVE_SPEC,
        ACTION_REQUEST_SPEC_REVISION,
        ACTION_REJECT_SPEC,
    },
    STATE_AWAITING_PLAN_REVISION: {ACTION_REVISE_PLAN},
    STATE_AWAITING_IMPLEMENTATION: {
        ACTION_CONTINUE_IMPLEMENTATION,
        ACTION_REJECT_SPEC,
    },
    STATE_AWAITING_IMPLEMENTATION_REWORK: {ACTION_REWORK, ACTION_REVISE_SPEC, ACTION_REVISE_PLAN},
    STATE_AWAITING_TEST_REWORK: {ACTION_REWORK},
    STATE_AWAITING_REWORK_DECISION: {
        ACTION_REWORK,
        ACTION_REVISE_SPEC,
        ACTION_REVISE_PLAN,
        ACTION_REJECT_FINAL,
    },
    STATE_AWAITING_FINAL: {
        ACTION_APPROVE_FINAL,
        ACTION_REJECT_FINAL,
        ACTION_REWORK,
        ACTION_REVISE_SPEC,
        ACTION_REVISE_PLAN,
    },
    STATE_AWAITING_MERGE: {ACTION_MERGE, ACTION_REJECT_FINAL},
    STATE_MERGED: set(),
    STATE_FAILED: set(),
}

_ACTION_ORDER: dict[str, List[str]] = {
    STATE_AWAITING_SPEC_HUMAN_GATE: [
        ACTION_APPROVE_SPEC,
        ACTION_REQUEST_SPEC_REVISION,
        ACTION_REJECT_SPEC,
    ],
    STATE_AWAITING_IMPLEMENTATION: [ACTION_CONTINUE_IMPLEMENTATION, ACTION_REJECT_SPEC],
    STATE_AWAITING_REWORK_DECISION: [
        ACTION_REWORK,
        ACTION_REVISE_SPEC,
        ACTION_REVISE_PLAN,
        ACTION_REJECT_FINAL,
    ],
    STATE_AWAITING_FINAL: [
        ACTION_APPROVE_FINAL,
        ACTION_REWORK,
        ACTION_REVISE_SPEC,
        ACTION_REVISE_PLAN,
        ACTION_REJECT_FINAL,
    ],
    STATE_AWAITING_MERGE: [ACTION_MERGE, ACTION_REJECT_FINAL],
}


def assert_action_allowed(state: str, action: str) -> None:
    if state in TERMINAL_STATES:
        raise NodeExecutionFailure(f"terminal state {state!r} does not accept action {action!r}")
    allowed = _ALLOWED.get(state)
    if allowed is None:
        raise NodeExecutionFailure(f"unknown flow state {state!r}")
    if action not in allowed:
        raise NodeExecutionFailure(
            f"action {action!r} not allowed in state {state!r} (allowed: {sorted(allowed)!r})"
        )


def allowed_actions_for_state(
    state: str,
    *,
    merge_ready: bool | None = None,
    spec_revision_needed: bool = False,
) -> List[str]:
    del merge_ready, spec_revision_needed
    pool = set(_ALLOWED.get(state, set()))
    order = _ACTION_ORDER.get(state, [])
    return [a for a in order if a in pool] or sorted(pool)


def next_action_for_state(allowed_actions: List[str]) -> Optional[str]:
    return allowed_actions[0] if allowed_actions else None
