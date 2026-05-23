"""Legal action/state transitions for dev-process."""

from __future__ import annotations

from typing import List, Optional, Set

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_APPROVE_SPEC,
    ACTION_MERGE,
    ACTION_REJECT_FINAL,
    ACTION_REJECT_SPEC,
    ACTION_REVISE_SPEC,
    ACTION_REWORK,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_REVIEW,
    STATE_AWAITING_SPEC,
    STATE_FAILED,
    STATE_INITIALIZED,
    STATE_MERGED,
    TERMINAL_STATES,
)

_ALLOWED: dict[str, Set[str]] = {
    STATE_INITIALIZED: set(),
    STATE_AWAITING_SPEC: {ACTION_APPROVE_SPEC, ACTION_REVISE_SPEC, ACTION_REJECT_SPEC},
    STATE_AWAITING_REVIEW: {
        ACTION_REWORK,
        ACTION_REVISE_SPEC,
        ACTION_APPROVE_FINAL,
        ACTION_REJECT_FINAL,
    },
    STATE_AWAITING_FINAL: {ACTION_MERGE, ACTION_REJECT_FINAL},
    STATE_MERGED: set(),
    STATE_FAILED: set(),
}

# Deterministic UX order for allowed_actions / next_action (not alphabetical).
_ACTION_ORDER: dict[str, List[str]] = {
    STATE_AWAITING_SPEC: [
        ACTION_APPROVE_SPEC,
        ACTION_REVISE_SPEC,
        ACTION_REJECT_SPEC,
    ],
    STATE_AWAITING_REVIEW: [
        ACTION_APPROVE_FINAL,
        ACTION_REWORK,
        ACTION_REVISE_SPEC,
        ACTION_REJECT_FINAL,
    ],
    STATE_AWAITING_FINAL: [
        ACTION_MERGE,
        ACTION_REJECT_FINAL,
    ],
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
    pool = set(_ALLOWED.get(state, set()))
    if state == STATE_AWAITING_REVIEW:
        if merge_ready is False or spec_revision_needed:
            pool.discard(ACTION_APPROVE_FINAL)
    if state == STATE_AWAITING_REVIEW and spec_revision_needed:
        order = [ACTION_REVISE_SPEC, ACTION_REWORK, ACTION_REJECT_FINAL]
    elif state == STATE_AWAITING_REVIEW and merge_ready is False:
        order = [ACTION_REWORK, ACTION_REVISE_SPEC, ACTION_REJECT_FINAL]
    else:
        order = _ACTION_ORDER.get(state, [])
    return [action for action in order if action in pool]


def next_action_for_state(allowed_actions: List[str]) -> Optional[str]:
    return allowed_actions[0] if allowed_actions else None
