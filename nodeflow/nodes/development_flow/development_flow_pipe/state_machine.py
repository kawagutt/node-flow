"""State transition helpers for development_flow_pipe."""

from __future__ import annotations

from typing import Any, Dict, List

from nodeflow.core.base_node import NodeExecutionFailure


def require_state(prev_flow: Dict[str, Any], expected: str, *, action: str) -> None:
    actual = prev_flow.get("state")
    if actual != expected:
        raise NodeExecutionFailure(f"{action} requires previous state {expected}, got {actual!r}")


def validate_merge_gate(prev_flow: Dict[str, Any]) -> None:
    if prev_flow.get("state") != "awaiting_review_decision":
        raise NodeExecutionFailure(
            "merge requires previous state awaiting_review_decision, "
            f"got {prev_flow.get('state')!r}"
        )
    review_st = prev_flow.get("review_stage_result")
    if not isinstance(review_st, dict):
        raise NodeExecutionFailure("merge requires previous review_stage_result in flow checkpoint")

    next_action = prev_flow.get("next_action")
    flow_ok = bool(prev_flow.get("ok"))
    impl_st = prev_flow.get("implement_stage_result")
    impl_ok = isinstance(impl_st, dict) and bool(impl_st.get("ok"))
    review_ok = bool(review_st.get("ok"))
    if not (flow_ok and impl_ok and next_action == "merge" and review_ok):
        raise NodeExecutionFailure(
            "merge requires flow_result.ok == true, implement_stage_result.ok == true, "
            "review next_action == 'merge', and review_stage_result.ok == true; "
            "use action=force_merge to override"
        )


def review_allowed_actions(*, flow_ok: bool, review_next_action: Any) -> List[str]:
    review_next = review_next_action if isinstance(review_next_action, str) else None
    if flow_ok and review_next == "merge":
        return ["merge", "rework_implementation", "revise_spec", "stop"]
    if review_next == "revise_spec":
        return ["revise_spec", "rework_implementation", "stop"]
    return ["rework_implementation", "revise_spec", "stop"]
