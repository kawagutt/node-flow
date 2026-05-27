"""Review-agent activation helpers for leaf nodes."""

from __future__ import annotations

from typing import Any

from nodeflow.workflows.dev_process.review_config import (
    FINAL_REVIEW_AGENTS,
    KNOWN_REVIEW_AGENTS,
    REVIEW_AGENT_TO_NODE,
)
from nodeflow.workflows.dev_process.review_presets import normalize_preset, reviewer_keys_for_preset


def active_review_agents(body: dict[str, Any], node_params: dict[str, Any]) -> list[str]:
    """Resolve active v1 review agent keys for this segment."""
    review_scope = str(node_params.get("review_scope") or "")
    if review_scope == "final":
        return list(FINAL_REVIEW_AGENTS)
    agents = node_params.get("review_agents")
    if agents is not None:
        if not isinstance(agents, list):
            return []
        return [str(a) for a in agents if str(a) in KNOWN_REVIEW_AGENTS]
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    preset = normalize_preset(
        str(node_params.get("review_depth_preset") or dp.get("review_depth_preset") or "standard")
    )
    return list(reviewer_keys_for_preset(preset))


def agent_for_review_node(node_name: str) -> str | None:
    for agent, nn in REVIEW_AGENT_TO_NODE.items():
        if nn == node_name:
            return agent
    return None
