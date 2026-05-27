"""Review target/agent allowlists for dev-process phase and final reviews."""

from __future__ import annotations

# --- v1 review agents (plan prompt + primary contract) ---

V1_REVIEW_AGENTS: frozenset[str] = frozenset(
    {
        "requirements",
        "architecture",
        "test_quality",
        "checklist_compliance",
    }
)

OPTIONAL_REVIEW_AGENTS: frozenset[str] = frozenset(
    {
        "impact",
        "diff_detail",
        "naming_doc",
    }
)

# Shown to the plan LLM when authoring phase review plans.
V1_PLAN_PROMPT_AGENTS: frozenset[str] = V1_REVIEW_AGENTS

# Fixed agent set for final review (not driven by per-phase plan agents).
FINAL_REVIEW_AGENTS: tuple[str, ...] = (
    "requirements",
    "test_quality",
    "checklist_compliance",
)

KNOWN_REVIEW_AGENTS: frozenset[str] = V1_REVIEW_AGENTS | OPTIONAL_REVIEW_AGENTS

KNOWN_REVIEW_TARGETS: frozenset[str] = frozenset(
    {
        "spec",
        "plan",
        "test",
        "test_phase",
        "implementation_phase",
        "final_diff",
    }
)

KNOWN_PHASE_REVIEW_TARGETS: frozenset[str] = frozenset(
    {
        "implementation_phase",
        "test_phase",
    }
)

KNOWN_FINAL_REVIEW_TARGETS: frozenset[str] = frozenset(
    {
        "final_diff",
    }
)

# Phase plan agent key -> dedicated reviewer node (exec_policy.nodes / node_runs / skill)
REVIEW_AGENT_TO_NODE: dict[str, str] = {
    "requirements": "review_requirements",
    "architecture": "review_architecture",
    "test_quality": "review_test_quality",
    "checklist_compliance": "review_checklist_compliance",
    "impact": "review_impact",
    "diff_detail": "review_diff_detail",
    "naming_doc": "review_naming_doc",
}

# Back-compat alias
AGENT_TO_NODE_NAME = REVIEW_AGENT_TO_NODE

REVIEW_AGENT_NODE_NAMES: frozenset[str] = frozenset(REVIEW_AGENT_TO_NODE.values())


def review_node_name(agent: str) -> str:
    """Return exec_policy / node_runs node name for a review *agent* or legacy preset key."""
    return REVIEW_AGENT_TO_NODE.get(agent, agent)
