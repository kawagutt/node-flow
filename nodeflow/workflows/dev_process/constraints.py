"""Constraint definitions and resolution for dev-process.

Constraints are behavioral rules injected into LLM prompts and AGENTS.md
to control agent actions (e.g. prohibiting git push, limiting file edits).

Resolution:
  exec_policy_snapshot.constraints (global)
  + exec_policy_snapshot.nodes.<name>.constraints (per-node additive)
  → merged ID list (deduplicated, order-stable)

Definition lookup:
  exec_policy.constraint_defs (project-specific, highest priority)
  > CONSTRAINT_DEFS (built-in defaults)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ConstraintDef:
    text: str
    severity: str  # "MUST" or "SHOULD"


CONSTRAINT_DEFS: Dict[str, ConstraintDef] = {
    "NO_GIT_PUSH": ConstraintDef(
        text=(
            "Never run `git push`, `git fetch`, `git pull`, or any network git command. "
            "Local git operations (commit, branch, merge, rebase, etc.) are permitted. "
            "NOTE: This is prompt-enforced only; hard prevention requires worker sandbox "
            "or network restrictions."
        ),
        severity="MUST",
    ),
    "NO_TOUCH_EXISTING_UNTRACKED": ConstraintDef(
        text=(
            "Do not modify or delete files that were already untracked before this node started. "
            "New files required by the approved task are allowed, but they must be intentionally "
            "created and committed when the node is expected to write code."
        ),
        severity="MUST",
    ),
    "EDIT_TARGET_ONLY": ConstraintDef(
        text="Only modify files explicitly listed in the approved specification and plan.",
        severity="MUST",
    ),
    "NO_DELETE_FOREIGN": ConstraintDef(
        text="Do not delete files that were not created by this task.",
        severity="MUST",
    ),
    "READ_ONLY_NODE": ConstraintDef(
        text=(
            "This node MUST NOT modify the repository. No file creation, modification, "
            "deletion, or git state changes are permitted. Violation is a hard failure."
        ),
        severity="MUST",
    ),
    "BRANCH_BEFORE_WORK": ConstraintDef(
        text=(
            "Verify that a dedicated branch exists before making changes. "
            "This is informational; the orchestrator handles branch creation."
        ),
        severity="SHOULD",
    ),
}

REVIEW_NODE_NAMES = frozenset(
    {
        "review_spec",
        "review_plan",
        "review_requirements",
        "review_architecture",
        "review_test_quality",
        "review_checklist_compliance",
        "review_impact",
        "review_diff_detail",
        "review_naming_doc",
    }
)


def resolve_constraints(
    snapshot: Dict[str, Any],
    node_name: Optional[str] = None,
) -> List[str]:
    """Merge global + per-node + implicit constraints into a deduplicated ordered list.

    Review nodes automatically receive READ_ONLY_NODE unless explicitly excluded.
    """
    global_ids: List[str] = []
    raw = snapshot.get("constraints")
    if isinstance(raw, list):
        global_ids = [str(x) for x in raw if isinstance(x, str) and x.strip()]

    node_ids: List[str] = []
    if node_name:
        nodes = snapshot.get("nodes")
        if isinstance(nodes, dict):
            entry = nodes.get(node_name)
            if isinstance(entry, dict):
                nc = entry.get("constraints")
                if isinstance(nc, list):
                    node_ids = [str(x) for x in nc if isinstance(x, str) and x.strip()]

    implicit_ids: List[str] = []
    if node_name and node_name in REVIEW_NODE_NAMES:
        implicit_ids = ["READ_ONLY_NODE"]

    seen: set[str] = set()
    merged: List[str] = []
    for cid in global_ids + node_ids + implicit_ids:
        if cid not in seen:
            seen.add(cid)
            merged.append(cid)
    return merged


def resolve_constraint_defs(
    constraint_ids: List[str],
    snapshot: Dict[str, Any],
) -> Dict[str, str]:
    """Resolve ID → text. Project constraint_defs override built-in CONSTRAINT_DEFS."""
    project_defs: Dict[str, str] = {}
    raw = snapshot.get("constraint_defs")
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(k, str) and isinstance(v, str) and v.strip():
                project_defs[k] = v.strip()

    result: Dict[str, str] = {}
    for cid in constraint_ids:
        if cid in project_defs:
            result[cid] = project_defs[cid]
        elif cid in CONSTRAINT_DEFS:
            result[cid] = CONSTRAINT_DEFS[cid].text
        else:
            result[cid] = f"(unknown constraint: {cid})"
    return result


def _severity_for(cid: str, snapshot: Dict[str, Any]) -> str:
    """Determine severity for a constraint ID."""
    if cid in CONSTRAINT_DEFS:
        return CONSTRAINT_DEFS[cid].severity
    return "MUST"


def format_constraints_for_prompt(
    constraint_ids: List[str],
    snapshot: Dict[str, Any],
) -> str:
    """Format constraints for injection into LLM prompts."""
    if not constraint_ids:
        return ""
    defs = resolve_constraint_defs(constraint_ids, snapshot)
    lines = ["## Constraints (MUST follow)"]
    for cid in constraint_ids:
        severity = _severity_for(cid, snapshot)
        text = defs.get(cid, "")
        lines.append(f"- [{severity}] {cid}: {text}")
    return "\n".join(lines) + "\n"


def generate_agents_md(
    constraint_ids: List[str],
    snapshot: Dict[str, Any],
) -> str:
    """Generate full AGENTS.md content with global and per-node constraint sections."""
    if not constraint_ids:
        return "# Agent Behavior Rules and Constraints\n\nNo constraints configured.\n"

    defs = resolve_constraint_defs(constraint_ids, snapshot)

    must_ids = [c for c in constraint_ids if _severity_for(c, snapshot) == "MUST"]
    should_ids = [c for c in constraint_ids if _severity_for(c, snapshot) == "SHOULD"]

    lines = ["# Agent Behavior Rules and Constraints", ""]

    lines.append("## Global constraints")
    lines.append("")

    if must_ids:
        lines.append("### MUST (mandatory — violation is a failure)")
        lines.append("")
        for cid in must_ids:
            lines.append(f"#### {cid}")
            lines.append(defs.get(cid, ""))
            lines.append("")

    if should_ids:
        lines.append("### SHOULD (recommended — best effort)")
        lines.append("")
        for cid in should_ids:
            lines.append(f"#### {cid}")
            lines.append(defs.get(cid, ""))
            lines.append("")

    per_node = _collect_per_node_constraints(snapshot)
    if per_node:
        lines.append("## Per-node constraints")
        lines.append("")
        for node_name, node_ids in sorted(per_node.items()):
            lines.append(f"### {node_name}")
            for cid in node_ids:
                lines.append(f"- {cid}")
            lines.append("")

    return "\n".join(lines)


def generate_node_agents_md(
    constraint_ids: List[str],
    snapshot: Dict[str, Any],
) -> str:
    """Generate AGENTS.md content for a specific node (no per-node overview section).

    Unlike generate_agents_md() which includes a per-node constraints overview,
    this function only renders the constraints that are effective for the given node.
    Used for per-node CODEX_HOME/AGENTS.md generation.
    """
    if not constraint_ids:
        return "# Agent Behavior Rules and Constraints\n\nNo constraints configured.\n"

    defs = resolve_constraint_defs(constraint_ids, snapshot)

    must_ids = [c for c in constraint_ids if _severity_for(c, snapshot) == "MUST"]
    should_ids = [c for c in constraint_ids if _severity_for(c, snapshot) == "SHOULD"]

    lines = ["# Agent Behavior Rules and Constraints", ""]

    if must_ids:
        lines.append("## MUST (mandatory — violation is a failure)")
        lines.append("")
        for cid in must_ids:
            lines.append(f"### {cid}")
            lines.append(defs.get(cid, ""))
            lines.append("")

    if should_ids:
        lines.append("## SHOULD (recommended — best effort)")
        lines.append("")
        for cid in should_ids:
            lines.append(f"### {cid}")
            lines.append(defs.get(cid, ""))
            lines.append("")

    return "\n".join(lines)


def generate_constraints_audit(snapshot: Dict[str, Any]) -> str:
    """Generate audit content showing global constraints and effective per-node breakdown.

    Unlike generate_agents_md() (used for Codex discovery), this is for human audit
    and clearly separates what is global vs what each node effectively receives.
    """
    global_ids = resolve_constraints(snapshot)
    defs = resolve_constraint_defs(
        list(
            set(global_ids)
            | {c for ids in _collect_per_node_effective(snapshot).values() for c in ids}
        ),
        snapshot,
    )

    lines = ["# Constraints Audit", ""]
    lines.append("## Global constraints (applied to all nodes)")
    lines.append("")
    if global_ids:
        for cid in global_ids:
            severity = _severity_for(cid, snapshot)
            lines.append(f"- [{severity}] **{cid}**: {defs.get(cid, '')}")
    else:
        lines.append("(none)")
    lines.append("")

    per_node = _collect_per_node_effective(snapshot)
    lines.append("## Effective constraints by node")
    lines.append("")
    for node_name, effective_ids in sorted(per_node.items()):
        lines.append(f"### {node_name}")
        for cid in effective_ids:
            lines.append(f"- {cid}")
        lines.append("")

    return "\n".join(lines)


def _collect_per_node_effective(snapshot: Dict[str, Any]) -> Dict[str, List[str]]:
    """Collect the full effective constraint list per known node (global + per-node + implicit)."""
    result: Dict[str, List[str]] = {}

    nodes = snapshot.get("nodes")
    if isinstance(nodes, dict):
        for name in nodes:
            result[name] = resolve_constraints(snapshot, name)

    for name in REVIEW_NODE_NAMES:
        if name not in result:
            result[name] = resolve_constraints(snapshot, name)

    return result


def _collect_per_node_constraints(snapshot: Dict[str, Any]) -> Dict[str, List[str]]:
    """Collect the effective per-node constraint additions (excluding global)."""
    result: Dict[str, List[str]] = {}
    nodes = snapshot.get("nodes")
    if isinstance(nodes, dict):
        for name, entry in nodes.items():
            if isinstance(entry, dict):
                nc = entry.get("constraints")
                if isinstance(nc, list) and nc:
                    result[name] = [str(c) for c in nc if isinstance(c, str) and c.strip()]

    for name in REVIEW_NODE_NAMES:
        if name not in result:
            result[name] = ["READ_ONLY_NODE"]
        elif "READ_ONLY_NODE" not in result[name]:
            result[name] = result[name] + ["READ_ONLY_NODE"]

    return result
