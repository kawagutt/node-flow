"""Per reviewer-node prompt contract: skill text, builder routing, diff limits.

Reviewer role instructions live in ``skills/dev-process/nodes/<node_name>/SKILL.md``.
``run_review_stage()`` only routes agents → node names; it does not embed role text.
"""

from __future__ import annotations

from pathlib import Path

_SKILLS_ROOT = Path(__file__).resolve().parents[3] / "skills" / "dev-process" / "nodes"

# Dedicated review node → development_flow prompt builder registry key
NODE_PROMPT_BUILDER: dict[str, str] = {
    "review_requirements": "review_spec_conformance",
    "review_architecture": "review_diff",
    "review_test_quality": "review_tests",
    "review_checklist_compliance": "review_diff",
    "review_impact": "review_wide",
    "review_diff_detail": "review_diff",
    "review_naming_doc": "review_diff",
    # legacy preset node names (no SKILL required)
    "review_diff": "review_diff",
    "review_tests": "review_tests",
    "review_spec_conformance": "review_spec_conformance",
    "review_wide": "review_wide",
    "review_spec_revision": "review_spec_revision",
}

# Base max_diff_chars per dedicated review node (preset scales this)
NODE_PROMPT_LIMITS: dict[str, dict[str, int]] = {
    "review_requirements": {"max_diff_chars": 6000},
    "review_architecture": {"max_diff_chars": 8000},
    "review_test_quality": {"max_diff_chars": 6000},
    "review_checklist_compliance": {"max_diff_chars": 8000},
    "review_impact": {"max_diff_chars": 12000},
    "review_diff_detail": {"max_diff_chars": 8000},
    "review_naming_doc": {"max_diff_chars": 8000},
}

# Finding area per review node (aggregate_reviews default_area)
REVIEW_NODE_AREA: dict[str, str] = {
    "review_requirements": "spec",
    "review_architecture": "diff",
    "review_test_quality": "tests",
    "review_checklist_compliance": "diff",
    "review_impact": "diff",
    "review_diff_detail": "diff",
    "review_naming_doc": "diff",
    "review_diff": "diff",
    "review_wide": "diff",
    "review_tests": "tests",
    "review_spec_conformance": "spec",
    "review_spec_revision": "spec",
}

# Default aggregate expectation when dev_process does not pass explicit node list (legacy preset)
LEGACY_PRESET_REVIEW_NODES: frozenset[str] = frozenset(
    {
        "review_diff",
        "review_wide",
        "review_tests",
        "review_spec_conformance",
        "review_spec_revision",
    }
)


def load_review_node_skill(node_name: str) -> str:
    """Load reviewer SKILL.md for *node_name* (empty string if missing)."""
    path = _SKILLS_ROOT / node_name / "SKILL.md"
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8").strip()


def prompt_builder_key(node_name: str) -> str:
    """Return development_flow prompt builder key for a review node."""
    key = NODE_PROMPT_BUILDER.get(node_name)
    if key is None:
        return node_name
    return key
