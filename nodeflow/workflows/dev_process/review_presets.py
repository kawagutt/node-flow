"""Review depth presets for dev-process (P2)."""

from __future__ import annotations

from typing import Tuple

PRESET_LIGHT = "light"
PRESET_STANDARD = "standard"
PRESET_DEEP = "deep"

VALID_PRESETS = frozenset({PRESET_LIGHT, PRESET_STANDARD, PRESET_DEEP})

# v1 review agent keys (resolved to review_* node names via review_config.review_node_name)
_REVIEWER_KEYS_LIGHT: Tuple[str, ...] = ("requirements", "test_quality")
_REVIEWER_KEYS_STANDARD: Tuple[str, ...] = (
    "requirements",
    "architecture",
    "test_quality",
    "checklist_compliance",
)
_REVIEWER_KEYS_DEEP: Tuple[str, ...] = (
    "requirements",
    "architecture",
    "test_quality",
    "checklist_compliance",
    "impact",
    "diff_detail",
    "naming_doc",
)


def normalize_preset(raw: str | None) -> str:
    preset = (raw or PRESET_STANDARD).strip().lower()
    if preset not in VALID_PRESETS:
        raise ValueError(
            f"unknown review_depth_preset {raw!r}; expected one of {sorted(VALID_PRESETS)!r}"
        )
    return preset


def reviewer_keys_for_preset(preset: str) -> Tuple[str, ...]:
    preset = normalize_preset(preset)
    if preset == PRESET_LIGHT:
        return _REVIEWER_KEYS_LIGHT
    if preset == PRESET_DEEP:
        return _REVIEWER_KEYS_DEEP
    return _REVIEWER_KEYS_STANDARD


def expected_review_evidence_count(preset: str) -> int:
    """Number of review-stage exec evidence files for one review run."""
    return len(reviewer_keys_for_preset(preset))


def expected_exec_evidence_count_for_full_flow(preset: str) -> int:
    """spec + plan + reviews + implementation + test_implementation + code reviewers."""
    return 6 + expected_review_evidence_count(preset)
