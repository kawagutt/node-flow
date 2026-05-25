"""Per-preset, per-reviewer prompt limits (contract with review_presets)."""

from __future__ import annotations

from typing import Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.review_presets import (
    PRESET_DEEP,
    PRESET_LIGHT,
    PRESET_STANDARD,
    normalize_preset,
    reviewer_keys_for_preset,
)

# reviewer_key -> max_diff_chars (0 = clip diff to empty; headings may remain)
_LIMITS: Dict[str, Dict[str, Dict[str, int]]] = {
    PRESET_LIGHT: {
        "review_diff": {"max_diff_chars": 4000},
        "review_tests": {"max_diff_chars": 2000},
    },
    PRESET_STANDARD: {
        "review_diff": {"max_diff_chars": 8000},
        "review_tests": {"max_diff_chars": 4000},
        "review_spec_conformance": {"max_diff_chars": 0},
    },
    PRESET_DEEP: {
        "review_diff": {"max_diff_chars": 12000},
        "review_wide": {"max_diff_chars": 12000},
        "review_tests": {"max_diff_chars": 6000},
        "review_spec_conformance": {"max_diff_chars": 0},
        "review_spec_revision": {"max_diff_chars": 0},
    },
}


def prompt_params_for_reviewer(preset: str, reviewer_key: str) -> Dict[str, int]:
    preset = normalize_preset(preset)
    table = _LIMITS.get(preset, {})
    if reviewer_key not in table:
        raise NodeExecutionFailure(
            f"no prompt limits for reviewer {reviewer_key!r} in preset {preset!r}"
        )
    return dict(table[reviewer_key])


def assert_preset_limits_cover_reviewers() -> None:
    """Guard: every reviewer in a preset has limit entries."""
    for preset in (PRESET_LIGHT, PRESET_STANDARD, PRESET_DEEP):
        keys = reviewer_keys_for_preset(preset)
        table = _LIMITS[preset]
        missing = [k for k in keys if k not in table]
        if missing:
            raise AssertionError(f"preset {preset!r} missing limits for {missing!r}")
        extra = [k for k in table if k not in keys]
        if extra:
            raise AssertionError(f"preset {preset!r} has limits for non-members {extra!r}")
