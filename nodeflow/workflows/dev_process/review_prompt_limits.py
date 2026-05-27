"""Per-preset, per-reviewer prompt limits (contract with review_presets)."""

from __future__ import annotations

from typing import Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.review_node_spec import NODE_PROMPT_LIMITS
from nodeflow.workflows.dev_process.review_presets import (
    PRESET_DEEP,
    PRESET_LIGHT,
    PRESET_STANDARD,
    normalize_preset,
    reviewer_keys_for_preset,
)

# Legacy preset -> prompt-key limits (non-agent phase flows without plan agents)
_LIMITS: Dict[str, Dict[str, Dict[str, int]]] = {
    PRESET_LIGHT: {
        "review_diff": {"max_diff_chars": 4000},
        "review_tests": {"max_diff_chars": 2000},
    },
    PRESET_STANDARD: {
        "review_diff": {"max_diff_chars": 8000},
        "review_tests": {"max_diff_chars": 4000},
        "review_spec_conformance": {"max_diff_chars": 6000},
    },
    PRESET_DEEP: {
        "review_diff": {"max_diff_chars": 12000},
        "review_wide": {"max_diff_chars": 12000},
        "review_tests": {"max_diff_chars": 6000},
        "review_spec_conformance": {"max_diff_chars": 6000},
        "review_spec_revision": {"max_diff_chars": 0},
    },
}

_PRESET_DIFF_SCALE: Dict[str, float] = {
    PRESET_LIGHT: 0.5,
    PRESET_STANDARD: 1.0,
    PRESET_DEEP: 1.5,
}


def _scale_prompt_limits(base: Dict[str, int], preset: str) -> Dict[str, int]:
    """Apply preset multiplier to diff char limits (0 stays 0)."""
    mult = _PRESET_DIFF_SCALE.get(normalize_preset(preset), 1.0)
    scaled: Dict[str, int] = {}
    for key, value in base.items():
        if key == "max_diff_chars" and value > 0:
            scaled[key] = max(1, int(value * mult))
        else:
            scaled[key] = value
    return scaled


def prompt_params_for_review_node(preset: str, node_name: str) -> Dict[str, int]:
    preset = normalize_preset(preset)
    if node_name in NODE_PROMPT_LIMITS:
        return _scale_prompt_limits(NODE_PROMPT_LIMITS[node_name], preset)
    table = _LIMITS.get(preset, {})
    if node_name not in table:
        raise NodeExecutionFailure(
            f"no prompt limits for review node {node_name!r} in preset {preset!r}"
        )
    return dict(table[node_name])


def prompt_params_for_reviewer(preset: str, reviewer_key: str) -> Dict[str, int]:
    """Resolve limits for legacy preset keys or dedicated review node names."""
    from nodeflow.workflows.dev_process.review_config import review_node_name

    return prompt_params_for_review_node(preset, review_node_name(reviewer_key))


def assert_preset_limits_cover_reviewers() -> None:
    """Guard: every v1 agent in a preset resolves to a node with prompt limits."""
    from nodeflow.workflows.dev_process.review_config import review_node_name
    from nodeflow.workflows.dev_process.review_node_spec import LEGACY_PRESET_REVIEW_NODES

    active_nodes: set[str] = set()
    for preset in (PRESET_LIGHT, PRESET_STANDARD, PRESET_DEEP):
        for agent in reviewer_keys_for_preset(preset):
            node_name = review_node_name(agent)
            active_nodes.add(node_name)
            prompt_params_for_review_node(preset, node_name)

    # Internal _LIMITS table may retain legacy keys for prompt builders; they must not
    # overlap active preset nodes and must stay within the legacy allowlist.
    for preset in (PRESET_LIGHT, PRESET_STANDARD, PRESET_DEEP):
        table = _LIMITS[preset]
        overlap = set(table) & active_nodes
        if overlap:
            raise AssertionError(
                f"preset {preset!r}: _LIMITS keys overlap active v1 nodes {sorted(overlap)!r}"
            )
        extra = set(table) - LEGACY_PRESET_REVIEW_NODES
        if extra:
            raise AssertionError(
                f"preset {preset!r}: _LIMITS has non-legacy keys {sorted(extra)!r}"
            )
