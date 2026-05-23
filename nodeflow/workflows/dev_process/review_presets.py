"""Review depth presets for dev-process (P2)."""

from __future__ import annotations

from typing import Tuple

PRESET_LIGHT = "light"
PRESET_STANDARD = "standard"
PRESET_DEEP = "deep"

VALID_PRESETS = frozenset({PRESET_LIGHT, PRESET_STANDARD, PRESET_DEEP})

# input_key values passed to AggregateReviewsNode
_REVIEWER_KEYS_LIGHT: Tuple[str, ...] = ("review_diff", "review_tests")
_REVIEWER_KEYS_STANDARD: Tuple[str, ...] = (
    "review_diff",
    "review_wide",
    "review_tests",
    "review_spec",
    "review_spec_revision",
)
_REVIEWER_KEYS_DEEP: Tuple[str, ...] = _REVIEWER_KEYS_STANDARD


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
