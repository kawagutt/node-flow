"""Shared validation for Codex/claude-code fixed-provider pipes (v1.6 dict-only payloads)."""

from __future__ import annotations

from typing import Any

from nodeflow.core.base_node import NodeExecutionFailure


def optional_child_params(raw_params: dict[str, Any], key: str) -> dict[str, Any]:
    """Return a shallow copy of ``raw_params[key]`` when it is a dict; otherwise reject or empty."""
    value = raw_params.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise NodeExecutionFailure(f"params.{key} must be a dict")
    return dict(value)


def validate_task_prompt_task_type_ports(inputs: dict[str, Any]) -> None:
    """Raise :class:`~nodeflow.core.base_node.NodeExecutionFailure` if ports are malformed.

    Core Runner delivers only dict payloads; these pipes expose exec-shaped ports at the boundary:
    ``task_prompt`` → ``{\"text\": <str>}``, ``task_type`` → ``{\"value\": <str>}``.
    """
    tp = inputs.get("task_prompt")
    tt = inputs.get("task_type")
    if not isinstance(tt, dict) or "value" not in tt or not isinstance(tt["value"], str):
        raise NodeExecutionFailure('inputs.task_type must be a dict with string key "value"')
    if not isinstance(tp, dict) or "text" not in tp or not isinstance(tp["text"], str):
        raise NodeExecutionFailure('inputs.task_prompt must be a dict with string key "text"')
