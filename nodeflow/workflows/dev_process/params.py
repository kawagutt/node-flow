"""Shared param parsing for dev_process.flow."""

from __future__ import annotations

from typing import Any


def parse_bool_param(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in ("0", "false", "no", "")
    if value is None:
        return default
    return bool(value)
