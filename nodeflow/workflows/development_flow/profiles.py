"""Profile merge helpers for workflows.development_flow."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure


def _as_path(base: Path, raw: str | None) -> Path | None:
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_absolute() else (base / p).resolve()


def _read_json_required(path: Path, *, label: str) -> Dict[str, Any]:
    if not path.exists():
        raise NodeExecutionFailure(f"{label} not found: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"{label} is not valid JSON: {path}") from e
    except OSError as e:
        raise NodeExecutionFailure(f"{label} could not be read: {path}") from e
    if not isinstance(obj, dict):
        raise NodeExecutionFailure(f"{label} must be a JSON object: {path}")
    return obj


def _deep_merge_dict(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(base)
    for k, v in overlay.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge_dict(out[k], v)
        else:
            out[k] = deepcopy(v) if isinstance(v, dict) else v
    return out


def _load_profile_layer(*, profiles_path: Path, profile_name: str) -> Dict[str, Any]:
    data = _read_json_required(profiles_path, label="profiles file")
    if profile_name not in data:
        raise NodeExecutionFailure(
            f"unknown profile {profile_name!r} in {profiles_path} (available: {sorted(data.keys())!r})"
        )
    layer = data.get(profile_name)
    if not isinstance(layer, dict):
        raise NodeExecutionFailure(
            f"profile {profile_name!r} must be a JSON object in {profiles_path}"
        )
    return layer


def apply_profiles_to_pipe_params(
    p: Dict[str, Any],
    *,
    workspace: Path,
    model_profiles_path: str | None,
    cost_profiles_path: str | None,
    model_profile: str | None,
    cost_profile: str | None,
) -> None:
    """Apply model/cost profile layers onto stage pipe params.

    Priority is: direct YAML stage params < model profile < cost profile.
    """
    profile_keys = {
        "model_profiles_path": model_profiles_path,
        "cost_profiles_path": cost_profiles_path,
        "model_profile": model_profile,
        "cost_profile": cost_profile,
    }
    if any(profile_keys.values()) and not all(profile_keys.values()):
        raise NodeExecutionFailure(
            "model_profiles_path, cost_profiles_path, model_profile, and cost_profile must be set together"
        )
    if not all(profile_keys.values()):
        return

    mp = _as_path(workspace, model_profiles_path)
    cp = _as_path(workspace, cost_profiles_path)
    if mp is None or cp is None:
        raise NodeExecutionFailure(
            "model_profile and cost_profile require both model_profiles_path and cost_profiles_path"
        )

    model_layer = _load_profile_layer(profiles_path=mp, profile_name=str(model_profile))
    cost_layer = _load_profile_layer(profiles_path=cp, profile_name=str(cost_profile))
    for stage_key in ("spec_plan", "implement", "review"):
        if stage_key not in model_layer and stage_key not in cost_layer:
            continue
        p[stage_key] = _deep_merge_dict(
            dict(p.get(stage_key) or {}),
            _deep_merge_dict(
                dict(model_layer.get(stage_key) or {}),
                dict(cost_layer.get(stage_key) or {}),
            ),
        )
