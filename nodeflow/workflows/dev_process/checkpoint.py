"""Flow checkpoint read/write for dev_process.flow.v2."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import SCHEMA_VERSION
from nodeflow.workflows.dev_process.paths import checkpoint_path_under_artifact_root


def read_json(path: Path, *, label: str) -> Dict[str, Any]:
    if not path.exists():
        raise NodeExecutionFailure(f"{label} not found: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"{label} is not valid JSON: {path}") from e
    if not isinstance(obj, dict):
        raise NodeExecutionFailure(f"{label} must be a JSON object: {path}")
    return obj


def assert_flow_checkpoint_resume(actual_path: Path, doc: Dict[str, Any]) -> None:
    """Resume guard: path under checkpoints/, self-reference, absolute run paths."""
    actual = actual_path.resolve()
    run_context = doc.get("run_context")
    if not isinstance(run_context, dict):
        raise NodeExecutionFailure("checkpoint missing run_context")

    artifact_raw = run_context.get("artifact_root")
    repo_raw = run_context.get("repo_root")
    if not isinstance(artifact_raw, str) or not artifact_raw:
        raise NodeExecutionFailure(
            "checkpoint run_context.artifact_root must be a non-empty string"
        )
    if not isinstance(repo_raw, str) or not repo_raw:
        raise NodeExecutionFailure("checkpoint run_context.repo_root must be a non-empty string")

    artifact_root = Path(artifact_raw)
    if not artifact_root.is_absolute():
        raise NodeExecutionFailure("checkpoint run_context.artifact_root must be absolute")
    repo_root = Path(repo_raw)
    if not repo_root.is_absolute():
        raise NodeExecutionFailure("checkpoint run_context.repo_root must be absolute")

    checkpoints = (artifact_root.resolve() / "checkpoints").resolve()
    try:
        actual.relative_to(checkpoints)
    except ValueError as e:
        raise NodeExecutionFailure(
            f"flow_checkpoint_path must be under {checkpoints}, got {actual}"
        ) from e

    flow_result = doc.get("flow_result")
    if not isinstance(flow_result, dict):
        raise NodeExecutionFailure("checkpoint missing flow_result")
    recorded_raw = flow_result.get("flow_checkpoint_path")
    if not isinstance(recorded_raw, str) or not recorded_raw.strip():
        raise NodeExecutionFailure("checkpoint flow_result.flow_checkpoint_path missing")
    recorded = Path(recorded_raw).resolve()
    if recorded != actual:
        raise NodeExecutionFailure(
            f"checkpoint self-reference mismatch: file has {recorded!r}, resume path is {actual!r}"
        )


def load_flow_checkpoint(path: str | Path) -> Dict[str, Any]:
    cp_path = Path(path).resolve()
    doc = read_json(cp_path, label="flow_checkpoint_path")
    if doc.get("schema_version") != SCHEMA_VERSION:
        raise NodeExecutionFailure(
            f"unsupported checkpoint schema_version {doc.get('schema_version')!r}; "
            f"expected {SCHEMA_VERSION!r}"
        )
    assert_flow_checkpoint_resume(cp_path, doc)
    return doc


def write_flow_checkpoint(
    *,
    artifact_root: str,
    run_id: str,
    action: str,
    body: Dict[str, Any],
    filename: str | None = None,
) -> Tuple[str, Dict[str, Any]]:
    """Write checkpoint; return (absolute path, full document)."""
    if filename is None:
        if action == "start" and body.get("flow_result", {}).get("state") == "initialized":
            fname = "flow_start.json"
        else:
            fname = f"{run_id}_{action}_flow.json"
    cp_path = checkpoint_path_under_artifact_root(artifact_root, fname)
    cp_path.parent.mkdir(parents=True, exist_ok=True)

    flow_result = dict(body.get("flow_result") or {})
    flow_result["flow_checkpoint_path"] = str(cp_path)
    body = dict(body)
    body["flow_result"] = flow_result
    body["schema_version"] = SCHEMA_VERSION
    body["written_at"] = datetime.now(timezone.utc).isoformat()

    cp_path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(cp_path), body
