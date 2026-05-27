"""FlowCtx helpers for dev-process leaf ActionNodes."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import SCHEMA_VERSION
from nodeflow.workflows.dev_process.flow_context import _phase_repo_root, _workspace_repo_root


def copy_flow_ctx(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Deep-copy FlowCtx and nested body (value-passing contract)."""
    if not isinstance(raw, dict):
        raise NodeExecutionFailure("FlowCtx must be a dict")
    ctx = deepcopy(raw)
    body_raw = ctx.get("body")
    if not isinstance(body_raw, dict):
        raise NodeExecutionFailure("FlowCtx.body must be a dict")
    body = deepcopy(body_raw)
    ctx["body"] = body
    return ctx, body


def flow_params(ctx: dict[str, Any]) -> dict[str, Any]:
    params = ctx.get("params")
    return params if isinstance(params, dict) else {}


def repo_root_from_ctx(ctx: dict[str, Any]) -> Any:
    """Resolve workspace repo root from ctx.params or checkpoint body."""
    from pathlib import Path

    p = flow_params(ctx).get("repo_root")
    if isinstance(p, str) and p.strip():
        return Path(p).resolve()
    body = ctx.get("body")
    if not isinstance(body, dict):
        raise NodeExecutionFailure("FlowCtx.body required to resolve repo_root")
    phase_id = flow_params(ctx).get("phase_id")
    if phase_id:
        return _phase_repo_root(body)
    return _workspace_repo_root(body)


def review_artifact_root_from_ctx(ctx: dict[str, Any], body: dict[str, Any]) -> str:
    """Review evidence root: explicit param, final scope → run root, else phase root."""
    params = flow_params(ctx)
    explicit = params.get("artifact_root")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    if str(params.get("review_scope") or "") == "final":
        return artifact_root_from_body(body, phase=False)
    return artifact_root_from_body(body, phase=True)


def artifact_root_from_body(body: dict[str, Any], *, phase: bool = False) -> str:
    run_context = body.get("run_context")
    if not isinstance(run_context, dict):
        raise NodeExecutionFailure("body.run_context required")
    if phase:
        dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
        phase_id = dp.get("current_phase_id")
        if phase_id:
            from pathlib import Path

            return str(Path(str(run_context["artifact_root"])) / "phases" / str(phase_id))
    return str(run_context["artifact_root"])


def run_id_from_body(body: dict[str, Any]) -> str:
    run_context = body.get("run_context")
    if not isinstance(run_context, dict):
        raise NodeExecutionFailure("body.run_context required")
    run_id = run_context.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise NodeExecutionFailure("body.run_context.run_id required")
    return run_id


def make_flow_ctx(
    body: dict[str, Any],
    *,
    segment: str = "",
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a minimal FlowCtx dict for tests and Runner smoke."""
    ctx: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "body": body,
        "segment": segment,
        "params": dict(params or {}),
    }
    return ctx
