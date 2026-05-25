"""Top-level dev-process orchestrator node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.dev_process.constants import ACTION_START
from nodeflow.workflows.dev_process.flow_runner import run_flow
from nodeflow.workflows.dev_process.params import parse_bool_param


def _port_scalar(inputs: Dict[str, Any], name: str) -> Any:
    """Read a scalar from direct execute inputs or Runner per-port dict payloads."""
    raw = inputs.get(name)
    if isinstance(raw, dict):
        if name in raw:
            return raw[name]
        if "value" in raw:
            return raw["value"]
        if len(raw) == 1:
            return next(iter(raw.values()))
    return raw


def _argv_list_param(raw: Any, *, label: str) -> Optional[list[str]]:
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        raise NodeExecutionFailure(f"{label} must be a list[str] when set")
    return raw


def _optional_config_scalar(
    inputs: Dict[str, Any],
    params: MappingProxyType,
    name: str,
    *,
    default: str | None = None,
) -> str | None:
    """Resolve a scalar from port input, then node params, then default (in that order)."""
    raw = _port_scalar(inputs, name)
    if raw is not None and str(raw).strip():
        return str(raw).strip()

    fallback = params.get(name)
    if fallback is not None and str(fallback).strip():
        return str(fallback).strip()

    return default


class DevProcessFlowNode(PythonActionNode):
    role = "dev_process_flow"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        del context  # orchestration is synchronous
        action = _port_scalar(inputs, "action")
        if not isinstance(action, str) or not action.strip():
            raise NodeExecutionFailure("action is required")
        action = action.strip()

        repo_root = _port_scalar(inputs, "repo_root")
        if action == ACTION_START:
            if not isinstance(repo_root, str) or not repo_root.strip():
                raise NodeExecutionFailure("repo_root is required for start")
        elif not isinstance(repo_root, str) or not repo_root.strip():
            repo_root = None

        flow_cp = _port_scalar(inputs, "flow_checkpoint_path")
        flow_cp_s = flow_cp if isinstance(flow_cp, str) and flow_cp.strip() else None

        task_prompt = str(_port_scalar(inputs, "task_prompt") or "")
        run_id = _port_scalar(inputs, "run_id")
        run_id_s = run_id if isinstance(run_id, str) and run_id.strip() else None

        run_spec_on_start = parse_bool_param(
            params.get("run_spec_on_start", True), default=True
        )

        exec_argv = _argv_list_param(
            _port_scalar(inputs, "exec_argv") or params.get("exec_argv"),
            label="exec_argv",
        )
        exec_model = _optional_config_scalar(inputs, params, "exec_model")

        force_blocking = parse_bool_param(params.get("force_review_blocking", False))

        if action == ACTION_START:
            workspace_strategy = _optional_config_scalar(
                inputs, params, "workspace_strategy", default="current_repo"
            )
            exec_worker_kind = _optional_config_scalar(
                inputs, params, "exec_worker_kind", default="codex"
            )
            merge_policy = _optional_config_scalar(
                inputs, params, "merge_policy", default="record_only"
            )
        else:
            workspace_strategy = _optional_config_scalar(inputs, params, "workspace_strategy")
            exec_worker_kind = _optional_config_scalar(inputs, params, "exec_worker_kind")
            merge_policy = _optional_config_scalar(inputs, params, "merge_policy")

        try:
            result = run_flow(
                action=action,
                repo_root=str(repo_root) if repo_root else "",
                task_prompt=task_prompt,
                flow_checkpoint_path=flow_cp_s,
                run_id=run_id_s,
                run_spec_on_start=run_spec_on_start,
                human_comment_text=str(_port_scalar(inputs, "human_comment_text") or ""),
                exec_argv=exec_argv,
                exec_model=exec_model,
                force_review_blocking=force_blocking,
                workspace_strategy=workspace_strategy,
                exec_worker_kind=exec_worker_kind,
                merge_policy=merge_policy,
            )
        except NodeExecutionFailure:
            raise
        return {"flow_output": result}

    def execute(self, inputs: Dict[str, Any], params: Mapping[str, Any]) -> Dict[str, Any]:
        obs = super().execute(inputs, params)
        if self._status == "fatal" and isinstance(self._error, NodeExecutionFailure):
            raise self._error
        return obs
