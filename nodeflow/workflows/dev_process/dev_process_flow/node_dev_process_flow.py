"""Top-level dev-process orchestrator node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, Mapping

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

        run_spec_plan_on_start = parse_bool_param(
            params.get("run_spec_plan_on_start", True), default=True
        )

        codex_argv = params.get("codex_argv")
        if codex_argv is not None and not (
            isinstance(codex_argv, list) and all(isinstance(x, str) for x in codex_argv)
        ):
            raise NodeExecutionFailure("params.codex_argv must be a list[str] when set")

        force_blocking = parse_bool_param(params.get("force_review_blocking", False))

        try:
            result = run_flow(
                action=action,
                repo_root=str(repo_root) if repo_root else "",
                task_prompt=task_prompt,
                flow_checkpoint_path=flow_cp_s,
                run_id=run_id_s,
                run_spec_plan_on_start=run_spec_plan_on_start,
                human_comment_text=str(_port_scalar(inputs, "human_comment_text") or ""),
                codex_argv=codex_argv,
                force_review_blocking=force_blocking,
            )
        except NodeExecutionFailure:
            raise
        return {"flow_output": result}

    def execute(self, inputs: Dict[str, Any], params: Mapping[str, Any]) -> Dict[str, Any]:
        obs = super().execute(inputs, params)
        if self._status == "fatal" and isinstance(self._error, NodeExecutionFailure):
            raise self._error
        return obs
