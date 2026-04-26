"""ImplementPipeNode — load approved checkpoints, implement, test, diff, checkpoint."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.node_kinds import PipeNode, reset_children_for_graph
from nodeflow.core.runner import Runner
from nodeflow.nodes.development_flow.common.collect_diff import CollectDiffNode
from nodeflow.nodes.development_flow.common.load_checkpoint import LoadCheckpointNode
from nodeflow.nodes.development_flow.common.pipe_helpers import raise_child_fatal_if_any
from nodeflow.nodes.development_flow.common.write_checkpoint import WriteCheckpointNode
from nodeflow.nodes.development_flow.implement_pipe.run_tests import RunTestsNode
from nodeflow.nodes.exec.codex_exec import CodexExecNode


class ImplementPipeNode(PipeNode):
    """Single-run implementation stage with test and diff collection."""

    def __init__(self) -> None:
        super().__init__()
        self._graph_node_order = [
            "load_checkpoint",
            "implement_with_codex",
            "run_tests",
            "collect_diff",
            "write_checkpoint",
        ]
        self._nodes: Dict[str, BaseNode] = {
            "load_checkpoint": LoadCheckpointNode(),
            "implement_with_codex": CodexExecNode(),
            "run_tests": RunTestsNode(),
            "collect_diff": CollectDiffNode(),
            "write_checkpoint": WriteCheckpointNode(),
        }
        self._node_input_bindings = {
            "load_checkpoint": {
                "approved_checkpoint_path": ("inputs", "approved_checkpoint_path"),
                "repo_root": ("inputs", "repo_root"),
                "rework_context": ("inputs", "rework_context"),
            },
            "implement_with_codex": {
                "prompt": ("node", "load_checkpoint", "codex_task_prompt", "text"),
                "task_type": ("inputs", "task_type"),
            },
            "run_tests": {
                "repo_root": ("inputs", "repo_root"),
            },
            "collect_diff": {
                "repo_root": ("inputs", "repo_root"),
                "base_ref": ("inputs", "base_ref"),
            },
            "write_checkpoint": {
                "execution_result": ("node", "implement_with_codex", "execution_result"),
                "test_result": ("node", "run_tests", "test_result"),
                "diff_result": ("node", "collect_diff", "diff_result"),
            },
        }

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        reset_children_for_graph(self._nodes)
        pipe_params = dict(params) if params else {}
        pipe_inputs = dict(inputs) if inputs else {}
        pipe_inputs.setdefault("rework_context", "")

        resolved_node_params = {
            "load_checkpoint": dict(pipe_params.get("load_checkpoint") or {}),
            "implement_with_codex": dict(pipe_params.get("codex_exec") or {}),
            "run_tests": dict(pipe_params.get("run_tests") or {}),
            "collect_diff": dict(pipe_params.get("collect_diff") or {}),
            "write_checkpoint": dict(pipe_params.get("write_checkpoint") or {}),
        }
        resolved_node_params["write_checkpoint"].setdefault("stage", "implement")
        resolved_node_params["write_checkpoint"].setdefault(
            "summary_default", "implementation completed; verify and continue to review"
        )
        resolved_node_params["write_checkpoint"].setdefault("next_action_default", "review")
        resolved_node_params["write_checkpoint"].setdefault(
            "next_action_on_failure", "rework_implementation"
        )
        rr = pipe_inputs.get("repo_root")
        if isinstance(rr, str):
            resolved_node_params["write_checkpoint"]["_repo_root_for_paths"] = rr
        workspace_dir = pipe_params.get("_workspace_dir")
        if isinstance(workspace_dir, str):
            resolved_node_params["implement_with_codex"].setdefault("_workspace_dir", workspace_dir)

        latest_output: Dict[str, Dict[str, Any]] = {}
        runner = Runner(
            graph_node_order=self._graph_node_order,
            nodes=self._nodes,
            node_params=resolved_node_params,
            node_input_bindings=self._node_input_bindings,
            pipeline_inputs=pipe_inputs,
            pipeline_params=pipe_params,
            latest_output=latest_output,
        )

        while True:
            progressed = runner.step()
            statuses = [self._nodes[nid].read_status() for nid in self._graph_node_order]
            raise_child_fatal_if_any(graph_node_order=self._graph_node_order, nodes=self._nodes)
            if "limit" in statuses:
                raise NodeExecutionLimit("child limit")
            if self._nodes["write_checkpoint"].read_status() == "done":
                break
            if not progressed:
                raise NodeExecutionFailure("invalid execution state")

        out: Dict[str, Any] = {}
        if "write_checkpoint" in latest_output:
            out["stage_result"] = latest_output["write_checkpoint"].get("stage_result", {})
        return out
