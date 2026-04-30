"""SpecPlanPipeNode — collect context, draft via exec, write checkpoint."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.node_kinds import PipeNode, reset_children_for_graph
from nodeflow.core.runner import Runner
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.workflows.development_flow.spec_plan.collect_repo_context import (
    CollectRepoContextNode,
)
from nodeflow.workflows.development_flow.write_checkpoint import WriteCheckpointNode


def _raise_child_fatal_if_any(
    *,
    graph_node_order: List[str],
    nodes: Dict[str, BaseNode],
    prefix: str = "child fatal",
) -> None:
    fatal_children = [nid for nid in graph_node_order if nodes[nid].read_status() == "fatal"]
    if fatal_children:
        raise NodeExecutionFailure(f"{prefix}: {fatal_children}")


def _run_until_node_done(
    *,
    runner: Runner,
    graph_node_order: List[str],
    nodes: Dict[str, BaseNode],
    done_node_id: str,
) -> None:
    while True:
        progressed = runner.step()
        statuses = [nodes[nid].read_status() for nid in graph_node_order]
        _raise_child_fatal_if_any(graph_node_order=graph_node_order, nodes=nodes)
        if "limit" in statuses:
            raise NodeExecutionLimit("child limit")
        if nodes[done_node_id].read_status() == "done":
            return
        if not progressed:
            raise NodeExecutionFailure("invalid execution state")


class SpecPlanPipeNode(PipeNode):
    """Single-run spec/plan stage with human checkpoint artifact."""

    def __init__(self) -> None:
        super().__init__()
        self._graph_node_order = ["collect_repo_context", "draft_spec_plan", "write_checkpoint"]
        self._nodes: Dict[str, BaseNode] = {
            "collect_repo_context": CollectRepoContextNode(),
            "draft_spec_plan": CodexExecNode(),
            "write_checkpoint": WriteCheckpointNode(),
        }
        self._node_input_bindings = {
            "collect_repo_context": {
                "task_prompt": ("inputs", "task_prompt"),
                "repo_root": ("inputs", "repo_root"),
                "base_ref": ("inputs", "base_ref"),
                "revision_context": ("inputs", "revision_context"),
            },
            "draft_spec_plan": {
                "prompt": ("node", "collect_repo_context", "codex_task_prompt", "text"),
                "task_type": ("node", "collect_repo_context", "task_meta", "task_type"),
            },
            "write_checkpoint": {
                "request": ("node", "collect_repo_context", "checkpoint_request"),
                "execution_result": ("node", "draft_spec_plan", "execution_result"),
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
        pipe_inputs.setdefault("revision_context", "")

        resolved_node_params = {
            "collect_repo_context": dict(pipe_params.get("collect_repo_context") or {}),
            "draft_spec_plan": dict(pipe_params.get("codex_exec") or {}),
            "write_checkpoint": dict(pipe_params.get("write_checkpoint") or {}),
        }
        resolved_node_params["write_checkpoint"].setdefault("stage", "spec_plan")
        resolved_node_params["write_checkpoint"].setdefault("next_action_default", "approve")
        resolved_node_params["write_checkpoint"].setdefault("next_action_on_failure", "revise_spec")
        resolved_node_params["write_checkpoint"].setdefault("write_spec_plan_candidate", True)
        resolved_node_params["write_checkpoint"].setdefault(
            "spec_plan_candidate_suffix", "approved_candidate"
        )
        rr = pipe_inputs.get("repo_root")
        if isinstance(rr, str):
            resolved_node_params["write_checkpoint"]["_repo_root_for_paths"] = rr
        art = pipe_inputs.get("artifact_root")
        if isinstance(art, str) and art.strip():
            write_checkpoint_raw_params = pipe_params.get("write_checkpoint")
            checkpoint_dir_explicit = isinstance(write_checkpoint_raw_params, dict) and (
                "checkpoint_dir" in write_checkpoint_raw_params
            )
            if checkpoint_dir_explicit:
                raise NodeExecutionFailure(
                    "spec_plan: artifact_root and write_checkpoint.checkpoint_dir cannot both be set"
                )
            resolved_node_params["write_checkpoint"]["checkpoint_dir"] = str(
                (Path(art.strip()) / "spec_plan").resolve()
            )
        workspace_dir = pipe_params.get("_workspace_dir")
        if not isinstance(workspace_dir, str) or not workspace_dir.strip():
            rr_for_workspace = pipe_inputs.get("repo_root")
            if isinstance(rr_for_workspace, str) and rr_for_workspace.strip():
                workspace_dir = rr_for_workspace
        if isinstance(workspace_dir, str) and workspace_dir.strip():
            resolved_node_params["draft_spec_plan"].setdefault("_workspace_dir", workspace_dir)

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

        _run_until_node_done(
            runner=runner,
            graph_node_order=self._graph_node_order,
            nodes=self._nodes,
            done_node_id="write_checkpoint",
        )

        out: Dict[str, Any] = {}
        if "write_checkpoint" in latest_output:
            out["stage_result"] = latest_output["write_checkpoint"].get("stage_result", {})
        return out
