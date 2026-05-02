"""ReviewPipeNode — load approved bundle, diff, prompt builders, reviews, aggregate, checkpoint."""

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
from nodeflow.core.node_kinds import PipeNode
from nodeflow.legacy.runner import Runner
from nodeflow.legacy.runner_frame import reset_children_for_graph
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.git.collect_diff import CollectDiffNode
from nodeflow.workflows.development_flow.load_checkpoint import LoadCheckpointNode
from nodeflow.workflows.development_flow.review.aggregate_reviews import AggregateReviewsNode
from nodeflow.workflows.development_flow.review.build_diff_review_prompt import (
    BuildDiffReviewPromptNode,
)
from nodeflow.workflows.development_flow.review.build_spec_review_prompt import (
    BuildSpecReviewPromptNode,
)
from nodeflow.workflows.development_flow.review.build_spec_revision_review_prompt import (
    BuildSpecRevisionReviewPromptNode,
)
from nodeflow.workflows.development_flow.review.build_test_review_prompt import (
    BuildTestReviewPromptNode,
)
from nodeflow.workflows.development_flow.review.build_wide_scan_review_prompt import (
    BuildWideScanReviewPromptNode,
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


class ReviewPipeNode(PipeNode):
    """Single-run review stage with multi-angle checks and aggregation."""

    def __init__(self) -> None:
        super().__init__()
        self._graph_node_order = [
            "load_checkpoint",
            "collect_diff",
            "build_diff_review_prompt",
            "review_diff_focused",
            "build_wide_scan_review_prompt",
            "review_wide_scan",
            "build_test_review_prompt",
            "review_test_focused",
            "build_spec_review_prompt",
            "review_spec_conformance",
            "build_spec_revision_review_prompt",
            "review_spec_revision",
            "aggregate_reviews",
            "write_checkpoint",
        ]
        self._nodes: Dict[str, BaseNode] = {
            "load_checkpoint": LoadCheckpointNode(),
            "collect_diff": CollectDiffNode(),
            "build_diff_review_prompt": BuildDiffReviewPromptNode(),
            "review_diff_focused": CodexExecNode(),
            "build_wide_scan_review_prompt": BuildWideScanReviewPromptNode(),
            "review_wide_scan": CodexExecNode(),
            "build_test_review_prompt": BuildTestReviewPromptNode(),
            "review_test_focused": CodexExecNode(),
            "build_spec_review_prompt": BuildSpecReviewPromptNode(),
            "review_spec_conformance": CodexExecNode(),
            "build_spec_revision_review_prompt": BuildSpecRevisionReviewPromptNode(),
            "review_spec_revision": CodexExecNode(),
            "aggregate_reviews": AggregateReviewsNode(),
            "write_checkpoint": WriteCheckpointNode(),
        }
        self._node_input_bindings = {
            "load_checkpoint": {
                "approved_checkpoint_path": ("inputs", "approved_checkpoint_path"),
                "repo_root": ("inputs", "repo_root"),
            },
            "collect_diff": {
                "repo_root": ("inputs", "repo_root"),
                "base_ref": ("inputs", "base_ref"),
            },
            "build_diff_review_prompt": {
                "diff_result": ("node", "collect_diff", "diff_result"),
                "base_ref": ("inputs", "base_ref"),
            },
            "review_diff_focused": {
                "prompt": ("node", "build_diff_review_prompt", "codex_task_prompt", "text"),
                "task_type": ("inputs", "task_type"),
            },
            "build_wide_scan_review_prompt": {
                "diff_result": ("node", "collect_diff", "diff_result"),
                "base_ref": ("inputs", "base_ref"),
            },
            "review_wide_scan": {
                "prompt": ("node", "build_wide_scan_review_prompt", "codex_task_prompt", "text"),
                "task_type": ("inputs", "task_type"),
            },
            "build_test_review_prompt": {
                "diff_result": ("node", "collect_diff", "diff_result"),
                "base_ref": ("inputs", "base_ref"),
            },
            "review_test_focused": {
                "prompt": ("node", "build_test_review_prompt", "codex_task_prompt", "text"),
                "task_type": ("inputs", "task_type"),
            },
            "build_spec_review_prompt": {
                "approved_spec_plan": ("node", "load_checkpoint", "approved_spec_plan"),
                "diff_result": ("node", "collect_diff", "diff_result"),
                "base_ref": ("inputs", "base_ref"),
            },
            "review_spec_conformance": {
                "prompt": ("node", "build_spec_review_prompt", "codex_task_prompt", "text"),
                "task_type": ("inputs", "task_type"),
            },
            "build_spec_revision_review_prompt": {
                "approved_spec_plan": ("node", "load_checkpoint", "approved_spec_plan"),
                "diff_result": ("node", "collect_diff", "diff_result"),
                "base_ref": ("inputs", "base_ref"),
            },
            "review_spec_revision": {
                "prompt": (
                    "node",
                    "build_spec_revision_review_prompt",
                    "codex_task_prompt",
                    "text",
                ),
                "task_type": ("inputs", "task_type"),
            },
            "aggregate_reviews": {
                "review_diff": ("node", "review_diff_focused", "execution_result"),
                "review_wide": ("node", "review_wide_scan", "execution_result"),
                "review_tests": ("node", "review_test_focused", "execution_result"),
                "review_spec": ("node", "review_spec_conformance", "execution_result"),
                "review_spec_revision": ("node", "review_spec_revision", "execution_result"),
                "diff_result": ("node", "collect_diff", "diff_result"),
            },
            "write_checkpoint": {
                "request": ("node", "aggregate_reviews", "checkpoint_request"),
                "review_result": ("node", "aggregate_reviews", "review_result"),
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

        resolved_node_params = {
            "load_checkpoint": dict(pipe_params.get("load_checkpoint") or {}),
            "collect_diff": dict(pipe_params.get("collect_diff") or {}),
            "build_diff_review_prompt": dict(pipe_params.get("build_diff_review_prompt") or {}),
            "review_diff_focused": dict(pipe_params.get("review_diff_focused") or {}),
            "build_wide_scan_review_prompt": dict(
                pipe_params.get("build_wide_scan_review_prompt") or {}
            ),
            "review_wide_scan": dict(pipe_params.get("review_wide_scan") or {}),
            "build_test_review_prompt": dict(pipe_params.get("build_test_review_prompt") or {}),
            "review_test_focused": dict(pipe_params.get("review_test_focused") or {}),
            "build_spec_review_prompt": dict(pipe_params.get("build_spec_review_prompt") or {}),
            "review_spec_conformance": dict(pipe_params.get("review_spec_conformance") or {}),
            "build_spec_revision_review_prompt": dict(
                pipe_params.get("build_spec_revision_review_prompt") or {}
            ),
            "review_spec_revision": dict(pipe_params.get("review_spec_revision") or {}),
            "aggregate_reviews": dict(pipe_params.get("aggregate_reviews") or {}),
            "write_checkpoint": dict(pipe_params.get("write_checkpoint") or {}),
        }
        resolved_node_params["write_checkpoint"].setdefault("stage", "review")
        resolved_node_params["write_checkpoint"].setdefault("next_action_default", "stop")
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
                    "review: artifact_root and write_checkpoint.checkpoint_dir cannot both be set"
                )
            resolved_node_params["write_checkpoint"]["checkpoint_dir"] = str(
                (Path(art.strip()) / "review").resolve()
            )
        workspace_dir = pipe_params.get("_workspace_dir")
        if not isinstance(workspace_dir, str) or not workspace_dir.strip():
            rr_for_workspace = pipe_inputs.get("repo_root")
            if isinstance(rr_for_workspace, str) and rr_for_workspace.strip():
                workspace_dir = rr_for_workspace
        if isinstance(workspace_dir, str) and workspace_dir.strip():
            resolved_node_params["review_diff_focused"].setdefault("_workspace_dir", workspace_dir)
            resolved_node_params["review_wide_scan"].setdefault("_workspace_dir", workspace_dir)
            resolved_node_params["review_test_focused"].setdefault("_workspace_dir", workspace_dir)
            resolved_node_params["review_spec_conformance"].setdefault(
                "_workspace_dir", workspace_dir
            )
            resolved_node_params["review_spec_revision"].setdefault("_workspace_dir", workspace_dir)

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
