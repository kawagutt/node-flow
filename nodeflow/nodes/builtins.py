"""Register built-in node types (YAML `type` registry keys)."""

from __future__ import annotations

from nodeflow.core.registry import registry
from nodeflow.nodes.exec.claude_code_exec import ClaudeCodeExecNode
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.exec.kimi_exec import KimiExecNode
from nodeflow.nodes.exec.qwen_exec import QwenExecNode
from nodeflow.nodes.routing.python_route_by_task_type import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode
from nodeflow.workflows.development_flow import (
    ApprovePipeNode,
    DevelopmentFlowPipeNode,
    ImplementPipeNode,
    MergePipeNode,
    ReviewPipeNode,
    ReviseSpecPipeNode,
    ReworkPipeNode,
    SpecPlanPipeNode,
    StartPipeNode,
)
from nodeflow.workflows.implement_with_codex.node_implement_with_codex import (
    ImplementWithCodexPipeNode,
)
from nodeflow.workflows.review_with_claude.node_review_with_claude import (
    ReviewWithClaudePipeNode,
)


def register_builtin_nodes() -> None:
    registry.register("python_route_by_task_type", PythonRouteByTaskTypeNode, override=True)
    registry.register("python_summarize_result", PythonSummarizeResultNode, override=True)
    registry.register("codex_exec", CodexExecNode, override=True)
    registry.register("claude_code_exec", ClaudeCodeExecNode, override=True)
    registry.register("kimi_exec", KimiExecNode, override=True)
    registry.register("qwen_exec", QwenExecNode, override=True)
    # Preferred v1.5 names for fixed provider pipes (no dynamic dispatch in PipeNode).
    registry.register("review_with_claude", ReviewWithClaudePipeNode, override=True)
    registry.register("implement_with_codex", ImplementWithCodexPipeNode, override=True)
    # Development flow nodes (path-style keys only).
    registry.register("workflows.development_flow.spec_plan", SpecPlanPipeNode, override=True)
    registry.register("workflows.development_flow.implement", ImplementPipeNode, override=True)
    registry.register("workflows.development_flow.review", ReviewPipeNode, override=True)
    registry.register("workflows.development_flow.start", StartPipeNode, override=True)
    registry.register("workflows.development_flow.revise_spec", ReviseSpecPipeNode, override=True)
    registry.register("workflows.development_flow.approve", ApprovePipeNode, override=True)
    registry.register("workflows.development_flow.rework", ReworkPipeNode, override=True)
    registry.register("workflows.development_flow.merge", MergePipeNode, override=True)
    registry.register("workflows.development_flow", DevelopmentFlowPipeNode, override=True)


register_builtin_nodes()
