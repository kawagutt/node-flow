"""Register built-in node types (YAML `type` registry keys)."""

from __future__ import annotations

from nodeflow.core.registry import registry
from nodeflow.nodes.development_flow import (
    DevelopmentFlowPipeNode,
    ImplementPipeNode,
    ReviewPipeNode,
    SpecPlanPipeNode,
)
from nodeflow.nodes.dispatch.implement_with_codex_pipe import ImplementWithCodexPipeNode
from nodeflow.nodes.dispatch.review_with_claude_pipe import ReviewWithClaudePipeNode
from nodeflow.nodes.exec.claude_code_exec import ClaudeCodeExecNode
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.exec.kimi_exec import KimiExecNode
from nodeflow.nodes.exec.qwen_exec import QwenExecNode
from nodeflow.nodes.routing.python_route_by_task_type import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode


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
    # Development flow stage pipes (single-run; human checkpoint via artifacts).
    registry.register("spec_plan_pipe", SpecPlanPipeNode, override=True)
    registry.register("implement_pipe", ImplementPipeNode, override=True)
    registry.register("review_pipe", ReviewPipeNode, override=True)
    registry.register("development_flow_pipe", DevelopmentFlowPipeNode, override=True)


register_builtin_nodes()
