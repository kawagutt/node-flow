"""Register built-in node types (registry keys)."""

from __future__ import annotations

from nodeflow.core.registry import registry
from nodeflow.nodes.exec.claude_code_exec import ClaudeCodeExecNode
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.exec.kimi_exec import KimiExecNode
from nodeflow.nodes.exec.qwen_exec import QwenExecNode
from nodeflow.nodes.routing.python_route_by_task_type import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode
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
    registry.register("review_with_claude", ReviewWithClaudePipeNode, override=True)
    registry.register("implement_with_codex", ImplementWithCodexPipeNode, override=True)


register_builtin_nodes()
