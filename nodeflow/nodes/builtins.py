"""Register built-in node types (v1.5 YAML vocabulary)."""

from __future__ import annotations

from nodeflow.core.registry import registry
from nodeflow.nodes.action.exec.claude_code_exec import ClaudeCodeExecNode
from nodeflow.nodes.action.exec.codex_exec import CodexExecNode
from nodeflow.nodes.action.exec.kimi_exec import KimiExecNode
from nodeflow.nodes.action.exec.qwen_exec import QwenExecNode
from nodeflow.nodes.action.routing.python_route_by_task_type import (
    PythonRouteByTaskTypeNode,
)
from nodeflow.nodes.action.transform.python_summarize_result import (
    PythonSummarizeResultNode,
)
from nodeflow.nodes.pipe.implement_dispatch_pipe import ImplementDispatchPipeNode
from nodeflow.nodes.pipe.review_dispatch_pipe import ReviewDispatchPipeNode
from nodeflow.nodes.pipe.serial_pipe import SerialPipeNode


def register_builtin_nodes() -> None:
    registry.register("compose", SerialPipeNode, override=True)
    registry.register("python_route_by_task_type", PythonRouteByTaskTypeNode, override=True)
    registry.register("python_summarize_result", PythonSummarizeResultNode, override=True)
    registry.register("codex_exec", CodexExecNode, override=True)
    registry.register("claude_code_exec", ClaudeCodeExecNode, override=True)
    registry.register("kimi_exec", KimiExecNode, override=True)
    registry.register("qwen_exec", QwenExecNode, override=True)
    registry.register("review_dispatch", ReviewDispatchPipeNode, override=True)
    registry.register("implement_dispatch", ImplementDispatchPipeNode, override=True)


register_builtin_nodes()
