"""Register built-in node types from ``nodeflow.nodes`` on the global registry."""

from __future__ import annotations

from nodeflow.core.registry import registry
from nodeflow.nodes.exec.node_exec import (
    ClaudeCodeExecNode,
    CodexExecNode,
    KimiExecNode,
    QwenExecNode,
)
from nodeflow.nodes.hello_demo import HelloDemoNode
from nodeflow.nodes.routing.node_routing import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.node_summarize import PythonSummarizeResultNode


def register_builtin_nodes() -> None:
    from nodeflow.workflows.dev_process.register_nodes import register_dev_process_nodes

    register_dev_process_nodes()
    registry.register("hello_demo", HelloDemoNode, override=True)
    registry.register("python_route_by_task_type", PythonRouteByTaskTypeNode, override=True)
    registry.register("python_summarize_result", PythonSummarizeResultNode, override=True)
    registry.register("codex_exec", CodexExecNode, override=True)
    registry.register("claude_code_exec", ClaudeCodeExecNode, override=True)
    registry.register("kimi_exec", KimiExecNode, override=True)
    registry.register("qwen_exec", QwenExecNode, override=True)


register_builtin_nodes()
