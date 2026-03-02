"""NodeFlow extensions — PipelineNode, PythonScriptNode, LLMNode, OpenRouterNode. Built-in 登録を担当。"""

from nodeflow.core.registry import RegistryConflictError, registry

from .llm import LLMNode
from .openrouter import OpenRouterNode
from .pipeline_node import PipelineNode
from .python_script import PythonScriptNode

# built-in を registry に登録（v1.4.4）。
# ポリシー: override=False で「ユーザーが先に登録したら built-in は上書きしない」。未登録なら登録、既に登録済みならスキップ。
for _name, _cls in [
    ("python_script", PythonScriptNode),
    ("llm", LLMNode),
    ("openrouter", OpenRouterNode),
    ("pipeline", PipelineNode),
]:
    try:
        registry.register(_name, _cls, override=False)
    except RegistryConflictError:
        pass

__all__ = ["LLMNode", "OpenRouterNode", "PipelineNode", "PythonScriptNode"]
