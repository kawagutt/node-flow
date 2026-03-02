"""
NodeFlow v1.4.4 — NodeRegistry テスト。
"""

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.registry import (
    RegistryConflictError,
    UnknownNodeTypeError,
    registry,
)
from types import MappingProxyType
from typing import Any, Dict


def test_registry_register_resolve():
    """built-in は extensions で登録済み。resolve で取得できる。"""
    cls = registry.resolve("python_script")
    assert cls is not None
    node = cls()
    assert isinstance(node, BaseNode)


def test_registry_unknown_type_raises():
    """未登録の type で resolve すると UnknownNodeTypeError。"""
    with pytest.raises(UnknownNodeTypeError) as exc_info:
        registry.resolve("unknown_type_xyz")
    assert exc_info.value.type_name == "unknown_type_xyz"


def test_registry_register_without_override_raises_on_duplicate():
    """既存の type に override なしで register すると RegistryConflictError。"""
    registry.unregister("_test_dup")
    # テスト用に強制登録（override=True）。本テストの目的は「重複時の例外」の確認。
    registry.register("_test_dup", BaseNode, override=True)
    with pytest.raises(RegistryConflictError):
        registry.register("_test_dup", BaseNode, override=False)
    registry.unregister("_test_dup")


def test_registry_get_and_unregister():
    """get は未登録で None、unregister で登録を削除できる。"""
    assert registry.get("_test_get") is None
    registry.register("_test_get", BaseNode, override=True)
    assert registry.get("_test_get") is BaseNode
    registry.unregister("_test_get")
    assert registry.get("_test_get") is None


def _restore_builtin_registry():
    """clear 後の他テスト用。built-in を再登録（import は module を再実行しないため明示登録）。"""
    from nodeflow.extensions import (
        LLMNode,
        OpenRouterNode,
        PipelineNode,
        PythonScriptNode,
    )

    registry.register("python_script", PythonScriptNode, override=True)
    registry.register("llm", LLMNode, override=True)
    registry.register("openrouter", OpenRouterNode, override=True)
    registry.register("pipeline", PipelineNode, override=True)


def test_registry_clear():
    """clear で全登録を削除できる。テスト・lifecycle 用。"""
    try:
        registry.register("_test_clear", BaseNode, override=True)
        assert registry.get("_test_clear") is BaseNode
        registry.clear()
        assert registry.get("_test_clear") is None
        assert registry.get("python_script") is None
    finally:
        _restore_builtin_registry()


def test_load_pipeline_fails_when_registry_empty(tmp_path):
    """registry が空のとき load_pipeline は UnknownNodeTypeError（extensions 未登録相当）。"""
    from nodeflow.execution.loader import load_pipeline

    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
version: "1.4"
graph:
  nodes:
    - id: a
      type: python_script
      inputs: {}
      params: {}
  final: a
"""
    )
    registry.clear()
    try:
        with pytest.raises(UnknownNodeTypeError):
            load_pipeline(str(tmp_path), str(yaml_path))
    finally:
        _restore_builtin_registry()


def test_registry_custom_node_register_and_load(tmp_path):
    """カスタム Node を register して loader 経由でパイプラインを組み立てられる。"""
    from nodeflow.execution.loader import load_pipeline

    class CustomNode(BaseNode):
        def run(
            self,
            inputs: Dict[str, Any],
            params: MappingProxyType,
            context: ExecutionContext,
        ) -> Dict[str, Any]:
            return {"out": {"value": "custom"}}

    registry.register("custom", CustomNode)
    try:
        yaml_path = tmp_path / "pipeline.yaml"
        yaml_path.write_text(
            """
version: "1.4"
graph:
  nodes:
    - id: c
      type: custom
      inputs: {}
      params: {}
  final: c
"""
        )
        root = load_pipeline(str(tmp_path), str(yaml_path))
        out = root.execute({}, {})
        assert "out" in out
        assert out["out"]["value"] == "custom"
    finally:
        registry.unregister("custom")


def test_loader_loop_type_raises_not_implemented(tmp_path):
    """type: loop のノードは registry に未登録のため UnknownNodeTypeError → loader が NotImplementedError に変換。"""
    from nodeflow.execution.loader import load_pipeline

    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
version: "1.4"
graph:
  nodes:
    - id: loop_node
      type: loop
      inputs: {}
      params: {}
  final: loop_node
"""
    )
    with pytest.raises(NotImplementedError, match="LoopNode は本版では未サポートです"):
        load_pipeline(str(tmp_path), str(yaml_path))
