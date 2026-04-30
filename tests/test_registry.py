"""NodeRegistry and built-in registration."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.registry import (
    RegistryConflictError,
    UnknownNodeTypeError,
    registry,
)
from nodeflow.nodes.builtins import register_builtin_nodes


def test_registry_register_resolve():
    cls = registry.resolve("python_route_by_task_type")
    node = cls()
    assert isinstance(node, BaseNode)


def test_registry_unknown_type_raises():
    with pytest.raises(UnknownNodeTypeError) as exc_info:
        registry.resolve("unknown_type_xyz")
    assert exc_info.value.type_name == "unknown_type_xyz"


def test_registry_register_without_override_raises_on_duplicate():
    registry.unregister("_test_dup")
    registry.register("_test_dup", BaseNode, override=True)
    with pytest.raises(RegistryConflictError):
        registry.register("_test_dup", BaseNode, override=False)
    registry.unregister("_test_dup")


def test_registry_get_and_unregister():
    assert registry.get("_test_get") is None
    registry.register("_test_get", BaseNode, override=True)
    assert registry.get("_test_get") is BaseNode
    registry.unregister("_test_get")
    assert registry.get("_test_get") is None


def test_registry_clear():
    try:
        registry.register("_test_clear", BaseNode, override=True)
        assert registry.get("_test_clear") is BaseNode
        registry.clear()
        assert registry.get("_test_clear") is None
        assert registry.get("python_route_by_task_type") is None
    finally:
        register_builtin_nodes()


def test_load_pipeline_fails_when_registry_empty(tmp_path):
    from nodeflow.core.loader import load_pipeline

    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: a
      type: python_route_by_task_type
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
        register_builtin_nodes()


def test_registry_custom_node_register_and_load(tmp_path):
    from nodeflow.core.loader import load_pipeline

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
version: "1.5"
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


def test_loader_unknown_type_raises(tmp_path):
    from nodeflow.core.loader import load_pipeline

    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: x
      type: not_a_registered_type
      inputs: {}
      params: {}
  final: x
"""
    )
    with pytest.raises(UnknownNodeTypeError):
        load_pipeline(str(tmp_path), str(yaml_path))
