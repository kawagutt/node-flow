"""NodeRegistry and built-in registration."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

import pytest

from nodeflow.builtins import register_builtin_nodes
from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.registry import (
    RegistryConflictError,
    UnknownNodeTypeError,
    registry,
)
from nodeflow.nodes.hello_demo import HelloDemoNode


def test_registry_register_resolve():
    cls = registry.resolve("python_route_by_task_type")
    node = cls()
    assert isinstance(node, BaseNode)


def test_registry_resolves_hello_demo() -> None:
    assert registry.resolve("hello_demo") is HelloDemoNode


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


def test_load_pipeline_is_removed(tmp_path) -> None:
    from nodeflow.core.loader import load_pipeline

    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text("x: 1")
    with pytest.raises(NotImplementedError, match="YAML"):
        load_pipeline(str(tmp_path), str(yaml_path))


def test_registry_custom_node_execute(tmp_path):
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
        n = CustomNode()
        out = n.execute({}, {})
        assert out["out"]["value"] == "custom"
    finally:
        registry.unregister("custom")


def test_development_flow_composite_registry_keys_removed() -> None:
    """Legacy path-style development_flow pipes are not registered (rebuild via v1.6 PipeSpec)."""
    keys = [
        "workflows.development_flow",
        "workflows.development_flow.spec_plan",
        "workflows.development_flow.implement",
        "workflows.development_flow.review",
        "workflows.development_flow.start",
        "workflows.development_flow.revise_spec",
        "workflows.development_flow.approve",
        "workflows.development_flow.rework",
        "workflows.development_flow.merge",
    ]
    for key in keys:
        assert registry.get(key) is None, f"expected {key} unregistered"


def test_development_flow_old_registry_keys_are_not_registered() -> None:
    old_keys = [
        "development_flow" + "_pipe",
        "spec_plan" + "_pipe",
        "implement" + "_pipe",
        "review" + "_pipe",
    ]
    for key in old_keys:
        assert registry.get(key) is None


def test_fixed_provider_pipe_registry_keys_are_not_registered() -> None:
    assert registry.get("implement_with_codex") is None
    assert registry.get("review_with_claude") is None
