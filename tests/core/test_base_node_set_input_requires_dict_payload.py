from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext


class _EmitNode(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"result": {}}


def test_set_input_requires_dict_payload() -> None:
    node = _EmitNode()
    with pytest.raises(TypeError, match="payload must be dict"):
        node.set_input("request", "not-a-dict")
