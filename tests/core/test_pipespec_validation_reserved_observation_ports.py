from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.pipe_spec import (
    NodeSpec,
    PipeDeclaration,
    PipeSpec,
    PipeSpecValidationError,
    validate_executable_pipe_spec,
)
from nodeflow.core.source_ref import SourceRef


class _PassNode(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


def test_validate_rejects_state_port_as_source_port_name() -> None:
    a = _PassNode()
    b = _PassNode()
    spec = PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"request"}),
            output_sources={"p": SourceRef(kind="node", node_id="b", port_name="out")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"in": SourceRef(kind="input", port_name="request")},
                output_ports=frozenset({"out"}),
            ),
            "b": NodeSpec(
                node_id="b",
                node=b,
                input_sources={"in": SourceRef(kind="node", node_id="a", port_name="_state")},
                output_ports=frozenset({"out"}),
            ),
        },
    )
    with pytest.raises(PipeSpecValidationError, match="invalid port name"):
        validate_executable_pipe_spec(spec)
