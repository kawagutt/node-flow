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


def test_validate_rejects_empty_pipe_output_sources() -> None:
    n = _PassNode()
    spec = PipeSpec(
        graph_node_order=("n",),
        pipe=PipeDeclaration(input_ports=frozenset({"request"}), output_sources={}),
        nodes={
            "n": NodeSpec(
                node_id="n",
                node=n,
                input_sources={"in": SourceRef(kind="input", port_name="request")},
                output_ports=frozenset({"out"}),
            ),
        },
    )
    with pytest.raises(PipeSpecValidationError, match="output_sources must not be empty"):
        validate_executable_pipe_spec(spec)
