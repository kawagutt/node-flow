from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.pipe_spec import (
    NodeSpec,
    PipeDeclaration,
    PipeSpec,
    validate_executable_pipe_spec,
)
from nodeflow.core.source_ref import SourceRef


class _PassNode(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


def test_validate_accepts_shared_source_for_child_input_and_pipe_output() -> None:
    src = _PassNode()
    sink = _PassNode()
    shared = SourceRef(kind="node", node_id="src", port_name="out")
    spec = PipeSpec(
        graph_node_order=("src", "sink"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"request"}),
            output_sources={"pout": shared},
        ),
        nodes={
            "src": NodeSpec(
                node_id="src",
                node=src,
                input_sources={"in": SourceRef(kind="input", port_name="request")},
                output_ports=frozenset({"out"}),
            ),
            "sink": NodeSpec(
                node_id="sink",
                node=sink,
                input_sources={"in": shared},
                output_ports=frozenset({"out"}),
            ),
        },
    )
    validate_executable_pipe_spec(spec)
