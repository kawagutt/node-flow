from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.runner import Runner, SourceRef


class _EchoPayloadNode(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"echo": {"payload": inputs["input"]}}


def test_runner_does_not_interpret_payload_fields():
    payload = {
        "ok": False,
        "action": "retry",
        "next": {"step": "manual"},
        "provider": "example",
    }
    node = _EchoPayloadNode()
    runner = Runner(
        graph_node_order=["n1"],
        nodes={"n1": node},
        node_params={"n1": {}},
        node_input_sources={"n1": {"input": SourceRef(kind="input", port_name="request")}},
        pipe_inputs={"request": payload},
    )

    assert runner.step() is True
    out = node.get_output_snapshot()
    assert out["echo"]["payload"] == payload
