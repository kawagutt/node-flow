from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.pipe_spec import NodeSpec, PipeDeclaration, PipeSpec
from nodeflow.core.runner import Runner
from nodeflow.core.source_ref import SourceRef


class _ForwardNode(BaseNode):
    def __init__(self) -> None:
        super().__init__()
        self.last_observation = None

    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        if "input" not in inputs:
            return {}
        return {"result": {"value": inputs["input"]["value"]}}

    def execute(self, inputs, params):
        out = super().execute(inputs, params)
        self.last_observation = out
        return out


def test_from_pipe_spec_delivers_to_pipe_output_buffer() -> None:
    a = _ForwardNode()
    b = _ForwardNode()
    spec = PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"request"}),
            output_sources={"pout": SourceRef(kind="node", node_id="b", port_name="result")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"input": SourceRef(kind="input", port_name="request")},
                output_ports=frozenset({"result"}),
            ),
            "b": NodeSpec(
                node_id="b",
                node=b,
                input_sources={"input": SourceRef(kind="node", node_id="a", port_name="result")},
                output_ports=frozenset({"result"}),
            ),
        },
    )
    runner = Runner.from_pipe_spec(spec, pipe_inputs={"request": {"value": 7}})
    assert not runner.all_pipe_outputs_filled()
    assert runner.step() is True
    assert runner.step() is True
    assert b.last_observation is not None
    assert b.last_observation["result"] == {"value": 7}
    assert runner.step() is True
    assert runner.all_pipe_outputs_filled()
    assert runner.filled_pipe_outputs() == {"pout": {"value": 7}}


def test_from_pipe_spec_accepts_full_node_params_override() -> None:
    a = _ForwardNode()
    b = _ForwardNode()
    spec = PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"request"}),
            output_sources={"pout": SourceRef(kind="node", node_id="b", port_name="result")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"input": SourceRef(kind="input", port_name="request")},
                output_ports=frozenset({"result"}),
                params={"marker": 0},
            ),
            "b": NodeSpec(
                node_id="b",
                node=b,
                input_sources={"input": SourceRef(kind="node", node_id="a", port_name="result")},
                output_ports=frozenset({"result"}),
                params={},
            ),
        },
    )
    override = {
        "a": {"marker": 1, "_workspace_dir": "/tmp/ws"},
        "b": {"_workspace_dir": "/tmp/ws"},
    }
    runner = Runner.from_pipe_spec(
        spec, pipe_inputs={"request": {"value": 7}}, node_params=override
    )
    assert runner.node_params["a"]["marker"] == 1
    assert runner.node_params["a"]["_workspace_dir"] == "/tmp/ws"
    assert runner.node_params["b"]["_workspace_dir"] == "/tmp/ws"


def test_from_pipe_spec_node_params_keys_must_match_spec_nodes() -> None:
    a = _ForwardNode()
    spec = PipeSpec(
        graph_node_order=("a",),
        pipe=PipeDeclaration(
            input_ports=frozenset({"request"}),
            output_sources={"pout": SourceRef(kind="node", node_id="a", port_name="result")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"input": SourceRef(kind="input", port_name="request")},
                output_ports=frozenset({"result"}),
            ),
        },
    )
    with pytest.raises(ValueError, match="node_params"):
        Runner.from_pipe_spec(spec, node_params={"b": {}})
