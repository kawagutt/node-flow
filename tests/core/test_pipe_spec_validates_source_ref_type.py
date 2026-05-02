from __future__ import annotations

from types import MappingProxyType
from typing import Any, cast

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


class _N(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"o": {}}


def test_validate_rejects_non_source_ref_in_input_sources() -> None:
    n = _N()
    spec = PipeSpec(
        graph_node_order=("a",),
        pipe=PipeDeclaration(
            input_ports=frozenset(),
            output_sources={"p": SourceRef(kind="node", node_id="a", port_name="o")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=n,
                input_sources=cast(Any, {"in": "not-a-SourceRef"}),
                output_ports=frozenset({"o"}),
            ),
        },
    )
    with pytest.raises(PipeSpecValidationError, match="must be SourceRef"):
        validate_executable_pipe_spec(spec)


def test_validate_rejects_non_source_ref_in_output_sources() -> None:
    n = _N()
    spec = PipeSpec(
        graph_node_order=("a",),
        pipe=PipeDeclaration(
            input_ports=frozenset(),
            output_sources=cast(Any, {"p": "bad"}),
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=n,
                input_sources={},
                output_ports=frozenset({"o"}),
            ),
        },
    )
    with pytest.raises(PipeSpecValidationError, match="must be SourceRef"):
        validate_executable_pipe_spec(spec)


def test_validate_rejects_node_spec_object_that_is_not_base_node() -> None:
    spec = PipeSpec(
        graph_node_order=("a",),
        pipe=PipeDeclaration(
            input_ports=frozenset(),
            output_sources={"p": SourceRef(kind="node", node_id="a", port_name="o")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=cast(Any, object()),
                input_sources={},
                output_ports=frozenset({"o"}),
            ),
        },
    )
    with pytest.raises(PipeSpecValidationError, match="must be BaseNode"):
        validate_executable_pipe_spec(spec)
