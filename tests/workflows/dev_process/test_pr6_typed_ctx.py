"""PR6 contracts: typed FlowCtx boundary with dict serialization."""

from __future__ import annotations

from copy import deepcopy

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import SCHEMA_VERSION
from nodeflow.workflows.dev_process.flow_ctx import FlowCtx, flow_params
from nodeflow.workflows.dev_process.nodes import copy_flow_ctx, make_flow_ctx


def test_flow_ctx_schema_validation() -> None:
    valid = {
        "schema_version": SCHEMA_VERSION,
        "body": {"stages": {}, "node_runs": []},
        "segment": "spec_cycle",
        "params": {"task_prompt": "x"},
    }
    assert FlowCtx.from_dict(valid).to_dict() == valid

    with pytest.raises(NodeExecutionFailure, match="schema_version"):
        FlowCtx.from_dict({**valid, "schema_version": "dev_process.flow.v2"})
    with pytest.raises(NodeExecutionFailure, match="body must be a dict"):
        FlowCtx.from_dict({**valid, "body": []})
    with pytest.raises(NodeExecutionFailure, match="params must be a dict"):
        FlowCtx.from_dict({**valid, "params": []})
    with pytest.raises(NodeExecutionFailure, match="segment must be a string"):
        FlowCtx.from_dict({**valid, "segment": 1})


def test_make_flow_ctx_deepcopies_input_body() -> None:
    body = {"stages": {}, "node_runs": [], "x": {"n": 1}}
    ctx = make_flow_ctx(body, segment="spec_cycle", params={"task_prompt": "abc"})
    ctx["body"]["x"]["n"] = 99
    ctx["params"]["task_prompt"] = "changed"

    assert body["x"]["n"] == 1


def test_copy_flow_ctx_deepcopies_output_and_preserves_checkpoint_shape() -> None:
    raw = make_flow_ctx(
        {"run_context": {"run_id": "r"}, "stages": {}, "node_runs": []},
        segment="phase_step",
        params={"phase_id": "phase_001"},
    )
    snapshot = deepcopy(raw)

    ctx, body = copy_flow_ctx(raw)
    body["stages"]["new"] = {"status": "completed"}
    ctx["params"]["phase_id"] = "phase_999"

    assert raw == snapshot
    assert set(ctx.keys()) == {"schema_version", "body", "segment", "params"}
    assert ctx["schema_version"] == SCHEMA_VERSION


@pytest.mark.parametrize(
    ("segment", "params"),
    [
        ("spec_cycle", {"task_prompt": "x", "previous_spec": "old"}),
        ("plan_cycle", {"task_prompt": "x", "approved_spec": "# Spec"}),
        ("plan_review", {"task_prompt": "x", "approved_spec": "# Spec"}),
        (
            "phase_step",
            {
                "phase_id": "phase_001",
                "approved_spec": "# Spec",
                "approved_plan": "# Plan",
                "base_revision": "abc",
                "review_scope": "phase",
            },
        ),
        (
            "final_review",
            {
                "review_scope": "final",
                "review_agents": ["requirements"],
                "approved_spec": "# Spec",
                "approved_plan": "# Plan",
                "base_revision": "abc",
                "diff_result": {},
            },
        ),
    ],
)
def test_segment_contract_params_shape(segment: str, params: dict[str, object]) -> None:
    ctx = make_flow_ctx(
        {"run_context": {"run_id": "run"}, "stages": {}, "node_runs": []},
        segment=segment,
        params=params,
    )
    got = flow_params(ctx)
    assert isinstance(got, dict)
    for key in params:
        assert key in got
