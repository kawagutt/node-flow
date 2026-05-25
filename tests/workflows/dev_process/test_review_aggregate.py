"""Stage review JSON contract helpers."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.stages.review_aggregate import (
    REVIEW_JSON_OUTPUT_CONTRACT,
    aggregate_stage_review,
    append_review_json_contract,
)


def test_append_review_json_contract_includes_schema() -> None:
    out = append_review_json_contract("Review this spec.")
    assert "Review this spec." in out
    assert REVIEW_JSON_OUTPUT_CONTRACT in out
    assert '"ok": true | false' in out


def test_aggregate_stage_review_rejects_non_boolean_ok() -> None:
    with pytest.raises(NodeExecutionFailure, match="'ok' must be boolean"):
        aggregate_stage_review(
            {"stdout": '{"ok": "false", "blocking_findings": []}'},
            stage="spec_review",
        )


def test_aggregate_stage_review_ok_false_with_blocking() -> None:
    agg = aggregate_stage_review(
        {
            "stdout": (
                '{"ok": false, "blocking_findings": [{"id": "S1", "summary": "gap"}], '
                '"non_blocking_findings": []}'
            )
        },
        stage="spec_review",
    )
    assert agg["decision"] == "fail"
    assert agg["blocking_count"] == 1
