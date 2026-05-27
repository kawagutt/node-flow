"""Review contract parsing and aggregate reviews."""

from __future__ import annotations

import json

from nodeflow.workflows.development_flow.review.node_review import (
    AggregateReviewsNode,
    parse_review_contract_from_execution_output,
    validate_review_contract_payload,
)


def test_parse_review_contract_braces_inside_json_string() -> None:
    payload = {
        "ok": True,
        "blocking_findings": [],
        "non_blocking_findings": [],
        "spec_revision_needed": False,
        "summary": "The returned dict {'ok': true} is noted.",
    }
    text = json.dumps(payload)
    er = {"ok": True, "stdout": text, "stderr": None, "raw_output": {}}
    parsed_ok, out = parse_review_contract_from_execution_output(er)
    assert parsed_ok
    assert out.get("summary") == payload["summary"]


def test_validate_review_contract_payload_requires_schema() -> None:
    assert validate_review_contract_payload(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    assert not validate_review_contract_payload({"summary": "looks good"})


def test_aggregate_reviews_blocks_when_diff_collect_failed() -> None:
    node = AggregateReviewsNode()
    valid = json.dumps(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    er = {"ok": True, "stdout": valid, "stderr": "", "raw_output": {}}
    out = node.execute(
        {
            "review_diff": er,
            "review_spec": er,
            "test_result": {"ok": True},
            "diff_result": {"ok": False, "diff": "", "untracked_files": []},
        },
        {},
    )
    rr = out["review_result"]
    assert rr["ok"] is False
    assert any(b.get("id") == "R_DIFF_COLLECT" for b in rr["blocking_findings"])
    assert rr["decision"] == "rework"
    assert rr["suggested_next_action"] == "rework"


def test_aggregate_reviews_schema_parse_failure_blocks() -> None:
    node = AggregateReviewsNode()
    bad = json.dumps({"summary": "looks good"})
    er_bad = {"ok": True, "stdout": bad, "stderr": "", "raw_output": {}}
    valid = json.dumps(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    er_ok = {"ok": True, "stdout": valid, "stderr": "", "raw_output": {}}
    out = node.execute(
        {
            "review_diff": er_bad,
            "review_spec": er_ok,
            "test_result": {"ok": True},
            "diff_result": {"ok": True, "diff": "x", "untracked_files": []},
        },
        {},
    )
    rr = out["review_result"]
    assert rr["ok"] is False
    assert any(b.get("id") == "R_REVIEW_DIFF_PARSE" for b in rr["blocking_findings"])


def test_aggregate_reviews_v1_review_nodes() -> None:
    node = AggregateReviewsNode()
    valid = json.dumps(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    er = {"ok": True, "stdout": valid, "stderr": "", "raw_output": {}}
    out = node.execute(
        {
            "review_architecture": er,
            "review_test_quality": er,
            "test_result": {"ok": True},
            "diff_result": {"ok": True, "diff": "x", "untracked_files": []},
        },
        {
            "expected_review_keys": [
                "review_architecture",
                "review_test_quality",
            ],
        },
    )
    assert out["review_result"]["ok"] is True


def test_aggregate_reviews_missing_review_input_blocks() -> None:
    node = AggregateReviewsNode()
    valid = json.dumps(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    er_ok = {"ok": True, "stdout": valid, "stderr": "", "raw_output": {}}
    out = node.execute(
        {
            "review_diff": er_ok,
            "review_tests": er_ok,
            "review_spec": er_ok,
            "review_spec_revision": er_ok,
            "test_result": {"ok": True},
            "diff_result": {"ok": True, "diff": "x", "untracked_files": []},
        },
        {},
    )
    rr = out["review_result"]
    assert rr["ok"] is False
    assert any(b.get("id") == "R_REVIEW_WIDE_MISSING" for b in rr["blocking_findings"])
