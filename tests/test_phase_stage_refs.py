"""Tests for checkpoint-safe phase stage references."""

from __future__ import annotations

from nodeflow.workflows.dev_process.phase_stage_refs import compact_stage_ref


def test_compact_stage_ref_drops_large_blobs() -> None:
    ref = compact_stage_ref(
        {
            "status": "completed",
            "diff_result": {"diff": "x" * 50000},
            "execution_output": {"stdout": "y" * 10000},
            "review_result": {
                "decision": "ok",
                "blocking_findings": [{"id": "f1"}],
            },
            "summary_artifact": "/run/plan/summary.md",
        }
    )
    assert ref["status"] == "completed"
    assert "diff_result" not in ref
    assert "execution_output" not in ref
    assert ref["summary_artifact"] == "/run/plan/summary.md"
    assert ref["review_summary"]["blocking_count"] == 1


def test_compact_stage_ref_empty_input() -> None:
    assert compact_stage_ref(None) == {}
    assert compact_stage_ref({}) == {}
