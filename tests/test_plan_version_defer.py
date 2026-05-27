"""Tests for deferred plan version commit on contract validation failure."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.artifact_versions import (
    PLAN_VERSION_STATUS_COMMITTED,
    PLAN_VERSION_STATUS_DRAFT_NOT_COMMITTED,
    clear_plan_draft_pending_contract_validation,
    commit_plan_version,
    mark_plan_draft_pending_contract_validation,
    write_plan_latest_only,
    write_versioned_plan,
)
from nodeflow.workflows.dev_process.contract_check import validate_rework_contracts
from nodeflow.workflows.dev_process.plan_phases import PlanData, PlanPhase, save_plan_json
from tests.test_plan_phases import _make_phase_md


def _phase(index: int, contract: str = "") -> PlanPhase:
    return PlanPhase(
        index=index,
        id=f"phase_{index:03d}",
        title="T",
        goal="g",
        scope_include=["s"],
        scope_exclude=[],
        test_plan=["t"],
        review_targets=["implementation_phase"],
        review_agents=["architecture"],
        review_checklist=["c"],
        acceptance_criteria=["a"],
        contract_sha256=contract or f"sha_{index}",
        source_heading=f"## Phase {index + 1}",
    )


def test_mark_and_clear_draft_pending_flags() -> None:
    dp: dict = {"current_plan_version": "plan_v00_00"}
    plan_st: dict = {"plan_version": "plan_v00_00"}

    mark_plan_draft_pending_contract_validation(dp, plan_st)
    assert dp["draft_plan_pending_contract_validation"] is True
    assert dp["plan_version_status"] == PLAN_VERSION_STATUS_DRAFT_NOT_COMMITTED
    assert plan_st["plan_version_deferred"] is True
    assert plan_st["plan_version_status"] == PLAN_VERSION_STATUS_DRAFT_NOT_COMMITTED
    assert plan_st["accepted_plan_version"] == "plan_v00_00"

    clear_plan_draft_pending_contract_validation(dp, plan_st)
    assert "draft_plan_pending_contract_validation" not in dp
    assert dp["plan_version_status"] == PLAN_VERSION_STATUS_COMMITTED
    assert "plan_version_deferred" not in plan_st


def test_deferred_commit_keeps_version_until_validation(tmp_path: Path) -> None:
    dp: dict = {
        "spec_epoch": 0,
        "total_phases": 2,
        "phase_results": {
            "phase_000": {"status": "completed", "contract_sha256": "sha_0"},
            "phase_001": {"status": "pending", "contract_sha256": "sha_1"},
        },
    }
    accepted = PlanData(
        phases=[_phase(0, "sha_0"), _phase(1, "sha_1")],
        raw_text="accepted",
        plan_sha256="a",
    )
    write_versioned_plan(str(tmp_path), accepted, dp)
    assert dp["current_plan_version"] == "plan_v00_00"

    rejected = PlanData(
        phases=[_phase(0, "CHANGED"), _phase(1, "sha_1")],
        raw_text="rejected",
        plan_sha256="r",
    )
    write_plan_latest_only(str(tmp_path), rejected)
    assert dp["current_plan_version"] == "plan_v00_00"

    with pytest.raises(NodeExecutionFailure, match="changed contract"):
        validate_rework_contracts(rejected, dp)

    assert dp["current_plan_version"] == "plan_v00_00"

    fixed = PlanData(
        phases=[_phase(0, "sha_0"), _phase(1, "new_1")],
        raw_text="fixed",
        plan_sha256="f",
    )
    validate_rework_contracts(fixed, dp)
    commit_plan_version(str(tmp_path), fixed, dp)
    assert dp["current_plan_version"] == "plan_v00_01"


@patch("nodeflow.workflows.dev_process.stages.plan._run_plan_generation")
def test_continuation_merged_md_no_duplicate_phase_one(mock_gen, tmp_path: Path) -> None:
    from nodeflow.workflows.dev_process.stages.plan import run_plan_stage

    existing_md = _make_phase_md(1) + "\n" + _make_phase_md(2) + "\n" + _make_phase_md(3)
    existing = PlanData(
        phases=[_phase(0), _phase(1), _phase(2)],
        raw_text=existing_md,
        plan_sha256="e",
    )
    cont_md = _make_phase_md(1, title="Add validation")
    mock_gen.return_value = (cont_md, None)

    artifact = tmp_path / "run"
    plan_dir = artifact / "plan"
    plan_dir.mkdir(parents=True)
    save_plan_json(existing, str(plan_dir))
    (plan_dir / "plan.md").write_text(existing_md, encoding="utf-8")

    dp: dict = {"spec_epoch": 0, "artifact_versions": {"plan": {"spec_epoch": 0, "revision": 0}}}
    body = {"dev_process": dp}
    run_plan_stage(
        repo_root=tmp_path,
        artifact_root=str(artifact),
        run_id="r1",
        task_prompt="t",
        approved_spec="s",
        continuation_findings=[],
        continuation_start_index=3,
        existing_plan=existing,
        existing_plan_text=existing_md,
        body=body,
    )
    merged = (artifact / "plan" / "plan.md").read_text(encoding="utf-8")
    assert merged.count("## Phase 1:") == 1
    assert "## Phase 4: Add validation" in merged
