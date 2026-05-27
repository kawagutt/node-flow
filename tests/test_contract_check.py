"""Tests for contract_check: plan rework contract protection."""

from __future__ import annotations

from typing import Any, Dict

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.contract_check import (
    apply_continuation_plan_update,
    apply_rework_plan_update,
    count_completed_prefix,
    get_completed_phase_info,
    merge_continuation_plan,
    validate_rework_contracts,
)
from nodeflow.workflows.dev_process.phase_loop import (
    clear_phase_run_state,
    continuation_plan_from_merged,
)
from nodeflow.workflows.dev_process.plan_phases import PlanData, PlanPhase


def _phase(index: int, contract: str = "", title: str = "T") -> PlanPhase:
    return PlanPhase(
        index=index,
        id=f"phase_{index:03d}",
        title=title,
        goal="g",
        scope_include=[],
        scope_exclude=[],
        test_plan=[],
        review_targets=["implementation_phase"],
        review_agents=["architecture"],
        review_checklist=[],
        acceptance_criteria=[],
        contract_sha256=contract or f"sha_{index}",
        source_heading=f"## Phase {index}: {title}",
    )


def _plan(*phases: PlanPhase) -> PlanData:
    return PlanData(phases=list(phases), raw_text="", plan_sha256="test")


def _dp_with_completed(completed_count: int, total: int = 3) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for i in range(total):
        pid = f"phase_{i:03d}"
        if i < completed_count:
            results[pid] = {"status": "completed", "contract_sha256": f"sha_{i}", "title": f"T{i}"}
        else:
            results[pid] = {"status": "pending", "contract_sha256": f"sha_{i}", "title": f"T{i}"}
    return {
        "total_phases": total,
        "phase_index": completed_count,
        "current_phase_id": f"phase_{completed_count:03d}" if completed_count < total else "",
        "phase_results": results,
    }


class TestValidateReworkContracts:
    def test_no_completed_passes(self) -> None:
        dp = _dp_with_completed(0, total=2)
        new_plan = _plan(_phase(0, "new_sha"), _phase(1, "new_sha2"))
        validate_rework_contracts(new_plan, dp)

    def test_completed_unchanged_passes(self) -> None:
        dp = _dp_with_completed(1, total=3)
        new_plan = _plan(_phase(0, "sha_0"), _phase(1, "new"), _phase(2, "new2"))
        validate_rework_contracts(new_plan, dp)

    def test_completed_contract_changed_raises(self) -> None:
        dp = _dp_with_completed(1, total=3)
        new_plan = _plan(_phase(0, "CHANGED"), _phase(1), _phase(2))
        with pytest.raises(NodeExecutionFailure, match="changed contract"):
            validate_rework_contracts(new_plan, dp)

    def test_completed_removed_raises(self) -> None:
        dp = _dp_with_completed(2, total=3)
        new_plan = _plan(_phase(0, "sha_0"))
        with pytest.raises(NodeExecutionFailure, match="removed completed"):
            validate_rework_contracts(new_plan, dp)

    def test_completed_reordered_raises(self) -> None:
        dp = _dp_with_completed(2, total=3)
        new_plan = _plan(_phase(1, "sha_1"), _phase(0, "sha_0"), _phase(2))
        with pytest.raises(NodeExecutionFailure, match="reordered"):
            validate_rework_contracts(new_plan, dp)


class TestApplyReworkPlanUpdate:
    def test_clears_stage_refs_on_invalidated_phases(self) -> None:
        dp = _dp_with_completed(1, total=3)
        dp["phase_results"]["phase_001"]["stage_refs"] = {"review": {"status": "completed"}}
        dp["phase_results"]["phase_001"]["lint_fix"] = "passed"
        new_plan = _plan(_phase(0, "sha_0"), _phase(1, "new_1"), _phase(2, "new_2"))
        apply_rework_plan_update(new_plan, dp)
        assert "stage_refs" not in dp["phase_results"]["phase_001"]
        assert "lint_fix" not in dp["phase_results"]["phase_001"]

    def test_preserves_completed_invalidates_rest(self) -> None:
        dp = _dp_with_completed(1, total=3)
        new_plan = _plan(
            _phase(0, "sha_0"), _phase(1, "new_1"), _phase(2, "new_2"), _phase(3, "new_3")
        )
        apply_rework_plan_update(new_plan, dp)
        assert dp["total_phases"] == 4
        assert dp["phase_results"]["phase_000"]["status"] == "completed"
        assert dp["phase_results"]["phase_001"]["status"] == "pending"
        assert dp["phase_results"]["phase_002"]["status"] == "pending"
        assert dp["phase_results"]["phase_003"]["status"] == "pending"
        assert dp["phase_index"] == 1
        assert dp["current_phase_id"] == "phase_001"

    def test_removes_stale_phases(self) -> None:
        dp = _dp_with_completed(1, total=3)
        new_plan = _plan(_phase(0, "sha_0"), _phase(1, "new"))
        apply_rework_plan_update(new_plan, dp)
        assert dp["total_phases"] == 2
        assert "phase_002" not in dp["phase_results"]

    def test_resets_loop_counters_for_invalidated_phases(self) -> None:
        dp = _dp_with_completed(1, total=3)
        dp["loop_counters"] = {
            "phase_001_implementation_rework": 3,
            "phase_001_test_rework": 2,
            "phase_002_implementation_rework": 1,
        }
        new_plan = _plan(_phase(0, "sha_0"), _phase(1, "new_1"), _phase(2, "new_2"))
        apply_rework_plan_update(new_plan, dp)
        counters = dp.get("loop_counters", {})
        assert counters.get("phase_001_implementation_rework") is None
        assert counters.get("phase_001_test_rework") is None
        assert counters.get("phase_002_implementation_rework") is None

    def test_resets_loop_counters_for_stale_phases(self) -> None:
        dp = _dp_with_completed(1, total=3)
        dp["loop_counters"] = {
            "phase_002_implementation_rework": 4,
            "phase_002_test_rework": 1,
        }
        new_plan = _plan(_phase(0, "sha_0"), _phase(1, "new"))
        apply_rework_plan_update(new_plan, dp)
        counters = dp.get("loop_counters", {})
        assert counters.get("phase_002_implementation_rework") is None
        assert counters.get("phase_002_test_rework") is None

    def test_clears_final_rework_flags(self) -> None:
        dp = _dp_with_completed(1, total=3)
        dp["final_rework_required"] = {"owner": "implementation"}
        dp["final_synthesis"] = {"result": "fail"}
        new_plan = _plan(_phase(0, "sha_0"), _phase(1, "new_1"), _phase(2, "new_2"))
        apply_rework_plan_update(new_plan, dp)
        assert "final_rework_required" not in dp
        assert "final_synthesis" not in dp


class TestGetCompletedPhaseInfo:
    def test_returns_completed(self) -> None:
        dp = _dp_with_completed(2, total=3)
        info = get_completed_phase_info(dp)
        assert len(info) == 2
        assert info[0]["id"] == "phase_000"
        assert info[1]["id"] == "phase_001"

    def test_empty_when_none_completed(self) -> None:
        dp = _dp_with_completed(0, total=3)
        assert get_completed_phase_info(dp) == []


class TestContinuationPlanFromMerged:
    def test_slices_new_phases_from_merged_executable_plan(self) -> None:
        existing = _plan(_phase(0, "sha_0"), _phase(1, "sha_1"), _phase(2, "sha_2"))
        cont = _plan(_phase(3, "cont_3"), _phase(4, "cont_4"))
        merged = merge_continuation_plan(existing, cont, completed_count=3)
        sliced = continuation_plan_from_merged(merged, 3)
        assert [p.id for p in sliced.phases] == ["phase_003", "phase_004"]
        assert sliced.phases[0].contract_sha256 == "cont_3"


class TestClearPhaseRunState:
    def test_clears_spec_and_plan_version_stamps(self) -> None:
        pr: Dict[str, Any] = {
            "spec_version": "spec_v00_00",
            "plan_version": "plan_v00_01",
            "implementation": {"status": "completed"},
        }
        clear_phase_run_state(pr)
        assert "spec_version" not in pr
        assert "plan_version" not in pr
        assert "implementation" not in pr


class TestMergeContinuationPlan:
    def test_merges_completed_prefix_and_continuation(self) -> None:
        existing = _plan(_phase(0, "sha_0"), _phase(1, "sha_1"), _phase(2, "sha_2"))
        existing = PlanData(
            phases=existing.phases,
            raw_text="## Phase 1: A\n\noriginal",
            plan_sha256="orig",
        )
        cont = _plan(_phase(3, "cont_3"), _phase(4, "cont_4"))
        cont = PlanData(
            phases=cont.phases, raw_text="## Phase 1: C\n\ncontinuation", plan_sha256="c"
        )
        merged = merge_continuation_plan(existing, cont, completed_count=3)
        assert len(merged.phases) == 5
        assert [p.id for p in merged.phases] == [
            "phase_000",
            "phase_001",
            "phase_002",
            "phase_003",
            "phase_004",
        ]
        assert "Continuation plan" in merged.raw_text
        assert "original" in merged.raw_text
        assert "continuation" in merged.raw_text


class TestApplyContinuationPlanUpdate:
    def test_appends_phases_after_completed(self) -> None:
        dp = _dp_with_completed(3, total=3)
        cont = _plan(_phase(3, "cont_sha_3", "Add validation"), _phase(4, "cont_sha_4", "Tests"))
        apply_continuation_plan_update(cont, dp)
        assert dp["total_phases"] == 5
        assert dp["phase_index"] == 3
        assert dp["current_phase_id"] == "phase_003"
        assert dp["phase_results"]["phase_003"]["status"] == "pending"
        assert dp["phase_results"]["phase_003"]["contract_sha256"] == "cont_sha_3"
        assert dp["phase_results"]["phase_004"]["status"] == "pending"
        assert dp["phase_results"]["phase_004"]["contract_sha256"] == "cont_sha_4"

    def test_preserves_completed_phases(self) -> None:
        dp = _dp_with_completed(3, total=3)
        cont = _plan(_phase(3, "new"))
        apply_continuation_plan_update(cont, dp)
        for i in range(3):
            pid = f"phase_{i:03d}"
            assert dp["phase_results"][pid]["status"] == "completed"
            assert dp["phase_results"][pid]["contract_sha256"] == f"sha_{i}"

    def test_increments_continuation_count(self) -> None:
        dp = _dp_with_completed(2, total=2)
        cont = _plan(_phase(2, "new"))
        apply_continuation_plan_update(cont, dp)
        assert dp["continuation_count"] == 1
        dp["phase_results"]["phase_002"]["status"] = "completed"
        cont2 = _plan(_phase(3, "new2"))
        apply_continuation_plan_update(cont2, dp)
        assert dp["continuation_count"] == 2

    def test_clears_final_rework_flags(self) -> None:
        dp = _dp_with_completed(2, total=2)
        dp["final_rework_required"] = {"owner": "plan"}
        dp["final_synthesis"] = {"result": "fail"}
        cont = _plan(_phase(2, "new"))
        apply_continuation_plan_update(cont, dp)
        assert "final_rework_required" not in dp
        assert "final_synthesis" not in dp

    def test_no_git_reset_happens(self) -> None:
        """apply_continuation_plan_update does not touch git at all."""
        dp = _dp_with_completed(2, total=2)
        dp["task_branch"] = {"name": "test-branch", "base_ref": "abc123", "created": True}
        cont = _plan(_phase(2, "new"))
        apply_continuation_plan_update(cont, dp)
        assert dp["task_branch"]["base_ref"] == "abc123"

    def test_empty_continuation_plan(self) -> None:
        dp = _dp_with_completed(2, total=2)
        cont = _plan()
        apply_continuation_plan_update(cont, dp)
        assert dp["total_phases"] == 2
        assert dp["current_phase_id"] == ""

    def test_rejects_phase_id_mismatch(self) -> None:
        dp = _dp_with_completed(2, total=2)
        bad = _plan(_phase(0, "wrong_id"))  # phase_000 not phase_002
        with pytest.raises(NodeExecutionFailure, match="continuation phase id mismatch"):
            apply_continuation_plan_update(bad, dp)


class TestCountCompletedPrefix:
    def test_all_completed(self) -> None:
        dp = _dp_with_completed(3, total=3)
        assert count_completed_prefix(dp["phase_results"], 3) == 3

    def test_partial(self) -> None:
        dp = _dp_with_completed(1, total=3)
        assert count_completed_prefix(dp["phase_results"], 3) == 1

    def test_none(self) -> None:
        dp = _dp_with_completed(0, total=3)
        assert count_completed_prefix(dp["phase_results"], 3) == 0
