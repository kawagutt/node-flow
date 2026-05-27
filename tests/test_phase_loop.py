"""Tests for phase_loop module."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict

from nodeflow.workflows.dev_process.phase_loop import (
    all_phases_completed,
    check_loop_limit,
    complete_phase,
    compute_max_auto_steps,
    get_current_phase_context,
    increment_loop_counter,
    init_phase_state,
    invalidate_phases_from,
    load_plan_data,
    record_phase_start,
    reset_loop_counter,
)
from nodeflow.workflows.dev_process.plan_phases import PlanData, PlanPhase


def _make_phase(index: int, title: str = "Test") -> PlanPhase:
    return PlanPhase(
        index=index,
        id=f"phase_{index:03d}",
        title=title,
        goal=f"Goal {index}",
        scope_include=[f"scope_{index}"],
        scope_exclude=[],
        test_plan=[f"test_{index}"],
        review_targets=["implementation_phase"],
        review_agents=["architecture"],
        review_checklist=[f"check_{index}"],
        acceptance_criteria=[f"criteria_{index}"],
        contract_sha256=f"sha_{index}",
        source_heading=f"## Phase {index}: {title}",
    )


def _make_plan_data(count: int = 3) -> PlanData:
    phases = [_make_phase(i, f"Phase {i}") for i in range(count)]
    return PlanData(phases=phases, raw_text="", plan_sha256="test_sha")


def _init_repo(path: Path) -> Path:
    subprocess.run(["git", "init", str(path)], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "config", "user.email", "test@test.com"],
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(path), "config", "user.name", "Test"],
        capture_output=True,
        check=True,
    )
    (path / "README.md").write_text("init", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "."], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "commit", "-m", "initial"],
        capture_output=True,
        check=True,
    )
    return path


class TestInitPhaseState:
    def test_initializes_all_phases(self) -> None:
        dp: Dict[str, Any] = {}
        plan = _make_plan_data(3)
        init_phase_state(dp, plan)
        assert dp["total_phases"] == 3
        assert dp["phase_index"] == 0
        assert dp["current_phase_id"] == "phase_000"
        assert len(dp["phase_results"]) == 3
        for i in range(3):
            pid = f"phase_{i:03d}"
            assert dp["phase_results"][pid]["status"] == "pending"
            assert dp["phase_results"][pid]["contract_sha256"] == f"sha_{i}"


class TestGetCurrentPhaseContext:
    def test_returns_first_phase(self) -> None:
        dp: Dict[str, Any] = {}
        plan = _make_plan_data(2)
        init_phase_state(dp, plan)
        ctx = get_current_phase_context(dp, plan)
        assert ctx is not None
        assert ctx["phase_id"] == "phase_000"
        assert ctx["phase_goal"] == "Goal 0"

    def test_returns_none_when_all_completed(self) -> None:
        dp: Dict[str, Any] = {}
        plan = _make_plan_data(2)
        init_phase_state(dp, plan)
        dp["phase_index"] = 2
        dp["current_phase_id"] = ""
        assert get_current_phase_context(dp, plan) is None


class TestRecordPhaseStart:
    def test_records_git_ref(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        dp: Dict[str, Any] = {"current_phase_id": "phase_000", "phase_results": {"phase_000": {}}}
        ref = record_phase_start(dp, repo)
        assert ref
        assert dp["phase_results"]["phase_000"]["phase_start_git_ref"] == ref
        assert dp["phase_results"]["phase_000"]["status"] == "in_progress"


class TestCompletePhase:
    def test_completes_and_advances(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        dp: Dict[str, Any] = {
            "total_phases": 3,
            "phase_index": 0,
            "current_phase_id": "phase_000",
            "phase_results": {
                "phase_000": {"status": "in_progress", "title": "First"},
                "phase_001": {"status": "pending", "title": "Second"},
                "phase_002": {"status": "pending", "title": "Third"},
            },
        }
        (repo / "src.py").write_text("x = 1", encoding="utf-8")
        result = complete_phase(dp, repo, artifact_roots=[])
        assert result["actual_commit_created"] is True
        assert dp["phase_index"] == 1
        assert dp["current_phase_id"] == "phase_001"
        assert dp["phase_results"]["phase_000"]["status"] == "completed"

    def test_empty_phase_no_commit(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        dp: Dict[str, Any] = {
            "total_phases": 2,
            "phase_index": 0,
            "current_phase_id": "phase_000",
            "phase_results": {
                "phase_000": {"status": "in_progress", "title": "Empty"},
                "phase_001": {"status": "pending", "title": "Next"},
            },
        }
        result = complete_phase(dp, repo, artifact_roots=[])
        assert result["actual_commit_created"] is False
        assert dp["phase_results"]["phase_000"]["status"] == "completed"


class TestAllPhasesCompleted:
    def test_not_completed(self) -> None:
        dp = {"phase_index": 1, "total_phases": 3}
        assert not all_phases_completed(dp)

    def test_completed(self) -> None:
        dp = {"phase_index": 3, "total_phases": 3}
        assert all_phases_completed(dp)


class TestInvalidatePhases:
    def test_invalidates_from_index(self) -> None:
        dp: Dict[str, Any] = {
            "total_phases": 3,
            "phase_index": 2,
            "current_phase_id": "phase_002",
            "phase_results": {
                "phase_000": {"status": "completed", "phase_commit": "abc"},
                "phase_001": {"status": "completed", "phase_commit": "def"},
                "phase_002": {"status": "in_progress"},
            },
        }
        invalidate_phases_from(dp, 1)
        assert dp["phase_results"]["phase_000"]["status"] == "completed"
        assert dp["phase_results"]["phase_001"]["status"] == "pending"
        assert dp["phase_results"]["phase_002"]["status"] == "pending"
        assert dp["phase_index"] == 1
        assert dp["current_phase_id"] == "phase_001"
        assert "phase_commit" not in dp["phase_results"]["phase_001"]


class TestResetLoopCounter:
    def test_reset_removes_key(self) -> None:
        dp: Dict[str, Any] = {"loop_counters": {"spec_revision": 3}}
        reset_loop_counter(dp, "spec_revision")
        assert "spec_revision" not in dp["loop_counters"]

    def test_reset_noop_when_absent(self) -> None:
        dp: Dict[str, Any] = {"loop_counters": {"other": 1}}
        reset_loop_counter(dp, "spec_revision")
        assert dp["loop_counters"] == {"other": 1}

    def test_reset_noop_when_no_counters(self) -> None:
        dp: Dict[str, Any] = {}
        reset_loop_counter(dp, "spec_revision")
        assert "loop_counters" not in dp

    def test_increment_then_reset_allows_fresh_cycle(self) -> None:
        dp: Dict[str, Any] = {}
        for _ in range(4):
            increment_loop_counter(dp, "plan_revision")
        check_loop_limit(dp, "plan_revision")
        reset_loop_counter(dp, "plan_revision")
        check_loop_limit(dp, "plan_revision")
        for _ in range(4):
            increment_loop_counter(dp, "plan_revision")
        check_loop_limit(dp, "plan_revision")


class TestComputeMaxAutoSteps:
    def test_minimum_30(self) -> None:
        assert compute_max_auto_steps(1) >= 30

    def test_scales_with_phases(self) -> None:
        assert compute_max_auto_steps(10) > compute_max_auto_steps(2)


class TestLoadPlanData:
    def test_loads_saved_plan(self, tmp_path: Path) -> None:
        import hashlib

        from nodeflow.workflows.dev_process.plan_phases import parse_new_plan, save_plan_json
        from tests.test_plan_phases import _make_phase_md

        plan_dir = tmp_path / "plan"
        plan_dir.mkdir()
        raw = _make_phase_md(1, title="Test", goal="Goal")
        parsed = parse_new_plan(raw)
        plan_data = PlanData(
            phases=parsed.phases,
            raw_text=raw,
            plan_sha256=hashlib.sha256(raw.encode()).hexdigest(),
        )
        (plan_dir / "plan.md").write_text(raw, encoding="utf-8")
        save_plan_json(plan_data, str(plan_dir))

        result = load_plan_data(str(tmp_path))
        assert len(result.phases) == 1
        assert result.phases[0].id == "phase_000"
        assert result.plan_sha256 == plan_data.plan_sha256
