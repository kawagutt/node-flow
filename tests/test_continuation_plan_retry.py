"""Continuation plan retry must not accumulate failed continuation in plan.md."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

from nodeflow.workflows.dev_process.artifact_versions import (
    load_versioned_plan,
    restore_plan_latest_from_version,
    restore_plan_version_pointer,
    write_versioned_plan,
)
from nodeflow.workflows.dev_process.contract_check import enter_continuation_planning_mode
from nodeflow.workflows.dev_process.phase_loop import load_plan_data
from nodeflow.workflows.dev_process.plan_phases import (
    PlanData,
    PlanPhase,
    parse_new_plan,
    save_plan_json,
)
from nodeflow.workflows.dev_process.stages.plan import run_plan_stage
from tests.test_plan_phases import _make_phase_md


def _phase(index: int, *, title: str = "T") -> PlanPhase:
    return PlanPhase(
        index=index,
        id=f"phase_{index:03d}",
        title=title,
        goal="g",
        scope_include=["s"],
        scope_exclude=[],
        test_plan=["t"],
        review_targets=["implementation_phase"],
        review_agents=["architecture"],
        review_checklist=["c"],
        acceptance_criteria=["a"],
        contract_sha256=f"sha_{index}",
        source_heading=f"## Phase {index + 1}: {title}",
    )


def _base_plan_three_phases() -> PlanData:
    raw = (
        _make_phase_md(1, title="One")
        + "\n"
        + _make_phase_md(2, title="Two")
        + "\n"
        + _make_phase_md(3, title="Three")
    )
    parsed = parse_new_plan(raw)
    return PlanData(
        phases=parsed.phases,
        raw_text=raw,
        plan_sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )


def _dp_three_completed() -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for i in range(3):
        pid = f"phase_{i:03d}"
        results[pid] = {"status": "completed", "contract_sha256": f"sha_{i}", "title": f"T{i}"}
    return {
        "total_phases": 3,
        "phase_index": 3,
        "current_plan_version": "plan_v00_00",
        "phase_results": results,
    }


@patch("nodeflow.workflows.dev_process.stages.plan._run_plan_generation")
def test_continuation_retry_uses_pinned_base_not_failed_latest(mock_gen, tmp_path: Path) -> None:
    """Attempt 2 merges from accepted version, not plan.md polluted by attempt 1."""
    artifact = tmp_path / "run"
    artifact.mkdir()
    base = _base_plan_three_phases()
    dp = _dp_three_completed()
    write_versioned_plan(str(artifact), base, dp)
    base_version = dp["current_plan_version"]
    enter_continuation_planning_mode(dp, findings=[{"description": "gap"}], completed_count=3)

    failed_cont = _make_phase_md(1, title="FAILED ONLY")
    failed_merged = (
        base.raw_text.rstrip() + "\n\n---\n\n## Continuation plan\n\n" + failed_cont.strip()
    )
    plan_dir = artifact / "plan"
    polluted = PlanData(
        phases=base.phases,
        raw_text=failed_merged,
        plan_sha256=hashlib.sha256(failed_merged.encode()).hexdigest(),
    )
    (plan_dir / "plan.md").write_text(failed_merged, encoding="utf-8")
    save_plan_json(polluted, str(plan_dir))

    good_cont = _make_phase_md(1, title="Add validation")
    mock_gen.return_value = (good_cont, None)

    pinned = load_versioned_plan(str(artifact), base_version)
    run_plan_stage(
        repo_root=tmp_path,
        artifact_root=str(artifact),
        run_id="run1",
        task_prompt="task",
        approved_spec="spec",
        continuation_findings=dp["continuation_findings"],
        continuation_start_index=3,
        existing_plan=pinned,
        existing_plan_text=pinned.raw_text,
        body={"dev_process": dp},
    )

    loaded = load_plan_data(str(artifact))
    merged_md = (plan_dir / "plan.md").read_text(encoding="utf-8")
    assert "FAILED ONLY" not in merged_md
    assert len(loaded.phases) == 4
    assert loaded.phases[3].title == "Add validation"
    parsed = parse_new_plan(merged_md)
    assert len(parsed.phases) == len(loaded.phases)
    assert [p.id for p in parsed.phases] == [p.id for p in loaded.phases]


def test_restore_plan_latest_from_version(tmp_path: Path) -> None:
    artifact = tmp_path / "run"
    artifact.mkdir()
    base = _base_plan_three_phases()
    dp = _dp_three_completed()
    write_versioned_plan(str(artifact), base, dp)
    base_version = dp["current_plan_version"]

    plan_dir = artifact / "plan"
    dirty = base.raw_text + "\n\n## Continuation plan\n\n## Phase 4: BAD\n"
    (plan_dir / "plan.md").write_text(dirty, encoding="utf-8")

    restore_plan_latest_from_version(str(artifact), base_version)
    restored = load_plan_data(str(artifact))
    assert restored.plan_sha256 == base.plan_sha256
    assert "Phase 4: BAD" not in (plan_dir / "plan.md").read_text(encoding="utf-8")


def test_restore_plan_version_pointer_resets_current(tmp_path: Path) -> None:
    dp: Dict[str, Any] = {
        "spec_epoch": 0,
        "current_plan_version": "plan_v00_02",
        "artifact_versions": {
            "plan": {
                "current": "plan_v00_02",
                "revision": 2,
                "spec_epoch": 0,
            }
        },
    }
    restore_plan_version_pointer(dp, "plan_v00_00")
    assert dp["current_plan_version"] == "plan_v00_00"
    assert dp["artifact_versions"]["plan"]["current"] == "plan_v00_00"
    assert dp["artifact_versions"]["plan"]["revision"] == 0


def test_enter_continuation_pins_base_once(tmp_path: Path) -> None:
    dp: Dict[str, Any] = {"current_plan_version": "plan_v00_00"}
    enter_continuation_planning_mode(dp, findings=[], completed_count=3)
    assert dp["continuation_base_plan_version"] == "plan_v00_00"
    dp["current_plan_version"] = "plan_v00_01"
    enter_continuation_planning_mode(dp, findings=[], completed_count=3)
    assert dp["continuation_base_plan_version"] == "plan_v00_00"
