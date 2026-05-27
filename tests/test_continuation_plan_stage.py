"""Tests for continuation plan merge in run_plan_stage."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

from nodeflow.workflows.dev_process.phase_loop import get_current_phase_context, load_plan_data
from nodeflow.workflows.dev_process.plan_phases import PlanData, PlanPhase, save_plan_json
from nodeflow.workflows.dev_process.stages.plan import run_plan_stage
from tests.test_plan_phases import _make_phase_md


def _phase(index: int, contract: str = "", title: str = "T") -> PlanPhase:
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
        contract_sha256=contract or f"sha_{index}",
        source_heading=f"## Phase {index + 1}: {title}",
    )


def _existing_plan_three_phases() -> PlanData:
    import hashlib

    from nodeflow.workflows.dev_process.plan_phases import parse_new_plan

    raw = _make_phase_md(1) + "\n" + _make_phase_md(2) + "\n" + _make_phase_md(3)
    parsed = parse_new_plan(raw)
    return PlanData(
        phases=parsed.phases,
        raw_text=raw,
        plan_sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )


def _dp_after_three_completed() -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for i in range(3):
        pid = f"phase_{i:03d}"
        results[pid] = {"status": "completed", "contract_sha256": f"sha_{i}", "title": f"T{i}"}
    return {
        "total_phases": 3,
        "phase_index": 3,
        "current_phase_id": "",
        "phase_results": results,
    }


@patch("nodeflow.workflows.dev_process.stages.plan._run_plan_generation")
def test_continuation_writes_merged_plan_json(mock_gen, tmp_path: Path) -> None:
    cont_md = (
        _make_phase_md(1, title="Add validation") + "\n" + _make_phase_md(2, title="More tests")
    )
    mock_gen.return_value = (cont_md, None)

    artifact = tmp_path / "run"
    artifact.mkdir()
    existing = _existing_plan_three_phases()
    plan_dir = artifact / "plan"
    plan_dir.mkdir()
    save_plan_json(existing, str(plan_dir))
    (plan_dir / "plan.md").write_text(existing.raw_text, encoding="utf-8")

    run_plan_stage(
        repo_root=tmp_path,
        artifact_root=str(artifact),
        run_id="run1",
        task_prompt="task",
        approved_spec="spec",
        continuation_findings=[{"description": "missing validation"}],
        continuation_start_index=3,
        existing_plan=existing,
        existing_plan_text=existing.raw_text,
        body={},
    )

    loaded = load_plan_data(str(artifact))
    assert len(loaded.phases) == 5
    assert [p.id for p in loaded.phases] == [
        "phase_000",
        "phase_001",
        "phase_002",
        "phase_003",
        "phase_004",
    ]
    merged_md = (artifact / "plan" / "plan.md").read_text(encoding="utf-8")
    assert "Continuation plan" in merged_md
    assert _make_phase_md(1, title="Add feature X")[:20] in merged_md or "Phase 1" in merged_md
    cont_files = list((artifact / "plan" / "continuations").glob("continuation_*.json"))
    assert len(cont_files) == 1

    dp = _dp_after_three_completed()
    dp["phase_index"] = 3
    dp["current_phase_id"] = "phase_003"
    ctx = get_current_phase_context(dp, loaded)
    assert ctx is not None
    assert ctx["phase_id"] == "phase_003"
