"""Tests for artifact_versions: spec/plan versioned snapshots."""

from __future__ import annotations

import hashlib
from pathlib import Path

from nodeflow.workflows.dev_process.artifact_versions import (
    commit_plan_version,
    write_plan_latest_only,
    write_versioned_plan,
    write_versioned_spec,
)
from nodeflow.workflows.dev_process.phase_loop import get_current_phase_context, load_plan_data
from nodeflow.workflows.dev_process.plan_phases import PlanData, PlanPhase, parse_new_plan
from tests.test_plan_phases import _make_phase_md


def _plan_from_phase_count(count: int, *, raw_suffix: str = "") -> PlanData:
    parts = [_make_phase_md(i + 1, title=f"P{i + 1}") for i in range(count)]
    raw = "\n\n".join(parts) + raw_suffix
    parsed = parse_new_plan(raw)
    return PlanData(
        phases=parsed.phases,
        raw_text=raw,
        plan_sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )


def _phase(index: int) -> PlanPhase:
    return _plan_from_phase_count(index + 1).phases[index]


class TestDeferredPlanVersion:
    def test_write_plan_latest_only_does_not_bump_version(self, tmp_path: Path) -> None:
        dp: dict = {
            "spec_epoch": 0,
            "current_plan_version": "plan_v00_00",
            "artifact_versions": {"plan": {"spec_epoch": 0, "revision": 0}},
        }
        plan = _plan_from_phase_count(1)
        write_plan_latest_only(str(tmp_path), plan)
        assert dp["current_plan_version"] == "plan_v00_00"
        commit_plan_version(str(tmp_path), plan, dp)
        assert dp["current_plan_version"] == "plan_v00_01"


class TestSpecVersions:
    def test_first_spec_is_v00_00(self, tmp_path: Path) -> None:
        dp: dict = {}
        write_versioned_spec(str(tmp_path), "spec body", dp, epoch_bump=False)
        assert dp["current_spec_version"] == "spec_v00_00"
        assert (tmp_path / "spec" / "spec.md").read_text(encoding="utf-8") == "spec body"
        assert (tmp_path / "spec" / "versions" / "spec_v00_00.md").is_file()

    def test_spec_review_revision_increments_yy(self, tmp_path: Path) -> None:
        dp: dict = {}
        write_versioned_spec(str(tmp_path), "v0", dp, epoch_bump=False)
        write_versioned_spec(str(tmp_path), "v1", dp, epoch_bump=False)
        assert dp["current_spec_version"] == "spec_v00_01"
        assert (tmp_path / "spec" / "versions" / "spec_v00_01.md").read_text(
            encoding="utf-8"
        ) == "v1"

    def test_spec_epoch_bump_resets_yy(self, tmp_path: Path) -> None:
        dp: dict = {}
        write_versioned_spec(str(tmp_path), "v0", dp, epoch_bump=False)
        write_versioned_spec(str(tmp_path), "v1 epoch", dp, epoch_bump=True)
        assert dp["spec_epoch"] == 1
        assert dp["current_spec_version"] == "spec_v01_00"


class TestPlanVersions:
    def test_plan_xx_matches_spec_epoch(self, tmp_path: Path) -> None:
        dp: dict = {"spec_epoch": 1}
        plan = _plan_from_phase_count(1)
        write_versioned_plan(str(tmp_path), plan, dp)
        assert dp["current_plan_version"] == "plan_v01_00"
        assert (tmp_path / "plan" / "versions" / "plan_v01_00.json").is_file()

    def test_plan_yy_increments_within_epoch(self, tmp_path: Path) -> None:
        dp: dict = {"spec_epoch": 0}
        plan = _plan_from_phase_count(1)
        write_versioned_plan(str(tmp_path), plan, dp)
        plan2 = _plan_from_phase_count(2)
        write_versioned_plan(str(tmp_path), plan2, dp)
        assert dp["current_plan_version"] == "plan_v00_01"
        loaded = load_plan_data(str(tmp_path))
        assert len(loaded.phases) == 2

    def test_continuation_artifact_named_after_plan_version(self, tmp_path: Path) -> None:
        dp: dict = {"spec_epoch": 0}
        existing = _plan_from_phase_count(3)
        cont_md = _make_phase_md(1, title="Continuation phase")
        cont = parse_new_plan(cont_md)
        cont_plan = PlanData(
            phases=cont.phases,
            raw_text=cont_md,
            plan_sha256=hashlib.sha256(cont_md.encode()).hexdigest(),
        )
        from nodeflow.workflows.dev_process.plan_phases import renumber_continuation_headings

        display_cont = renumber_continuation_headings(cont_md, start_index=3)
        merged_raw = (
            existing.raw_text.rstrip()
            + "\n\n---\n\n## Continuation plan\n\n"
            + display_cont.strip()
        )
        merged_parsed = parse_new_plan(merged_raw)
        merged = PlanData(
            phases=merged_parsed.phases,
            raw_text=merged_raw,
            plan_sha256=hashlib.sha256(merged_raw.encode()).hexdigest(),
        )
        write_versioned_plan(str(tmp_path), existing, dp)
        info = write_versioned_plan(
            str(tmp_path),
            merged,
            dp,
            continuation_raw_md=cont_md,
            continuation_plan=cont_plan,
        )
        assert info["version"] == "plan_v00_01"
        assert (tmp_path / "plan" / "continuations" / "continuation_plan_v00_01.md").is_file()
        assert (tmp_path / "plan" / "continuations" / "continuation_plan_v00_01.json").is_file()


class TestPhaseContextWithVersionedPlan:
    def test_get_current_phase_context_after_merged_plan(self, tmp_path: Path) -> None:
        dp: dict = {
            "spec_epoch": 0,
            "total_phases": 4,
            "phase_index": 3,
            "current_phase_id": "phase_003",
            "phase_results": {
                f"phase_{i:03d}": {"status": "completed" if i < 3 else "pending"} for i in range(4)
            },
        }
        plan = _plan_from_phase_count(4)
        write_versioned_plan(str(tmp_path), plan, dp)
        loaded = load_plan_data(str(tmp_path))
        ctx = get_current_phase_context(dp, loaded)
        assert ctx is not None
        assert ctx["phase_id"] == "phase_003"
