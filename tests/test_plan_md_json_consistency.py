"""plan.md / plan.json consistency checks in load_plan_data."""

from __future__ import annotations

import hashlib
import json

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.phase_loop import load_plan_data
from nodeflow.workflows.dev_process.plan_phases import PlanData, save_plan_json
from tests.test_plan_phases import _make_phase_md


def _write_consistent_plan(artifact, plan_data: PlanData) -> None:
    plan_dir = artifact / "plan"
    plan_dir.mkdir(parents=True, exist_ok=True)
    (plan_dir / "plan.md").write_text(plan_data.raw_text, encoding="utf-8")
    save_plan_json(plan_data, str(plan_dir))


def test_load_plan_data_rejects_sha256_mismatch(tmp_path) -> None:
    from nodeflow.workflows.dev_process.plan_phases import parse_new_plan

    raw_text = _make_phase_md(1)
    parsed = parse_new_plan(raw_text)
    plan_data = PlanData(
        phases=parsed.phases,
        raw_text=raw_text,
        plan_sha256=hashlib.sha256(b"wrong").hexdigest(),
    )
    plan_dir = tmp_path / "plan"
    plan_dir.mkdir()
    (plan_dir / "plan.md").write_text(raw_text, encoding="utf-8")
    (plan_dir / "plan.json").write_text(
        json.dumps(plan_data.to_dict()),
        encoding="utf-8",
    )

    with pytest.raises(NodeExecutionFailure, match="plan_sha256 mismatch"):
        load_plan_data(str(tmp_path))


def test_load_plan_data_rejects_extra_phase_in_markdown(tmp_path) -> None:
    from nodeflow.workflows.dev_process.plan_phases import parse_new_plan

    md = _make_phase_md(1) + "\n" + _make_phase_md(2)
    parsed = parse_new_plan(md)
    plan_data = PlanData(
        phases=[parsed.phases[0]],
        raw_text=md,
        plan_sha256=hashlib.sha256(md.encode()).hexdigest(),
    )
    _write_consistent_plan(tmp_path, plan_data)

    with pytest.raises(NodeExecutionFailure, match="phase count mismatch"):
        load_plan_data(str(tmp_path))
