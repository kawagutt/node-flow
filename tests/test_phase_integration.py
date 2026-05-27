"""Integration tests for multi-phase dev_process loop (git + phase state)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    STATE_AWAITING_FINAL,
    STATE_AWAITING_IMPLEMENTATION,
    STATE_AWAITING_REWORK_DECISION,
)
from nodeflow.workflows.dev_process.flow_actions import (
    _handle_continue_implementation,
    _handle_rework,
)
from nodeflow.workflows.dev_process.phase_loop import init_phase_state, load_plan_data
from nodeflow.workflows.dev_process.plan_phases import PlanData, parse_new_plan, save_plan_json
from nodeflow.workflows.dev_process.reuse import collect_diff
from tests.test_plan_phases import _make_phase_md
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def _three_phase_plan_md() -> str:
    parts = [
        _make_phase_md(
            i + 1,
            title=f"P{i}",
            goal=f"Goal{i}",
            test_plan=f"- UNIQUE_TEST_PLAN_PHASE_{i:03d}",
        )
        for i in range(3)
    ]
    return "\n\n".join(parts)


def _setup_run(tmp_path: Path) -> tuple[dict[str, Any], Path, Path]:
    repo = tmp_path / "repo"
    artifact = tmp_path / "artifacts"
    repo.mkdir()
    artifact.mkdir()
    git_repo_with_commit(repo)

    raw = _three_phase_plan_md()
    parsed = parse_new_plan(raw)
    plan_data = PlanData(
        phases=parsed.phases,
        raw_text=raw,
        plan_sha256=hashlib.sha256(raw.encode()).hexdigest(),
    )
    plan_dir = artifact / "plan"
    plan_dir.mkdir(parents=True)
    (plan_dir / "plan.md").write_text(raw, encoding="utf-8")
    save_plan_json(plan_data, str(plan_dir))
    (artifact / "spec").mkdir()
    (artifact / "spec" / "spec.md").write_text("# Spec\n", encoding="utf-8")

    from nodeflow.workflows.dev_process.phase_git import create_task_branch

    task_branch = create_task_branch(repo, "integration-run", workspace_strategy="current_repo")

    dp: dict[str, Any] = {"review_depth_preset": "standard"}
    init_phase_state(dp, plan_data)
    dp["task_branch"] = task_branch

    body: dict[str, Any] = {
        "task_prompt": "integration task",
        "run_context": {
            "artifact_root": str(artifact),
            "repo_root": str(repo),
            "run_id": "integration-run",
            "source_base_revision": task_branch["base_ref"],
            "workspace_strategy": "current_repo",
        },
        "dev_process": dp,
        "stages": {},
        "flow_result": {"state": STATE_AWAITING_IMPLEMENTATION},
    }
    return body, repo, artifact


def _ok_review(**_kwargs: Any) -> dict[str, Any]:
    return {
        "review_result": {"blocking_findings": [], "non_blocking_findings": []},
        "status": "completed",
    }


def _impl_touch_file(**kwargs: Any) -> dict[str, Any]:
    body = kwargs["body"]
    phase_id = body["dev_process"]["current_phase_id"]
    repo_root: Path = kwargs["repo_root"]
    (repo_root / f"{phase_id}.py").write_text(f"# {phase_id}\n", encoding="utf-8")
    return {"status": "completed", "evidence_paths": []}


def _capture_test_plan(**kwargs: Any) -> dict[str, Any]:
    captured: list[str] = kwargs["body"].setdefault("_captured_test_plans", [])
    captured.append(kwargs.get("approved_plan") or "")
    return {"status": "completed", "evidence_paths": []}


def _patch_phase_stages(review_fn=_ok_review):
    def _fake_subpipe(spec_path: str, ctx: dict[str, Any], *, workspace: str) -> dict[str, Any]:
        del workspace
        body = ctx["body"]
        params = ctx.get("params") or {}
        if spec_path.endswith("phase_step.json"):
            repo_root = Path(str(body["run_context"]["repo_root"]))
            if not params.get("skip_implementation"):
                _impl_touch_file(body=body, repo_root=repo_root)
            body.setdefault("stages", {})["implementation"] = {"status": "completed"}
            _capture_test_plan(body=body, approved_plan=params.get("phase_plan_text"))
            body["stages"]["test_implementation"] = {"status": "completed"}
            body["stages"]["lint_fix"] = {"lint_fix": "skipped"}
            diff_result = collect_diff(
                repo_root=repo_root,
                base_revision=str(
                    params.get("base_revision") or body["run_context"]["source_base_revision"]
                ),
            )
            body["stages"]["run_tests"] = {
                "status": "completed",
                "test_result": {"ok": True},
                "diff_result": diff_result,
            }
            body["stages"]["review"] = review_fn(body=body, diff_result=diff_result)
            ctx["body"] = body
            return ctx
        if spec_path.endswith("final_review.json"):
            body.setdefault("stages", {})["review"] = _ok_review(body=body, diff_result={})
            ctx["body"] = body
            return ctx
        return ctx

    return (
        patch(
            "nodeflow.workflows.dev_process.nodes.stage_nodes.run_implementation_stage",
            side_effect=_impl_touch_file,
        ),
        patch(
            "nodeflow.workflows.dev_process.nodes.stage_nodes.run_test_implementation_stage",
            side_effect=_capture_test_plan,
        ),
        patch(
            "nodeflow.workflows.dev_process.stages.lint_fix.run_lint_fix_stage",
            return_value={"lint_fix": "skipped"},
        ),
        patch(
            "nodeflow.workflows.dev_process.nodes.stage_nodes.run_run_tests_stage",
            return_value={"status": "completed", "test_result": {"ok": True}},
        ),
        patch(
            "nodeflow.workflows.dev_process.stages.review.run_review_stage", side_effect=review_fn
        ),
        patch(
            "nodeflow.workflows.dev_process.flow_actions.run_subpipe",
            side_effect=_fake_subpipe,
        ),
        patch(
            "nodeflow.workflows.dev_process.flow_actions._run_final_review",
            side_effect=lambda body, *, run_id: {
                **body,
                "flow_result": {
                    "state": STATE_AWAITING_FINAL,
                    "ok": True,
                    "allowed_actions": [],
                },
            },
        ),
    )


def _reload_body_from_out(body: dict[str, Any], out: dict[str, Any]) -> None:
    cp_path = str(out.get("flow_checkpoint_path") or "")
    if not cp_path:
        body.clear()
        body.update(out)
        return
    body.clear()
    body.update(load_flow_checkpoint(cp_path))


class TestThreePhaseHappyPath:
    def test_three_phases_then_final_review(self, tmp_path: Path) -> None:
        body, repo, artifact = _setup_run(tmp_path)
        patches = _patch_phase_stages()
        for p in patches:
            p.start()
        try:
            for _ in range(4):
                out = _handle_continue_implementation(
                    body, run_id="integration-run", force_review_blocking=False
                )
                _reload_body_from_out(body, out)
                state = body["flow_result"]["state"]
                if state == STATE_AWAITING_FINAL:
                    break
                assert state == STATE_AWAITING_IMPLEMENTATION
            else:
                pytest.fail("expected awaiting_final")
        finally:
            for p in patches:
                p.stop()

        dp = body["dev_process"]
        assert dp["phase_index"] == 3
        assert all(dp["phase_results"][f"phase_{i:03d}"]["status"] == "completed" for i in range(3))
        assert (repo / "phase_000.py").is_file()
        assert (repo / "phase_002.py").is_file()
        loaded = load_plan_data(str(artifact))
        assert len(loaded.phases) == 3


class TestPhaseDiffBaseline:
    def test_phase_review_diff_excludes_prior_phase_files(self, tmp_path: Path) -> None:
        body, repo, _artifact = _setup_run(tmp_path)
        captured_diffs: list[dict[str, Any]] = []

        def review_capture(**kwargs: Any) -> dict[str, Any]:
            captured_diffs.append(dict(kwargs.get("diff_result") or {}))
            return _ok_review(**kwargs)

        patches = _patch_phase_stages(review_fn=review_capture)
        for p in patches:
            p.start()
        try:
            for _ in range(2):
                out = _handle_continue_implementation(
                    body, run_id="integration-run", force_review_blocking=False
                )
                _reload_body_from_out(body, out)
        finally:
            for p in patches:
                p.stop()

        assert len(captured_diffs) >= 2
        phase_001_diff = captured_diffs[1]
        paths_text = json.dumps(phase_001_diff)
        assert "phase_001.py" in paths_text
        assert "phase_000.py" not in paths_text

        dp = body["dev_process"]
        start_ref = dp["phase_results"]["phase_001"]["phase_start_git_ref"]
        manual = collect_diff(repo_root=repo, base_revision=start_ref)
        manual_text = json.dumps(manual)
        assert "phase_001.py" in manual_text
        assert "phase_000.py" not in manual_text


class TestCurrentPhaseTestPlan:
    def test_test_stage_prompt_only_includes_current_phase_test_plan(self, tmp_path: Path) -> None:
        body, _repo, _artifact = _setup_run(tmp_path)
        patches = _patch_phase_stages()
        for p in patches:
            p.start()
        try:
            out = _handle_continue_implementation(
                body, run_id="integration-run", force_review_blocking=False
            )
            _reload_body_from_out(body, out)
        finally:
            for p in patches:
                p.stop()

        plans = body.get("_captured_test_plans") or []
        assert len(plans) == 1
        prompt = plans[0]
        assert "UNIQUE_TEST_PLAN_PHASE_000" in prompt
        assert "UNIQUE_TEST_PLAN_PHASE_001" not in prompt
        assert "UNIQUE_TEST_PLAN_PHASE_002" not in prompt


class TestPhaseReworkRetry:
    def test_phase_001_review_fail_then_rework_advances(self, tmp_path: Path) -> None:
        body, repo, _artifact = _setup_run(tmp_path)
        review_attempts: dict[str, int] = {}

        def review_block_once(**kwargs: Any) -> dict[str, Any]:
            phase_id = kwargs["body"]["dev_process"]["current_phase_id"]
            n = review_attempts.get(phase_id, 0)
            review_attempts[phase_id] = n + 1
            if phase_id == "phase_001" and n == 0:
                return {
                    "review_result": {
                        "blocking_findings": [
                            {
                                "summary": "needs fix",
                                "owner": "implementation",
                            }
                        ],
                    },
                    "status": "completed",
                }
            return _ok_review(**kwargs)

        patches = _patch_phase_stages(review_fn=review_block_once)
        for p in patches:
            p.start()
        try:
            out = _handle_continue_implementation(
                body, run_id="integration-run", force_review_blocking=False
            )
            _reload_body_from_out(body, out)
            assert body["flow_result"]["state"] == STATE_AWAITING_IMPLEMENTATION

            out = _handle_continue_implementation(
                body, run_id="integration-run", force_review_blocking=False
            )
            _reload_body_from_out(body, out)
            assert body["flow_result"]["state"] == STATE_AWAITING_REWORK_DECISION
            assert body["dev_process"]["current_phase_id"] == "phase_001"
            assert "workspace_context" in body

            body["rework_owner"] = "implementation"
            out = _handle_rework(
                body,
                run_id="integration-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={"rework_comment": "fix phase 1"},
            )
            _reload_body_from_out(body, out)
            assert body["flow_result"]["state"] == STATE_AWAITING_IMPLEMENTATION
            assert body["dev_process"]["phase_results"]["phase_001"]["status"] == "completed"
            assert body["dev_process"]["current_phase_id"] == "phase_002"
            assert (repo / "phase_001.py").is_file()
        finally:
            for p in patches:
                p.stop()
