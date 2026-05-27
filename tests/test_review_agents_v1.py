"""v1 review agent selection, prompts, and exec_policy node wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from nodeflow.workflows.dev_process.argv_builder import resolve_node_exec
from nodeflow.workflows.dev_process.exec_policy import default_node_entries
from nodeflow.workflows.dev_process.plan_phases import PlanParseError, parse_new_plan
from nodeflow.workflows.dev_process.review_config import (
    FINAL_REVIEW_AGENTS,
    V1_PLAN_PROMPT_AGENTS,
    V1_REVIEW_AGENTS,
    review_node_name,
)
from nodeflow.workflows.dev_process.review_prompt_limits import prompt_params_for_reviewer
from nodeflow.workflows.dev_process.stages.review import run_review_stage
from tests.test_plan_phases import _make_phase_md


def _phase_md(*, agents: str = "architecture, checklist_compliance") -> str:
    return _make_phase_md(1, review_agents=agents)


class TestPlanPromptAgents:
    def test_v1_plan_prompt_excludes_optional_agents(self) -> None:
        from nodeflow.workflows.dev_process.plan_prompt import build_plan_prompt

        text = build_plan_prompt(task_prompt="t", approved_spec="s")
        assert "diff_detail" not in text
        assert "naming_doc" not in text
        for agent in V1_PLAN_PROMPT_AGENTS:
            assert agent in text

    def test_unknown_agent_rejected_at_parse(self) -> None:
        md = _phase_md(agents="unknown_agent")
        with pytest.raises(PlanParseError, match="unknown review agent"):
            parse_new_plan(md)


class TestReviewAgentSelection:
    @patch("nodeflow.workflows.dev_process.node_runner.run_node_exec")
    @patch("nodeflow.workflows.dev_process.stages.review.build_review_prompt")
    def test_review_agents_recorded_as_independent_nodes(
        self, mock_build: MagicMock, mock_exec: MagicMock
    ) -> None:
        mock_build.return_value = "prompt"
        mock_exec.return_value = (
            {"ok": True, "blocking_findings": [], "non_blocking_findings": []},
            "/tmp/evidence.json",
            {},
        )
        body: dict[str, Any] = {"dev_process": {"exec_policy_snapshot": {"nodes": {}}}}
        run_review_stage(
            repo_root=Path("/tmp"),
            artifact_root="/tmp/art",
            run_id="r1",
            base_revision="abc",
            approved_spec="spec",
            approved_plan="plan",
            diff_result={},
            test_result={},
            body=body,
            review_agents=["architecture", "test_quality"],
            review_scope="phase",
        )
        node_names = [c.kwargs["node_name"] for c in mock_exec.call_args_list]
        assert node_names == [
            review_node_name("architecture"),
            review_node_name("test_quality"),
        ]
        assert review_node_name("checklist_compliance") not in node_names

    @patch("nodeflow.workflows.dev_process.stages.review.aggregate_reviews")
    @patch("nodeflow.workflows.dev_process.stages.review._run_one_reviewer_via_node")
    def test_plan_agents_select_only_those_reviewers(
        self, mock_run: MagicMock, mock_aggregate: MagicMock
    ) -> None:
        mock_aggregate.return_value = (
            {"ok": True, "blocking_findings": [], "non_blocking_findings": []},
            {"ok": True},
        )
        mock_run.return_value = (
            {
                "ok": True,
                "blocking_findings": [],
                "non_blocking_findings": [],
            },
            "/tmp/evidence.json",
        )
        body: dict[str, Any] = {"dev_process": {"exec_policy_snapshot": {"nodes": {}}}}
        run_review_stage(
            repo_root=Path("/tmp"),
            artifact_root="/tmp/art",
            run_id="r1",
            base_revision="abc",
            approved_spec="spec",
            approved_plan="plan",
            diff_result={"files": []},
            test_result={},
            body=body,
            review_agents=["architecture", "checklist_compliance"],
            review_targets=["implementation_phase"],
            review_scope="phase",
        )
        called_agents = [c.kwargs["reviewer_key"] for c in mock_run.call_args_list]
        assert called_agents == ["architecture", "checklist_compliance"]
        expected_nodes = [review_node_name(a) for a in called_agents]
        assert mock_aggregate.call_args.kwargs["expected_review_keys"] == expected_nodes

    @patch("nodeflow.workflows.dev_process.stages.review._run_one_reviewer_via_node")
    def test_targets_do_not_add_extra_reviewers(self, mock_run: MagicMock) -> None:
        mock_run.return_value = (
            {"ok": True, "blocking_findings": [], "non_blocking_findings": []},
            "/tmp/evidence.json",
        )
        body: dict[str, Any] = {"dev_process": {"exec_policy_snapshot": {"nodes": {}}}}
        run_review_stage(
            repo_root=Path("/tmp"),
            artifact_root="/tmp/art",
            run_id="r1",
            base_revision="abc",
            approved_spec="spec",
            approved_plan="plan",
            diff_result={},
            test_result={},
            body=body,
            review_agents=["architecture"],
            review_targets=["implementation_phase", "test_phase"],
            review_scope="phase",
        )
        assert [c.kwargs["reviewer_key"] for c in mock_run.call_args_list] == ["architecture"]

    @patch("nodeflow.workflows.dev_process.stages.review._run_one_reviewer_via_node")
    def test_final_review_uses_fixed_agent_set(self, mock_run: MagicMock) -> None:
        mock_run.return_value = (
            {"ok": True, "blocking_findings": [], "non_blocking_findings": []},
            "/tmp/evidence.json",
        )
        body: dict[str, Any] = {"dev_process": {"exec_policy_snapshot": {"nodes": {}}}}
        run_review_stage(
            repo_root=Path("/tmp"),
            artifact_root="/tmp/art",
            run_id="r1",
            base_revision="abc",
            approved_spec="spec",
            approved_plan="plan",
            diff_result={},
            test_result={},
            body=body,
            review_scope="final",
        )
        called = [c.kwargs["reviewer_key"] for c in mock_run.call_args_list]
        assert called == list(FINAL_REVIEW_AGENTS)


class TestChecklistCompliancePrompt:
    @patch("nodeflow.workflows.dev_process.node_runner.run_node_exec")
    @patch("nodeflow.workflows.dev_process.stages.review.build_review_prompt")
    def test_checklist_compliance_gets_diff_checklist_and_criteria(
        self, mock_build: MagicMock, mock_exec: MagicMock
    ) -> None:
        mock_build.return_value = "prompt"
        mock_exec.return_value = (
            {"ok": True, "blocking_findings": [], "non_blocking_findings": []},
            "/tmp/evidence.json",
            {},
        )

        body: dict[str, Any] = {"dev_process": {"exec_policy_snapshot": {"nodes": {}}}}
        run_review_stage(
            repo_root=Path("/tmp"),
            artifact_root="/tmp/art",
            run_id="r1",
            base_revision="abc",
            approved_spec="spec",
            approved_plan="base plan",
            diff_result={"files": [{"path": "a.py"}]},
            test_result={"ok": True},
            body=body,
            review_agents=["checklist_compliance"],
            review_checklist=["Item A"],
            review_acceptance_criteria=["Criterion B"],
            review_scope="phase",
        )

        assert mock_build.call_args.args[0] == review_node_name("checklist_compliance")
        plan_arg = mock_build.call_args.kwargs["approved_plan"]
        assert "Review checklist:" in plan_arg
        assert "Item A" in plan_arg
        assert "Acceptance criteria:" in plan_arg
        assert "Criterion B" in plan_arg
        assert mock_build.call_args.kwargs["diff_result"] == {"files": [{"path": "a.py"}]}
        assert prompt_params_for_reviewer("standard", "checklist_compliance")["max_diff_chars"] > 0


class TestReviewNodeSkills:
    def test_architecture_skill_injected_into_prompt(self) -> None:
        from nodeflow.workflows.dev_process.reuse import build_review_prompt

        text = build_review_prompt(
            review_node_name("architecture"),
            repo_root="/tmp",
            base_revision="abc",
            diff_result={"diff": "", "status_short": "", "untracked_files": []},
            test_result={},
            approved_spec="spec",
            approved_plan="plan",
        )
        assert "architecture" in text.lower()
        assert "module boundaries" in text.lower()


class TestPerAgentExecPolicy:
    def test_default_node_entries_include_v1_agents_with_model(self) -> None:
        entries = default_node_entries()
        for agent in V1_REVIEW_AGENTS:
            node = review_node_name(agent)
            assert node in entries
            assert entries[node].get("model")

    def test_resolve_node_exec_uses_per_agent_argv(self) -> None:
        body = {
            "dev_process": {
                "exec_policy_snapshot": {
                    "default_worker": "codex",
                    "default_argv": ["python", "-c", "print('default')"],
                    "nodes": {
                        review_node_name("architecture"): {
                            "argv": ["python", "-c", "print('arch')"],
                            "model": "code_main",
                        },
                        review_node_name("impact"): {
                            "argv": ["python", "-c", "print('impact')"],
                            "model": "strong_reasoning",
                        },
                    },
                }
            }
        }
        _w1, m1, a1 = resolve_node_exec(body, review_node_name("architecture"))
        _w2, m2, a2 = resolve_node_exec(body, review_node_name("impact"))
        assert a1 == ["python", "-c", "print('arch')"]
        assert a2 == ["python", "-c", "print('impact')"]
        assert m1 == "code_main"
        assert m2 == "strong_reasoning"

    @patch("nodeflow.workflows.dev_process.node_runner.run_exec")
    @patch("nodeflow.workflows.dev_process.stages.review.build_review_prompt")
    @patch("nodeflow.workflows.dev_process.node_runner.record_exec_evidence")
    def test_review_agent_uses_own_exec_policy_argv(
        self,
        mock_evidence: MagicMock,
        mock_build: MagicMock,
        mock_run_exec: MagicMock,
    ) -> None:
        import sys

        mock_build.return_value = "prompt"
        mock_run_exec.return_value = {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
        }
        mock_evidence.return_value = "/tmp/evidence.json"
        arch_argv = [sys.executable, "-c", "print('marker-arch')"]
        test_argv = [sys.executable, "-c", "print('marker-test')"]
        body: dict[str, Any] = {
            "dev_process": {
                "exec_policy_snapshot": {
                    "default_worker": "codex",
                    "default_argv": [sys.executable, "-c", "print('default')"],
                    "nodes": {
                        review_node_name("architecture"): {"argv": arch_argv},
                        review_node_name("test_quality"): {"argv": test_argv},
                    },
                }
            }
        }
        run_review_stage(
            repo_root=Path("/tmp"),
            artifact_root="/tmp/art",
            run_id="r1",
            base_revision="abc",
            approved_spec="spec",
            approved_plan="plan",
            diff_result={},
            test_result={},
            body=body,
            review_agents=["architecture", "test_quality"],
            review_scope="phase",
        )
        argv_by_node = {nr["node_name"]: nr["argv"] for nr in body.get("node_runs", [])}
        assert argv_by_node[review_node_name("architecture")] == arch_argv
        assert argv_by_node[review_node_name("test_quality")] == test_argv
        assert mock_run_exec.call_args_list[0].kwargs["argv"] == arch_argv
        assert mock_run_exec.call_args_list[1].kwargs["argv"] == test_argv
