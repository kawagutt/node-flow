"""Tests for final review routing schema."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.final_review import (
    parse_final_synthesis,
    route_final_synthesis,
)
from nodeflow.workflows.dev_process.flow_actions import _build_final_synthesis


def _make_body_with_final_rework(
    tmp_path: Path,
    *,
    target_phase_required: bool = False,
    decision_required: bool = False,
    owners: list[str] | None = None,
) -> dict:
    """Build a minimal body dict that simulates post-final-review rework state."""
    art = tmp_path / "artifacts"
    art.mkdir(parents=True, exist_ok=True)
    (art / "rework_inputs.json").write_text(json.dumps({"rework_comment": "fix it"}))

    repo = tmp_path / "repo"
    repo.mkdir(parents=True, exist_ok=True)

    final_req: dict = {
        "owner": "implementation" if target_phase_required else "mixed",
        "target_phase_required": target_phase_required,
        "decision_required": decision_required,
        "findings": [{"summary": "issue"}],
    }
    if owners:
        final_req["owners"] = owners

    return {
        "workspace_context": {"repo_root": str(repo)},
        "run_context": {"artifact_root": str(art)},
        "stages": {},
        "dev_process": {
            "current_phase_id": "phase_002",
            "phase_index": 3,
            "total_phases": 3,
            "final_rework_required": final_req,
            "phase_results": {
                "phase_002": {"phase_start_git_ref": "abc123"},
            },
        },
    }


class TestParseFinalSynthesis:
    def test_valid_ok(self) -> None:
        result = parse_final_synthesis(
            {"owner": "implementation", "target_phase": "phase_001", "findings": []}
        )
        assert result["owner"] == "implementation"
        assert result["findings"] == []

    def test_missing_owner_raises(self) -> None:
        with pytest.raises(NodeExecutionFailure, match="missing 'owner'"):
            parse_final_synthesis({"findings": []})

    def test_invalid_owner_raises(self) -> None:
        with pytest.raises(NodeExecutionFailure, match="not valid"):
            parse_final_synthesis({"owner": "unknown", "findings": []})

    def test_impl_without_target_raises(self) -> None:
        with pytest.raises(NodeExecutionFailure, match="target_phase is required"):
            parse_final_synthesis({"owner": "implementation", "findings": [{"x": 1}]})

    def test_test_without_target_raises(self) -> None:
        with pytest.raises(NodeExecutionFailure, match="target_phase is required"):
            parse_final_synthesis({"owner": "test", "findings": [{"x": 1}]})

    def test_impl_without_findings_ok(self) -> None:
        result = parse_final_synthesis({"owner": "implementation", "findings": []})
        assert result["owner"] == "implementation"
        assert result["target_phase"] is None

    def test_plan_without_target_ok(self) -> None:
        result = parse_final_synthesis({"owner": "plan", "findings": [{"x": 1}]})
        assert result["owner"] == "plan"
        assert result["target_phase"] is None

    def test_spec_without_target_ok(self) -> None:
        result = parse_final_synthesis({"owner": "spec", "findings": [{"x": 1}]})
        assert result["owner"] == "spec"


class TestRouteFinalSynthesis:
    def test_ok_no_findings(self) -> None:
        synthesis = {"owner": "implementation", "target_phase": "phase_001", "findings": []}
        result = route_final_synthesis(synthesis)
        assert result["decision"] == "ok"
        assert result["next_state"] == "awaiting_final_approval"

    def test_plan_rework(self) -> None:
        synthesis = {"owner": "plan", "findings": [{"msg": "bad plan"}]}
        result = route_final_synthesis(synthesis)
        assert result["decision"] == "rework"
        assert result["next_state"] == "awaiting_plan_revision"

    def test_spec_rework(self) -> None:
        synthesis = {"owner": "spec", "findings": [{"msg": "bad spec"}]}
        result = route_final_synthesis(synthesis)
        assert result["decision"] == "rework"
        assert result["next_state"] == "awaiting_spec_revision"

    def test_impl_rewind_not_implemented(self) -> None:
        synthesis = {
            "owner": "implementation",
            "target_phase": "phase_001",
            "findings": [{"msg": "x"}],
        }
        with pytest.raises(NodeExecutionFailure, match="rewind not yet implemented"):
            route_final_synthesis(synthesis, rewind_implemented=False)

    def test_impl_rewind_when_implemented(self) -> None:
        synthesis = {
            "owner": "implementation",
            "target_phase": "phase_001",
            "findings": [{"msg": "x"}],
        }
        result = route_final_synthesis(synthesis, rewind_implemented=True)
        assert result["decision"] == "rework"
        assert result["target_phase"] == "phase_001"
        assert result["owner"] == "implementation"

    def test_test_rewind_when_implemented(self) -> None:
        synthesis = {"owner": "test", "target_phase": "phase_002", "findings": [{"msg": "x"}]}
        result = route_final_synthesis(synthesis, rewind_implemented=True)
        assert result["decision"] == "rework"
        assert result["target_phase"] == "phase_002"
        assert result["owner"] == "test"


class TestBuildFinalSynthesis:
    """Tests for _build_final_synthesis owner routing logic."""

    def test_impl_findings_require_target_phase(self) -> None:
        findings = [{"owner": "implementation", "summary": "bug"}]
        dp: dict = {"total_phases": 2}
        result = _build_final_synthesis(findings, dp)
        assert result["target_phase_required"] is True
        assert result["owner"] == "implementation"

    def test_test_findings_require_target_phase(self) -> None:
        findings = [{"owner": "test", "summary": "missing test"}]
        dp: dict = {"total_phases": 2}
        result = _build_final_synthesis(findings, dp)
        assert result["target_phase_required"] is True
        assert result["owner"] == "test"

    def test_plan_only_auto_routes(self) -> None:
        findings = [{"owner": "plan", "summary": "plan issue"}]
        dp: dict = {"total_phases": 2}
        result = _build_final_synthesis(findings, dp)
        assert result["owner"] == "plan"
        assert result.get("target_phase_required") is None

    def test_spec_only_auto_routes(self) -> None:
        findings = [{"owner": "spec", "summary": "spec issue"}]
        dp: dict = {"total_phases": 2}
        result = _build_final_synthesis(findings, dp)
        assert result["owner"] == "spec"
        assert result.get("target_phase_required") is None

    def test_mixed_plan_and_impl_requires_decision(self) -> None:
        findings = [
            {"owner": "plan", "summary": "plan issue"},
            {"owner": "implementation", "summary": "impl issue"},
        ]
        dp: dict = {"total_phases": 2}
        result = _build_final_synthesis(findings, dp)
        assert result["decision_required"] is True
        assert result["target_phase_required"] is True
        assert result["owner"] == "mixed"
        assert set(result["owners"]) == {"implementation", "plan"}

    def test_mixed_plan_spec_requires_decision(self) -> None:
        findings = [
            {"owner": "plan", "summary": "plan issue"},
            {"owner": "spec", "summary": "spec issue"},
        ]
        dp: dict = {"total_phases": 2}
        result = _build_final_synthesis(findings, dp)
        assert result["decision_required"] is True
        assert result["target_phase_required"] is False
        assert result["owner"] == "mixed"
        assert set(result["owners"]) == {"plan", "spec"}

    def test_mixed_impl_test_requires_decision(self) -> None:
        findings = [
            {"owner": "implementation", "summary": "impl issue"},
            {"owner": "test", "summary": "test issue"},
        ]
        dp: dict = {"total_phases": 2}
        result = _build_final_synthesis(findings, dp)
        assert result["decision_required"] is True
        assert result["target_phase_required"] is True
        assert result["owner"] == "mixed"
        assert set(result["owners"]) == {"implementation", "test"}


class TestHandleReworkFinalRequired:
    """Tests for _handle_rework when final_rework_required is set."""

    _REWORK_BASE = {"rework_comment": "fix it"}

    def test_rework_without_target_phase_raises(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(tmp_path, target_phase_required=True)
        with pytest.raises(NodeExecutionFailure, match="requires explicit target_phase"):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=True,
            )

    def test_mixed_rework_uses_cli_owner_for_loop_counter(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(
            tmp_path,
            decision_required=True,
            target_phase_required=False,
            owners=["plan", "implementation"],
        )
        body["rework_owner"] = "mixed"
        body["run_context"]["repo_root"] = str(tmp_path / "repo")
        dp = body["dev_process"]
        dp["loop_counters"] = {"plan_revision": 0, "implementation_rework": 0}
        with (
            patch(
                "nodeflow.workflows.dev_process.flow_actions._rework_save_and_reset",
            ),
            patch(
                "nodeflow.workflows.dev_process.flow_actions._handle_revise_plan",
                return_value={"flow_result": {"state": "awaiting_plan_revision"}},
            ),
        ):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE, "owner": "plan"},
                from_human_gate=False,
            )
        assert dp["loop_counters"]["plan_revision"] == 1
        assert dp["loop_counters"]["implementation_rework"] == 0

    def test_rework_validation_failure_does_not_increment_loop_counter(
        self, tmp_path: Path
    ) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(tmp_path, target_phase_required=True)
        dp = body["dev_process"]
        dp["loop_counters"] = {"implementation_rework": 0}
        with pytest.raises(NodeExecutionFailure, match="requires explicit target_phase"):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=False,
            )
        assert dp["loop_counters"]["implementation_rework"] == 0

    def test_rework_does_not_auto_rewind_to_last_phase(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(tmp_path, target_phase_required=True)
        dp = body["dev_process"]
        assert dp["phase_index"] >= dp["total_phases"]

        with pytest.raises(NodeExecutionFailure, match="requires explicit target_phase"):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=True,
            )
        assert dp["phase_index"] == dp["total_phases"]
        assert "last_rewind" not in dp

    def test_mixed_rework_without_owner_raises(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(
            tmp_path,
            decision_required=True,
            target_phase_required=False,
            owners=["plan", "spec"],
        )
        with pytest.raises(NodeExecutionFailure, match="requires explicit owner"):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=True,
            )

    def test_mixed_rework_impl_without_target_phase_raises(self, tmp_path: Path) -> None:
        """Mixed decision_required + owner=implementation still needs target_phase."""
        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(
            tmp_path,
            decision_required=True,
            target_phase_required=True,
            owners=["implementation", "plan"],
        )
        with pytest.raises(NodeExecutionFailure, match="requires explicit target_phase"):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE, "owner": "implementation"},
                from_human_gate=True,
            )

    def test_spec_rework_clears_final_rework_required(self, tmp_path: Path) -> None:
        """Spec rework path must clear stale final_rework_required flag."""
        from unittest.mock import patch

        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(tmp_path, target_phase_required=False)
        body["rework_owner"] = "spec"
        dp = body["dev_process"]
        dp["total_phases"] = 0
        assert "final_rework_required" in dp

        with patch(
            "nodeflow.workflows.dev_process.flow_actions._handle_revise_spec",
            return_value={"ok": True},
        ), patch(
            "nodeflow.workflows.dev_process.flow_actions.mark_stale",
        ):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=True,
            )
        assert "final_rework_required" not in dp

    def test_plan_rework_clears_final_rework_required(self, tmp_path: Path) -> None:
        """Plan rework path must clear stale final_rework_required flag."""
        from unittest.mock import patch

        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        body = _make_body_with_final_rework(tmp_path, target_phase_required=False)
        body["rework_owner"] = "plan"
        dp = body["dev_process"]
        dp["total_phases"] = 0
        assert "final_rework_required" in dp

        with patch(
            "nodeflow.workflows.dev_process.flow_actions._handle_revise_plan",
            return_value={"ok": True},
        ), patch(
            "nodeflow.workflows.dev_process.flow_actions.mark_stale",
        ):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=True,
            )
        assert "final_rework_required" not in dp


class TestHandleReworkFallbackSkipImpl:
    """Regression: fallback rework after rewind must not skip implementation."""

    _REWORK_BASE = {"rework_comment": "fix it"}

    def test_rewind_forces_skip_impl_false(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock, patch

        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        repo = tmp_path / "repo"
        repo.mkdir(parents=True)
        art = tmp_path / "artifacts"
        art.mkdir(parents=True)
        (art / "rework_inputs.json").write_text(json.dumps({"rework_comment": "fix"}))

        body = {
            "rework_owner": "test",
            "workspace_context": {"repo_root": str(repo)},
            "run_context": {"artifact_root": str(art), "repo_root": str(repo)},
            "stages": {},
            "dev_process": {
                "current_phase_id": "",
                "phase_index": 3,
                "total_phases": 3,
                "phase_results": {
                    "phase_000": {"status": "completed"},
                    "phase_001": {"status": "completed"},
                    "phase_002": {
                        "status": "completed",
                        "phase_start_git_ref": "abc123",
                    },
                },
            },
        }

        mock_continue = MagicMock(return_value={"ok": True})
        mock_rewind = MagicMock(return_value={"target_phase": "phase_002", "ref": "abc123"})

        with patch(
            "nodeflow.workflows.dev_process.flow_actions._handle_continue_implementation",
            mock_continue,
        ), patch(
            "nodeflow.workflows.dev_process.phase_rewind.rewind_to_phase",
            mock_rewind,
        ):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=True,
            )

        mock_continue.assert_called_once()
        _, kwargs = mock_continue.call_args
        assert kwargs["skip_implementation"] is False

    def test_no_rewind_test_owner_skips_impl(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock, patch

        from nodeflow.workflows.dev_process.flow_actions import _handle_rework

        repo = tmp_path / "repo"
        repo.mkdir(parents=True)
        art = tmp_path / "artifacts"
        art.mkdir(parents=True)
        (art / "rework_inputs.json").write_text(json.dumps({"rework_comment": "fix"}))

        body = {
            "rework_owner": "test",
            "workspace_context": {"repo_root": str(repo)},
            "run_context": {"artifact_root": str(art)},
            "stages": {},
            "dev_process": {
                "current_phase_id": "",
                "phase_index": 3,
                "total_phases": 3,
                "phase_results": {
                    "phase_000": {"status": "completed"},
                    "phase_001": {"status": "completed"},
                    "phase_002": {
                        "status": "completed",
                    },
                },
            },
        }

        mock_continue = MagicMock(return_value={"ok": True})
        mock_invalidate = MagicMock()

        with patch(
            "nodeflow.workflows.dev_process.flow_actions._handle_continue_implementation",
            mock_continue,
        ), patch(
            "nodeflow.workflows.dev_process.phase_loop.invalidate_phases_from",
            mock_invalidate,
        ):
            _handle_rework(
                body,
                run_id="test-run",
                force_review_blocking=False,
                interactive=False,
                rework_provided={**self._REWORK_BASE},
                from_human_gate=True,
            )

        mock_continue.assert_called_once()
        _, kwargs = mock_continue.call_args
        assert kwargs["skip_implementation"] is True


class TestIsTestsOk:
    """Tests for _is_tests_ok (requires test_result.ok from run_tests stage)."""

    def test_ok_true_via_test_result(self) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _is_tests_ok

        assert _is_tests_ok({"test_result": {"ok": True}}) is True

    def test_ok_false_via_test_result(self) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _is_tests_ok

        assert _is_tests_ok({"test_result": {"ok": False}}) is False

    def test_missing_ok_is_false(self) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _is_tests_ok

        assert _is_tests_ok({"status": "completed"}) is False
        assert _is_tests_ok({}) is False


class TestInvalidatePhasesFromCounterReset:
    """Verify invalidate_phases_from clears loop counters for invalidated phases."""

    def test_counters_cleared(self) -> None:
        from nodeflow.workflows.dev_process.phase_loop import invalidate_phases_from

        dp: dict = {
            "total_phases": 3,
            "phase_index": 3,
            "current_phase_id": "",
            "phase_results": {
                "phase_000": {"status": "completed"},
                "phase_001": {"status": "completed"},
                "phase_002": {"status": "completed"},
            },
            "loop_counters": {
                "phase_000_implementation_rework": 2,
                "phase_001_implementation_rework": 3,
                "phase_001_test_rework": 1,
                "phase_002_implementation_rework": 4,
            },
        }
        invalidate_phases_from(dp, 1)
        counters = dp["loop_counters"]
        assert counters.get("phase_000_implementation_rework") == 2
        assert "phase_001_implementation_rework" not in counters
        assert "phase_001_test_rework" not in counters
        assert "phase_002_implementation_rework" not in counters
