"""Tests for phase tracking in checkpoint/discovery and CLI status."""

from __future__ import annotations

from nodeflow.workflows.dev_process.discovery import _extract_phase_status, checkpoint_status


class TestExtractPhaseStatus:
    def test_no_phases_returns_empty(self) -> None:
        result = _extract_phase_status({})
        assert result == {}

    def test_basic_phase_tracking(self) -> None:
        dp = {
            "total_phases": 3,
            "phase_index": 1,
            "current_phase_id": "phase_001",
            "phase_results": {
                "phase_000": {"status": "completed", "title": "Setup"},
                "phase_001": {"status": "in_progress", "title": "Core"},
                "phase_002": {"status": "pending", "title": "Polish"},
            },
        }
        result = _extract_phase_status(dp)
        assert result["total_phases"] == 3
        assert result["phase_index"] == 1
        assert result["current_phase_id"] == "phase_001"
        assert len(result["phases"]) == 3
        assert result["phases"][0]["status"] == "completed"
        assert result["phases"][1]["status"] == "current"
        assert result["phases"][2]["status"] == "pending"

    def test_all_completed(self) -> None:
        dp = {
            "total_phases": 2,
            "phase_index": 2,
            "current_phase_id": "",
            "phase_results": {
                "phase_000": {"status": "completed", "title": "A"},
                "phase_001": {"status": "completed", "title": "B"},
            },
        }
        result = _extract_phase_status(dp)
        assert result["phases"][0]["status"] == "completed"
        assert result["phases"][1]["status"] == "completed"

    def test_legacy_no_phases(self) -> None:
        dp = {"review_depth_preset": "standard"}
        result = _extract_phase_status(dp)
        assert result == {}


class TestCheckpointStatusPhaseInfo:
    def test_includes_phase_info_when_present(self) -> None:
        doc = {
            "flow_result": {"state": "awaiting_implementation", "ok": True, "allowed_actions": []},
            "run_context": {"run_id": "001", "artifact_root": "/tmp/test"},
            "dev_process": {
                "total_phases": 2,
                "phase_index": 0,
                "current_phase_id": "phase_000",
                "phase_results": {
                    "phase_000": {"status": "in_progress", "title": "First"},
                    "phase_001": {"status": "pending", "title": "Second"},
                },
            },
        }
        status = checkpoint_status(doc, checkpoint_path="/tmp/test/checkpoints/flow.json")
        assert status["total_phases"] == 2
        assert status["phase_index"] == 0
        assert len(status["phases"]) == 2

    def test_no_phase_info_for_legacy(self) -> None:
        doc = {
            "flow_result": {"state": "awaiting_implementation", "ok": True, "allowed_actions": []},
            "run_context": {"run_id": "001", "artifact_root": "/tmp/test"},
            "dev_process": {"review_depth_preset": "standard"},
        }
        status = checkpoint_status(doc, checkpoint_path="/tmp/test/checkpoints/flow.json")
        assert "total_phases" not in status
