"""Spec epoch bump must be explicit, not implied by human revision action."""

from __future__ import annotations

from unittest.mock import patch

from nodeflow.workflows.dev_process.constants import ACTION_REQUEST_SPEC_REVISION


def test_request_spec_revision_does_not_set_epoch_bump_flag() -> None:
    """Human spec revision uses revision bump only unless spec_rework_epoch_bump is set."""
    body: dict = {
        "dev_process": {},
        "run_context": {
            "repo_root": "/tmp/repo",
            "artifact_root": "/tmp/art",
            "source_base_revision": "HEAD",
        },
        "stages": {},
    }
    captured: dict = {}

    def fake_run_spec_stage(**kwargs: object) -> dict:
        captured["spec_epoch_bump"] = kwargs.get("body", {}).get("spec_epoch_bump")
        return {"status": "completed", "spec_artifact": "/x/spec.md"}

    with patch(
        "nodeflow.workflows.dev_process.flow_actions.run_spec_stage",
        side_effect=fake_run_spec_stage,
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions.run_spec_review_stage",
        return_value={"status": "completed"},
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions._finalize",
        return_value={},
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions._status",
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions.timeline",
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions._clear_git_worktree_on_revise",
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions.load_stored_spec_inputs",
        return_value=("", None),
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions._read_spec_text",
        return_value="old spec",
    ):
        from nodeflow.workflows.dev_process.flow_actions import _run_spec_cycle

        _run_spec_cycle(
            body,
            run_id="run1",
            task_prompt="build feature",
            action=ACTION_REQUEST_SPEC_REVISION,
            revision_context="fix wording",
        )

    assert captured.get("spec_epoch_bump") is False
