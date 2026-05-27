"""Spec epoch bump must be explicit, not implied by human revision action."""

from __future__ import annotations

from pathlib import Path
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
            "run_id": "run1",
        },
        "stages": {},
    }
    captured: dict = {}

    def fake_run_spec_stage(**kwargs: object) -> dict:
        captured["spec_epoch_bump"] = kwargs.get("body", {}).get("spec_epoch_bump")
        return {"status": "completed", "spec_artifact": "/x/spec.md"}

    def fake_run_subpipe(spec_path: str, ctx: dict, *, workspace: str) -> dict:
        del workspace
        if spec_path.endswith("spec_cycle.json"):
            from nodeflow.workflows.dev_process.nodes import stage_nodes as stage_nodes_mod

            body = ctx["body"]
            params = ctx.get("params", {})
            repo_root = Path(str(body["run_context"]["repo_root"]))
            artifact_root = str(body["run_context"]["artifact_root"])
            run_id = str(body["run_context"]["run_id"])
            body.setdefault("stages", {})["spec"] = stage_nodes_mod.run_spec_stage(
                repo_root=repo_root,
                artifact_root=artifact_root,
                run_id=run_id,
                task_prompt=str(params.get("task_prompt") or body.get("task_prompt") or ""),
                base_revision=str(body["run_context"].get("source_base_revision") or ""),
                revision_context=params.get("revision_context"),
                notes=params.get("notes"),
                reference_materials=params.get("reference_materials"),
                previous_spec=params.get("previous_spec"),
                body=body,
            )
            body["stages"]["spec_review"] = stage_nodes_mod.run_spec_review_stage(
                repo_root=repo_root,
                artifact_root=artifact_root,
                run_id=run_id,
                task_prompt=str(params.get("task_prompt") or body.get("task_prompt") or ""),
                spec_text="old spec",
                body=body,
            )
            ctx["body"] = body
        return ctx

    with patch(
        "nodeflow.workflows.dev_process.nodes.stage_nodes.run_spec_stage",
        side_effect=fake_run_spec_stage,
    ), patch(
        "nodeflow.workflows.dev_process.nodes.stage_nodes.run_spec_review_stage",
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
    ), patch(
        "nodeflow.workflows.dev_process.flow_actions.run_subpipe",
        side_effect=fake_run_subpipe,
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
