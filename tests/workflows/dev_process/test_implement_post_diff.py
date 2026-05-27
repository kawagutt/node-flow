"""implementation stage collects diff after Codex execution."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from nodeflow.workflows.dev_process.stages.implementation import run_implementation_stage


def test_implement_collects_diff_after_codex(tmp_path: Path) -> None:
    call_order: list[str] = []

    def _run_node_exec(body, **kwargs):
        del body, kwargs
        call_order.append("codex")
        return (
            {
                "ok": True,
                "stdout": "done",
                "stderr": "",
                "raw_output": {"returncode": 0},
                "provider_meta": {},
            },
            "/tmp/evidence.json",
            None,
        )

    def _collect_diff(**_kwargs):
        call_order.append("diff")
        return {"files": []}

    repo = tmp_path / "repo"
    repo.mkdir()
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()

    with patch(
        "nodeflow.workflows.dev_process.stages.implementation.run_node_exec",
        side_effect=_run_node_exec,
    ):
        with patch(
            "nodeflow.workflows.dev_process.stages.implementation.collect_diff",
            side_effect=_collect_diff,
        ):
            run_implementation_stage(
                repo_root=repo,
                artifact_root=str(artifact_root),
                run_id="r1",
                task_prompt="t",
                base_revision="HEAD",
                approved_spec="s",
                approved_plan="p",
                body={"node_runs": [], "dev_process": {"exec_policy_snapshot": {"nodes": {}}}},
            )

    assert call_order == ["codex", "diff"]
