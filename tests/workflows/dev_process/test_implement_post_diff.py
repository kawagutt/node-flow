"""implement stage collects diff after Codex execution."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from nodeflow.workflows.dev_process.stages.implement import run_implement_stage


def test_implement_collects_diff_after_codex(tmp_path: Path) -> None:
    call_order: list[str] = []

    def _codex_execute(self, inputs, params):
        call_order.append("codex")
        return {
            "execution_output": {
                "ok": True,
                "stdout": "done",
                "stderr": "",
                "raw_output": {"returncode": 0},
                "provider_meta": {},
            }
        }

    def _collect_diff(**_kwargs):
        call_order.append("diff")
        return {"files": []}

    def _run_tests(**_kwargs):
        call_order.append("tests")
        return {"ok": True}

    repo = tmp_path / "repo"
    repo.mkdir()
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()

    with patch(
        "nodeflow.workflows.dev_process.stages.implement.CodexExecNode.execute", _codex_execute
    ):
        with patch(
            "nodeflow.workflows.dev_process.stages.implement.collect_diff",
            side_effect=_collect_diff,
        ):
            with patch(
                "nodeflow.workflows.dev_process.stages.implement.run_tests",
                side_effect=_run_tests,
            ):
                with patch(
                    "nodeflow.workflows.dev_process.stages.implement.write_stage_checkpoint",
                    return_value={"ok": True, "artifacts": []},
                ):
                    run_implement_stage(
                        repo_root=repo,
                        artifact_root=str(artifact_root),
                        run_id="r1",
                        task_prompt="t",
                        base_revision="HEAD",
                        approved_spec="s",
                        approved_plan="p",
                    )

    assert call_order[:2] == ["codex", "diff"]
