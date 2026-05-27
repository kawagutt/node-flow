"""write_spec node."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.node_runner import run_node_exec
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import collect_repo_context
from nodeflow.workflows.dev_process.spec_prompt import build_spec_prompt


def _parse_spec_stdout(stdout: str) -> str:
    try:
        parsed = json.loads(stdout.strip())
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"spec stdout must be JSON object: {e}") from e
    if not isinstance(parsed, dict):
        raise NodeExecutionFailure("spec stdout must be a JSON object")
    spec = parsed.get("spec")
    if not isinstance(spec, str) or not spec.strip():
        raise NodeExecutionFailure("spec JSON must include non-empty string field 'spec'")
    return spec.strip()


def run_spec_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    base_revision: str,
    revision_context: str | None = None,
    notes: str | None = None,
    reference_materials: list[dict[str, Any]] | None = None,
    previous_spec: str | None = None,
    body: Dict[str, Any],
) -> Dict[str, Any]:
    repo_context = collect_repo_context(
        repo_root=repo_root,
        task_prompt=task_prompt,
        base_revision=base_revision,
        revision_context=revision_context,
    )
    prompt_text = build_spec_prompt(
        task_prompt=task_prompt,
        repo_context=repo_context,
        notes=notes,
        revision_context=revision_context,
        reference_materials=reference_materials,
        previous_spec=previous_spec,
    )
    cwd = str(repo_root)

    execution_output, evidence_path, _rec = run_node_exec(
        body,
        node_name="write_spec",
        stage="spec",
        prompt=prompt_text,
        cwd=cwd,
        run_id=run_id,
        artifact_root=artifact_root,
    )

    spec_text = _parse_spec_stdout(str(execution_output.get("stdout") or ""))
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else None
    if dp is not None:
        from nodeflow.workflows.dev_process.artifact_versions import write_versioned_spec

        epoch_bump = bool(body.get("spec_epoch_bump", False))
        version_info = write_versioned_spec(artifact_root, spec_text, dp, epoch_bump=epoch_bump)
        spec_path = version_info["latest_path"]
    else:
        spec_dir = Path(artifact_root) / "spec"
        spec_dir.mkdir(parents=True, exist_ok=True)
        spec_path = str(spec_dir / "spec.md")
        Path(spec_path).write_text(spec_text, encoding="utf-8")
    assert_path_under_run_dir(artifact_root, str(spec_path))
    result: Dict[str, Any] = {
        "status": "completed",
        "spec_artifact": str(spec_path),
        "evidence_paths": [evidence_path],
    }
    if dp is not None:
        result["spec_version"] = version_info["version"]
        result["versioned_spec_path"] = version_info["versioned_path"]
    return result
