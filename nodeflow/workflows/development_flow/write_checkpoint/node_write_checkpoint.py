"""Write stage checkpoint artifact and emit unified stage_result."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode


def _extract_first_json_object(text: str) -> str | None:
    if not text or not text.strip():
        return None
    s = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", s, re.IGNORECASE)
    if fence:
        s = fence.group(1).strip()

    decoder = json.JSONDecoder()
    for i, ch in enumerate(s):
        if ch != "{":
            continue
        try:
            _, end = decoder.raw_decode(s[i:])
        except json.JSONDecodeError:
            continue
        return s[i : i + end]
    return None


def _loads_first_json_object(text: str) -> Any | None:
    blob = _extract_first_json_object(text)
    if not blob:
        return None
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        return None


class WriteCheckpointNode(PythonActionNode):
    role = "write_checkpoint"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        request = inputs.get("request") if isinstance(inputs.get("request"), dict) else {}
        execution_result = (
            inputs.get("execution_result")
            if isinstance(inputs.get("execution_result"), dict)
            else None
        )
        test_result = (
            inputs.get("test_result") if isinstance(inputs.get("test_result"), dict) else None
        )
        diff_result = (
            inputs.get("diff_result") if isinstance(inputs.get("diff_result"), dict) else None
        )
        review_result = (
            inputs.get("review_result") if isinstance(inputs.get("review_result"), dict) else None
        )

        stage = str(request.get("stage") or params.get("stage") or "unknown")
        checkpoint_dir = Path(str(params.get("checkpoint_dir") or ".nodeflow/checkpoints"))
        if not checkpoint_dir.is_absolute():
            repo_root = params.get("_repo_root_for_paths")
            if isinstance(repo_root, str) and repo_root:
                checkpoint_dir = (Path(repo_root) / checkpoint_dir).resolve()
            else:
                workspace_dir = params.get("_workspace_dir")
                if isinstance(workspace_dir, str) and workspace_dir:
                    checkpoint_dir = (Path(workspace_dir) / checkpoint_dir).resolve()
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        run_id = str(params.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        file_path = checkpoint_dir / f"{run_id}_{stage}.json"

        merged_raw = dict(request.get("raw_results") or {})
        if execution_result is not None:
            merged_raw["execution_result"] = execution_result
        if test_result is not None:
            merged_raw["test_result"] = test_result
        if diff_result is not None:
            merged_raw["diff_result"] = diff_result
        if review_result is not None:
            merged_raw["review_result"] = review_result

        artifacts: List[Dict[str, Any]] = list(request.get("artifacts") or [])

        child_ok_values: List[bool] = []
        if execution_result is not None:
            child_ok_values.append(bool(execution_result.get("ok")))
        if test_result is not None:
            child_ok_values.append(bool(test_result.get("ok")))
        if diff_result is not None:
            child_ok_values.append(bool(diff_result.get("ok")))
        if review_result is not None:
            child_ok_values.append(bool(review_result.get("ok")))
        computed_ok = all(child_ok_values) if child_ok_values else True
        if "ok" in request:
            final_ok = computed_ok and bool(request["ok"])
        else:
            final_ok = computed_ok

        next_action_default = str(params.get("next_action_default") or "stop")
        if not final_ok:
            # Do not trust request.next_action on failure (e.g. stale "approve" from upstream checkpoint).
            next_action = str(
                request.get("next_action_on_failure")
                or params.get("next_action_on_failure")
                or "stop"
            )
        else:
            next_action = str(request.get("next_action") or next_action_default)

        approved_candidate_path: str | None = None
        if (
            bool(params.get("write_spec_plan_candidate"))
            and stage == "spec_plan"
            and final_ok
            and execution_result is not None
        ):
            stdout = execution_result.get("stdout")
            if isinstance(stdout, str):
                obj = _loads_first_json_object(stdout)
                if isinstance(obj, dict) and "spec" in obj and "plan" in obj:
                    suffix = str(params.get("spec_plan_candidate_suffix") or "approved_candidate")
                    candidate_path = checkpoint_dir / f"{run_id}_{suffix}.json"
                    slim = {"spec": obj["spec"], "plan": obj["plan"]}
                    candidate_path.write_text(
                        json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                    artifacts.append({"path": str(candidate_path), "kind": "spec_plan_candidate"})
                    approved_candidate_path = str(candidate_path)

        artifacts.append({"path": str(file_path), "kind": "checkpoint"})

        stage_result: Dict[str, Any] = {
            "ok": final_ok,
            "stage": stage,
            "summary": str(
                request.get("summary")
                or params.get("summary_default")
                or f"{stage} stage completed"
            ),
            "artifacts": artifacts,
            "next_action": next_action,
            "human_decision_required": bool(
                request.get(
                    "human_decision_required", params.get("human_decision_required_default", True)
                )
            ),
            "raw_results": merged_raw,
        }
        if approved_candidate_path:
            stage_result["approved_candidate_path"] = approved_candidate_path

        payload = {
            "schema_version": str(params.get("checkpoint_schema_version") or "development_flow.v1"),
            "written_at": datetime.now(timezone.utc).isoformat(),
            "stage_result": stage_result,
        }
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "checkpoint": {"path": str(file_path), "kind": "checkpoint"},
            "stage_result": stage_result,
        }
