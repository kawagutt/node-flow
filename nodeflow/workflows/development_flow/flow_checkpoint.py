from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure


def read_json_required(path: Path, *, label: str) -> Dict[str, Any]:
    if not path.exists():
        raise NodeExecutionFailure(f"{label} not found: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"{label} is not valid JSON: {path}") from e
    except OSError as e:
        raise NodeExecutionFailure(f"{label} could not be read: {path}") from e
    if not isinstance(obj, dict):
        raise NodeExecutionFailure(f"{label} must be a JSON object: {path}")
    return obj


def write_flow_checkpoint(
    *,
    repo_root: Path,
    params: Dict[str, Any],
    flow_result: Dict[str, Any],
    run_id: str,
    action: str,
) -> str:
    cp_dir = Path(str(params.get("checkpoint_dir") or ".nodeflow/checkpoints"))
    if not cp_dir.is_absolute():
        cp_dir = (repo_root / cp_dir).resolve()
    cp_dir.mkdir(parents=True, exist_ok=True)
    fp = cp_dir / f"{run_id}_{action}_flow.json"
    flow_for_disk = dict(flow_result)
    flow_for_disk["flow_checkpoint_path"] = str(fp)
    payload = {
        "schema_version": str(
            params.get("flow_checkpoint_schema_version") or "development_flow.flow.v1"
        ),
        "written_at": datetime.now(timezone.utc).isoformat(),
        "flow_result": flow_for_disk,
    }
    fp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(fp)
