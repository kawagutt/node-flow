"""Immutable versioned artifacts for spec/plan with latest aliases."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from nodeflow.workflows.dev_process.plan_phases import PlanData, save_plan_json

PLAN_VERSION_STATUS_COMMITTED = "committed"
PLAN_VERSION_STATUS_DRAFT_NOT_COMMITTED = "draft_not_committed"


def mark_plan_draft_pending_contract_validation(
    dp: Dict[str, Any],
    plan_stage: Dict[str, Any],
) -> None:
    """Record that ``plan/plan.md`` is a draft while ``current_plan_version`` is still accepted."""
    accepted = dp.get("current_plan_version", "")
    dp["draft_plan_pending_contract_validation"] = True
    dp["plan_version_status"] = PLAN_VERSION_STATUS_DRAFT_NOT_COMMITTED
    plan_stage["plan_version_deferred"] = True
    plan_stage["plan_version_status"] = PLAN_VERSION_STATUS_DRAFT_NOT_COMMITTED
    plan_stage["accepted_plan_version"] = accepted


def clear_plan_draft_pending_contract_validation(
    dp: Dict[str, Any],
    plan_stage: Dict[str, Any] | None = None,
) -> None:
    """Clear deferred draft flags after version commit or restore to accepted plan."""
    dp.pop("draft_plan_pending_contract_validation", None)
    dp["plan_version_status"] = PLAN_VERSION_STATUS_COMMITTED
    if plan_stage is None:
        return
    plan_stage.pop("plan_version_deferred", None)
    plan_stage["plan_version_status"] = PLAN_VERSION_STATUS_COMMITTED
    plan_stage.pop("accepted_plan_version", None)


def _version_tag(epoch: int, revision: int) -> str:
    return f"v{epoch:02d}_{revision:02d}"


def allocate_spec_version(dp: Dict[str, Any], *, epoch_bump: bool) -> str:
    """Allocate next spec version id (``spec_vXX_YY``) and update *dp* pointers."""
    if "spec_epoch" not in dp:
        dp["spec_epoch"] = 0

    epoch = int(dp["spec_epoch"])
    av = dp.setdefault("artifact_versions", {})
    spec_meta = av.setdefault("spec", {})
    rev = int(spec_meta.get("revision", -1))

    if epoch_bump:
        epoch += 1
        dp["spec_epoch"] = epoch
        rev = 0
    else:
        rev += 1

    spec_meta["revision"] = rev
    version_id = f"spec_{_version_tag(epoch, rev)}"
    dp["current_spec_version"] = version_id

    rel_versioned = f"spec/versions/{version_id}.md"
    rel_latest = "spec/spec.md"
    spec_meta.update(
        {
            "current": version_id,
            "latest_path": rel_latest,
            "versioned_path": rel_versioned,
        }
    )
    return version_id


def allocate_plan_version(dp: Dict[str, Any]) -> str:
    """Allocate next plan version id (``plan_vXX_YY``) for current spec epoch."""
    epoch = int(dp.get("spec_epoch", 0))
    av = dp.setdefault("artifact_versions", {})
    plan_meta = av.setdefault("plan", {})
    stored_epoch = plan_meta.get("spec_epoch")

    if stored_epoch != epoch:
        rev = 0
        plan_meta["spec_epoch"] = epoch
    else:
        rev = int(plan_meta.get("revision", -1)) + 1

    plan_meta["revision"] = rev
    version_id = f"plan_{_version_tag(epoch, rev)}"
    dp["current_plan_version"] = version_id

    rel_versioned_md = f"plan/versions/{version_id}.md"
    rel_versioned_json = f"plan/versions/{version_id}.json"
    rel_latest_md = "plan/plan.md"
    rel_latest_json = "plan/plan.json"
    plan_meta.update(
        {
            "current": version_id,
            "latest_path": rel_latest_md,
            "versioned_path": rel_versioned_md,
            "json_path": rel_versioned_json,
            "latest_json_path": rel_latest_json,
        }
    )
    return version_id


def write_versioned_spec(
    artifact_root: str,
    spec_text: str,
    dp: Dict[str, Any],
    *,
    epoch_bump: bool,
) -> Dict[str, str]:
    """Write immutable spec version and update latest ``spec/spec.md``."""
    version_id = allocate_spec_version(dp, epoch_bump=epoch_bump)
    root = Path(artifact_root) / "spec"
    versions_dir = root / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)
    versioned_path = versions_dir / f"{version_id}.md"
    latest_path = root / "spec.md"
    versioned_path.write_text(spec_text, encoding="utf-8")
    latest_path.write_text(spec_text, encoding="utf-8")
    return {
        "version": version_id,
        "versioned_path": str(versioned_path),
        "latest_path": str(latest_path),
    }


def write_plan_latest_only(artifact_root: str, plan_data: PlanData) -> Dict[str, str]:
    """Update latest ``plan/plan.md`` and ``plan/plan.json`` without bumping version metadata."""
    plan_dir = Path(artifact_root) / "plan"
    plan_dir.mkdir(parents=True, exist_ok=True)
    latest_md = plan_dir / "plan.md"
    latest_json = plan_dir / "plan.json"
    latest_md.write_text(plan_data.raw_text, encoding="utf-8")
    save_plan_json(plan_data, str(plan_dir))
    return {
        "latest_path": str(latest_md),
        "latest_json_path": str(latest_json),
    }


def commit_plan_version(
    artifact_root: str,
    plan_data: PlanData,
    dp: Dict[str, Any],
    *,
    continuation_raw_md: str | None = None,
    continuation_plan: PlanData | None = None,
) -> Dict[str, str]:
    """Allocate plan version and write immutable snapshot under ``plan/versions/``."""
    version_id = allocate_plan_version(dp)
    plan_dir = Path(artifact_root) / "plan"
    versions_dir = plan_dir / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)

    versioned_md = versions_dir / f"{version_id}.md"
    versioned_json = versions_dir / f"{version_id}.json"
    versioned_md.write_text(plan_data.raw_text, encoding="utf-8")
    versioned_json.write_text(
        json.dumps(plan_data.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    result: Dict[str, str] = {
        "version": version_id,
        "versioned_path": str(versioned_md),
        "versioned_json_path": str(versioned_json),
        "latest_path": str(plan_dir / "plan.md"),
        "latest_json_path": str(plan_dir / "plan.json"),
    }

    if continuation_plan is not None and continuation_raw_md is not None:
        cont_dir = plan_dir / "continuations"
        cont_dir.mkdir(parents=True, exist_ok=True)
        cont_md = cont_dir / f"continuation_{version_id}.md"
        cont_json = cont_dir / f"continuation_{version_id}.json"
        cont_md.write_text(continuation_raw_md, encoding="utf-8")
        cont_json.write_text(
            json.dumps(continuation_plan.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        result["continuation_md_path"] = str(cont_md)
        result["continuation_json_path"] = str(cont_json)

    return result


def write_versioned_plan(
    artifact_root: str,
    plan_data: PlanData,
    dp: Dict[str, Any],
    *,
    continuation_raw_md: str | None = None,
    continuation_plan: PlanData | None = None,
) -> Dict[str, str]:
    """Write latest plan files and immutable version snapshot."""
    latest = write_plan_latest_only(artifact_root, plan_data)
    committed = commit_plan_version(
        artifact_root,
        plan_data,
        dp,
        continuation_raw_md=continuation_raw_md,
        continuation_plan=continuation_plan,
    )
    return {**latest, **committed}


def stamp_phase_artifact_versions(dp: Dict[str, Any], phase_id: str) -> None:
    """Record which spec/plan versions a phase execution is based on."""
    if not phase_id:
        return
    results = dp.setdefault("phase_results", {})
    pr = results.setdefault(phase_id, {})
    pr["spec_version"] = dp.get("current_spec_version", "")
    pr["plan_version"] = dp.get("current_plan_version", "")


def current_spec_path(artifact_root: str) -> Path:
    return Path(artifact_root) / "spec" / "spec.md"


def current_plan_path(artifact_root: str) -> Path:
    return Path(artifact_root) / "plan" / "plan.md"


def ensure_continuation_base_plan_version(dp: Dict[str, Any]) -> str:
    """Pin accepted plan version for continuation retries (set once per continuation cycle)."""
    pinned = dp.get("continuation_base_plan_version", "")
    if pinned:
        return str(pinned)
    base = str(dp.get("current_plan_version", "") or "").strip()
    if not base:
        from nodeflow.core.base_node import NodeExecutionFailure

        raise NodeExecutionFailure(
            "continuation planning requires current_plan_version (accepted plan snapshot)"
        )
    dp["continuation_base_plan_version"] = base
    return base


def load_versioned_plan(artifact_root: str, version_id: str) -> PlanData:
    """Load immutable plan version under ``plan/versions/`` (with md/json consistency checks)."""
    from nodeflow.workflows.dev_process.phase_loop import load_plan_data_from_json

    plan_dir = Path(artifact_root) / "plan" / "versions"
    json_path = plan_dir / f"{version_id}.json"
    if not json_path.exists():
        from nodeflow.core.base_node import NodeExecutionFailure

        raise NodeExecutionFailure(f"versioned plan.json not found: {json_path}")
    return load_plan_data_from_json(json_path)


def restore_plan_latest_from_version(artifact_root: str, version_id: str) -> None:
    """Restore ``plan/plan.md`` and ``plan/plan.json`` from a committed version snapshot."""
    plan_data = load_versioned_plan(artifact_root, version_id)
    write_plan_latest_only(artifact_root, plan_data)


def restore_plan_version_pointer(dp: Dict[str, Any], version_id: str) -> None:
    """Reset ``current_plan_version`` and ``artifact_versions.plan`` to a committed version."""
    import re

    if not version_id:
        return
    dp["current_plan_version"] = version_id
    dp["plan_version_status"] = PLAN_VERSION_STATUS_COMMITTED
    dp.pop("draft_plan_pending_contract_validation", None)
    epoch = int(dp.get("spec_epoch", 0))
    av = dp.setdefault("artifact_versions", {})
    plan_meta = av.setdefault("plan", {})
    rev = int(plan_meta.get("revision", 0))
    m = re.match(r"plan_v\d+_(\d+)$", version_id)
    if m:
        rev = int(m.group(1))
    plan_meta.update(
        {
            "current": version_id,
            "revision": rev,
            "spec_epoch": epoch,
            "latest_path": "plan/plan.md",
            "versioned_path": f"plan/versions/{version_id}.md",
            "json_path": f"plan/versions/{version_id}.json",
            "latest_json_path": "plan/plan.json",
        }
    )


def review_aggregate_metadata(dp: Dict[str, Any], *, review_scope: str) -> Dict[str, str]:
    """Build version metadata for review aggregate.json."""
    meta: Dict[str, str] = {
        "review_scope": review_scope,
        "spec_version": str(dp.get("current_spec_version", "")),
        "plan_version": str(dp.get("current_plan_version", "")),
    }
    if review_scope == "phase":
        meta["phase_id"] = str(dp.get("current_phase_id", ""))
    return meta


def spec_review_dir(artifact_root: str, dp: Dict[str, Any]) -> Path:
    version = dp.get("current_spec_version", "spec_unknown")
    return Path(artifact_root) / "reviews" / f"spec_review_{version}"


def plan_review_dir(artifact_root: str, dp: Dict[str, Any]) -> Path:
    version = dp.get("current_plan_version", "plan_unknown")
    return Path(artifact_root) / "reviews" / f"plan_review_{version}"
