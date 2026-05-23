"""Exec evidence recording and validation (P3)."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from nodeflow.core.base_node import NodeExecutionFailure

_LOG = logging.getLogger(__name__)

_STUB_MARKERS = frozenset({"stub", "manual", "synthetic", "fabricated"})
_SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

_REQUIRED_EVIDENCE_FIELDS = (
    "evidence_id",
    "execution_fingerprint",
    "stdout_sha256",
    "stderr_sha256",
    "prompt_sha256",
    "argv",
    "cwd",
    "started_at",
    "ended_at",
    "stage",
    "invoker",
    "run_id",
)

_STAGES_REQUIRING_EVIDENCE = ("spec_plan", "implement", "review")


def _sha256_text(text: str | None) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_name_component(label: str, value: str) -> str:
    if not _SAFE_COMPONENT_RE.fullmatch(value):
        raise NodeExecutionFailure(
            f"{label} {value!r} contains unsafe characters; allowed [A-Za-z0-9_.-]+"
        )
    return value


def _execution_fingerprint(
    *,
    stdout_sha256: str,
    stderr_sha256: str,
    prompt_sha256: str,
    argv: Optional[List[str]],
    cwd: Optional[str],
    started_at: str,
    ended_at: str,
    exit_code: Optional[int],
) -> str:
    blob = json.dumps(
        {
            "stdout_sha256": stdout_sha256,
            "stderr_sha256": stderr_sha256,
            "prompt_sha256": prompt_sha256,
            "argv": argv or [],
            "cwd": cwd or "",
            "started_at": started_at,
            "ended_at": ended_at,
            "exit_code": exit_code,
        },
        sort_keys=True,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _reject_stub_or_manual_in_doc(doc: Dict[str, Any], *, where: str) -> None:
    provider_meta = doc.get("provider_meta")
    if isinstance(provider_meta, dict):
        for key in ("marker", "evidence_class", "source"):
            val = provider_meta.get(key)
            if isinstance(val, str) and val.strip().lower() in _STUB_MARKERS:
                raise NodeExecutionFailure(
                    f"exec evidence rejected ({where}): stub/manual marker in provider_meta.{key}"
                )
    summary = doc.get("summary")
    if isinstance(summary, str) and summary.strip().lower() in _STUB_MARKERS:
        raise NodeExecutionFailure(
            f"exec evidence rejected ({where}): stub/manual marker in summary"
        )


def _reject_stub_or_manual(execution_output: Dict[str, Any]) -> None:
    _reject_stub_or_manual_in_doc(execution_output, where="record")


def _validate_evidence_doc(path: Path, doc: Dict[str, Any]) -> None:
    for field in _REQUIRED_EVIDENCE_FIELDS:
        if field not in doc:
            raise NodeExecutionFailure(f"evidence missing required field {field!r}: {path}")
    eid = doc.get("evidence_id")
    if not isinstance(eid, str) or not eid.strip():
        raise NodeExecutionFailure(f"evidence missing evidence_id: {path}")
    fp = doc.get("execution_fingerprint")
    if not isinstance(fp, str) or not fp:
        raise NodeExecutionFailure(f"evidence missing execution_fingerprint: {path}")

    _reject_stub_or_manual_in_doc(doc, where=str(path))

    exit_code = doc.get("exit_code")
    if exit_code is not None:
        try:
            code = int(exit_code)
        except (TypeError, ValueError) as e:
            raise NodeExecutionFailure(f"evidence exit_code must be int: {path}") from e
        if code != 0:
            raise NodeExecutionFailure(f"evidence records non-zero exit_code={code}: {path}")


def _load_evidence_docs(evidence_dir: Path, *, run_id: str) -> List[tuple[Path, Dict[str, Any]]]:
    docs: List[tuple[Path, Dict[str, Any]]] = []
    if not evidence_dir.is_dir():
        return docs
    for path in sorted(evidence_dir.glob("*.json")):
        try:
            raw = path.read_text(encoding="utf-8")
            doc = json.loads(raw)
        except OSError as e:
            raise NodeExecutionFailure(f"cannot read evidence file {path}: {e}") from e
        except json.JSONDecodeError as e:
            raise NodeExecutionFailure(f"invalid evidence JSON in {path}: {e}") from e
        if not isinstance(doc, dict):
            raise NodeExecutionFailure(f"evidence root must be object in {path}")
        if doc.get("run_id") != run_id:
            continue
        _validate_evidence_doc(path, doc)
        docs.append((path, doc))
    return docs


def validate_evidence_store(artifact_root: str, *, run_id: str) -> List[str]:
    """Validate evidence files for a run. Returns warning messages (no side effects)."""
    evidence_dir = Path(artifact_root) / "evidence"
    docs = _load_evidence_docs(evidence_dir, run_id=run_id)
    warnings: List[str] = []

    seen_ids: dict[str, str] = {}
    seen_fingerprints: dict[str, str] = {}
    seen_stdout: dict[str, str] = {}

    for path, doc in docs:
        eid = str(doc["evidence_id"])
        prev_id = seen_ids.get(eid)
        if prev_id is not None:
            raise NodeExecutionFailure(
                f"duplicate exec evidence evidence_id {eid!r} ({prev_id} and {path})"
            )
        seen_ids[eid] = str(path)

        fp = str(doc["execution_fingerprint"])
        prev_fp = seen_fingerprints.get(fp)
        if prev_fp is not None:
            raise NodeExecutionFailure(
                f"duplicate exec evidence execution_fingerprint {fp!r} ({prev_fp} and {path})"
            )
        seen_fingerprints[fp] = str(path)

        stdout_sha = doc.get("stdout_sha256")
        if isinstance(stdout_sha, str) and stdout_sha:
            prev_stdout = seen_stdout.get(stdout_sha)
            if prev_stdout is not None and prev_stdout != str(path):
                warnings.append(
                    f"stdout_sha256 {stdout_sha[:12]}... repeated across "
                    f"{prev_stdout} and {path}"
                )
            seen_stdout[stdout_sha] = str(path)

    return warnings


def assert_expected_stage_evidence(body: Dict[str, Any], *, run_id: str) -> List[str]:
    """Require stages.*.evidence_paths exist on disk, then validate the evidence store."""
    run_context = body.get("run_context") or {}
    artifact_root = str(run_context.get("artifact_root") or "")
    if not artifact_root:
        raise NodeExecutionFailure("run_context.artifact_root is required for evidence validation")

    stages = body.get("stages") or {}
    evidence_dir = (Path(artifact_root).resolve() / "evidence").resolve()

    for name in _STAGES_REQUIRING_EVIDENCE:
        st = stages.get(name) or {}
        if st.get("status") != "completed":
            raise NodeExecutionFailure(f"stages.{name}.status must be completed before final gate")
        paths = st.get("evidence_paths")
        if not isinstance(paths, list) or not paths:
            raise NodeExecutionFailure(f"stages.{name}.evidence_paths missing or empty")
        for raw in paths:
            if not isinstance(raw, str) or not raw.strip():
                raise NodeExecutionFailure(f"stages.{name}.evidence_paths contains invalid entry")
            p = Path(raw).resolve()
            try:
                p.relative_to(evidence_dir)
            except ValueError as e:
                raise NodeExecutionFailure(
                    f"stages.{name}.evidence_paths entry escapes evidence/: {p}"
                ) from e
            if not p.is_file():
                raise NodeExecutionFailure(f"evidence file missing: {p}")
            doc = read_json_evidence(p)
            if doc.get("run_id") != run_id:
                raise NodeExecutionFailure(
                    f"evidence run_id mismatch in {p}: {doc.get('run_id')!r} != {run_id!r}"
                )

    return validate_evidence_store(artifact_root, run_id=run_id)


def read_json_evidence(path: Path) -> Dict[str, Any]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise NodeExecutionFailure(f"cannot read evidence {path}: {e}") from e
    if not isinstance(doc, dict):
        raise NodeExecutionFailure(f"evidence must be object: {path}")
    return doc


def record_exec_evidence(
    *,
    artifact_root: str,
    run_id: str,
    stage: str,
    invoker: str,
    execution_output: Dict[str, Any],
    argv: Optional[List[str]] = None,
    prompt: Optional[str] = None,
    cwd: Optional[str] = None,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
) -> str:
    """Write one evidence record and validate the store for this run."""
    _reject_stub_or_manual(execution_output)
    stage = _sanitize_name_component("stage", stage)
    invoker = _sanitize_name_component("invoker", invoker)

    started = started_at or _utc_now_iso()
    ended = ended_at or _utc_now_iso()
    stdout = execution_output.get("stdout")
    stderr = execution_output.get("stderr")
    raw = execution_output.get("raw_output")
    exit_code: Optional[int] = None
    if isinstance(raw, dict) and raw.get("returncode") is not None:
        try:
            exit_code = int(raw["returncode"])
        except (TypeError, ValueError):
            exit_code = None

    if exit_code is not None and exit_code != 0:
        raise NodeExecutionFailure(f"exec evidence rejected: non-zero exit_code={exit_code}")

    stdout_sha256 = _sha256_text(stdout if isinstance(stdout, str) else None)
    stderr_sha256 = _sha256_text(stderr if isinstance(stderr, str) else None)
    prompt_sha256 = _sha256_text(prompt)
    execution_fingerprint = _execution_fingerprint(
        stdout_sha256=stdout_sha256,
        stderr_sha256=stderr_sha256,
        prompt_sha256=prompt_sha256,
        argv=argv,
        cwd=cwd,
        started_at=started,
        ended_at=ended,
        exit_code=exit_code,
    )

    evidence_id = uuid.uuid4().hex
    provider_meta = execution_output.get("provider_meta")
    if not isinstance(provider_meta, dict):
        provider_meta = {}

    doc: Dict[str, Any] = {
        "evidence_id": evidence_id,
        "run_id": run_id,
        "stage": stage,
        "invoker": invoker,
        "execution_fingerprint": execution_fingerprint,
        "stdout_sha256": stdout_sha256,
        "stderr_sha256": stderr_sha256,
        "prompt_sha256": prompt_sha256,
        "argv": argv,
        "cwd": cwd,
        "started_at": started,
        "ended_at": ended,
        "exit_code": exit_code,
        "ok": bool(execution_output.get("ok")),
        "provider": execution_output.get("provider"),
        "external_executor": execution_output.get("external_executor"),
        "provider_meta": provider_meta,
    }
    for optional in ("token_count", "tokens", "session_id"):
        if optional in provider_meta:
            doc[optional] = provider_meta[optional]

    evidence_dir = Path(artifact_root) / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    path = evidence_dir / f"{stage}_{invoker}_{evidence_id[:12]}.json"
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")

    warnings = validate_evidence_store(artifact_root, run_id=run_id)
    for msg in warnings:
        _LOG.warning("dev_process evidence warning [%s]: %s", run_id, msg)
    return str(path)


assert_no_duplicate_composite = validate_evidence_store
