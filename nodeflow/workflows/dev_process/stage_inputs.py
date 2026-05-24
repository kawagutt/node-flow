"""Stage-level interactive input collection for dev-process."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional

import click

from nodeflow.core.base_node import NodeExecutionFailure

STAGE_INPUT_SCHEMA = "dev_process.stage_input.v1"
REFERENCE_MATERIALS_SCHEMA = "dev_process.reference_materials.v1"

TEXT_SUFFIXES = {
    ".md",
    ".txt",
    ".json",
    ".yaml",
    ".yml",
    ".py",
    ".rst",
    ".toml",
    ".ini",
    ".cfg",
    ".sh",
    ".bash",
    ".zsh",
    ".csv",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".go",
    ".rs",
    ".java",
    ".kt",
    ".c",
    ".h",
    ".cpp",
    ".hpp",
    ".sql",
    ".xml",
    ".html",
    ".css",
}

MAX_REFERENCE_CHARS_PER_FILE = 20_000

InputKind = Literal["text", "path_list"]


@dataclass(frozen=True)
class InputQuestion:
    key: str
    label: str
    required: bool
    kind: InputKind = "text"


PromptFn = Callable[[InputQuestion, Optional[str]], str]


def default_prompt_fn(question: InputQuestion, default: Optional[str] = None) -> str:
    if question.kind == "path_list":
        raw = click.prompt(question.label, default=default or "", show_default=bool(default))
        return raw
    return click.prompt(question.label, default=default or None, show_default=bool(default))


def _parse_path_list(raw: str) -> List[str]:
    text = (raw or "").strip()
    if not text:
        return []
    parts = [p.strip() for p in text.split(",")]
    return [p for p in parts if p]


def _normalize_value(question: InputQuestion, raw: Any) -> Any:
    if question.kind == "path_list":
        if raw is None:
            return []
        if isinstance(raw, list):
            return [str(p).strip() for p in raw if str(p).strip()]
        if isinstance(raw, str):
            return _parse_path_list(raw)
        raise NodeExecutionFailure(
            f"{question.key} must be a comma-separated string or list of paths"
        )
    if raw is None:
        return ""
    return str(raw).strip()


def _is_missing(question: InputQuestion, value: Any) -> bool:
    if question.kind == "path_list":
        return question.required and not value
    return question.required and not str(value or "").strip()


def _load_existing_input_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise NodeExecutionFailure(f"invalid stage input artifact {path}: {e}") from e
    if not isinstance(doc, dict):
        raise NodeExecutionFailure(f"stage input artifact must be a JSON object: {path}")
    inputs = doc.get("inputs")
    if isinstance(inputs, dict):
        return dict(inputs)
    return {}


def collect_stage_inputs(
    *,
    stage: str,
    questions: List[InputQuestion],
    provided: Dict[str, Any],
    interactive: bool,
    input_artifact_path: Optional[Path] = None,
    prompt_fn: PromptFn | None = None,
) -> Dict[str, Any]:
    """Resolve stage inputs: provided → input.json → interactive → fail."""
    prompt = prompt_fn or default_prompt_fn
    existing: Dict[str, Any] = {}
    if input_artifact_path is not None:
        existing = _load_existing_input_json(input_artifact_path)

    out: Dict[str, Any] = {}
    has_provided = any(
        provided.get(q.key) not in (None, "", []) for q in questions if q.key in provided
    )
    for q in questions:
        raw: Any = None
        if q.key in provided and provided[q.key] not in (None, ""):
            raw = provided[q.key]
        elif q.key in existing and existing[q.key] not in (None, ""):
            raw = existing[q.key]
        elif interactive and (q.required or not has_provided):
            default = None
            if q.key in existing:
                default = existing[q.key]
            elif q.key in provided:
                default = provided[q.key]
            if q.kind == "path_list" and isinstance(default, list):
                default = ", ".join(str(p) for p in default)
            prompted = prompt(q, str(default) if default not in (None, "") else None)
            raw = prompted
        else:
            raw = None

        value = _normalize_value(q, raw)
        if _is_missing(q, value):
            raise NodeExecutionFailure(
                f"{stage} requires {q.key!r} in non-interactive mode "
                f"(provide via CLI override or {stage}/input.json)"
            )
        out[q.key] = value
    return out


def write_stage_input_artifact(
    *,
    artifact_dir: Path,
    stage: str,
    inputs: Dict[str, Any],
) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / "input.json"
    doc = {
        "schema_version": STAGE_INPUT_SCHEMA,
        "stage": stage,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "inputs": inputs,
    }
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path.resolve()


def _read_text_excerpt(path: Path) -> tuple[str, bool]:
    suffix = path.suffix.lower()
    if suffix and suffix not in TEXT_SUFFIXES:
        return "", False
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return "", False
    except OSError as e:
        raise NodeExecutionFailure(f"cannot read reference file {path}: {e}") from e
    truncated = len(text) > MAX_REFERENCE_CHARS_PER_FILE
    if truncated:
        text = text[:MAX_REFERENCE_CHARS_PER_FILE]
    return text, truncated


def load_reference_materials(
    repo_root: Path,
    reference_paths: List[str],
) -> tuple[List[Dict[str, Any]], Path | None]:
    """Load reference file excerpts; return materials list and optional artifact path."""
    if not reference_paths:
        return [], None

    materials: List[Dict[str, Any]] = []
    for raw in reference_paths:
        p = Path(raw)
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        else:
            p = p.resolve()
        if not p.is_file():
            raise NodeExecutionFailure(f"reference path is not a file: {p}")
        text, truncated = _read_text_excerpt(p)
        entry: Dict[str, Any] = {"path": str(p)}
        if text:
            entry["text"] = text
            if truncated:
                entry["truncated"] = True
        else:
            entry["binary_or_unsupported"] = True
        materials.append(entry)
    return materials, None


def write_reference_materials_artifact(
    artifact_dir: Path,
    materials: List[Dict[str, Any]],
) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / "reference_materials.json"
    doc = {
        "schema_version": REFERENCE_MATERIALS_SCHEMA,
        "materials": materials,
    }
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path.resolve()


# --- Stage-specific question sets ---

SPEC_PLAN_QUESTIONS = [
    InputQuestion("task_prompt", "Task prompt", required=True, kind="text"),
    InputQuestion(
        "reference_paths",
        "Reference material paths (optional, comma-separated)",
        required=False,
        kind="path_list",
    ),
    InputQuestion(
        "notes",
        "Additional constraints or notes (optional)",
        required=False,
        kind="text",
    ),
]

REVISION_QUESTIONS = [
    InputQuestion("revision_comment", "Revision comment", required=True, kind="text"),
    InputQuestion(
        "reference_paths",
        "Additional reference paths (optional, comma-separated)",
        required=False,
        kind="path_list",
    ),
]

REWORK_QUESTIONS = [
    InputQuestion(
        "rework_comment",
        "Implementation feedback",
        required=True,
        kind="text",
    ),
]


def stage_input_dir(artifact_root: str, stage: str) -> Path:
    if stage == "revision":
        return Path(artifact_root) / "revision"
    if stage == "rework":
        return Path(artifact_root) / "rework"
    return Path(artifact_root) / stage


def collect_spec_plan_inputs(
    *,
    artifact_root: str,
    repo_root: Path,
    provided: Dict[str, Any],
    interactive: bool,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], Path, Optional[Path]]:
    art_dir = stage_input_dir(artifact_root, "spec_plan")
    inputs = collect_stage_inputs(
        stage="spec_plan",
        questions=SPEC_PLAN_QUESTIONS,
        provided=provided,
        interactive=interactive,
        input_artifact_path=art_dir / "input.json",
    )
    input_path = write_stage_input_artifact(
        artifact_dir=art_dir,
        stage="spec_plan",
        inputs=inputs,
    )
    ref_paths = inputs.get("reference_paths") or []
    materials, _ = load_reference_materials(repo_root, ref_paths)
    ref_artifact: Optional[Path] = None
    if materials:
        ref_artifact = write_reference_materials_artifact(art_dir, materials)
    return inputs, materials, input_path, ref_artifact


def collect_revision_inputs(
    *,
    artifact_root: str,
    repo_root: Path,
    provided: Dict[str, Any],
    interactive: bool,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], Path, Optional[Path]]:
    art_dir = stage_input_dir(artifact_root, "revision")
    inputs = collect_stage_inputs(
        stage="revision",
        questions=REVISION_QUESTIONS,
        provided=provided,
        interactive=interactive,
        input_artifact_path=art_dir / "input.json",
    )
    input_path = write_stage_input_artifact(
        artifact_dir=art_dir,
        stage="revision",
        inputs=inputs,
    )
    ref_paths = inputs.get("reference_paths") or []
    materials, _ = load_reference_materials(repo_root, ref_paths)
    ref_artifact: Optional[Path] = None
    if materials:
        ref_artifact = write_reference_materials_artifact(art_dir, materials)
    return inputs, materials, input_path, ref_artifact


def collect_rework_inputs(
    *,
    artifact_root: str,
    provided: Dict[str, Any],
    interactive: bool,
) -> tuple[Dict[str, Any], Path]:
    art_dir = stage_input_dir(artifact_root, "rework")
    inputs = collect_stage_inputs(
        stage="rework",
        questions=REWORK_QUESTIONS,
        provided=provided,
        interactive=interactive,
        input_artifact_path=art_dir / "input.json",
    )
    input_path = write_stage_input_artifact(
        artifact_dir=art_dir,
        stage="rework",
        inputs=inputs,
    )
    return inputs, input_path


def format_revision_context(
    revision_comment: str,
    reference_materials: List[Dict[str, Any]],
) -> str:
    parts = [revision_comment.strip()]
    if reference_materials:
        parts.append(
            "\n\nAdditional reference materials:\n"
            + json.dumps(reference_materials, ensure_ascii=False, indent=2)[:12000]
        )
    return "\n".join(parts)


def build_rework_context(
    rework_comment: str,
    review_stage: Optional[Dict[str, Any]],
) -> str:
    parts = [rework_comment.strip()]
    if isinstance(review_stage, dict):
        agg = review_stage.get("aggregate")
        if isinstance(agg, dict) and agg:
            parts.append(
                "\n\nPrior review aggregate:\n"
                + json.dumps(agg, ensure_ascii=False, indent=2)[:8000]
            )
        findings = review_stage.get("findings")
        if isinstance(findings, list) and findings:
            parts.append(
                "\n\nPrior review findings:\n"
                + json.dumps(findings, ensure_ascii=False, indent=2)[:8000]
            )
    return "\n".join(parts)
