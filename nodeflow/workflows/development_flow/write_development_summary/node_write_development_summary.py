"""Write development summary and suggest commit message."""

from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode


def _default_ignored_dirty_prefixes() -> List[str]:
    return [".nodeflow/"]


def _run_git(repo_root: Path, argv: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *argv],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )


def _first_existing(repo_root: Path, paths: List[str]) -> Path | None:
    for raw in paths:
        p = Path(raw)
        cand = p if p.is_absolute() else (repo_root / p).resolve()
        if cand.exists() and cand.is_file():
            return cand
    return None


def _resolve_commit_template_from_git_config(repo_root: Path) -> Path | None:
    cp = _run_git(repo_root, ["config", "--get", "commit.template"])
    if cp.returncode != 0:
        return None
    raw = (cp.stdout or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = (repo_root / p).resolve()
    return p if p.exists() and p.is_file() else None


def _extract_subject_style(subjects: List[str]) -> str:
    if not subjects:
        return "title"
    cc = re.compile(r"^([a-z]+)(\([^)]+\))?(!)?:\s+.+$")
    prefix_like = 0
    for s in subjects:
        m = cc.match(s)
        if m:
            prefix_like += 1
        elif ":" in s.split(" ", 1)[0]:
            prefix_like += 1
    return "prefix" if subjects and (prefix_like * 2 >= len(subjects)) else "title"


def _subject_lead(text: str, max_len: int = 72) -> str:
    if not text.strip():
        return ""
    first = text.strip().splitlines()[0].strip()
    first = re.sub(r"\s+", " ", first)
    return first[:max_len].rstrip()


def _suggest_subject(
    *, development_name: str, task_prompt: str, files: List[str], style: str
) -> str:
    lead = _subject_lead(development_name) or _subject_lead(task_prompt)
    area = "project"
    if files:
        first = Path(files[0])
        if first.parts:
            area = first.parts[0].replace("_", "-")
    if style == "prefix":
        prompt_lower = task_prompt.lower()
        cc_type = "fix" if ("fix" in prompt_lower or "bug" in prompt_lower) else "feat"
        if lead:
            return f"{cc_type}: {lead}"
        return f"{cc_type}: update {area}"
    if lead:
        return lead
    return f"Update {area}"


def _render_message_from_template(
    template_text: str,
    *,
    subject: str,
    task_prompt: str,
    area: str,
    changed_files: List[str],
) -> str:
    why = task_prompt.strip() or "Address the requested development task."
    what = f"Update {area} based on the approved implementation flow."
    impact = f"Affects {len(changed_files)} changed file(s)."

    replaced = template_text
    token_map = {
        "{{SUBJECT}}": subject,
        "{{WHY}}": why,
        "{{WHAT}}": what,
        "{{IMPACT}}": impact,
        "__SUBJECT__": subject,
        "__WHY__": why,
        "__WHAT__": what,
        "__IMPACT__": impact,
    }
    for token, value in token_map.items():
        replaced = replaced.replace(token, value)

    lines = [ln.rstrip() for ln in replaced.splitlines()]
    if not lines:
        return subject

    first_non_empty = -1
    for i, ln in enumerate(lines):
        if ln.strip():
            first_non_empty = i
            break
    if first_non_empty == -1:
        return subject

    if lines[first_non_empty].strip().startswith("#"):
        lines[first_non_empty] = subject
    elif "subject" in lines[first_non_empty].lower():
        lines[first_non_empty] = subject
    elif lines[first_non_empty].strip() == "":
        lines[first_non_empty] = subject
    else:
        lines.insert(first_non_empty, subject)

    has_why = any("why" in ln.lower() for ln in lines)
    has_what = any("what" in ln.lower() for ln in lines)
    has_impact = any("impact" in ln.lower() for ln in lines)
    if has_why or has_what or has_impact:
        content = "\n".join(lines).strip()
        if "why" not in content.lower():
            content += f"\n\nWhy\n{why}"
        if "what" not in content.lower():
            content += f"\n\nWhat\n{what}"
        if "impact" not in content.lower():
            content += f"\n\nImpact\n{impact}"
        return content
    return "\n".join(lines).strip()


class WriteDevelopmentSummaryNode(PythonActionNode):
    role = "write_development_summary"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        workspace_context = (
            inputs.get("workspace_context")
            if isinstance(inputs.get("workspace_context"), dict)
            else None
        )
        if not workspace_context:
            raise NodeExecutionFailure("workspace_context is required")
        run_context = (
            inputs.get("run_context") if isinstance(inputs.get("run_context"), dict) else None
        )
        if not run_context:
            raise NodeExecutionFailure("run_context is required")
        workspace_root_raw = workspace_context.get("workspace_root")
        if not isinstance(workspace_root_raw, str) or not workspace_root_raw.strip():
            raise NodeExecutionFailure("workspace_context.workspace_root is required")
        base_revision = workspace_context.get("base_revision")
        if not isinstance(base_revision, str) or not base_revision.strip():
            raise NodeExecutionFailure("workspace_context.base_revision is required")
        source_base_revision = str(run_context.get("source_base_revision") or "").strip()
        if not source_base_revision:
            raise NodeExecutionFailure("run_context.source_base_revision is required")
        if base_revision.strip() != source_base_revision:
            raise NodeExecutionFailure(
                "workspace_context.base_revision must match run_context.source_base_revision"
            )
        run_id = str(run_context.get("run_id") or "").strip()
        if not run_id:
            raise NodeExecutionFailure("run_context.run_id is required")
        rc_artifact_root = str(run_context.get("artifact_root") or "").strip()
        if not rc_artifact_root:
            raise NodeExecutionFailure("run_context.artifact_root is required")
        run_source_repo_raw = str(run_context.get("source_repo_root") or "").strip()
        if not run_source_repo_raw:
            raise NodeExecutionFailure("run_context.source_repo_root is required")

        repo_root = Path(workspace_root_raw).resolve()
        source_repo_raw = workspace_context.get("source_repo_root")
        if not isinstance(source_repo_raw, str) or not source_repo_raw.strip():
            raise NodeExecutionFailure("workspace_context.source_repo_root is required")
        source_repo_root = Path(source_repo_raw).resolve()
        run_source_repo_root = Path(run_source_repo_raw).resolve()
        if source_repo_root != run_source_repo_root:
            raise NodeExecutionFailure(
                "workspace_context.source_repo_root must match run_context.source_repo_root"
            )
        if not repo_root.exists():
            raise NodeExecutionFailure(f"repo_root does not exist: {repo_root}")
        raw_action = inputs.get("action")
        if not isinstance(raw_action, str) or not raw_action.strip():
            raise NodeExecutionFailure("action is required")
        action = raw_action.strip()
        task_prompt = str(inputs.get("task_prompt") or "")
        development_name = str(run_context.get("development_name") or "")

        if _run_git(repo_root, ["rev-parse", "--is-inside-work-tree"]).returncode != 0:
            raise NodeExecutionFailure(f"repo_root is not a git repository: {repo_root}")
        if not source_repo_root.exists():
            raise NodeExecutionFailure(f"source_repo_root does not exist: {source_repo_root}")
        if _run_git(source_repo_root, ["rev-parse", "--is-inside-work-tree"]).returncode != 0:
            raise NodeExecutionFailure(
                f"source_repo_root is not a git repository: {source_repo_root}"
            )

        template_path = _resolve_commit_template_from_git_config(source_repo_root)
        template_candidates = params.get("commit_template_candidates")
        if template_path is None:
            if isinstance(template_candidates, list):
                raw_candidates = [str(x) for x in template_candidates if isinstance(x, (str, Path))]
            else:
                raw_candidates = [
                    ".gitmessage",
                    ".gitmessage.txt",
                    ".github/commit_template.txt",
                    ".github/COMMIT_TEMPLATE.md",
                ]
            template_path = _first_existing(source_repo_root, raw_candidates)
        template_text = ""
        if template_path is not None:
            try:
                template_text = template_path.read_text(encoding="utf-8").strip()
            except OSError as e:
                raise NodeExecutionFailure(
                    f"failed to read commit template: {template_path}"
                ) from e
            except UnicodeDecodeError as e:
                raise NodeExecutionFailure(
                    f"commit template is not valid UTF-8: {template_path}"
                ) from e

        log_cp = _run_git(source_repo_root, ["log", "--format=%s", "-n", "20"])
        recent_subjects = (
            [ln.strip() for ln in (log_cp.stdout or "").splitlines() if ln.strip()]
            if log_cp.returncode == 0
            else []
        )

        diff_cp = _run_git(repo_root, ["diff", "--name-only", base_revision])
        if diff_cp.returncode != 0:
            err = (diff_cp.stderr or diff_cp.stdout or "").strip() or "git diff failed"
            raise NodeExecutionFailure(err)
        untracked_cp = _run_git(repo_root, ["ls-files", "--others", "--exclude-standard"])
        if untracked_cp.returncode != 0:
            err = (
                untracked_cp.stderr or untracked_cp.stdout or ""
            ).strip() or "git ls-files failed"
            raise NodeExecutionFailure(err)
        diff_files = [ln.strip() for ln in (diff_cp.stdout or "").splitlines() if ln.strip()]
        untracked_files = [
            ln.strip() for ln in (untracked_cp.stdout or "").splitlines() if ln.strip()
        ]
        raw_changed = sorted(set(diff_files + untracked_files))
        ignored_cf = params.get("ignored_changed_file_prefixes")
        if isinstance(ignored_cf, list):
            ign_prefixes = [str(x) for x in ignored_cf if isinstance(x, str)]
        else:
            ign_prefixes = _default_ignored_dirty_prefixes()
        changed_files = [
            f for f in raw_changed if not any(f.startswith(prefix) for prefix in ign_prefixes)
        ]

        style = _extract_subject_style(recent_subjects)
        subject = _suggest_subject(
            development_name=development_name,
            task_prompt=task_prompt,
            files=changed_files,
            style=style,
        )
        area = "project"
        if changed_files:
            first = Path(changed_files[0])
            if first.parts:
                area = first.parts[0].replace("_", "-")
        suggestion = subject
        if template_text:
            suggestion = _render_message_from_template(
                template_text,
                subject=subject,
                task_prompt=task_prompt,
                area=area,
                changed_files=changed_files,
            )

        impl_sr = inputs.get("implement_stage_result")
        review_sr = inputs.get("review_stage_result")
        summary_lines = [
            f"- action: {action}",
            f"- run_id: {run_id}",
            f"- task_prompt: {task_prompt or '(empty)'}",
            f"- base_revision: {base_revision}",
            f"- implement_ok: {bool(impl_sr.get('ok')) if isinstance(impl_sr, dict) else False}",
            f"- review_ok: {bool(review_sr.get('ok')) if isinstance(review_sr, dict) else False}",
            f"- next_action: {inputs.get('next_action')}",
            f"- merge_ready: {bool(inputs.get('merge_ready'))}",
            f"- changed_files_count: {len(changed_files)}",
        ]

        out_dir = Path(rc_artifact_root).resolve()
        summary_dir = out_dir / "summary"
        summary_dir.mkdir(parents=True, exist_ok=True)
        if action == "rework":
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            out_path = summary_dir / f"{action}_{stamp}_development_summary.json"
        else:
            out_path = summary_dir / f"{action}_development_summary.json"
        payload = {
            "schema_version": str(params.get("schema_version") or "development_flow.summary.v1"),
            "written_at": datetime.now(timezone.utc).isoformat(),
            "summary_lines": summary_lines,
            "commit_message_suggestion": suggestion,
            "commit_style_source": {
                "template_path": str(template_path) if template_path else None,
                "recent_subjects": recent_subjects[:5],
                "detected_style": style,
            },
            "changed_files": changed_files,
            "run_context": {
                "run_id": run_context.get("run_id"),
                "development_name": run_context.get("development_name"),
                "artifact_root": run_context.get("artifact_root"),
            },
            "workspace_context": {
                "strategy": workspace_context.get("strategy"),
                "source_repo_root": workspace_context.get("source_repo_root"),
                "workspace_root": workspace_context.get("workspace_root"),
                "current_branch": workspace_context.get("current_branch"),
                "planned_branch_name": workspace_context.get("planned_branch_name"),
                "base_revision": workspace_context.get("base_revision"),
            },
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "development_summary": {
                "summary_lines": summary_lines,
                "commit_message_suggestion": suggestion,
                "artifact_path": str(out_path),
            }
        }
