"""Apply exec_policy model/session fields to worker argv before subprocess exec.

Codex argv shape when both model and resume are set (exec-level flags before
``resume``; verified against Codex CLI)::

    codex exec --model <slug> [other codex flags...] resume <provider_session_id> [-- user cmd...]

Only the option zone (between ``exec`` and the first top-level ``--``, if present) is
rewritten; passthrough arguments after that ``--`` are never modified. ``codex exec`` after
a top-level ``--`` is not an injection target.

``provider_session_id`` is strict at **argv application**: the argv must embed
``codex exec``; otherwise execution fails instead of recording an unapplied resume
request. The provider may return a different canonical session id; both requested and
applied ids are recorded in evidence.

If ``exec_policy`` omits ``model`` but argv already contains ``--model`` in the option
zone, argv is left as-is and ``NodeRun.model`` / evidence ``model`` may remain null;
``argv`` in evidence is the source of truth in that case.

For argv without ``codex exec`` (e.g. ``review_argv_override``), argv is unchanged and
``model`` from policy is recorded for audit only when no injection occurs.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import EXEC_WORKER_CODEX
from nodeflow.workflows.dev_process.review_agent_model import (
    PROFILE_CHEAP_AUX,
    PROFILE_CODE_MAIN,
    PROFILE_STRONG_REASONING,
)

# Audit profile keys → default Codex model slugs (override via exec_policy.nodes.<name>.model).
_PROFILE_TO_CODEX_MODEL: dict[str, str] = {
    PROFILE_STRONG_REASONING: "claude-opus-4-7-thinking-xhigh",
    PROFILE_CODE_MAIN: "gpt-5.3-codex-high-fast",
    PROFILE_CHEAP_AUX: "gpt-5.5-medium",
}


def resolve_worker_model(model: Optional[str]) -> Optional[str]:
    """Map audit profile keys to Codex model slugs; pass through explicit slugs."""
    if not isinstance(model, str) or not model.strip():
        return None
    key = model.strip()
    return _PROFILE_TO_CODEX_MODEL.get(key, key)


def _argv_before_first_double_dash(argv: List[str]) -> List[str]:
    """Argv tokens before the first top-level ``--`` delimiter (exclusive)."""
    if "--" in argv:
        return argv[: argv.index("--")]
    return argv


def _is_codex_exec_argv(argv: List[str]) -> bool:
    """True when ``codex exec`` appears before the first top-level ``--``."""
    zone = _argv_before_first_double_dash(argv)
    try:
        codex_idx = zone.index("codex")
    except ValueError:
        return False
    return codex_idx + 1 < len(zone) and zone[codex_idx + 1] == "exec"


def prepare_worker_argv(
    worker_kind: str,
    argv: List[str],
    *,
    model: Optional[str],
    provider_session_id: Optional[str] = None,
) -> Tuple[List[str], Optional[str]]:
    """Return argv ready for subprocess exec and the resolved model slug for audit."""
    resolved_model = resolve_worker_model(model)
    if worker_kind == EXEC_WORKER_CODEX:
        if _is_codex_exec_argv(argv):
            if provider_session_id or resolved_model:
                out = _apply_codex_exec_policy(
                    argv,
                    model=resolved_model,
                    provider_session_id=(
                        provider_session_id.strip() if provider_session_id else None
                    ),
                )
                return out, resolved_model
            return list(argv), resolved_model
        if provider_session_id:
            raise NodeExecutionFailure("provider_session_id requires argv containing 'codex exec'")
        return list(argv), resolved_model
    if resolved_model or provider_session_id:
        raise NodeExecutionFailure(
            f"worker {worker_kind!r} does not support exec_policy model/session injection"
        )
    return list(argv), resolved_model


def _split_codex_exec_zones(argv: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """Return ``(prefix through exec, option_zone, passthrough from -- inclusive)``."""
    exec_idx = _codex_exec_index(argv)
    prefix = argv[: exec_idx + 1]
    rest = argv[exec_idx + 1 :]
    if "--" in rest:
        sep = rest.index("--")
        return prefix, rest[:sep], rest[sep:]
    return prefix, rest, []


def _apply_codex_exec_policy(
    argv: List[str],
    *,
    model: Optional[str],
    provider_session_id: Optional[str],
) -> List[str]:
    prefix, option_zone, passthrough = _split_codex_exec_zones(argv)
    zone = _remove_resume_from_zone(option_zone)
    zone = _remove_model_from_zone(zone)
    head: List[str] = []
    if model:
        head.extend(["--model", model])
    tail: List[str] = []
    if provider_session_id:
        tail.extend(["resume", provider_session_id])
    return prefix + head + zone + tail + passthrough


def _codex_exec_index(argv: List[str]) -> int:
    zone = _argv_before_first_double_dash(argv)
    try:
        codex_idx = zone.index("codex")
    except ValueError as e:
        raise NodeExecutionFailure("codex argv must contain 'codex' before passthrough '--'") from e
    if codex_idx + 1 >= len(zone) or zone[codex_idx + 1] != "exec":
        raise NodeExecutionFailure("codex argv must contain 'codex exec' before passthrough '--'")
    return codex_idx + 1


def _remove_resume_from_zone(zone: List[str]) -> List[str]:
    out: List[str] = []
    i = 0
    while i < len(zone):
        if zone[i] == "resume":
            if i + 1 >= len(zone):
                raise NodeExecutionFailure("codex exec resume requires a session id")
            i += 2
            continue
        out.append(zone[i])
        i += 1
    return out


def _remove_model_from_zone(zone: List[str]) -> List[str]:
    return _remove_option_with_value(zone, ("--model", "-m"))


def _remove_option_with_value(argv: List[str], flags: Tuple[str, ...]) -> List[str]:
    out: List[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        removed = False
        for flag in flags:
            if arg == flag:
                if i + 1 >= len(argv):
                    raise NodeExecutionFailure(f"{arg!r} requires a value")
                i += 2
                removed = True
                break
            prefix = f"{flag}="
            if arg.startswith(prefix):
                i += 1
                removed = True
                break
        if removed:
            continue
        out.append(arg)
        i += 1
    return out
