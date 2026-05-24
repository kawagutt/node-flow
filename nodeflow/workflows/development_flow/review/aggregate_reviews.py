"""Aggregate review signals: parse JSON from LLM stdout, merge with exec/diff signals."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, List, Tuple

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.review.review_parse import (
    parse_review_contract_from_execution_output,
    validate_review_contract_payload,
)


def _normalize_findings(items: Any, *, default_area: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(items, list):
        return out
    for i, it in enumerate(items):
        if not isinstance(it, dict):
            continue
        fid = str(it.get("id") or f"R_{default_area}_{i}")
        out.append(
            {
                "id": fid,
                "area": str(it.get("area") or default_area),
                "summary": str(it.get("summary") or ""),
                "suggested_fix": it.get("suggested_fix"),
            }
        )
    return out


def _consume_review(
    er: Dict[str, Any],
    *,
    label: str,
    default_area: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool | None, bool]:
    """
    Returns (blocking, non_blocking, spec_revision_flag_or_none, parse_failed).
    spec_revision None means absent from parsed payload.
    """
    blocking: List[Dict[str, Any]] = []
    non_blocking: List[Dict[str, Any]] = []
    spec_revision: bool | None = None

    tag = label.upper()

    if er.get("ok") is False:
        blocking.append(
            {
                "id": f"R_{tag}_EXEC",
                "area": default_area,
                "summary": f"{label} review subprocess reported ok=false",
                "suggested_fix": "inspect argv, cwd, and CLI logs",
            }
        )
        return blocking, non_blocking, spec_revision, False

    parsed_ok, payload = parse_review_contract_from_execution_output(er)
    if not parsed_ok:
        blocking.append(
            {
                "id": f"R_{tag}_PARSE",
                "area": "review",
                "summary": f"{label} review output was not valid JSON",
                "suggested_fix": "require the model to emit only the JSON object described in the prompt",
            }
        )
        return blocking, non_blocking, spec_revision, True

    if not validate_review_contract_payload(payload):
        blocking.append(
            {
                "id": f"R_{tag}_PARSE",
                "area": "review",
                "summary": f"{label} review output did not match the required JSON contract",
                "suggested_fix": "emit ok, blocking_findings, non_blocking_findings, and spec_revision_needed",
            }
        )
        return blocking, non_blocking, spec_revision, True

    blocking.extend(
        _normalize_findings(payload.get("blocking_findings"), default_area=default_area)
    )
    non_blocking.extend(
        _normalize_findings(payload.get("non_blocking_findings"), default_area=default_area)
    )
    if "spec_revision_needed" in payload:
        spec_revision = bool(payload.get("spec_revision_needed"))

    llm_ok = bool(payload.get("ok", True))
    if not llm_ok or blocking:
        if not blocking:
            blocking.append(
                {
                    "id": f"R_{tag}_OK_FALSE",
                    "area": default_area,
                    "summary": f'{label} review reported "ok": false with no structured findings',
                    "suggested_fix": "list blocking_findings in the JSON contract",
                }
            )
    return blocking, non_blocking, spec_revision, False


class AggregateReviewsNode(PythonActionNode):
    role = "aggregate_reviews"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        review_map = {
            "diff": ("review_diff", "diff"),
            "wide": ("review_wide", "diff"),
            "tests": ("review_tests", "tests"),
            "spec": ("review_spec", "spec"),
            "spec_revision": ("review_spec_revision", "spec"),
        }
        test_result = (
            inputs.get("test_result") if isinstance(inputs.get("test_result"), dict) else {}
        )
        diff_result = (
            inputs.get("diff_result") if isinstance(inputs.get("diff_result"), dict) else {}
        )

        raw_expected = params.get("expected_review_keys")
        if raw_expected is None:
            expected_keys = {input_key for _, (input_key, _) in review_map.items()}
        else:
            if not isinstance(raw_expected, (list, tuple)) or not all(
                isinstance(x, str) for x in raw_expected
            ):
                raise NodeExecutionFailure(
                    "expected_review_keys must be a list of reviewer input_key strings"
                )
            expected_keys = set(raw_expected)
            valid_keys = {input_key for _, (input_key, _) in review_map.items()}
            unknown = expected_keys - valid_keys
            if unknown:
                raise NodeExecutionFailure(
                    f"expected_review_keys contains unknown keys: {sorted(unknown)!r}"
                )

        blocking_findings: List[Dict[str, Any]] = []
        non_blocking_findings: List[Dict[str, Any]] = []
        spec_flags: List[bool] = []

        for label, (input_key, area) in review_map.items():
            if input_key not in expected_keys:
                continue
            er = inputs.get(input_key)
            if not isinstance(er, dict):
                blocking_findings.append(
                    {
                        "id": f"R_{label.upper()}_MISSING",
                        "area": "review",
                        "summary": f"{label} review result is missing",
                        "suggested_fix": f"wire {input_key} output into aggregate_reviews inputs",
                    }
                )
                continue
            b, nb, s, _ = _consume_review(er, label=label, default_area=area)
            blocking_findings.extend(b)
            non_blocking_findings.extend(nb)
            if s is not None:
                spec_flags.append(s)

        if test_result.get("ok") is False:
            blocking_findings.append(
                {
                    "id": "R_TEST",
                    "area": "tests",
                    "summary": "tests did not pass",
                    "suggested_fix": "fix failing tests before merge decision",
                }
            )

        if diff_result.get("ok") is False:
            blocking_findings.append(
                {
                    "id": "R_DIFF_COLLECT",
                    "area": "diff",
                    "summary": "failed to collect git diff/status for review",
                    "suggested_fix": "inspect git_returncodes and fix repo/base_ref before review",
                }
            )

        untracked = diff_result.get("untracked_files")
        if isinstance(untracked, list) and len(untracked) > 0:
            blocking_findings.append(
                {
                    "id": "R_UNTRACKED_FILES",
                    "area": "diff",
                    "summary": "untracked files exist and may not be fully represented in git diff",
                    "suggested_fix": "git add tracked paths or extend review to include untracked contents",
                }
            )

        if (
            diff_result.get("ok") is not False
            and not diff_result.get("diff")
            and not (isinstance(untracked, list) and len(untracked) > 0)
        ):
            non_blocking_findings.append(
                {
                    "id": "N_DIFF_EMPTY",
                    "area": "diff",
                    "summary": "no diff text and no untracked paths were captured for review context",
                }
            )

        spec_revision_needed = bool(params.get("spec_revision_needed_default", False))
        spec_revision_needed = spec_revision_needed or any(spec_flags)

        if spec_revision_needed:
            decision = "revise_spec_plan"
        elif blocking_findings:
            decision = "rework"
        else:
            decision = "merge_ok"

        mapped_next = {
            "merge_ok": "merge",
            "rework": "rework",
            "revise_spec_plan": "revise_spec",
        }.get(decision, "stop")

        checkpoint_request = {
            "ok": not blocking_findings,
            "stage": "review",
            "summary": "review aggregation completed",
            "artifacts": [],
            "next_action": mapped_next,
            # WriteCheckpointNode ignores request.next_action when final_ok=false; mirror driver hint here.
            "next_action_on_failure": mapped_next,
            "human_decision_required": True,
            "raw_results": {
                "decision": decision,
                "blocking_findings": blocking_findings,
                "non_blocking_findings": non_blocking_findings,
                "spec_revision_needed": spec_revision_needed,
            },
        }

        return {
            "review_result": {
                "ok": not blocking_findings,
                "decision": decision,
                "blocking_findings": blocking_findings,
                "non_blocking_findings": non_blocking_findings,
                "spec_revision_needed": spec_revision_needed,
                "human_decision_required": True,
                "suggested_next_action": checkpoint_request["next_action"],
            },
            "checkpoint_request": checkpoint_request,
        }
