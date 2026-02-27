"""
NodeFlow v1.2 — LoopNode (StructuralNode). Graph iteration until condition, same instance reuse.
"""

from __future__ import annotations

from typing import Any, Dict

from .node import BaseNode, LimitSignal
from .pipeline_node import InvalidStateError, PipelineNode


def _get_value_by_path(obj: Any, path: str) -> Any:
    """Simple JSONPath-like: $.key.nested -> obj['key']['nested']. Root $ is object itself."""
    if not path or path == "$":
        return obj
    if path.startswith("$."):
        path = path[2:]
    keys = path.split(".")
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur


def _evaluate_condition_impl(output: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    """
    Evaluate condition on output. Returns True to break loop.
    §12.2.2.5: Error message MUST include JSONPath, operator, actual value, actual type.
    """
    path = condition.get("path") or "$"
    value = _get_value_by_path(output, path)
    value_ty = type(value).__name__
    if value is None and path != "$":
        raise ValueError(
            f"condition path not found: path={path!r} operator=N/A actual_value=None actual_type=missing"
        )
    if "equals" in condition:
        ref = condition["equals"]
        return value == ref
    if "not_equals" in condition:
        ref = condition["not_equals"]
        return value != ref
    if "less_than" in condition:
        ref = condition["less_than"]
        if not isinstance(value, (int, float)) or not isinstance(ref, (int, float)):
            raise TypeError(
                f"condition less_than type mismatch: path={path!r} operator=less_than "
                f"actual_value={value!r} actual_type={value_ty} ref={ref!r} ref_type={type(ref).__name__}"
            )
        return value < ref
    if "greater_than" in condition:
        ref = condition["greater_than"]
        if not isinstance(value, (int, float)) or not isinstance(ref, (int, float)):
            raise TypeError(
                f"condition greater_than type mismatch: path={path!r} operator=greater_than "
                f"actual_value={value!r} actual_type={value_ty} ref={ref!r} ref_type={type(ref).__name__}"
            )
        return value > ref
    return False


class LoopNode(BaseNode):
    """
    LoopNode — Graph iteration until condition. Reuses one inner PipelineNode (same node instances).
    Condition evaluated on final node latest output only when final status == done (§12.2.2.5).
    """

    def __init__(
        self,
        workspace_dir: str,
        pipeline_data: Dict[str, Any],
        condition: Dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.workspace_dir = workspace_dir
        self.pipeline_data = pipeline_data or {}
        self.condition = condition or {}
        self._validate_condition_at_construction()
        self._pipeline: PipelineNode | None = None

    def _validate_condition_at_construction(self) -> None:
        """§12.2.2: Fail at construction if condition is missing or invalid."""
        if not self.condition:
            raise ValueError(
                "LoopNode requires condition (path + equals/not_equals/less_than/greater_than)"
            )
        if not any(
            k in self.condition
            for k in ("equals", "not_equals", "less_than", "greater_than")
        ):
            raise ValueError(
                "LoopNode condition requires one of: equals, not_equals, less_than, greater_than"
            )

    def _get_pipeline(self) -> PipelineNode:
        if self._pipeline is None:
            self._pipeline = PipelineNode(self.workspace_dir, self.pipeline_data)
        return self._pipeline

    def read_status(self) -> str:
        return self._status

    def read_error(self) -> list:
        """§9.1: Aggregate fatal cause from all descendants (including self if fatal). Returns list[Exception]."""
        out: list = []
        if self._pipeline is not None and hasattr(self._pipeline, "read_error"):
            e = self._pipeline.read_error()
            if isinstance(e, list):
                out.extend(e)
            else:
                out.append(e)
        if self._status == "fatal" and self._error is not None:
            out.append(self._error)
        return out

    def read_node_calls(self) -> int:
        """§3.8: Aggregate node_calls of self and all descendants."""
        total = self._my_node_calls
        if self._pipeline is not None:
            total += self._pipeline.read_node_calls()
        return total

    def resume(self, resume_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """§6.7, §12.2.2.6. Same API as PipelineNode. Delegates to inner PipelineNode."""
        if self._status != "pause":
            raise InvalidStateError("resume() only when status is pause")
        pipeline = self._get_pipeline()
        return pipeline.resume(resume_inputs)

    def get_final_output(self) -> Dict[str, Any]:
        """Return final output from inner pipeline (§6.7)."""
        if self._pipeline is None:
            return {}
        return self._pipeline.get_final_output()

    def run(self, inputs: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """§12.2.2. Use only pipeline.execute(), read_status(), get_latest_output(), get_final_output()."""
        pipeline = self._get_pipeline()
        limit_cfg = (params or {}).get("limit") or {}
        max_iterations = (
            limit_cfg.get("max_iterations") if isinstance(limit_cfg, dict) else None
        )
        iteration = 0
        while True:
            iteration += 1
            if max_iterations is not None and iteration > max_iterations:
                raise LimitSignal(f"max_iterations={max_iterations} exceeded")
            pipeline.execute(inputs, params)
            self._status = pipeline.read_status()

            if self._status in ("fatal", "pause"):
                return {}

            if self._status == "limit":
                return pipeline.get_final_output()

            if self._status != "done":
                continue

            latest = pipeline.get_latest_output(pipeline.final_id)
            condition_input = latest if isinstance(latest, dict) else {}
            try:
                if _evaluate_condition_impl(condition_input, self.condition):
                    break
            except (ValueError, TypeError) as e:
                self._status = "fatal"
                self._error = e
                return {}

        self._status = "done"
        return pipeline.get_final_output()
