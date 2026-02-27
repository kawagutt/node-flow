"""
NodeFlow v1.2 — PipelineNode (StructuralNode). Graph 1-shot execution, final node, resume.
"""

from __future__ import annotations

import time
from typing import Any, Dict

from .loader import resolve_params
from .node import BaseNode, LimitSignal
from .runner import Runner, build_runner


_STATUS_PRIORITY = ["fatal", "limit", "pause", "executing", "done", "ready"]


def _aggregate_status(statuses: list[str]) -> str:
    """§6.7: fatal > limit > pause > executing > done > ready."""
    for s in _STATUS_PRIORITY:
        if s in statuses:
            return s
    return "ready"


class PipelineNode(BaseNode):
    """
    PipelineNode — Graph 1-shot execution. Holds graph (nodes + final), Runner, latest_outputs.
    Reuses Runner and node instances within same Execution Scope.
    """

    def __init__(self, workspace_dir: str, pipeline_data: Dict[str, Any]) -> None:
        super().__init__()
        self.workspace_dir = workspace_dir
        self.pipeline_data = pipeline_data
        self.graph = (pipeline_data or {}).get("graph") or {}
        self.nodes_list = self.graph.get("nodes") or []
        self.final_id = self.graph.get("final") or ""
        self._runner: Runner | None = None
        self._latest_outputs: Dict[str, Dict[str, Any]] | None = None
        self._node_instances: Dict[str, BaseNode] | None = None
        self._idle_since: float | None = None

    def read_status(self) -> str:
        """§6.7: return self._status (aggregated from children when not executing)."""
        return self._status

    def read_error(self) -> list:
        """§9.1: Aggregate fatal cause from all descendants (including self if fatal). Returns list[Exception]."""
        out: list = []
        for node in (self._node_instances or {}).values():
            e = node.read_error()
            if e is None:
                continue
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
        for node in (self._node_instances or {}).values():
            total += node.read_node_calls()
        return total

    def get_latest_output(self, node_id: str) -> Dict[str, Any] | None:
        """§12.2.2.5: Return latest output from Context for node_id (for condition evaluation)."""
        if self._runner is None:
            return None
        return self._runner.get_latest_output(node_id)

    def get_final_output(self) -> Dict[str, Any]:
        """Return final node output from Context (§6.7)."""
        out = (self._latest_outputs or {}).get(self.final_id)
        return out if isinstance(out, dict) else {}

    def _init_context_if_needed(
        self, pipeline_inputs: Dict[str, Any], pipeline_params: Dict[str, Any]
    ) -> None:
        if self._runner is not None:
            self._runner.pipeline_inputs = pipeline_inputs
            self._runner.pipeline_params = pipeline_params
            return
        self._latest_outputs = {}
        self._runner, self._node_instances = build_runner(
            self.workspace_dir,
            self.graph,
            pipeline_inputs,
            pipeline_params,
            self._latest_outputs,
        )

    def _aggregate_children_status(self) -> str:
        if not self._node_instances:
            return "ready"
        statuses = [n.read_status() for n in self._node_instances.values()]
        return _aggregate_status(statuses)

    def _check_limit(self, pipeline_params: Dict[str, Any]) -> bool:
        limit_cfg = (pipeline_params or {}).get("limit") or {}
        max_calls = limit_cfg.get("max_total_node_calls")
        if max_calls is not None and self.read_node_calls() >= max_calls:
            return True
        max_idle = limit_cfg.get("max_idle_sec")
        if max_idle is not None and self._idle_since is not None:
            if time.monotonic() - self._idle_since >= max_idle:
                return True
        return False

    def _should_terminate(self) -> bool:
        agg = self._aggregate_children_status()
        if agg in ("fatal", "limit", "pause"):
            return True
        if agg != "done":
            return False
        final_node = (self._node_instances or {}).get(self.final_id)
        if final_node is None:
            return False
        return final_node.read_status() == "done"

    def _is_idle(self) -> bool:
        """Executable == 0 and Executing == 0 (§12.2.1.9). Uses monotonic clock for max_idle_sec (§12.2.1.9, C-05)."""
        if not self._runner or not self._node_instances:
            return True
        any_executable = any(
            self._runner.is_executable(nid)
            for nd in self.nodes_list
            for nid in [nd.get("id")]
            if nid
        )
        if any_executable:
            return False
        any_executing = any(
            n.read_status() == "executing" for n in self._node_instances.values()
        )
        return not any_executing

    def run(self, inputs: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """§6.7, §12.2.1. One-shot subgraph execution. Limit check at start and after each step."""
        pipeline_params = params or {}
        self._init_context_if_needed(inputs, pipeline_params)
        runner = self._runner
        if runner is None:
            return {}

        if self._check_limit(pipeline_params):
            raise LimitSignal()

        self._idle_since = None
        while True:
            progressed = runner.step()
            self._status = self._aggregate_children_status()
            if progressed:
                self._idle_since = None
            else:
                if self._is_idle():
                    if self._idle_since is None:
                        self._idle_since = time.monotonic()
                    if self._check_limit(pipeline_params):
                        self._status = "limit"
                        return self.get_final_output()
                else:
                    self._idle_since = None

            if self._check_limit(pipeline_params):
                self._status = "limit"
                return self.get_final_output()

            agg = self._aggregate_children_status()
            if agg in ("fatal", "limit", "pause"):
                self._status = agg
                return {}

            if self._should_terminate():
                self._status = agg
                return self.get_final_output()

    def resume(self, resume_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """§6.7. Call only when status == pause. Resume all pause nodes in graph order."""
        if self._status != "pause":
            raise InvalidStateError("resume() only when status is pause")
        runner = self._runner
        node_instances = self._node_instances
        if not runner or not node_instances:
            return {"resumed": [], "statuses": {}}
        resumed = []
        statuses = {}
        for nd in self.nodes_list:
            nid = nd.get("id")
            if not nid:
                continue
            node = node_instances.get(nid)
            if node is None or node.read_status() != "pause":
                continue
            # §6.7: StructuralNode子の場合はresume()を呼ぶ。DataNodeはexecute()を呼ぶ。
            if hasattr(node, "resume") and callable(node.resume):
                node.resume(resume_inputs)
                resumed.append(nid)
                statuses[nid] = node.read_status()
                # §6.7: 子StructuralNodeがdoneになった場合、出力を保存する
                if statuses[nid] == "done" and hasattr(node, "get_final_output"):
                    final_out = node.get_final_output()
                    if final_out:
                        runner.save_output(nid, final_out)
                if statuses[nid] == "fatal":
                    break
            else:
                params_def = nd.get("params") or {}
                params = resolve_params_for_node(
                    params_def,
                    runner.pipeline_params,
                    self._latest_outputs or {},
                    runner.pipeline_inputs,
                )
                out = node.execute(resume_inputs, params)
                if out != {}:
                    runner.save_output(nid, out)
                resumed.append(nid)
                statuses[nid] = node.read_status()
                if statuses[nid] == "fatal":
                    break
        return {"resumed": resumed, "statuses": statuses}


def resolve_params_for_node(
    params_def: Dict[str, Any],
    pipeline_params: Dict[str, Any],
    latest_outputs: Dict[str, Dict[str, Any]],
    pipeline_inputs: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolve ${params.xxx} in node params."""
    return resolve_params(params_def, pipeline_params, latest_outputs, pipeline_inputs)


class InvalidStateError(Exception):
    """Raised when resume() is called and status is not pause."""
