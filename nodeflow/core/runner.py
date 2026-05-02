"""NodeFlow v1.6 source-based runner (occupancy-driven)."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Literal, Tuple

from nodeflow.core.base_node import BaseNode


@dataclass(frozen=True)
class SourceRef:
    kind: Literal["input", "node"]
    port_name: str
    node_id: str | None = None
    inner_path: str | None = None


class Runner:
    """Occupancy-only runner.

    Phase A: delivery by source declarations.
    Phase B: execute ready/done nodes only.
    """

    def __init__(
        self,
        graph_node_order: list[str],
        nodes: Dict[str, BaseNode],
        node_params: Dict[str, Dict[str, Any]],
        node_input_sources: Dict[str, Dict[str, SourceRef | Tuple[str, ...]]],
        pipe_inputs: Dict[str, Any] | None = None,
    ) -> None:
        self.graph_node_order = graph_node_order
        self.nodes = nodes
        self.node_params = node_params
        self.node_input_sources = {
            node_id: {
                port_name: self._normalize_source(source) for port_name, source in sources.items()
            }
            for node_id, sources in node_input_sources.items()
        }
        self.pipe_inputs = dict(pipe_inputs or {})
        self.pipe_input_occupancy: Dict[str, bool] = {key: True for key in self.pipe_inputs.keys()}
        self._source_remaining: Dict[tuple[str, str | None, str, str | None], int] = {}
        self._edge_consumed: set[tuple[str, str]] = set()
        for node_id, sources in self.node_input_sources.items():
            for port_name, source in sources.items():
                key = self._source_key(source)
                self._source_remaining[key] = self._source_remaining.get(key, 0) + 1

    def _normalize_source(self, source: SourceRef | Tuple[str, ...]) -> SourceRef:
        if isinstance(source, SourceRef):
            return source
        if not source:
            raise ValueError("input source tuple must not be empty")
        if source[0] == "inputs" and len(source) >= 2:
            inner = source[2] if len(source) >= 3 else None
            return SourceRef(kind="input", port_name=source[1], inner_path=inner)
        if source[0] == "node" and len(source) >= 3:
            inner = source[3] if len(source) >= 4 else None
            return SourceRef(kind="node", node_id=source[1], port_name=source[2], inner_path=inner)
        raise ValueError("unsupported input source tuple")

    def _extract_inner(self, payload: Any, inner_path: str | None) -> Any | None:
        if not inner_path:
            return payload
        value = payload
        for key in inner_path.split("."):
            if not isinstance(value, dict) or key not in value:
                return None
            value = value[key]
        return value

    def _resolve_source_payload(self, source: SourceRef) -> Any | None:
        if source.kind == "input":
            if not self.pipe_input_occupancy.get(source.port_name, False):
                return None
            payload = self.pipe_inputs.get(source.port_name)
            return self._extract_inner(payload, source.inner_path)
        if source.kind == "node":
            if not source.node_id:
                return None
            src_node = self.nodes.get(source.node_id)
            if src_node is None:
                return None
            if not src_node.is_output_filled(source.port_name):
                return None
            snapshot = src_node.get_output_snapshot()
            payload = snapshot.get(source.port_name)
            return self._extract_inner(payload, source.inner_path)
        return None

    def _clear_source_occupancy(self, source: SourceRef) -> None:
        if source.kind == "input":
            if source.port_name in self.pipe_input_occupancy:
                self.pipe_input_occupancy[source.port_name] = False
            return
        if source.kind == "node" and source.node_id:
            src_node = self.nodes.get(source.node_id)
            if src_node is not None:
                src_node.clear_output_occupancy(source.port_name)

    def _source_key(self, source: SourceRef) -> tuple[str, str | None, str, str | None]:
        return (source.kind, source.node_id, source.port_name, source.inner_path)

    def _mark_edge_consumed(
        self, source: SourceRef, target_node_id: str, target_port_name: str
    ) -> None:
        edge_key = (target_node_id, target_port_name)
        if edge_key in self._edge_consumed:
            return
        self._edge_consumed.add(edge_key)
        source_key = self._source_key(source)
        remaining = self._source_remaining.get(source_key, 0) - 1
        self._source_remaining[source_key] = remaining
        if remaining <= 0:
            self._clear_source_occupancy(source)

    def _delivery_phase(self) -> bool:
        progressed = False
        for target_node_id, target_ports in self.node_input_sources.items():
            target = self.nodes.get(target_node_id)
            if target is None:
                continue
            for target_port_name, source in target_ports.items():
                if target.is_input_filled(target_port_name):
                    continue
                payload = self._resolve_source_payload(source)
                if payload is None:
                    continue
                target.set_input(target_port_name, deepcopy(payload))
                self._mark_edge_consumed(source, target_node_id, target_port_name)
                progressed = True
        return progressed

    def _execution_phase(self) -> bool:
        progressed = False
        for node_id in self.graph_node_order:
            node = self.nodes.get(node_id)
            if node is None:
                continue
            status = node.read_status()
            if status not in ("ready", "done"):
                continue
            # Progress is only for synchronous exhaustion inside PipeNode.execute().
            # This must not be treated as caller-visible graph-completion condition.
            if status == "done" and node.get_output_snapshot():
                continue
            input_snapshot = node.get_input_snapshot()
            declared_sources = self.node_input_sources.get(node_id, {})
            declared_ports = set(declared_sources.keys())
            if len(declared_ports) > 1 and any(
                port not in input_snapshot for port in declared_ports
            ):
                continue
            if len(declared_ports) == 1 and not input_snapshot:
                continue
            ports_to_clear = list(input_snapshot.keys())
            node.execute(input_snapshot, self.node_params.get(node_id, {}))
            for port_name in ports_to_clear:
                node.clear_input_occupancy(port_name)
            progressed = True
        return progressed

    def step(self) -> bool:
        """Run one runner step: delivery then execution."""
        progressed = self._delivery_phase()
        if self._execution_phase():
            progressed = True
        return progressed
