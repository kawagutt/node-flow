"""NodeFlow v1.6 source-based runner (occupancy-driven, PipeSpec-shaped core only)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

from nodeflow.core.base_node import BaseNode
from nodeflow.core.pipe_spec import PipeSpec, validate_executable_pipe_spec
from nodeflow.core.source_ref import SourceRef

_PIPE_OUTPUT_TARGET = "__pipe__"
_DELIVER_UNAVAILABLE = object()


class Runner:
    """Occupancy-only runner for validated :class:`~nodeflow.core.pipe_spec.PipeSpec` wiring.

    * ``node_input_sources`` values must be :class:`~nodeflow.core.source_ref.SourceRef` only
      (no tuple bindings or nested field selection in core).
    * Port payloads delivered to children are **dict-only** via :meth:`BaseNode.set_input`.
    """

    def __init__(
        self,
        graph_node_order: list[str],
        nodes: Dict[str, BaseNode],
        node_params: Dict[str, Dict[str, Any]],
        node_input_sources: Dict[str, Dict[str, SourceRef]],
        pipe_inputs: Dict[str, Any] | None = None,
        *,
        pipe_output_sources: Dict[str, SourceRef] | None = None,
    ) -> None:
        self.graph_node_order = graph_node_order
        self.nodes = nodes
        self.node_params = node_params
        self.node_input_sources: Dict[str, Dict[str, SourceRef]] = {}
        for node_id, sources in node_input_sources.items():
            norm: Dict[str, SourceRef] = {}
            for port_name, source in sources.items():
                if not isinstance(source, SourceRef):
                    raise TypeError(
                        f"node_input_sources[{node_id!r}][{port_name!r}] must be SourceRef "
                        f"(tuple wiring is not supported in core Runner)."
                    )
                norm[port_name] = source
            self.node_input_sources[node_id] = norm
        self.pipe_inputs = dict(pipe_inputs or {})
        self.pipe_input_occupancy: Dict[str, bool] = {key: True for key in self.pipe_inputs.keys()}
        self.pipe_output_sources = dict(pipe_output_sources or {})
        for _name, src in self.pipe_output_sources.items():
            if not isinstance(src, SourceRef):
                raise TypeError("pipe_output_sources values must be SourceRef")
        self.pipe_outputs: Dict[str, Any] = {}
        self._pipe_output_filled: Dict[str, bool] = {
            name: False for name in self.pipe_output_sources
        }
        self._source_remaining: Dict[tuple[str, str | None, str], int] = {}
        self._edge_consumed: set[tuple[str, str]] = set()
        for node_id, sources in self.node_input_sources.items():
            for port_name, source in sources.items():
                key = self._source_key(source)
                self._source_remaining[key] = self._source_remaining.get(key, 0) + 1
        for _out_name, source in self.pipe_output_sources.items():
            key = self._source_key(source)
            self._source_remaining[key] = self._source_remaining.get(key, 0) + 1
        self._zero_input_completed: set[str] = set()
        self._idle_done_invoked: set[str] = set()

    @classmethod
    def from_pipe_spec(
        cls,
        spec: PipeSpec,
        pipe_inputs: Dict[str, Any] | None = None,
        *,
        node_params: Dict[str, Dict[str, Any]] | None = None,
    ) -> Runner:
        """Build a Runner from an executable PipeSpec (Phase A–validated).

        If ``node_params`` is given, it replaces the per-node ``params`` taken from
        ``spec.nodes[*].params`` (keys must match ``spec.nodes`` exactly).
        """
        validate_executable_pipe_spec(spec)
        nodes = {nid: ns.node for nid, ns in spec.nodes.items()}
        if node_params is None:
            merged_params = {nid: dict(ns.params) for nid, ns in spec.nodes.items()}
        else:
            keys_spec = frozenset(nodes)
            keys_arg = frozenset(node_params)
            if keys_spec != keys_arg:
                raise ValueError(
                    "from_pipe_spec(node_params=...): keys must match spec.nodes exactly "
                    f"(spec={sorted(keys_spec)!r} arg={sorted(keys_arg)!r})"
                )
            merged_params = {nid: dict(node_params[nid]) for nid in nodes}
        node_input_sources = {nid: dict(ns.input_sources) for nid, ns in spec.nodes.items()}
        return cls(
            graph_node_order=list(spec.graph_node_order),
            nodes=nodes,
            node_params=merged_params,
            node_input_sources=node_input_sources,
            pipe_inputs=pipe_inputs,
            pipe_output_sources=dict(spec.pipe.output_sources),
        )

    def all_pipe_outputs_filled(self) -> bool:
        """True when every declared pipe output buffer slot is filled.

        Executable :class:`~nodeflow.core.pipe_spec.PipeSpec` rejects empty ``output_sources``;
        the empty-dict case exists only for constructing a low-level ``Runner`` without outputs.
        """
        if not self.pipe_output_sources:
            return True
        return all(self._pipe_output_filled.get(name, False) for name in self.pipe_output_sources)

    def filled_pipe_outputs(self) -> Dict[str, Any]:
        """Shallow copy of filled pipe output buffers (domain payloads)."""
        out: Dict[str, Any] = {}
        for name in self.pipe_output_sources:
            if self._pipe_output_filled.get(name, False):
                out[name] = deepcopy(self.pipe_outputs[name])
        return out

    def _resolve_source_payload(self, source: SourceRef) -> Any:
        if source.kind == "input":
            if not self.pipe_input_occupancy.get(source.port_name, False):
                return _DELIVER_UNAVAILABLE
            if source.port_name not in self.pipe_inputs:
                return _DELIVER_UNAVAILABLE
            return self.pipe_inputs[source.port_name]
        if source.kind == "node":
            if not source.node_id:
                return _DELIVER_UNAVAILABLE
            src_node = self.nodes.get(source.node_id)
            if src_node is None:
                return _DELIVER_UNAVAILABLE
            if not src_node.is_output_filled(source.port_name):
                return _DELIVER_UNAVAILABLE
            snapshot = src_node.get_output_snapshot()
            return snapshot.get(source.port_name)
        return _DELIVER_UNAVAILABLE

    def _clear_source_occupancy(self, source: SourceRef) -> None:
        if source.kind == "input":
            if source.port_name in self.pipe_input_occupancy:
                self.pipe_input_occupancy[source.port_name] = False
            return
        if source.kind == "node" and source.node_id:
            src_node = self.nodes.get(source.node_id)
            if src_node is not None:
                src_node.clear_output_occupancy(source.port_name)

    def _source_key(self, source: SourceRef) -> tuple[str, str | None, str]:
        return (source.kind, source.node_id, source.port_name)

    @staticmethod
    def _port_occupancy_snapshot(node: BaseNode) -> tuple[str, frozenset[str], frozenset[str]]:
        """Hashable runner-visible state (status + which ports hold payloads); not payload semantics."""
        return (
            node.read_status(),
            frozenset(node.get_input_snapshot().keys()),
            frozenset(node.get_output_snapshot().keys()),
        )

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

    def _deliver_to_node_input(
        self, target: BaseNode, target_node_id: str, target_port_name: str, source: SourceRef
    ) -> bool:
        if target.is_input_filled(target_port_name):
            return False
        payload = self._resolve_source_payload(source)
        if payload is _DELIVER_UNAVAILABLE:
            return False
        pl = deepcopy(payload)
        if not isinstance(pl, dict):
            raise TypeError(
                f"Runner delivery to {target_node_id!r}.{target_port_name!r} requires dict "
                f"payload, got {type(pl).__name__}"
            )
        target.set_input(target_port_name, pl)
        self._mark_edge_consumed(source, target_node_id, target_port_name)
        return True

    def _delivery_phase(self) -> bool:
        progressed = False
        for target_node_id, target_ports in self.node_input_sources.items():
            target = self.nodes.get(target_node_id)
            if target is None:
                continue
            for target_port_name, source in target_ports.items():
                if self._deliver_to_node_input(target, target_node_id, target_port_name, source):
                    progressed = True
        for out_name, source in self.pipe_output_sources.items():
            if self._pipe_output_filled.get(out_name, False):
                continue
            payload = self._resolve_source_payload(source)
            if payload is _DELIVER_UNAVAILABLE:
                continue
            self.pipe_outputs[out_name] = deepcopy(payload)
            self._pipe_output_filled[out_name] = True
            self._mark_edge_consumed(source, _PIPE_OUTPUT_TARGET, out_name)
            progressed = True
        return progressed

    def _execution_phase(self) -> bool:
        """Call ``execute`` on ``ready`` / ``done`` children; progress is occupancy/status deltas only.

        The Runner does **not** decide semantic “required inputs”: it forwards the current input
        snapshot and lets :meth:`BaseNode.execute` / ``run()`` decide. A synchronous ``step()``
        counts execution as progress only when ``execute`` changes runner-visible **status or
        port occupancy** (not dict payload contents).

        Zero-declared-input nodes that produced a material change are not executed again in the
        same runner instance (prevents duplicate runs after outputs are consumed). Idle ``done``
        nodes without filled outputs are executed at most once per instance (the template’s
        no-op ``done`` path).
        """
        progressed = False
        for node_id in self.graph_node_order:
            node = self.nodes.get(node_id)
            if node is None:
                continue
            status = node.read_status()
            if status not in ("ready", "done"):
                continue
            if status == "done" and node.get_output_snapshot():
                continue
            declared_ports = set(self.node_input_sources.get(node_id, {}).keys())
            if len(declared_ports) == 0 and node_id in self._zero_input_completed:
                continue
            empty_done = status == "done" and not node.get_output_snapshot()
            if empty_done and node_id in self._idle_done_invoked:
                continue

            input_snapshot = node.get_input_snapshot()
            ports_to_clear = list(input_snapshot.keys())
            before = self._port_occupancy_snapshot(node)
            node.execute(input_snapshot, self.node_params.get(node_id, {}))
            after = self._port_occupancy_snapshot(node)
            material = before != after

            if empty_done:
                self._idle_done_invoked.add(node_id)

            if material:
                for port_name in ports_to_clear:
                    node.clear_input_occupancy(port_name)
                progressed = True
                if len(declared_ports) == 0:
                    self._zero_input_completed.add(node_id)
        return progressed

    def step(self) -> bool:
        """Run one runner step: delivery then execution."""
        progressed = self._delivery_phase()
        if self._execution_phase():
            progressed = True
        return progressed
