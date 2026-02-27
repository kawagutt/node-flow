"""
BaseNode, PauseSignal, LimitSignal — NodeFlow v1.2 Execution Layer.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from types import MappingProxyType
from typing import Any, Dict

# RFC 8785 for revision canonicalization (§5.9)
try:
    import rfc8785

    def _canonical_bytes(obj: Any) -> bytes:
        return rfc8785.dumps(obj)
except ImportError:

    def _canonical_bytes(obj: Any) -> bytes:
        s = json.dumps(
            obj,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return s.encode("utf-8")


class PauseSignal(Exception):
    """
    run() 内から raise することで pause を宣言する。
    resume_inputs_schema は外部に「何を渡せばよいか」を伝える参考情報。エンジンは解釈しない。
    """

    def __init__(self, reason: str = "", resume_inputs_schema: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.resume_inputs_schema = resume_inputs_schema or {}


class LimitSignal(Exception):
    """run() 内から raise することで limit を宣言する。limit pre/post による検出と併用可能。"""

    def __init__(self, reason: str = ""):
        super().__init__(reason)
        self.reason = reason


def _freeze(params: dict) -> MappingProxyType:
    """Shallow freeze for params (§4.4)."""
    return MappingProxyType(params.copy() if params else {})


def _strip_meta(value: Any) -> Any:
    """Recursively remove _meta from all levels (§5.9.4)."""
    if isinstance(value, dict):
        return {k: _strip_meta(v) for k, v in value.items() if k != "_meta"}
    if isinstance(value, list):
        return [_strip_meta(v) for v in value]
    return value


def _apply_revision_to_output(output: Dict[str, Any]) -> None:
    """
    Apply _meta.revision (content-hash) to each output port. MUST run before limit post.
    §5.7, §5.9, §5.10. Modifies output in place. Non-JSON types → TypeError (caller sets fatal).
    """
    for port_name, port_value in list(output.items()):
        if not isinstance(port_value, dict):
            raise TypeError(
                f"Output port '{port_name}' must be a dict, got {type(port_value).__name__}"
            )
        if "_meta" not in port_value:
            port_value["_meta"] = {}
        if "revision" in port_value["_meta"]:
            continue
        if port_value.get("_meta", {}).get("hash_skip") is True:
            port_value["_meta"]["revision"] = str(uuid.uuid4())
            continue
        payload = _strip_meta(port_value)
        raw = _canonical_bytes(payload)
        digest = hashlib.sha256(raw).hexdigest()
        port_value["_meta"]["revision"] = digest


class BaseNode:
    """
    BaseNode — NodeFlow v1.2. All nodes inherit this.
    execute(inputs, params) -> dict. Subclasses implement run(inputs, params) -> dict.
    """

    # Reserved for future use (v1.1 had config/system_info; v1.2 uses params only).
    DEFAULT_CONFIG: Dict[str, Any] = {}
    SCHEMA: Dict[str, Any] = {}

    def __init__(self) -> None:
        self._status = "ready"
        self._error: Exception | None = None
        self._my_node_calls: int = 0

    def read_status(self) -> str:
        """Return current status. Control is caller's responsibility (§2.3.3)."""
        return self._status

    def read_error(self) -> Exception | None:
        """§9.1: Return cause exception when status is fatal; None otherwise."""
        return self._error if self._status == "fatal" else None

    def read_node_calls(self) -> int:
        """§3.8: Return number of times this node's execute() was invoked (DataNode)."""
        return self._my_node_calls

    def run(self, inputs: Dict[str, Any], params: MappingProxyType) -> Dict[str, Any]:
        """Override in subclass. Must return a dict (output ports)."""
        raise NotImplementedError("Subclass must implement run(inputs, params)")

    def _check_limit_pre(self, params: MappingProxyType) -> bool:
        """True if limit exceeded (pre). Override to interpret params.get('limit')."""
        return False

    def _check_limit_post(self, params: MappingProxyType, run_succeeded: bool) -> bool:
        """True if limit exceeded (post). Override to interpret params.get('limit')."""
        return False

    def execute(self, inputs: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """
        §2.2, §6.3. Always returns a dict. Status is internal; use read_status().
        §3.8: node_calls is incremented on entry (before limit pre).
        Order: node_calls += 1 → freeze → executing → limit pre → run → revision complement → limit post → done → return.
        """
        self._my_node_calls += 1
        frozen = _freeze(params)
        self._status = "executing"

        if self._check_limit_pre(frozen):
            self._status = "limit"
            return {}

        try:
            result = self.run(inputs, frozen)
        except PauseSignal:
            self._status = "pause"
            return {}
        except LimitSignal:
            self._status = "limit"
            return {}
        except Exception as e:
            self._status = "fatal"
            self._error = e
            return {}

        if not isinstance(result, dict):
            self._status = "fatal"
            self._error = TypeError("run() must return a dict")
            return {}

        try:
            _apply_revision_to_output(result)
        except (TypeError, ValueError) as e:
            self._status = "fatal"
            self._error = e
            return {}

        if self._check_limit_post(frozen, run_succeeded=True):
            self._status = "limit"
            return result

        if self._status == "executing":
            self._status = "done"
        return result
