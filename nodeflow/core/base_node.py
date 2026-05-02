"""
NodeFlow — Exceptions, ExecutionContext, BaseNode, execution template (_runtime / _usage).
Taxonomy (PipeNode / ActionNode) lives in nodeflow.core.node_kinds.
"""

from __future__ import annotations

import time
import uuid
from types import MappingProxyType
from typing import Any, Dict

# --- Execution control exceptions (see doc/nodeflow_spec.md) ---


class NodeExecutionLimit(Exception):
    """Node の実行制約（limit）到達を示す例外"""


class NodeExecutionFailure(Exception):
    """Node の実行失敗（fatal）を示す例外。reason で区別する。"""

    def __init__(self, reason: str = ""):
        self.reason = reason
        super().__init__(reason)


# --- PauseSignal / LimitSignal: 定義のみ。raise 時は NotImplementedError（この版） ---


class PauseSignal(Exception):
    """Reserved; raising in run() is NotImplementedError in this package version."""

    def __init__(self, reason: str = "", resume_inputs_schema: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.resume_inputs_schema = resume_inputs_schema or {}


class LimitSignal(Exception):
    """Reserved; raising in run() is NotImplementedError in this package version."""

    def __init__(self, reason: str = ""):
        super().__init__(reason)
        self.reason = reason


# --- ExecutionContext ---


class ExecutionContext:
    """execute 呼び出し単位で生成。永続化しない。"""

    def __init__(self) -> None:
        self._stop_requested = False
        self._start_time = time.monotonic()

    def request_stop(self) -> None:
        self._stop_requested = True

    def should_stop(self) -> bool:
        return self._stop_requested

    def elapsed_time(self) -> float:
        return time.monotonic() - self._start_time


# --- utils ---


def _freeze(params: dict) -> MappingProxyType:
    """Shallow freeze for params."""
    return MappingProxyType(params.copy() if params else {})


RESERVED_TOP_LEVEL_FROM_RUN = frozenset({"_state", "_runtime", "_usage"})


def domain_ports_from_observation(obs: Dict[str, Any]) -> Dict[str, Any]:
    """Strip reserved top-level keys from a child ``execute()`` return.

    Reserved observation keys are removed (see ``RESERVED_TOP_LEVEL_FROM_RUN``).
    Do not use this to drop arbitrary other keys—extend the frozenset only if the
    port contract adds a new reserved name.

    **Call sites:** default/custom ``PipeNode.run()`` when forwarding
    the **final** child’s observation to this pipe’s
    domain output. Not a general-purpose dict helper—avoid importing it elsewhere.
    """
    return {k: v for k, v in obs.items() if k not in RESERVED_TOP_LEVEL_FROM_RUN}


def _attach_runtime(output: dict) -> dict:
    """テンプレートが _runtime['ports'][port_name]['revision'] を付与する。domain port には触れない。"""
    ports: Dict[str, Dict[str, str]] = {}
    for port_key, port_value in list(output.items()):
        if port_key in RESERVED_TOP_LEVEL_FROM_RUN:
            continue
        if not isinstance(port_value, dict):
            raise TypeError(
                f"Port {port_key!r} payload must be dict, got {type(port_value).__name__}"
            )
        ports[port_key] = {"revision": str(uuid.uuid4())}
    output["_runtime"] = {"ports": ports}
    return output


# --- BaseNode ---


class BaseNode:
    """
    すべての Node が継承する基底クラス（実行テンプレート）。
    execute の構造は固定: pre-limit → executing → run → _usage 除去 → _runtime 付与 → post-limit → status 設定 → result 返却。
    post-limit 時も result は返す（_runtime 付与済み）。status = limit により PipeNode が以降の実行を止める。
    """

    def __init__(self) -> None:
        self._status = "ready"
        self._error: Exception | None = None
        self._limit_state: Dict[str, int] = {"calls": 0, "tokens": 0}
        self._current_context: ExecutionContext | None = None
        self._input_ports: Dict[str, Any] = {}
        self._input_occupancy: Dict[str, bool] = {}
        self._output_ports: Dict[str, Any] = {}
        self._output_occupancy: Dict[str, bool] = {}
        self._output_runtime_ports: Dict[str, Dict[str, str]] = {}
        self._last_usage: Dict[str, Any] = {}

    def execute(self, inputs: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """共通実行テンプレート。サブクラスは run() を実装する。"""
        if self._status == "done":
            # done node may be called again by Runner; must not duplicate execution.
            return self._build_observation()
        if self._status != "ready":
            raise RuntimeError("execute called when status is not ready")

        # pre-limit は freeze 前の params を意図的に受け取る（execute の引数そのまま）
        if self._check_pre_limit(params):
            self._status = "limit"
            return self._build_observation()

        self._status = "executing"
        context = ExecutionContext()
        self._current_context = context
        self._limit_state["calls"] += 1  # run 開始前にのみ増加（pre_limit では増やさない）

        try:
            frozen_params = _freeze(params)
            result = self.run(inputs, frozen_params, context)
        except PauseSignal:
            raise NotImplementedError("PauseSignal not implemented in this version")
        except LimitSignal:
            raise NotImplementedError("LimitSignal not implemented in this version")
        except NodeExecutionLimit:
            self._status = "limit"
            return self._build_observation()
        except NodeExecutionFailure as e:
            self._status = "fatal"
            self._error = e
            return self._build_observation()
        except Exception as e:
            self._status = "fatal"
            self._error = e
            return self._build_observation()
        finally:
            self._current_context = None

        if not isinstance(result, dict):
            self._status = "fatal"
            self._error = TypeError("run() must return a dict")
            return self._build_observation()

        if "_runtime" in result:
            self._status = "fatal"
            self._error = ValueError(
                "run() must not return _runtime; execution template owns _runtime"
            )
            return self._build_observation()
        if "_state" in result:
            self._status = "fatal"
            self._error = ValueError("run() must not return _state; execution template owns _state")
            return self._build_observation()

        try:
            usage = self._extract_usage(result)
            self._store_output_ports(result)
        except Exception as e:
            self._status = "fatal"
            self._error = e
            return self._build_observation()

        if self._check_post_limit(params):
            self._status = "limit"
        else:
            self._status = self._status_after_run(result)

        return self._build_observation(usage=usage)

    def _status_after_run(self, result: Dict[str, Any]) -> str:
        """Derive ``self._status`` after ``run()`` returns (fatal/limit already handled)."""
        self._status = "ready"
        self._refresh_status_from_output_occupancy()
        return self._status

    def _check_pre_limit(self, params: Dict[str, Any]) -> bool:
        """実行前の limit 判定。本版では max_calls のみ。params は freeze 前の生の dict。"""
        limit_cfg = params.get("limit")
        if not isinstance(limit_cfg, dict):
            return False
        max_calls = limit_cfg.get("max_calls")
        if max_calls is None:
            return False
        return self._limit_state["calls"] >= max_calls

    def _check_post_limit(self, params: Dict[str, Any]) -> bool:
        """実行後の limit 判定。_limit_state と params のみ参照。run の result は参照しない。"""
        limit_cfg = params.get("limit")
        if not isinstance(limit_cfg, dict):
            return False
        max_tokens = limit_cfg.get("max_tokens")
        if isinstance(max_tokens, int) and self._limit_state.get("tokens", 0) >= max_tokens:
            return True
        return False

    def _extract_usage(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """run の戻り値から _usage を取り除き、tokens を _limit_state に加算する。"""
        usage = result.pop("_usage", None)
        if not isinstance(usage, dict):
            self._last_usage = {}
            return {}
        total = usage.get("total_tokens")
        if isinstance(total, int):
            self._limit_state["tokens"] += total
        self._last_usage = dict(usage)
        return self._last_usage

    def _store_output_ports(self, output_ports: Dict[str, Any]) -> None:
        for port_key, port_value in list(output_ports.items()):
            if port_key in RESERVED_TOP_LEVEL_FROM_RUN:
                continue
            if not isinstance(port_value, dict):
                raise TypeError(
                    f"Port {port_key!r} payload must be dict, got {type(port_value).__name__}"
                )
            self._output_ports[port_key] = dict(port_value)
            self._output_occupancy[port_key] = True
            self._output_runtime_ports[port_key] = {"revision": str(uuid.uuid4())}

    def _refresh_status_from_output_occupancy(self) -> None:
        if self._status in {"fatal", "limit", "executing"}:
            return
        self._status = "done" if any(self._output_occupancy.values()) else "ready"

    def _build_observation(self, usage: Dict[str, Any] | None = None) -> Dict[str, Any]:
        domain_output = self.get_output_snapshot()
        runtime_ports: Dict[str, Dict[str, str]] = {}
        for port_key in list(domain_output.keys()):
            runtime = self._output_runtime_ports.get(port_key)
            if runtime is not None:
                runtime_ports[port_key] = dict(runtime)
        error = None
        if self._error is not None:
            error = {
                "type": type(self._error).__name__,
                "message": str(self._error),
            }
        domain_output["_state"] = {"value": self._status, "error": error}
        domain_output["_runtime"] = {"ports": runtime_ports}
        domain_output["_usage"] = dict(self._last_usage if usage is None else usage)
        return domain_output

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        """Override in subclass. Must return a dict (output ports)."""
        raise NotImplementedError

    def read_status(self) -> str:
        """現在の status を返す。"""
        return self._status

    def read_error(self) -> Exception | None:
        """status が fatal のとき原因例外を返す。それ以外は None。"""
        return self._error if self._status == "fatal" else None

    def reset_status(self) -> None:
        """status を ready に戻す。executing 状態では呼び出してはならない。"""
        if self._status == "executing":
            raise RuntimeError("cannot reset while executing")
        self._status = "ready"
        self._error = None
        self._input_ports = {}
        self._input_occupancy = {}
        self._output_ports = {}
        self._output_occupancy = {}
        self._output_runtime_ports = {}
        self._last_usage = {}

    def reset_limit_state(self, name: str) -> None:
        """指定した limit state をリセットする。status は変更しない。"""
        if name not in self._limit_state:
            raise KeyError(f"Unknown limit state: {name}")
        self._limit_state[name] = 0

    # v1.6 port API: delivery uses occupancy state, not observable output mutation.
    def set_input(self, port_name: str, payload: dict[str, Any]) -> None:
        """v1.6 port delivery: input payloads must be dict (shallow-copied)."""
        if not isinstance(payload, dict):
            raise TypeError(
                f"Input port {port_name!r} payload must be dict, got {type(payload).__name__}"
            )
        self._input_ports[port_name] = dict(payload)
        self._input_occupancy[port_name] = True

    def get_output_snapshot(self, *, filled_only: bool = True) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        for key, value in self._output_ports.items():
            if filled_only and not self._output_occupancy.get(key, False):
                continue
            snapshot[key] = dict(value)
        return snapshot

    def is_output_filled(self, port_name: str) -> bool:
        return bool(self._output_occupancy.get(port_name, False))

    def clear_output_occupancy(self, port_name: str) -> None:
        if port_name in self._output_occupancy:
            self._output_occupancy[port_name] = False
        self._refresh_status_from_output_occupancy()

    def is_input_filled(self, port_name: str) -> bool:
        return bool(self._input_occupancy.get(port_name, False))

    def get_input_snapshot(self, *, filled_only: bool = True) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        for key, value in self._input_ports.items():
            if filled_only and not self._input_occupancy.get(key, False):
                continue
            snapshot[key] = dict(value) if isinstance(value, dict) else value
        return snapshot

    def clear_input_occupancy(self, port_name: str) -> None:
        if port_name in self._input_occupancy:
            self._input_occupancy[port_name] = False
