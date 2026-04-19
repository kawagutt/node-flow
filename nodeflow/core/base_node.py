"""
NodeFlow v1.5 — Exceptions, ExecutionContext, BaseNode, utils (runtime template).
Taxonomy (PipeNode / ActionNode) lives in nodeflow.nodes.
"""

from __future__ import annotations

import time
import uuid
from types import MappingProxyType
from typing import Any, Dict

# --- Execution control exceptions (§2.2.1) ---


class NodeExecutionLimit(Exception):
    """Node の実行制約（limit）到達を示す例外"""


class NodeExecutionFailure(Exception):
    """Node の実行失敗（fatal）を示す例外。reason で区別する。"""

    def __init__(self, reason: str = ""):
        self.reason = reason
        super().__init__(reason)


# --- PauseSignal / LimitSignal: 定義のみ。raise 時は NotImplementedError（この版） ---


class PauseSignal(Exception):
    """v1.5 で正式実装。本版では raise すると NotImplementedError。"""

    def __init__(self, reason: str = "", resume_inputs_schema: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.resume_inputs_schema = resume_inputs_schema or {}


class LimitSignal(Exception):
    """v1.5 で正式実装。本版では raise すると NotImplementedError。"""

    def __init__(self, reason: str = ""):
        super().__init__(reason)
        self.reason = reason


# --- ExecutionContext (§2.4) ---


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
    """Shallow freeze for params (§4.3)."""
    return MappingProxyType(params.copy() if params else {})


RESERVED_KEYS = frozenset({"_meta", "_usage"})


def _attach_revision(output: dict) -> dict:
    """各 output port に UUID4 のダミー revision を付与。予約キー _meta/_usage はスキップ（§5.1）。
    Port payload は dict のみ。scalar は禁止（TypeError）。_meta を付与するのみ（二重 value にしない）。"""
    for port_key, port_value in list(output.items()):
        if port_key in RESERVED_KEYS:
            continue
        if not isinstance(port_value, dict):
            raise TypeError(
                f"Port {port_key!r} payload must be dict, got {type(port_value).__name__}"
            )
        port_value.setdefault("_meta", {})
        port_value["_meta"]["revision"] = str(uuid.uuid4())
    return output


# --- BaseNode (§6) ---


class BaseNode:
    """
    NodeFlow v1.5 — すべての Node が継承する基底クラス。
    execute の構造は固定: pre-limit → executing → run → _apply_usage → revision 付与 → post-limit → status 設定 → result 返却。
    post-limit 時も result は返す（revision 付与済み）。status = limit により PipeNode が以降の実行を止める。
    """

    def __init__(self) -> None:
        self._status = "ready"
        self._error: Exception | None = None
        self._limit_state: Dict[str, int] = {"calls": 0, "tokens": 0}
        self._current_context: ExecutionContext | None = None

    def execute(self, inputs: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """共通実行テンプレート。サブクラスは run() を実装する。"""
        if self._status != "ready":
            raise RuntimeError("execute called when status is not ready")

        # pre-limit は freeze 前の params を意図的に受け取る（execute の引数そのまま）
        if self._check_pre_limit(params):
            self._status = "limit"
            return {}

        self._status = "executing"
        context = ExecutionContext()
        self._current_context = context
        self._limit_state["calls"] += (
            1  # run 開始前にのみ増加（pre_limit では増やさない）
        )

        try:
            frozen_params = _freeze(params)
            result = self.run(inputs, frozen_params, context)
        except PauseSignal:
            raise NotImplementedError("PauseSignal not implemented in this version")
        except LimitSignal:
            raise NotImplementedError("LimitSignal not implemented in this version")
        except NodeExecutionLimit:
            self._status = "limit"
            return {}
        except NodeExecutionFailure as e:
            self._status = "fatal"
            self._error = e
            return {}
        except Exception as e:
            self._status = "fatal"
            self._error = e
            return {}
        finally:
            self._current_context = None

        if not isinstance(result, dict):
            self._status = "fatal"
            self._error = TypeError("run() must return a dict")
            return {}

        self._apply_usage(result)
        result = _attach_revision(result)

        if self._check_post_limit(params):
            self._status = "limit"
        else:
            self._status = "done"

        return result

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
        if (
            isinstance(max_tokens, int)
            and self._limit_state.get("tokens", 0) >= max_tokens
        ):
            return True
        return False

    def _apply_usage(self, result: Dict[str, Any]) -> None:
        """run の戻り値から _usage を取り除き、tokens を _limit_state に加算する。"""
        usage = result.pop("_usage", None)
        if not isinstance(usage, dict):
            return
        total = usage.get("total_tokens")
        if isinstance(total, int):
            self._limit_state["tokens"] += total

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

    def reset_limit_state(self, name: str) -> None:
        """指定した limit state をリセットする。status は変更しない。"""
        if name not in self._limit_state:
            raise KeyError(f"Unknown limit state: {name}")
        self._limit_state[name] = 0
