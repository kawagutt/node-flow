"""
Context クラス実装
"""
from typing import Any, Dict, Optional
import copy


class Usage:
    """
    集計型 usage（limit 判定用）。
    単一インスタンスを共有し、add_usage で累積するのみ。
    """
    __slots__ = ("wall_time_sec", "cost_usd", "token_used", "node_calls")

    def __init__(
        self,
        wall_time_sec: float = 0.0,
        cost_usd: float = 0.0,
        token_used: int = 0,
        node_calls: int = 0,
    ):
        self.wall_time_sec = wall_time_sec
        self.cost_usd = cost_usd
        self.token_used = token_used
        self.node_calls = node_calls

    def add(
        self,
        wall_time_sec: float = 0.0,
        cost_usd: float = 0.0,
        token_used: int = 0,
        node_calls: int = 0,
    ) -> None:
        """
        加算（add_usage 用・BaseNode の node_calls 用）。
        wall_time_sec / cost_usd は float に変換。token_used / node_calls は int または str(int) のみ許可（float は切り捨てせず TypeError）。
        """
        self.wall_time_sec += float(wall_time_sec)
        self.cost_usd += float(cost_usd)
        self.token_used += self._require_int(token_used, "token_used")
        self.node_calls += self._require_int(node_calls, "node_calls")

    @staticmethod
    def _require_int(value: Any, name: str) -> int:
        """int または str(int) のみ許可。float 等は TypeError（暗黙の切り捨てを防ぐ）。"""
        if type(value) is int:
            return value
        if isinstance(value, str):
            return int(value)
        raise TypeError(f"{name} must be int or str(int), got {type(value).__name__}")


class Context:
    """
    Context クラス
    
    内部構造: {inputs, artifacts, metrics, flags}, usage (Usage)
    execution state は含まない（system_info.execution に分離）
    """
    
    def __init__(
        self,
        initial_data: Optional[Dict[str, Any]] = None,
        usage: Optional[Usage] = None,
    ):
        """
        Context を初期化
        
        Args:
            initial_data: 初期データ（inputs, artifacts, metrics, flags）
            usage: 共有する Usage インスタンス。None の場合は新規作成。
        """
        if initial_data is None:
            initial_data = {}
        
        self._data = {
            "inputs": initial_data.get("inputs", {}),
            "artifacts": initial_data.get("artifacts", {}),
            "metrics": initial_data.get("metrics", {}),
            "flags": initial_data.get("flags", {}),
        }
        self.usage = usage if usage is not None else Usage()
    
    def get(self, path: str, default: Any = None) -> Any:
        """
        path ベースの取得
        
        例: "artifacts.plan.result"
        """
        parts = path.split(".")
        value = self._data
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
        
        return value
    
    def set(self, path: str, value: Any, step_id: Optional[str] = None) -> None:
        """
        path ベースの設定（書き込み制限チェック付き）
        
        Args:
            path: パス（例: "artifacts.plan.result"）
            value: 設定する値
            step_id: step ID（artifacts 書き込み時に必要）
        
        Raises:
            ValueError: 書き込み制限違反
        """
        self._validate_write_path(path, step_id)
        
        parts = path.split(".")
        target = self._data
        
        # 最後のキー以外の部分を作成
        for part in parts[:-1]:
            if part not in target:
                target[part] = {}
            target = target[part]
        
        # 最後のキーに値を設定
        target[parts[-1]] = value
    
    def append(self, path: str, value: Any, step_id: Optional[str] = None) -> None:
        """
        リストへの追加（書き込み制限チェック付き）
        
        Args:
            path: パス（例: "artifacts.plan.logs"）
            value: 追加する値
            step_id: step ID（artifacts 書き込み時に必要）
        
        Raises:
            ValueError: 書き込み制限違反
        """
        self._validate_write_path(path, step_id)
        
        parts = path.split(".")
        target = self._data
        
        for part in parts[:-1]:
            if part not in target:
                target[part] = {}
            target = target[part]
        
        final_key = parts[-1]
        if final_key not in target:
            target[final_key] = []
        
        if not isinstance(target[final_key], list):
            raise ValueError(f"Path {path} is not a list")
        
        target[final_key].append(value)
    
    def snapshot(self) -> Dict[str, Any]:
        """現在の状態を dict として返す（deep copy）"""
        return copy.deepcopy(self._data)
    
    def _validate_write_path(self, path: str, step_id: Optional[str] = None) -> None:
        """
        書き込みパスの検証
        
        許可されるパス:
        - artifacts.<step_id>.*
        - metrics.*
        - flags.*
        
        Args:
            path: パス
            step_id: step ID（artifacts 書き込み時に必要）
        
        Raises:
            ValueError: 書き込み制限違反
        """
        parts = path.split(".")
        
        if not parts:
            raise ValueError("Empty path")
        
        first_part = parts[0]
        
        if first_part == "usage":
            # usage は add_usage 経由のみ。直接 set/append 禁止
            raise ValueError("usage is read-only for set/append; use add_usage update only")
        if first_part == "artifacts":
            # artifacts.<step_id>.* のみ許可
            if len(parts) < 2:
                raise ValueError(f"Invalid artifacts path: {path}")
            if step_id is None:
                raise ValueError(f"step_id is required for artifacts path: {path}")
            if parts[1] != step_id:
                raise ValueError(
                    f"Artifacts path must start with 'artifacts.{step_id}.*', got: {path}"
                )
        elif first_part == "metrics":
            # metrics.* は許可（観測用）。step_id 不要。limit 判定には usage を使い、metrics は使わない。
            pass
        elif first_part == "flags":
            # flags.* は許可
            pass
        elif first_part == "inputs":
            # inputs は読み取り専用
            raise ValueError("inputs is read-only")
        else:
            raise ValueError(f"Forbidden path: {path}")
