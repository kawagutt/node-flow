"""
Context クラス実装
"""
from typing import Any, Dict, Optional
import copy


class Context:
    """
    Context クラス
    
    内部構造: {inputs, artifacts, metrics, flags}
    execution state は含まない（system_info.execution に分離）
    """
    
    def __init__(self, initial_data: Optional[Dict[str, Any]] = None):
        """
        Context を初期化
        
        Args:
            initial_data: 初期データ（inputs, artifacts, metrics, flags）
        """
        if initial_data is None:
            initial_data = {}
        
        self._data = {
            "inputs": initial_data.get("inputs", {}),
            "artifacts": initial_data.get("artifacts", {}),
            "metrics": initial_data.get("metrics", {}),
            "flags": initial_data.get("flags", {}),
        }
    
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
            # metrics.* は許可
            pass
        elif first_part == "flags":
            # flags.* は許可
            pass
        elif first_part == "inputs":
            # inputs は読み取り専用
            raise ValueError("inputs is read-only")
        else:
            raise ValueError(f"Forbidden path: {path}")
