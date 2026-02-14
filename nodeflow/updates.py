"""
Updates モデル実装
"""
from typing import List, Dict, Any
from .context import Context


def apply_updates(context: Context, updates: List[Dict[str, Any]], step_id: str) -> None:
    """
    updates を context に適用（書き込み制限強制）
    
    apply_updates は BaseNode.execute だけが呼ぶ
    
    Args:
        context: Context インスタンス
        updates: updates リスト
        step_id: step ID（書き込み制限に使用）
    
    Raises:
        ValueError: 無効な update 操作
    """
    for update in updates:
        op = update.get("op")
        
        if op == "set":
            path = update.get("path")
            value = update.get("value")
            if path is None:
                raise ValueError("Update 'set' requires 'path'")
            context.set(path, value, step_id=step_id)
        
        elif op == "append":
            path = update.get("path")
            value = update.get("value")
            if path is None:
                raise ValueError("Update 'append' requires 'path'")
            context.append(path, value, step_id=step_id)
        
        elif op == "add_metric":
            key = update.get("key")
            value = update.get("value")
            if key is None:
                raise ValueError("Update 'add_metric' requires 'key'")
            # metrics は累積加算
            current_value = context.get(f"metrics.{key}", 0)
            if isinstance(current_value, (int, float)) and isinstance(value, (int, float)):
                context.set(f"metrics.{key}", current_value + value, step_id=None)
            else:
                # 数値でない場合は上書き
                context.set(f"metrics.{key}", value, step_id=None)
        
        else:
            raise ValueError(f"Unknown update operation: {op}")
