"""
Updates モデル実装
"""
import copy
from typing import List, Dict, Any
from .context import Context


def apply_updates(context: Context, updates: List[Dict[str, Any]], step_id: str) -> None:
    """
    updates を context に適用（書き込み制限強制）。
    atomic：一連の updates は「すべて適用」または「いずれかで失敗したら未適用のまま」。
    node_calls は BaseNode が run 前に加算するため、本関数の rollback 対象外（attempt を数える設計）。

    apply_updates は BaseNode.execute だけが呼ぶ。

    Args:
        context: Context インスタンス
        updates: updates リスト
        step_id: step ID（書き込み制限に使用）

    Raises:
        ValueError: 無効な update 操作

    注: 現状は context._data 全体の deepcopy で atomic を実現。Context が肥大すると重くなる。
    将来の最適化案: undo ログ方式、更新対象のみ snapshot、Context に snapshot/restore API を用意する等。
    """
    snapshot_data = copy.deepcopy(context._data)
    usage_backup = (
        context.usage.wall_time_sec,
        context.usage.cost_usd,
        context.usage.token_used,
        context.usage.node_calls,
    )
    try:
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
                # metrics は step_id 不要（制限対象外）。apply_updates のみが metrics を更新する。
                key = update.get("key")
                value = update.get("value")
                if key is None:
                    raise ValueError("Update 'add_metric' requires 'key'")
                current_value = context.get(f"metrics.{key}", 0)
                if isinstance(current_value, (int, float)) and isinstance(value, (int, float)):
                    context.set(f"metrics.{key}", current_value + value, step_id=None)
                else:
                    context.set(f"metrics.{key}", value, step_id=None)

            elif op == "add_usage":
                context.usage.add(
                    wall_time_sec=float(update.get("wall_time_sec", 0) or 0),
                    cost_usd=float(update.get("cost_usd", 0) or 0),
                    token_used=int(update.get("token_used", 0) or 0),
                    node_calls=int(update.get("node_calls", 0) or 0),
                )

            else:
                raise ValueError(f"Unknown update operation: {op}")
    except Exception:
        # snapshot_data は既に deepcopy 済みなのでそのまま代入（二重 deepcopy を避ける）
        context._data = snapshot_data
        (
            context.usage.wall_time_sec,
            context.usage.cost_usd,
            context.usage.token_used,
            context.usage.node_calls,
        ) = usage_backup
        raise
