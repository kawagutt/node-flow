"""
BaseNode 基底クラス実装
"""
from typing import Dict, Any, Optional
from .context import Context
from .updates import apply_updates


class BaseNode:
    """
    BaseNode 基底クラス
    
    v1.11 の統一実行モデル:
    1. check_limits_pre（「今から実行していいか？」を判定）
    2. call_count++（「実行した回数」を記録）
    3. run（actual execution）
    4. apply_updates（BaseNode.execute だけが呼ぶ）
    5. check_limits_post（updates 適用後）
    """
    
    DEFAULT_CONFIG: Dict[str, Any] = {}
    SCHEMA: Dict[str, Any] = {}
    
    def __init__(self, config: Dict[str, Any], system_info: Dict[str, Any]):
        """
        Node を初期化
        
        Args:
            config: Runner が deep merge 済みの最終 config
            system_info: workspace_dir, run_id, execution など
        """
        self.config = config
        self.system_info = system_info
    
    def run(self, context: Context) -> Dict[str, Any]:
        """
        Node の実行（Node が override する）
        
        Args:
            context: Context インスタンス
        
        Returns:
            {
                "status": "success" | "retry" | "escalate" | "abort" | "timeout",
                "updates": [
                    {"op": "set", "path": "...", "value": ...},
                    ...
                ]
            }
        
        Raises:
            NotImplementedError: 未実装の場合
        """
        raise NotImplementedError("Subclass must implement run()")
    
    def check_limits_pre(self, context: Context) -> Optional[str]:
        """
        pre-limit 評価（Node が override、任意）
        
        updates 適用前に実行される
        
        Args:
            context: Context インスタンス
        
        Returns:
            status 文字列（limit 違反の場合）または None
        """
        return None
    
    def check_limits_post(self, context: Context) -> Optional[str]:
        """
        post-limit 評価（Node が override、任意）
        
        updates 適用後に実行される
        
        Args:
            context: Context インスタンス
        
        Returns:
            status 文字列（limit 違反の場合）または None
        """
        return None
    
    def execute(self, context: Context, step_id: str) -> Dict[str, Any]:
        """
        v1.11 の統一実行モデル
        
        limit 呼び出しは BaseNode.execute に完全固定
        
        Args:
            context: Context インスタンス
            step_id: step ID（artifacts 書き込み制限に使用）
        
        Returns:
            {
                "status": "success" | "retry" | "escalate" | "abort" | "timeout",
                "updates": [...]
            }
        """
        exec_state = self.system_info.get("execution", {})
        
        # 1. pre-limit（「今から実行していいか？」を判定）
        status = self.check_limits_pre(context)
        if status:
            return {"status": status, "updates": []}
        
        # 2. call_count を増やす（「実行した回数」を記録）
        if "call_count" not in exec_state:
            exec_state["call_count"] = 0
        exec_state["call_count"] += 1
        
        # 3. actual execution
        result = self.run(context)
        
        # 4. apply_updates（BaseNode.execute だけが呼ぶ、step_id で書き込み制限）
        apply_updates(context, result.get("updates", []), step_id)
        
        # 5. post-limit（updates 適用後）
        status = self.check_limits_post(context)
        if status:
            return {"status": status, "updates": []}
        
        return result
