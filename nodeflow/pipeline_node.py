"""
PipelineNode 実装（Pipeline も Node）
"""
from typing import Dict, Any, Optional
import importlib.util
from pathlib import Path
from .node import BaseNode
from .context import Context
from .config import load_node_config


class PipelineNode(BaseNode):
    """
    PipelineNode - Pipeline も Node として実装
    
    PipelineNode も通常 Node と同じく BaseNode.execute() を使用
    limit 評価は BaseNode.execute に完全固定（PipelineNode.run 内では呼ばない）
    """
    
    def __init__(self, config: Dict[str, Any], system_info: Dict[str, Any], pipeline_data: Dict[str, Any]):
        """
        PipelineNode を初期化
        
        Args:
            config: Runner が deep merge 済みの最終 config
            system_info: workspace_dir, run_id, execution など
            pipeline_data: Pipeline YAML のデータ
        """
        super().__init__(config, system_info)
        self.pipeline_data = pipeline_data
        self.steps = pipeline_data.get("steps", [])
        self.workspace_dir = system_info.get("workspace_dir", ".")
    
    def run(self, context: Context) -> Dict[str, Any]:
        """
        Pipeline の実行（limit 評価は呼ばない）。
        単一レベルの pipeline のみ。Nested pipeline / depth limit は Phase 2。

        Args:
            context: Context インスタンス
        
        Returns:
            {
                "status": "success" | "retry" | "escalate" | "abort" | "timeout",
                "updates": []
            }
        """
        # step loop
        step_index = 0
        while step_index < len(self.steps):
            step = self.steps[step_index]

            # Node を実行
            result = self._execute_step(step, context)
            
            # abort / timeout / escalate は即伝播
            status = result.get("status")
            if status in ["abort", "timeout", "escalate"]:
                return result
            
            # 状態遷移解決
            next_step_id = self._resolve_transition(step, status)
            
            if next_step_id == "STOP":
                break
            
            if next_step_id == "NEXT":
                # success → 次の step
                step_index += 1
                continue
            
            # 次の step を探す
            next_index = self._find_step_index(next_step_id)
            if next_index is None:
                break
            step_index = next_index
        
        return {"status": "success", "updates": []}
    
    def _execute_step(self, step: Dict[str, Any], context: Context) -> Dict[str, Any]:
        """
        単一 step を実行
        
        Args:
            step: step 定義
            context: Context インスタンス
        
        Returns:
            Node の実行結果
        """
        step_id = step.get("id")
        node_name = step.get("node")
        
        if not node_name:
            return {"status": "abort", "updates": []}
        
        # Node クラスをロード
        node_class = self._load_node_class(node_name)
        if node_class is None:
            return {"status": "abort", "updates": []}
        
        # Node config を deep merge（DEFAULT_CONFIG → config.yaml → step config）
        pipeline_config = step.get("config", {})
        default_config = getattr(node_class, "DEFAULT_CONFIG", {})
        node_config = load_node_config(
            node_name, self.workspace_dir, pipeline_config, default_config=default_config
        )
        
        # Node インスタンス生成
        node = node_class(node_config, self.system_info)
        
        # Node.execute を呼ぶ（limit 評価・node_calls 加算は BaseNode.execute 内で実施）
        return node.execute(context, step_id=step_id)
    
    def _load_node_class(self, node_name: str):
        """
        Node クラスをロード
        
        Args:
            node_name: Node 名
        
        Returns:
            Node クラスまたは None
        """
        node_path = Path(self.workspace_dir) / "nodes" / node_name / "node.py"
        
        if not node_path.exists():
            return None
        
        # モジュールを動的にロード
        spec = importlib.util.spec_from_file_location(f"nodes.{node_name}.node", str(node_path))
        if spec is None or spec.loader is None:
            return None
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Node クラスを探す（BaseNode を継承しているクラス）
        for name in dir(module):
            obj = getattr(module, name)
            if (isinstance(obj, type) and 
                issubclass(obj, BaseNode) and 
                obj is not BaseNode):
                return obj
        
        return None
    
    def _resolve_transition(self, step: Dict[str, Any], status: str) -> str:
        """
        状態遷移を解決
        
        Args:
            step: step 定義
            status: Node の実行結果 status
        
        Returns:
            次の step ID または "STOP"
        """
        on_config = step.get("on", {})
        
        # on: で定義されている場合はそれを使用
        if status in on_config:
            return on_config[status]
        
        # デフォルト遷移
        if status == "success":
            # success → 次Step（呼び出し側で処理）
            return "NEXT"
        else:
            # retry / escalate / abort / timeout → STOP
            return "STOP"
    
    def _find_step_index(self, step_id: str) -> Optional[int]:
        """
        step ID から step のインデックスを探す
        
        Args:
            step_id: step ID
        
        Returns:
            step のインデックスまたは None
        """
        for i, step in enumerate(self.steps):
            if step.get("id") == step_id:
                return i
        return None
    
    def create_child_context(self, parent_context: Context) -> Context:
        """
        sub pipeline 用の context を作成
        
        Args:
            parent_context: 親 context
        
        Returns:
            新しい context
        """
        # sub_context を構築（usage は親と同一参照で共有＝単一累積）
        sub_context = Context(
            {
                "inputs": {},
                "artifacts": {},
                "metrics": {},
                "flags": parent_context.snapshot().get("flags", {}).copy(),
            },
            usage=parent_context.usage,
        )
        return sub_context
    
    def create_child_system_info(self, parent_system_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        sub pipeline 用の system_info を作成
        
        Args:
            parent_system_info: 親 system_info
        
        Returns:
            新しい system_info（execution はシャローコピー）
        """
        child_system_info = parent_system_info.copy()
        if "execution" in child_system_info:
            child_system_info["execution"] = dict(child_system_info["execution"])
        return child_system_info
