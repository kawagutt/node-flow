"""
Runner 極小実装
"""
import time
from typing import Dict, Any, Optional
from pathlib import Path
from .pipeline_node import PipelineNode
from .context import Context
from .config import load_yaml, load_global_config, load_node_config


class Runner:
    """
    Runner - 極小化
    
    Runner の責務:
    - root PipelineNode を execute
    - 初期 context を作成
    - 初期 system_info.execution を作成
    - deep merge
    - logging（Execution Log v2）
    """
    
    def __init__(self, workspace_dir: str, run_id: Optional[str] = None):
        self.workspace_dir = Path(workspace_dir).resolve()
        self.run_id = run_id or f"run_{int(time.time())}"
        self.global_config = load_global_config(str(self.workspace_dir))
    
    def run(self, pipeline_path: str, initial_inputs: Optional[Dict[str, Any]] = None) -> Context:
        """Pipeline を実行"""
        pipeline_data = load_yaml(pipeline_path)
        
        context = Context({
            "inputs": initial_inputs or {},
            "artifacts": {},
            "metrics": {},
            "flags": {},
        })
        
        system_info = {
            "workspace_dir": str(self.workspace_dir),
            "run_id": self.run_id,
            "execution": {},
        }
        
        pipeline_config = self.global_config.get("pipeline", {})
        default_config = getattr(PipelineNode, "DEFAULT_CONFIG", {})
        node_config = load_node_config(
            "pipeline", str(self.workspace_dir), pipeline_config, default_config=default_config
        )
        
        pipeline_node = PipelineNode(node_config, system_info, pipeline_data)
        pipeline_node.execute(context, step_id="root")
        
        return context
