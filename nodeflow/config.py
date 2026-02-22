"""
設定管理モジュール（YAML読み込み、deep merge）
"""
import copy
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


def load_yaml(file_path: str) -> Dict[str, Any]:
    """YAML ファイルを読み込む"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    deep merge 実装
    
    ルール:
    - dict: 再帰マージ
    - list: 上書き
    - scalar: 上書き
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # dict の場合は再帰マージ
            result[key] = deep_merge(result[key], value)
        else:
            # list や scalar の場合は上書き
            result[key] = value
    
    return result


def load_node_config(
    node_name: str,
    workspace_dir: str,
    pipeline_config: Optional[Dict[str, Any]] = None,
    default_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Node config を読み込んで deep merge。

    優先順位（仕様 §6）:
    1. DEFAULT_CONFIG（Node クラスから取得し default_config で渡す）
    2. nodes/<node>/config.yaml
    3. Pipeline step config
    """
    # クラス変数 DEFAULT_CONFIG の参照共有を避けるため deepcopy
    base = copy.deepcopy(default_config) if default_config else {}
    config_path = Path(workspace_dir) / "nodes" / node_name / "config.yaml"
    folder_config = {}
    if config_path.exists():
        folder_config = load_yaml(str(config_path))

    pipeline_config = pipeline_config or {}
    merged_config = deep_merge(deep_merge(base, folder_config), pipeline_config)
    return merged_config


def load_global_config(workspace_dir: str) -> Dict[str, Any]:
    """グローバル設定（nodeflow.yaml）を読み込む"""
    config_path = Path(workspace_dir) / "nodeflow.yaml"
    if config_path.exists():
        return load_yaml(str(config_path))
    return {}
