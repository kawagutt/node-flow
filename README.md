# NodeFlow

**Everything is a Node** - 再帰可能なワークフロー実行基盤

## 概要

NodeFlow は、LLM、スクリプト、外部ツールを統合する Pipeline 主導のワークフロー実行基盤です。本版は **runtime-min** であり、直列 1-shot Pipeline・ScriptNode・LLMNode・limit（max_calls / max_tokens）をサポートします。LoopNode / pause-resume は未実装です。

### 設計思想

- **Everything is a Node** - Pipeline も Node として実装
- **Limit evaluation is also a Node's responsibility** - limit 評価は Node 側で行う
- **Runner is dumb** - Runner は極小化され、root PipelineNode を execute するだけ
- **metrics = updates(add_metric) only** - metrics は updates の add_metric のみで更新

## インストール

```bash
pip install -e .
```

## クイックスタート

```bash
nodeflow run examples/pipelines/hello.yaml
```

### 実 API（OpenRouter）を使う場合

pipeline YAML でノードに `type: openrouter` を指定し、環境変数 `OPENROUTER_API_KEY` を設定してください。

```yaml
# graph.nodes の例
- id: llm_call
  type: openrouter
  params:
    model: "openai/gpt-4o-mini"
```

```bash
export OPENROUTER_API_KEY=your_key_here
nodeflow run your_pipeline.yaml
```

## プロジェクト構造

```
nodeflow/
├── nodeflow/
│   ├── __init__.py
│   ├── runner.py          # Runner（極小化）
│   ├── node.py            # BaseNode 基底クラス
│   ├── pipeline_node.py   # PipelineNode（Pipeline も Node）
│   ├── config.py          # 設定管理（YAML読み込み、deep merge）
│   ├── context.py         # Context クラス
│   ├── updates.py         # Updates モデル
│   ├── schema.py          # Schema 検証
│   ├── logger.py          # Execution Log v2
│   ├── cli.py             # CLI エントリーポイント
│   └── sdk/
│       ├── __init__.py
│       ├── templates.py   # Jinja2 テンプレートレンダリング
│       ├── llm.py         # LLM 呼び出し
│       └── shell.py       # シェル実行
├── examples/
│   ├── pipelines/
│   └── nodes/
├── tests/
└── pyproject.toml
```

## ライセンス

MIT
