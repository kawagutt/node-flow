# NodeFlow

**NodeFlow v1.4.4 (runtime-min)** — Everything is a Node、再帰可能なワークフロー実行基盤

## 概要

NodeFlow は、LLM、スクリプト、外部ツールを統合する Pipeline 主導のワークフロー実行基盤です。本版は **runtime-min** であり、直列 1-shot Pipeline・ScriptNode・LLMNode をサポートします。

### runtime-min の制限

本版では以下は未実装です。

- Loop（LoopNode）
- resume
- re_execute
- usage 集計

limit は **max_calls のみ有効**です。max_tokens は将来実装予定です。

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

examples は self-contained です。workspace に `examples` を指定して実行します。

```bash
nodeflow run pipelines/hello.yaml -w examples
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

## Workspace について

**workspace** は CLI の `-w` / `--workspace` で指定する「作業ディレクトリ」です（リポジトリ内の `workspace` という名前のフォルダではありません）。

- デフォルトはカレントディレクトリ（`.`）
- `nodes/<ノード名>/config.yaml` や、python_script の `script` パスは、この workspace からの相対パスで解決されます
- 例: プロジェクトルートで実行するときは `-w .`（省略可）。別のディレクトリを workspace にしたいときは `nodeflow run pipeline.yaml -w /path/to/my/project` のように指定します

## プロジェクト構造

- **Core**（`nodeflow/core/`）: 抽象実行モデル。BaseNode・StructuralNode（抽象）・Runner・**NodeRegistry**。IO に依存しない。
- **Extensions**（`nodeflow/extensions/`）: 公式実装。PipelineNode・PythonScriptNode・LLMNode・OpenRouterNode。起動時に **registry に登録**され、loader は type 文字列でクラスを解決する。
- **Execution**（`nodeflow/execution/`）: IO adapter 層。YAML 読み込み・設定・Pipeline 組み立て・「ロードして実行」の入口（`load_pipeline`, `load_and_kick_pipeline`）。

```
nodeflow/
├── nodeflow/
│   ├── __init__.py        # import nodeflow.extensions で registry を埋める
│   ├── core/
│   │   ├── base_node.py   # BaseNode, StructuralNode（抽象）
│   │   ├── runner.py      # Runner（極小化）
│   │   └── registry.py    # NodeRegistry（type → クラス）
│   ├── extensions/
│   │   ├── pipeline_node.py
│   │   ├── python_script.py
│   │   ├── llm.py
│   │   └── openrouter.py
│   ├── execution/
│   │   ├── loader.py      # pipeline.yaml の parse、registry.resolve で Node 組み立て
│   │   ├── config.py      # YAML 読み込み、deep merge
│   │   └── run.py         # load_and_kick_pipeline（CLI の入口）
│   ├── cli.py             # CLI エントリーポイント
│   └── sdk/
│       └── __init__.py
├── examples/
│   ├── pipelines/
│   └── nodes/
├── nodes/                 # このリポジトリを workspace にしたときのノード定義（-w . のとき参照）
├── tests/
└── pyproject.toml
```

### 外部拡張（カスタム Node）

自作の Node クラスを `nodeflow.core.registry.registry` に `register("my_type", MyNode)` で登録すると、pipeline YAML の `type: my_type` で利用できる。未登録の type は `UnknownNodeTypeError`、`loop` は「未実装」として `NotImplementedError` になる。

## ライセンス

MIT
