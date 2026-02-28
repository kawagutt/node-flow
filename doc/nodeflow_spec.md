# NodeFlow v1.4-runtime-min 仕様

**ブランチ**: `v1.3-runtime-min`  
**目的**: `ScriptNode → LLMNode → ScriptNode` が動くことを確認する最小構成  
**ベース**: NodeFlow v1.3

---

## このバージョンの方針

v1.3 の全機能を実装しようとすると作業範囲が広すぎて「動く状態」に到達しにくい。
本バージョンでは **「まず動く」** を最優先とし、以下の原則で機能を絞る。

- **実装する**：直列 Pipeline・ScriptNode・LLMNode・status・execute の基本フロー
- **スタブにする**：revision（ダミー値で十分）・usage（記録しない）
- **削除する（この版では実装しない）**：pause / resume / re_execute / invalidate / LoopNode / 循環グラフ / 並列実行 / limit（max_idle_sec 等の複雑なもの）

削除した機能は v1.5 以降で順次追加する。削除した API を呼んだ場合は `NotImplementedError` を raise する（サイレント無視はしない）。

---

## スコープ外（この版では実装しない）

| 機能 | 理由 | 追加予定 |
|------|------|---------|
| pause / PauseSignal / resume | 動作確認の本筋ではない | v1.5 |
| re_execute / invalidate | pause 実装後に意味が出る | v1.5 |
| execution_cursor | re_execute と一体 | v1.5 |
| clear_limit() | limit 整理と一体 | v1.5 |
| LoopNode | まず直列を動かす | v1.6 |
| 循環グラフ | LoopNode と一体 | v1.6 |
| 並列実行 | 同期直列で十分 | v1.6 |
| revision（完全実装） | content-hash は後回し。ダミー UUID で代替 | v1.5 |
| usage | 観測のみ。limit 判定には使わない | v1.5 |
| limit（複雑なもの） | max_calls のみ実装する | v1.5 で拡張 |
| hash_skip | revision が仮実装なので不要 | v1.5 |
| node_params_override | re_execute と一体 | v1.5 |

---

# Part I — Core Model

v1.3 から変更なし。本バージョンでは Loop 演算子は定義するが実装しない（§4 は仕様として存在するが、LoopNode の実装は v1.6 以降）。

---

# Part II — Execution Layer v1.4-runtime-min

---

## 0. Execution Scope

トップレベル `PipelineNode.execute()` の 1 回の呼び出しのライフタイムを指す。

本バージョンでは Execution Scope は常に 1 つ（ネストなし）。

**空 dict `{}` の扱い**：`execute()` が `{}` を返した場合、Runner は当該ノードの `latest_output` を更新しない。

---

## 1. 設計原則（この版での絞り込み）

- Everything is a Node
- PipelineNode は Graph を直列 1-shot 実行する Node
- Runner is dumb
- データは input / output のみで流れる
- 終了は final ノード（graph で明示指定）による
- 例外は Node が吸収する

**この版では採用しない原則**（将来版で復活）：

- Loop も Node（LoopNode は未実装）
- graceful stop（本版では fatal が発生したらそのまま停止）
- revision は I/O 契約（本版では UUID ダミーで代替）

### 1.1 Node 分類（この版）

```
Node (abstract)
 ├── DataNode (abstract)
 │       ├── PythonScriptNode   ← 実装する
 │       └── LLMNode            ← 実装する
 └── StructuralNode (abstract)
         └── PipelineNode       ← 実装する（LoopNode は未実装）
```

---

## 2. Node

### 2.1 実行インターフェース

```python
execute(inputs: dict, params: dict) -> dict
```

- inputs：他ノードから渡された dict
- params：静的実行設定（immutable）
- return：必ず dict

`execute` は常に dict を返す。`None` や非 dict 型は仕様違反。

### 2.2 execute の流れ（この版）

pause / resume / limit post の複雑なケースはスコープ外のため、シンプルに定義する。

```
execute(inputs, params)
  ├ status = executing
  ├ limit pre     → max_calls 超過なら status = limit, return {}
  ├ run(inputs, params)
  │   ├ その他例外 → status = fatal, return {}
  │   └ dict 返却  → 正常継続
  ├ revision 補完  → UUID4() をダミーとして付与（§5.1 参照）
  ├ status = done
  └ return dict
```

**この版では PauseSignal・LimitSignal は定義するが、raise しても NotImplementedError を発生させる。**
run 内でこれらを raise した場合の挙動は未定義とする（v1.5 で正式実装）。

### 2.3 Node 状態モデル（この版）

状態集合は **ready / executing / done / fatal / limit** の 5 値とする（pause は v1.5 で追加）。

**状態遷移（この版）：**

```
ready → executing → done
                 ↘ fatal
                 ↘ limit
```

- `done` は再実行可能
- `reset` は不要
- `pause` は v1.5 まで未対応

| status | 意味 |
|--------|------|
| ready | 実行待ち |
| executing | 実行中 |
| done | 実行完了（再実行可能） |
| fatal | 異常終了 |
| limit | limit 到達 |

**status の制約：**

- status は Node 自身のみが変更可能
- StructuralNode は子の status を直接変更してはならない
- `done / fatal / limit → ready` の遷移は禁止

#### 2.3.1 read_status()

```python
def read_status(self) -> str:
    """現在の status を返す（"ready" / "executing" / "done" / "fatal" / "limit"）"""
```

status は execute の戻り値には含まれない。

---

## 3. 入出力モデル

### 3.1 inputs のバインディング

Graph 定義（pipeline.yaml）で記述する。

```yaml
inputs:
  key: ${node_id.port}
```

参照形式：

- `${node_id.port}` — 他ノードの output port を参照
- `${inputs.port}` — PipelineNode が受け取った inputs の port を参照
- `${params.<name>}` — PipelineNode の params を参照

**この版での制約：**

- 未定義参照は「実行不能」として扱う（fatal にしない）
- 循環参照はサポートしない（静的診断なし）
- required / optional の区別はすべて required 扱い

### 3.2 outputs

Node は必ず dict を返す。各 key が output port である。

```json
{
  "result": {
    "_meta": { "revision": "dummy-uuid-..." },
    "value": "hello"
  }
}
```

`_meta.revision` は UUID4 のダミーを付与する（§5.1 参照）。

### 3.3 出力保持ルール

Runner は各ノードの最新出力のみ保持する。

```python
output = node.execute(...)
if output != {}:
    latest_output[node_id] = output
```

`{}` は保存しない。

### 3.4 制御とデータの分離

| 種類 | 表現 |
|------|------|
| データ | execute の戻り dict |
| 制御 | read_status() |
| 出力更新 | output != {} のときのみ |

---

## 4. Param モデル

### 4.1 params の役割

Node の静的実行設定。実行中に変化しない。

### 4.2 limit（この版では max_calls のみ）

この版で実装する limit は `max_calls` のみとする。

```yaml
params:
  limit:
    max_calls: 10
```

`max_calls` を超えた場合、`execute` は `{}` を返し `status = limit` とする。

**この版では実装しない limit 種別**：max_wall_time_sec / max_idle_sec / max_total_node_calls / max_iterations

### 4.3 params は immutable

BaseNode.execute は params を shallow freeze してから run に渡す。

---

## 5. Revision モデル（この版：ダミー実装）

revision の完全実装（content-hash / RFC 8785 / SHA-256）は v1.5 で行う。

### 5.1 この版での revision 実装

```python
import uuid

def _attach_revision(output: dict) -> dict:
    """各 output port に UUID4 のダミー revision を付与する"""
    for port_key, port_value in output.items():
        if isinstance(port_value, dict):
            port_value.setdefault("_meta", {})
            port_value["_meta"]["revision"] = str(uuid.uuid4())
    return output
```

**この版での性質：**

- revision は毎回異なる値になる（deterministic ではない）
- 同一内容でも異なる revision が付与される
- revision を比較しての no-op 最適化はこの版では動作しない（v1.5 で修正）
- `_meta` は予約キーである（この点は v1.5 以降と同じ）

### 5.2 Node 実装者への注意

Node の run() は `_meta` キーを直接設定してはならない。BaseNode が自動付与する。これは v1.5 以降も変わらない。

---

## 6. BaseNode

### 6.1 役割

すべての Node が継承する基底クラス。

```python
class BaseNode:
    def __init__(self):
        self._status = "ready"
        self._error: Exception | None = None
        self._call_count = 0

    def execute(self, inputs: dict, params: dict) -> dict:
        """共通実行テンプレート。サブクラスは run() のみ実装する。"""
        self._status = "executing"
        self._call_count += 1

        # limit pre（max_calls のみ）
        max_calls = params.get("limit", {}).get("max_calls")
        if max_calls is not None and self._call_count > max_calls:
            self._status = "limit"
            return {}

        # run 呼び出し
        try:
            frozen_params = _freeze(params)
            result = self.run(inputs, frozen_params)
        except Exception as e:
            self._status = "fatal"
            self._error = e
            return {}

        # revision 付与（ダミー）
        result = _attach_revision(result)

        self._status = "done"
        return result

    def run(self, inputs: dict, params: dict) -> dict:
        raise NotImplementedError

    def read_status(self) -> str:
        return self._status

    def read_error(self) -> Exception | None:
        return self._error
```

### 6.2 DataNode の責任（この版）

- `run()` の実装
- `execute()` は BaseNode の共通テンプレートを使う
- revision 付与は BaseNode が行う（run 実装者は `_meta` を触らない）
- usage は記録しない（v1.5 で追加）

### 6.3 StructuralNode の責任（この版）

- PipelineNode のみ実装する
- 子ノードの execute を管理する
- status の集約（§6.7 の简略版）
- 終了判定（final ノードが done になったら終了）

**StructuralNode.run（この版）：**

```python
def run(self, inputs, params):
    self._init_context(inputs)
    while True:
        progressed = self._step()
        if not progressed:
            break  # 実行可能ノードなし → deadlock or 完了
        if self._is_terminated():
            break
    return self._get_final_output()
```

**終了条件（この版）：**

- **done**：final ノードの status が done
- **fatal**：いずれかの子ノードが fatal → StructuralNode も fatal
- **limit**：いずれかの子ノードが limit → StructuralNode も limit

status の優先順位：**fatal > limit > done > executing > ready**

**StructuralNode.execute の戻り値（この版）：**

| 終了理由 | 戻り値 |
|----------|--------|
| done | final ノードの出力 |
| fatal | `{}` |
| limit | `{}` |

---

## 7. Runner（この版）

### 7.1 同期直列実行

この版では並列実行は行わない。graph.nodes の記述順に走査し、実行可能なノードを 1 つ execute する。

```python
def step(self) -> bool:
    """実行可能なノードを 1 つ見つけて execute する。実行した場合 True を返す。"""
    for node_id in self.graph_node_order:
        node = self.nodes[node_id]
        if node.read_status() not in ("ready", "done"):
            continue
        inputs = self._resolve_inputs(node_id)
        if inputs is None:
            continue  # 実行不能（input 未解決）
        output = node.execute(inputs, self._get_params(node_id))
        if output != {}:
            self.latest_output[node_id] = output
        return True
    return False
```

### 7.2 Runner の責務（この版）

Runner が行うこと：

- inputs 解決
- 実行可能判定（status が ready または done かつ全 input が解決済み）
- node.execute 呼び出し
- 最新出力保存

Runner が行わないこと（v1.3 と同じ）：

- status の意味を解釈しない
- limit / usage を評価しない
- revision を解釈しない
- 例外処理をしない

### 7.3 実行可能判定（この版）

```
実行可能 ⟺ status ∈ {ready, done}
           ∧ 全 input port の参照先が latest_output に存在する
```

`required` の概念はこの版では省略し、全 input を required 扱いとする。

### 7.4 deadlock の扱い（この版）

step() が False を返し続ける（実行可能ノードが 0）かつ final ノードが done でない場合は、PipelineNode が `{}` を返して終了する（status = limit、理由 = "no progress"）。

これは簡易的な deadlock 保護である。max_idle_sec 等の精緻な実装は v1.5 で行う。

---

## 8. 循環グラフ

この版では**サポートしない**。YAML ロード時に静的チェックを行い、循環を検出したら `CyclicGraphError` を raise する。

---

## 9. 例外処理モデル（この版）

### 9.1 Node が吸収

BaseNode.execute が例外を捕捉し status = fatal とする。

```python
def read_error(self) -> Exception | None:
    return self._error
```

StructuralNode は子の read_error() を集約して返す。

### 9.2 この版では扱わないもの

- PauseSignal・LimitSignal（define only、raise すると NotImplementedError）
- resume / re_execute（NotImplementedError）

---

## 10. スコープルール

### 10.1 Node が参照できるもの

常に次のみ：

- 自身の `inputs`
- 自身の `params`

### 10.2 入力バインディングのソース（この版）

- `${node_id.port}` — 他ノードの output port
- `${inputs.port}` — PipelineNode が受け取った inputs
- `${params.<name>}` — PipelineNode の params

---

## 11. 定義ファイル

### 11.1 pipeline.yaml（この版）

```yaml
version: "1.4-min"
name: my_pipeline

inputs:
  raw_data: { type: object }

params:
  model: { type: string, default: "gpt-4o-mini" }

graph:
  nodes:
    - id: preprocess
      type: python_script
      inputs:
        data: ${inputs.raw_data}
      params:
        script: "scripts/preprocess.py"
        limit:
          max_calls: 3

    - id: llm_call
      type: llm
      inputs:
        prompt: ${preprocess.result}
      params:
        model: ${params.model}
        limit:
          max_calls: 3

    - id: postprocess
      type: python_script
      inputs:
        data: ${llm_call.response}
      params:
        script: "scripts/postprocess.py"
        limit:
          max_calls: 3

  final: postprocess
```

### 11.2 version フィールド

`version: "1.4-min"` を必須とする。不一致時は `VersionMismatchError` を raise する。

### 11.3 Node Type Registry（この版）

| type 文字列 | クラス |
|-------------|--------|
| `"pipeline"` | PipelineNode |
| `"python_script"` | PythonScriptNode |
| `"llm"` | LLMNode |

LoopNode は未実装。`type: loop` を指定した場合は `NotImplementedError` を raise する。

---

# Part III — Concrete Nodes（この版）

---

## 13.1 PythonScriptNode

### 13.1.1 役割

指定した Python スクリプトを実行し、inputs を渡して result を返す。

### 13.1.2 node.yaml 相当の定義

```yaml
type: python_script
inputs:
  data: { type: object }
outputs:
  result: { type: object }
params:
  script: { type: string, description: "実行する .py のパス" }
  limit:
    max_calls: { type: integer }
```

### 13.1.3 run() の実装

```python
class PythonScriptNode(BaseNode):
    def run(self, inputs: dict, params: dict) -> dict:
        script_path = params["script"]
        # スクリプトを import して main(inputs) を呼ぶ
        spec = importlib.util.spec_from_file_location("script", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result = module.main(inputs)
        if not isinstance(result, dict):
            raise TypeError(f"script must return dict, got {type(result)}")
        return {"result": result}
```

**スクリプト規約**：スクリプトは `main(inputs: dict) -> dict` を実装すること。

```python
# scripts/preprocess.py の例
def main(inputs: dict) -> dict:
    data = inputs["data"]
    prompt = f"以下のデータを分析してください:\n{data}"
    return {"prompt_text": prompt}
```

### 13.1.4 status の扱い

| 状態 | 意味 |
|------|------|
| done | 正常終了 |
| fatal | スクリプト例外・TypeError 等 |
| limit | max_calls 超過 |

### 13.1.5 特記事項

- この版ではスクリプトはインプロセスで実行する（subprocess は使わない）
- timeout は実装しない（v1.5 で追加）
- スクリプトの例外は Node が吸収して fatal とする

---

## 13.2 LLMNode

### 13.2.1 役割

LLM API を呼び出す Node。この版では OpenAI Chat Completions API を想定する。

### 13.2.2 定義

```yaml
type: llm
inputs:
  prompt: { type: string }
outputs:
  response: { type: string }
params:
  model: { type: string, default: "gpt-4o-mini" }
  temperature: { type: number, default: 0.7 }
  system_prompt: { type: string, default: "" }
  limit:
    max_calls: { type: integer }
```

### 13.2.3 run() の実装

```python
class LLMNode(BaseNode):
    def run(self, inputs: dict, params: dict) -> dict:
        messages = []
        if params.get("system_prompt"):
            messages.append({"role": "system", "content": params["system_prompt"]})
        messages.append({"role": "user", "content": inputs["prompt"]})

        response = openai.chat.completions.create(
            model=params["model"],
            messages=messages,
            temperature=params["temperature"],
        )
        text = response.choices[0].message.content
        return {"response": text}
```

### 13.2.4 status の扱い

| 状態 | 意味 |
|------|------|
| done | 正常終了 |
| fatal | API 例外・ネットワークエラー等 |
| limit | max_calls 超過 |

### 13.2.5 特記事項

- この版では LLM セッション履歴は保持しない（ステートレス）
- pause（human-in-the-loop）は v1.5 で実装する
- usage（prompt_tokens 等）は記録しない（v1.5 で追加）

---

## 13.3 PipelineNode

### 13.3.1 役割

Graph を直列 1-shot 実行する StructuralNode。

### 13.3.2 定義ファイル

§11.1 の pipeline.yaml 参照。

### 13.3.3 実行モデル

```python
class PipelineNode(BaseNode):
    def run(self, inputs: dict, params: dict) -> dict:
        self._context = {"latest_output": {}, "pipeline_inputs": inputs}
        runner = Runner(self._graph, self._context)

        while True:
            progressed = runner.step()
            if not progressed:
                # 進捗なし
                break
            # 終了判定
            final_status = self._get_node(self._final_id).read_status()
            if final_status == "done":
                break
            if final_status in ("fatal", "limit"):
                break

        return self._collect_output()

    def _collect_output(self) -> dict:
        final_node = self._get_node(self._final_id)
        if final_node.read_status() != "done":
            return {}
        return self._context["latest_output"].get(self._final_id, {})
```

### 13.3.4 status の集約（この版）

| 子ノードの状態 | PipelineNode の status |
|----------------|------------------------|
| いずれかが fatal | fatal |
| いずれかが limit | limit |
| final が done | done |
| 進捗なし（deadlock） | limit（理由 = "no progress"） |

優先順位：**fatal > limit > done**

### 13.3.5 特記事項

- この版では `resume()` / `re_execute()` / `clear_limit()` は `NotImplementedError` を raise する
- execution_cursor は持たない（v1.5 で追加）
- LoopNode のネストは未対応

---

# Part IV — Invariants（この版）

## 14. 不変条件

v1.3 の全不変条件のうち、この版で意味を持つものを列挙する。

1. **Node の内部状態は Node のみが変更可能**
2. **StructuralNode は子を black-box として扱う**
3. **execute は ready または done のときのみ通常経路で呼べる**
4. **Node インスタンスは同一 Execution Scope で再利用する**
5. **revision は BaseNode が付与する。Node 実装者は `_meta` を直接操作してはならない**（この版では UUID ダミー）
6. **StructuralNode は子ノードの出力内容を変更してはならない**
7. **BaseNode は fatal 発生時に原因例外を保持し、read_error() で公開する**
8. **execute は常に dict を返す。None を返してはならない**
9. **PauseSignal・LimitSignal を run() 内で raise した場合は NotImplementedError が発生する**（この版のみ）

---

# v1.5 に向けた TODO リスト

本バージョンで「動く」ことを確認したら、v1.5 で以下を追加する。

| 優先 | 機能 | 内容 |
|------|------|------|
| 高 | revision 完全実装 | SHA-256 content-hash / RFC 8785 Canonical JSON |
| 高 | PauseSignal / resume | LLMNode の human-in-the-loop |
| 高 | usage 記録 | prompt_tokens / completion_tokens |
| 中 | limit 拡張 | max_wall_time_sec / max_idle_sec / max_total_node_calls |
| 中 | re_execute + invalidate | node2 から再スタート + params_override |
| 中 | execution_cursor | resume / re_execute の統一基盤 |
| 中 | clear_limit() | limit 解除 + resume |
| 低 | LoopNode | 条件付き反復実行 |
| 低 | 並列実行 | async 対応 |
| 低 | 循環グラフ | LoopNode と一体 |

---

# チェックリスト（動作確認）

`ScriptNode → LLMNode → ScriptNode` が動くことを確認するための最小チェック項目。

```
[ ] PipelineNode.execute() が dict を返す
[ ] ScriptNode(preprocess) の run() が呼ばれ、result が返る
[ ] LLMNode の run() が呼ばれ、response が返る
[ ] ScriptNode(postprocess) の run() が呼ばれ、result が返る
[ ] 各ノードの status が done になる
[ ] PipelineNode の status が done になる
[ ] final ノードの出力が PipelineNode の戻り値として返る
[ ] 各 output port に _meta.revision が付与されている（UUID ダミーでよい）
[ ] ScriptNode が例外を throw した場合に status = fatal になる
[ ] LLMNode が例外を throw した場合に status = fatal になる
[ ] fatal 発生時に PipelineNode が {} を返す
[ ] max_calls を超えた場合に status = limit になる
[ ] read_error() で原因例外が取得できる
```