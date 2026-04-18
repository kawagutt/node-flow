# NodeFlow 仕様書

**収録内容**

- **Part I–IV**: NodeFlow **v1.4.2 (runtime-min)** のコアモデルおよび実行層仕様（ブランチ `v14-runtime-min`、ベース v1.3）。`ScriptNode → LLMNode → ScriptNode` が動く最小構成を対象とする。**過去層・実装参照用**として残す。
- **Part V**: **NodeFlow v1.5 Dispatcher Model**（本書における **v1.5 の正本仕様**）。node-flow を dispatcher として定義する node model。**node taxonomy・class 構造・project 構成の語彙は Part V を正本とする。** role と implementation kind の分離、単発ジョブ原則、Runner 禁止事項、および **§17 Project Layout Guidance**（仕様本文ではない実装整理方針）を含む。

Part II に現れる「v1.5 で追加」等の表記は **実行基盤**（pause / revision / usage 等）の拡張を指す。実行基盤は Part V の dispatcher model と **独立に**検討・実装でき、同一リリースに束ねる必要はない（詳細は **Part V §18**）。

---

## v1.4.2（Part I–IV）の方針

v1.3 の全機能を実装しようとすると作業範囲が広すぎて「動く状態」に到達しにくい。
本バージョンでは **「まず動く」** を最優先とし、以下の原則で機能を絞る。

- **実装する**：直列 Pipeline・ScriptNode・LLMNode・status・execute の基本フロー
- **v1.4.2**：すべての output port を dict に統一し、`_meta.revision` を必須とする（scalar port は存在しない）。
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
| reset_limit_state(name) | 本版で BaseNode に実装 | — |
| LoopNode | まず直列を動かす | v1.6 |
| 循環グラフ | LoopNode と一体 | v1.6 |
| 並列実行 | 同期直列で十分 | v1.6 |
| revision（完全実装） | content-hash は後回し。ダミー UUID で代替 | v1.5 |
| usage | 観測のみ。limit 判定には使わない | v1.5 |
| limit（複雑なもの） | max_calls のみ実装する | v1.5 で拡張 |
| hash_skip | revision が仮実装なので不要 | v1.5 |
| node_params_override | re_execute と一体 | v1.5 |

**LoopNode（v1.6）での再実行：** iteration 開始前に StructuralNode が子ノードの `reset_status()` を呼ぶことで再実行を行う。本版では StructuralNode が `reset_status()` を自動呼び出しすることはない（§6.3 と整合）。

---

# Part I — Core Model

v1.3 から変更なし。本バージョンでは Loop 演算子は定義するが実装しない（§4 は仕様として存在するが、LoopNode の実装は v1.6 以降）。

---

# Part II — Execution Layer v1.4-runtime-min

---

## 0. Execution Scope

トップレベル `PipelineNode.execute()` の 1 回の呼び出しのライフタイムを指す。本バージョンでは**概念定義のみ**であり、実質未使用である（ネストなし・常に 1 つ）。

**空 dict `{}` の扱い**：`execute()` が `{}` を返した場合、Runner は当該ノードの `latest_output` を更新しない。

---

## 1. 設計原則（この版での絞り込み）

- Everything is a Node
- PipelineNode は Graph を直列 1-shot 実行する Node
- Runner is dumb
- データは input / output のみで流れる
- 終了は final ノード（graph で明示指定）による
- 例外は Node が吸収する
- **limit state は Node に持つ**（条件は params、消費量は Node）
- **status は維持する**（観測用。制御は例外に委ねる）
- **Node の内部状態は Node のみが変更できる**
- **StructuralNode は子ノードの内部属性に直接アクセスしてはならない**。子に対する操作は `execute`, `read_status`, `read_error`, `reset_status`, `reset_limit_state` の呼び出しに限定する
- **execute は status == "ready" のときのみ呼び出される**。それ以外で呼ばれた場合は BaseNode.execute が RuntimeError を raise する
- **revision は本版ではダミー UUID 実装**（no-op 最適化は行わない）

### 1.1.1 Node の本質

Node は **「同期・原子的な状態遷移器」** である。

* execute は atomic に完了する
* 途中状態（executing 中）は外部から観測・操作できない（reset_status は呼べない）
* 状態遷移は execute（および reset_status / reset_limit_state）のみが行う

### 1.1.2 状態の三層構造

Node は次の三層を持つ：

| 種類 | 内容 | 永続性 |
|------|------|--------|
| 永続内部状態 | S, `_limit_state` | 持続 |
| 観測状態 | `_status`, `_error` | 持続 |
| 実行コンテキスト | ExecutionContext | execute 単位 |

**ExecutionContext** は execute 呼び出し単位で生成され、永続化しない。reset の対象ではない。詳細は §2.4 を参照。

**この版では採用しない原則**（将来版で復活）：

- Loop も Node（LoopNode は未実装）
- graceful stop（本版では fatal が発生したらそのまま停止）
- revision は I/O 契約（本版では UUID ダミーで代替）

### 1.2 Node 分類（この版）

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

**呼び出し前提条件：**  
execute() は status == "ready" のときのみ通常経路で呼び出される。status が "ready" 以外のときに execute() が呼ばれた場合、BaseNode.execute は RuntimeError を raise する。

**構造の固定（最終形）：** pre-limit → status=executing → ExecutionContext 生成 → run → usage 適用 → post-limit → revision → status=done。制御用例外は §2.2.1、ExecutionContext は §2.4 を参照。

```
execute(inputs, params)
  ├ pre-limit     → 超過なら status = limit, return {}
  ├ status = executing
  ├ ExecutionContext 生成・run に渡す
  ├ run(inputs, params, context)
  │   ├ NodeExecutionLimit   → status = limit, return {}
  │   ├ NodeExecutionFailure → status = fatal, return {}
  │   ├ その他例外           → status = fatal, return {}
  │   └ dict 返却            → 正常継続
  ├ finally: context 破棄
  ├ _apply_usage(result)     → _usage を result から取り除く（本版では集計なし。v1.5 で usage 集計）
  ├ post-limit    → 本版では常に False
  ├ revision 補完  → UUID4() をダミーとして付与（§5.1 参照）
  ├ status = done
  └ return dict
```

**この版では PauseSignal・LimitSignal は定義するが、raise しても NotImplementedError を発生させる。**
run 内でこれらを raise した場合の挙動は未定義とする（v1.5 で正式実装）。

### 2.2.1 Execution Control Exceptions（この版）

Node の実行制御専用の例外を定義する。RuntimeError は使わない。

```python
class NodeExecutionLimit(Exception):
    """Node の実行制約（limit）到達を示す例外"""

class NodeExecutionFailure(Exception):
    """Node の実行失敗（fatal）を示す例外。reason で区別する（例: child fatal / invalid execution state）。"""

    def __init__(self, reason: str = ""):
        self.reason = reason
        super().__init__(reason)
```

**意味の統一：**  
**NodeExecutionLimit は run 内で発生する制御例外である。** pre-limit / post-limit 判定では例外を使わず、execute が直接 `status = "limit"` を設定する。

**役割分離：**

| 要素 | 役割 |
|------|------|
| 例外（run 内） | 制御フロー |
| pre/post-limit | execute が status を直接変更 |
| status | 実行状態の観測 |
| limit_state | 制約消費量（Node が保持） |

### 2.3 Node 状態モデル（この版）

状態集合は **ready / executing / done / fatal / limit** の 5 値とする（pause は v1.5 で追加）。

**状態遷移（この版）：**

```
ready → executing → done
                 ↘ fatal
                 ↘ limit
```

- 本バージョンでは `done` 状態の Node は自動再実行されない。再実行する場合は明示的に `reset_status()` を呼ぶ必要がある。
- `pause` は v1.5 まで未対応

**実行可能条件（この版）：**

```
実行可能 ⟺ status == "ready"
```

| status | 意味 |
|--------|------|
| ready | 実行待ち（このときのみ実行可能） |
| executing | 実行中 |
| done | 実行完了（再実行されない） |
| fatal | 異常終了 |
| limit | limit 到達 |

**重要原則：**

* status は観測用の状態機械である
* 例外は制御用である
* 二重管理ではない（役割分離）

**status の制約：**

- status は Node 自身の execute() または reset_status() によってのみ変更される。外部から `_status` を直接変更してはならない。
- StructuralNode は子の status を直接変更してはならない
- `done / fatal / limit → ready` の遷移は `reset_status()` による場合のみ許容する

#### 2.3.1 read_status()

```python
def read_status(self) -> str:
    """現在の status を返す（"ready" / "executing" / "done" / "fatal" / "limit"）"""
```

status は execute の戻り値には含まれない。

#### 2.3.2 reset_status()

```python
def reset_status(self) -> None:
    """
    Node の status を ready に戻す。
    internal state S は変更しない。
    executing 状態では呼び出してはならない。
    """
```

**reset_status の動作：**

* **executing 状態では呼べない。** 呼び出した場合は RuntimeError を raise する。
* **limit / fatal / done からのみ ready へ遷移可能**（`reset_status()` による場合のみ許容）。
* `_status = "ready"`、`_error = None`。**limit_state は変更しない。**

#### 2.3.3 reset_limit_state()

```python
def reset_limit_state(self, name: str) -> None:
    """
    指定した limit state をリセットする。
    status は変更しない。
    """
```

**reset_limit_state の動作：**

* 指定した name（例: `"calls"`）に対応する limit state を 0 にリセットする
* status は変更しない
* 未定義の limit state 名を指定した場合は KeyError を raise する

**limit 状態から再実行する方法（この版）：**

1. **方法1**：limit state をリセットしてから status を戻す — `reset_limit_state("calls")` → `reset_status()`
2. **方法2**：limit 設定を緩和した params で再実行 — `reset_status()` → `execute(inputs, new_params)`（`new_params` の `max_calls` が現在の `calls` より大きい場合のみ有効。`reset_limit_state` は省略可能）

### 2.4 ExecutionContext（この版）

**スコープ：** Node ごと・execute 呼び出しごと。**永続化しない。**  
ExecutionContext は execute 呼び出し単位で生成され、**reset_status や reset_limit_state の対象ではない。**

**役割：** cooperative stop、経過時間取得、将来の pause 拡張基盤。本バージョンでは BaseNode が生成し run に渡すが、DataNode の run() は context を参照しなくてもよい。

```python
class ExecutionContext:
    def __init__(self):
        self._stop_requested = False
        self._start_time = time.monotonic()

    def request_stop(self) -> None:
        self._stop_requested = True

    def should_stop(self) -> bool:
        return self._stop_requested

    def elapsed_time(self) -> float:
        return time.monotonic() - self._start_time
```

**Node の責務：** BaseNode が context を生成し run に渡す。実行終了時（finally）に破棄する。永続保存しない。

**pause/resume 方針（v1.41）：** resume = 新しい execute 呼び出し。ExecutionContext は再利用しない（B 方針）。generator モデルは採用しない。

**v1.5 への引継ぎ：** v1.5 では `context.request_stop()` による cooperative stop は採用しない。pause は `PauseSignal` の raise で宣言する（v1.3 の設計に戻す）。`context.should_stop()` は将来の外部 trigger pause（v1.4 TBD）のために予約する。

### 2.5 run の契約（この版）

責務分離のため、run は次の契約を満たす。

* **run は必ず dict を返す。** None や非 dict は仕様違反。
* **run は `_meta` および `_usage` をキーとする output port を返してはならない。** これらは BaseNode が使用する予約キーである。usage を返す場合は戻り値のトップレベルに `_usage` を含め、BaseNode の `_apply_usage` が取り除く。run は `_meta` を設定してはならない（§5.2）。
* **run は `_status` を変更してはならない。** 状態遷移は execute のみが行う。
* **run は `_limit_state` を直接変更してはならない。** 消費量の更新は execute の `_apply_usage` が行う。
* **run が参照してよい外部状態は、inputs・params・ExecutionContext（context）のみである。** 自身の `_status` / `_limit_state` / `_error` への書き込みは禁止。

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

- 入力解決に失敗した場合、Runner は当該ノードの execute() を呼ばない。この場合、ノードの status は変更されない。
- 循環参照はサポートしない（静的診断なし）
- required / optional の区別はすべて required 扱い

### Output Port Contract (v1.4.2)

- 出力 port の値は常に dict である。
- すべての port は `_meta.revision` を持つ。
- Node.run の各 port payload は dict でなければならない。scalar を返した場合は `TypeError` とする。
- scalar port は存在しない。

### Revision Semantics (v1.4.2)

- revision は **port 出力イベントの識別子**である。
- revision は値の内容に依存しない。
- 同じ値を再出力した場合でも、revision は必ず新しい値になる。
- revision は「値が変わったかどうか」を保証するものではなく、
  **「その port が再出力された」ことを示すシグナル**である。
- 入力側 Node は、revision の変化によって更新を検出する。
- revision は content-hash ではない。
  将来 content-based revision を導入する場合は、
  このセマンティクスを明示的に変更する必要がある。

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

### 4.2 limit（この版では max_calls と max_tokens）

この版で実装する limit は **pre-limit の max_calls** と **post-limit の max_tokens** とする。**limit 判定は pre / post に分離される。**

* **pre-limit**：実行前に判定。純粋関数。状態変更は execute が行う。本版では max_calls のみ。超過時は run を実行せず `{}` を返し status = limit。
* **post-limit**：実行後に判定。本版では max_tokens を評価。run は実行済みであり、usage 加算・revision 付与後に判定。超過時も **result は返す**（revision 付き）。status = limit により PipelineNode が以降の実行を止める。**max_tokens は 1 以上の int を想定する**（0 を指定した場合も 0 >= 0 で即 post-limit となり仕様上は正しいが、運用では 1 以上を推奨）。

run() 内で NodeExecutionLimit を raise した場合も limit として扱う。

```yaml
params:
  limit:
    max_calls: 10
    max_tokens: 2000   # 省略時は post-limit 判定なし
```

`max_calls` を超えた場合（pre-limit）、`execute` は `{}` を返し `status = limit` とする。`max_tokens` を超えた場合（post-limit）、`execute` は **revision 付きの result を返し** `status = limit` とする。

**call count の扱い：** `_limit_state["calls"]` は **run を開始する直前にのみ** 増加する（実行開始回数。pre_limit で止まった呼び出しはカウントしない）。これにより max_calls=3 のとき 3 回実行後に calls=3 となり、4 回目の execute で pre_limit が True になって status=limit で return する。limit 到達後は `reset_limit_state("calls")` を呼ばない限り再実行できない。

**この版では実装しない limit 種別**：max_wall_time_sec / max_idle_sec / max_total_node_calls / max_iterations

### 4.2.1 limit state の位置（この版）

limit の**条件**（max_calls 等）は params 側に記述する。limit の**消費量**は Node が内部に保持する。

Node は `_limit_state` を dict で持つ（将来拡張可能）：

```python
# この版（Phase 8 以降）
self._limit_state = {"calls": 0, "tokens": 0}

# 将来拡張例
# {"calls": 0, "tokens": 0, "budget": 0}
```

**reset_limit_state(name)**：指定した name が `_limit_state` に存在しない場合は **KeyError** を raise する。`"calls"` / `"tokens"` など、実装が保持する key のみ指定可能。

### 4.3 params は immutable

BaseNode.execute は params を shallow freeze してから run に渡す。

---

## 5. Revision モデル（この版：ダミー実装）

revision の完全実装（content-hash / RFC 8785 / SHA-256）は v1.5 で行う。

### 5.1 この版での revision 実装

revision は **port 単位で必須**である。各 port の payload は dict であり、scalar を返した場合は `TypeError` とする。

```python
import uuid

RESERVED_KEYS = frozenset({"_meta", "_usage"})

def _attach_revision(output: dict) -> dict:
    """各 output port に UUID4 のダミー revision を付与。予約キーはスキップ。payload は dict のみ（scalar は TypeError）。"""
    for port_key, port_value in list(output.items()):
        if port_key in RESERVED_KEYS:
            continue
        if not isinstance(port_value, dict):
            raise TypeError(f"Port {port_key!r} payload must be dict in v1.4.2, got {type(port_value).__name__}")
        port_value.setdefault("_meta", {})
        port_value["_meta"]["revision"] = str(uuid.uuid4())
    return output
```

**この版での性質：**

- 本バージョンでは revision の値は実行ごとに異なる。
- revision に基づく no-op 最適化は一切行わない。
- `_meta` は予約キーである（この点は v1.5 以降と同じ）

### 5.2 Node 実装者への注意

Node の run() は `_meta` キーを直接設定してはならない。BaseNode が自動付与する。これは v1.5 以降も変わらない。各 port の payload は dict でなければならず、scalar を返した場合は `TypeError` となる。

---

## 6. BaseNode

### 6.1 役割

すべての Node が継承する基底クラス。execute の構造は固定し、pre-limit / ExecutionContext / run / usage 適用 / post-limit / revision の順で行う。

```python
class BaseNode:
    def __init__(self):
        self._status = "ready"
        self._error: Exception | None = None
        self._limit_state = {"calls": 0}  # limit 判定用（将来拡張可能）
        self._current_context: ExecutionContext | None = None

    def execute(self, inputs: dict, params: dict) -> dict:
        """共通実行テンプレート。サブクラスは run() を実装する。"""
        if self._status != "ready":
            raise RuntimeError("execute called when status is not ready")

        if self._check_pre_limit(params):
            self._status = "limit"
            return {}

        self._status = "executing"
        context = ExecutionContext()
        self._current_context = context
        self._limit_state["calls"] += 1  # run 開始前にのみ増加（pre_limit では増やさない）

        try:
            frozen_params = _freeze(params)
            result = self.run(inputs, frozen_params, context)
        except NodeExecutionLimit:
            self._status = "limit"
            return {}
        except NodeExecutionFailure as e:
            self._status = "fatal"
            self._error = e
            return {}
        except Exception as e:
            self._status = "fatal"
            self._error = e
            return {}
        finally:
            self._current_context = None

        self._apply_usage(result)
        if self._check_post_limit(result, params):
            self._status = "limit"
            return {}

        result = _attach_revision(result)
        self._status = "done"
        return result

    def _check_pre_limit(self, params: dict) -> bool:
        """実行前の limit 判定。本版では max_calls のみ。"""
        max_calls = params.get("limit", {}).get("max_calls")
        if max_calls is None:
            return False
        return self._limit_state["calls"] >= max_calls

    def _check_post_limit(self, result: dict, params: dict) -> bool:
        """実行後の limit 判定。本版では空実装。"""
        return False

    def _apply_usage(self, result: dict) -> None:
        """run の戻り値から _usage を取り除く（output port に残さない）。本版では集計は行わず pop のみ。v1.5 で limit_state への集計を追加する。"""
        result.pop("_usage", None)

    def run(self, inputs: dict, params: dict, context: ExecutionContext) -> dict:
        raise NotImplementedError

    def read_status(self) -> str:
        return self._status

    def read_error(self) -> Exception | None:
        return self._error

    def reset_status(self) -> None:
        """Node の status を ready に戻す。internal state S は変更しない。executing 状態では呼び出してはならない。"""
        if self._status == "executing":
            raise RuntimeError("cannot reset while executing")
        self._status = "ready"
        self._error = None

    def reset_limit_state(self, name: str) -> None:
        """指定した limit state をリセットする。status は変更しない。"""
        if name not in self._limit_state:
            raise KeyError(f"Unknown limit state: {name}")
        self._limit_state[name] = 0
```

### 6.2 DataNode の責任（この版）

- `run(inputs, params, context)` の実装。本版では context を参照しなくてもよい。
- `execute()` は BaseNode の共通テンプレートを使う
- revision 付与は BaseNode が行う（run 実装者は `_meta` を触らない）
- **run は `_limit_state` を直接変更してはならない**。usage は戻り値の `_usage` に含め、BaseNode の `_apply_usage` が集計する（本版では no-op）
- usage の記録は v1.5 で有効化

### 6.3 StructuralNode の責任（この版）

- PipelineNode のみ実装する
- 子ノードの execute を管理する
- status の集約（§13.3.4 参照）。**全子ノードの fatal / limit を検知したら即 raise。final が done で終了。進捗なしで終了していなければ fatal。**
- 終了判定：いずれかの子が fatal → Pipeline fatal、いずれかの子が limit → Pipeline limit、final が done → Pipeline done、進捗なし → fatal（詳細は §13.3.3）

**操作境界：**  
StructuralNode は子ノードの内部属性（`_status`, `_limit_state`, `_error`, internal state S）に直接アクセスしてはならない。

StructuralNode が子ノードに対して行える操作は、次のメソッド呼び出しに限定される：

* `execute(inputs, params)`
* `read_status()`
* `read_error()`
* `reset_status()`
* `reset_limit_state(name)`

本バージョンでは StructuralNode は子ノードの `reset_status()` を自動的に呼ばない。

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
        if node.read_status() != "ready":
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
- 実行可能判定（status が ready かつ全 input が解決済み）
- node.execute 呼び出し
- 最新出力保存

Runner が行わないこと（v1.3 と同じ）：

- status の意味を解釈しない
- limit / usage を評価しない
- revision を解釈しない
- 例外を捕捉しない

### 7.3 実行可能判定（この版）

```
実行可能 ⟺ status == "ready"
           ∧ 全 input port の参照先が latest_output に存在する
```

`required` の概念はこの版では省略し、全 input を required 扱いとする。

Runner は「ready なノードを 1 つ execute する。なければ False を返す」のみを行う。Runner は終了理由を解釈しない。

---

## 8. 循環グラフ

この版では**サポートしない**。YAML ロード時に静的チェックを行い、循環を検出したら `CyclicGraphError` を raise する。

---

## 9. 例外処理モデル（この版）

### 9.1 制御用例外と観測用 status の分離

BaseNode.execute は次のように例外を扱う：

* **NodeExecutionLimit** → status = limit、return `{}`
* **NodeExecutionFailure** およびその他 **Exception** → status = fatal、`_error` に保持、return `{}`

例外は制御専用、status は観測専用である（§2.2.1 役割分離）。

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
version: "1.4"
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

`version: "1.4"` を必須とする。不一致時は `VersionMismatchError` を raise する。

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
    def run(self, inputs: dict, params: dict, context: ExecutionContext) -> dict:
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
    def run(self, inputs: dict, params: dict, context: ExecutionContext) -> dict:
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

step を繰り返し、**子ノードの fatal / limit を検知したら即 raise**。final が done で終了。進捗がなく終了していない場合は `NodeExecutionFailure("invalid execution state")` を raise する。final のみの監視は行わず、全子の状態を対象とする。

**実行コンテキスト：** PipelineNode の `_context`（latest_output を含む）は execute 開始時に run 内で初期化される。§1.1.2 の三層構造において実行コンテキスト層に属し、永続状態ではない。

```python
class PipelineNode(BaseNode):
    def run(self, inputs: dict, params: dict, context: ExecutionContext) -> dict:
        self._context = {"latest_output": {}, "pipeline_inputs": inputs}
        runner = Runner(self._graph, self._context)

        while True:
            progressed = runner.step()

            statuses = [c.read_status() for c in self._get_children()]
            if "fatal" in statuses:
                raise NodeExecutionFailure("child fatal")
            if "limit" in statuses:
                raise NodeExecutionLimit("child limit")

            if self._get_node(self._final_id).read_status() == "done":
                break

            if not progressed:
                raise NodeExecutionFailure("invalid execution state")

        return self._collect_output()

    def _collect_output(self) -> dict:
        final_node = self._get_node(self._final_id)
        if final_node.read_status() != "done":
            return {}
        return self._context["latest_output"].get(self._final_id, {})
```

BaseNode.execute が `NodeExecutionLimit` / `NodeExecutionFailure` を捕捉し、それぞれ status = limit / fatal として扱う。

### 13.3.4 status の集約（この版）

| 条件 | PipelineNode の status |
|------|------------------------|
| いずれかの子が fatal | fatal |
| いずれかの子が limit | limit |
| final が done | done |
| 進捗なしで終了していない | fatal（invalid execution state） |

**停止条件の優先順位（順序を厳守）：**  
1. **fatal 優先** — いずれかの子が fatal → Pipeline fatal  
2. **limit 次** — いずれかの子が limit → Pipeline limit  
3. **final done** — final が done → Pipeline done  
4. **no progress → fatal** — 上記のいずれでもないのに進捗なし → invalid execution state（fatal）

final が done であっても、他の子に fatal または limit がある場合はそれを優先する。

### 13.3.5 特記事項

- この版では `resume()` / `re_execute()` は `NotImplementedError` を raise する。`reset_status()` / `reset_limit_state(name)` は実装する。
- execution_cursor は持たない（v1.5 で追加）
- LoopNode のネストは未対応
- **PipelineNode の `_limit_state["calls"]` は PipelineNode 自身の execute 呼び出し回数をカウントする。** 子ノードの呼び出し回数ではない。`max_calls` を PipelineNode に設定した場合、Pipeline 全体の実行回数を制限することになる（v1.6 LoopNode 実装時に意味が出る）。

---

# Part IV — Invariants（この版）

## 14. 不変条件

v1.3 の全不変条件のうち、この版で意味を持つものを列挙する。

1. **Node の内部状態は Node のみが変更可能**
2. **StructuralNode は子の内部属性に直接アクセスしてはならない。** 子に対する操作は execute / read_status / read_error / reset_status / reset_limit_state の呼び出しに限定する
3. **execute は status == "ready" のときのみ呼び出される。** それ以外で呼ばれた場合は BaseNode.execute が RuntimeError を raise する
4. **Node インスタンスは同一 Execution Scope で再利用する**
5. **revision は BaseNode が付与する。Node 実装者は `_meta` を直接操作してはならない**（この版では UUID ダミー）
6. **StructuralNode は子ノードの出力内容を変更してはならない**
7. **BaseNode は fatal 発生時に原因例外を保持し、read_error() で公開する**
8. **execute は常に dict を返す。None を返してはならない**
9. **PauseSignal・LimitSignal を run() 内で raise した場合は NotImplementedError が発生する**（この版のみ）

10. **reset_status() は status と error のみを変更する。** limit_state は変更しない。executing 状態の Node に対して呼び出してはならない。呼び出した場合は RuntimeError を raise する。

11. **reset_limit_state(name) は指定した limit state のみを変更する。** status は変更しない。

12. **done 状態の Node は自動再実行されない。** 再実行する場合は明示的に reset_status() を呼ぶ必要がある。

13. **limit state は Node に保持される。**（消費量は Node、条件は params）

14. **例外は制御専用であり、status は観測専用である。**

15. **実行状態の変更は execute および reset_status / reset_limit_state のみが行う。** run は `_status`, `_limit_state`, `_error` を直接変更してはならない。

16. **run は limit_state を直接変更してはならない。** usage は戻り値の `_usage` で返し、BaseNode の `_apply_usage` が集計する。

17. **ExecutionContext は永続状態ではない。** execute 呼び出し単位で生成・破棄され、reset_status および reset_limit_state の対象ではない。

18. **limit 判定は pre / post に分離される。** pre-limit は実行前（max_calls）、post-limit は実行後（max_tokens）。pre/post とも `_limit_state` と params のみ参照し、run の result は参照しない。

19. **Node は同期・原子的に実行される。** execute は atomic に完了する。

---

# Part V — NodeFlow v1.5 Dispatcher Model

**Part V では v1.5 の node model を正本とする。** Part I–IV に現れる `DataNode` / `StructuralNode` 等の分類語彙は、**Part V 本文では参照しない**（過去層の説明は Part I–IV にのみ存在する）。実装は Part I–IV の runtime と併存し得るが、dispatcher としての型・配置・契約の読み取りは Part V に従う。

---

## 1. Purpose

v1.5 では、node-flow を単なる汎用 workflow 実行基盤としてではなく、**外部 LLM / coding agent / API を task 単位で振り分けて実行する dispatcher** として、実際に使える形に整理する。

本バージョンでは、抽象的な汎用性の追求よりも、まず以下を確実に満たすことを優先する。

* task を適切な外部実行器へ振り分けられる
* 外部実行を単発ジョブとして扱える
* 実行結果を共通形式で扱える
* node の接続と composite による再利用を維持できる
* 具体ノードを増やしやすい構造を持つ
* base class と concrete node の責務を明確に分離できる

---

## 2. Scope and Non-Goals

### 2.1 Primary Goal

v1.5 の primary goal は、node-flow を以下のような dispatcher として使えるようにすることである。

* review task を Claude Code に渡す
* implement task を Codex に渡す
* summarize / cheap analysis を OpenAI-compatible API に渡す
* route / exec / summarize のような流れを composite 化して再利用できる
* task ごとに外部実行を独立 run として扱う

### 2.2 Non-Goals

v1.5 では以下を primary scope に含めない。

* 高度な multi-agent negotiation
* 複雑な scheduler / queue / worker system
* provider 全体を横断する完全な plugin framework
* 長期 session orchestration
* Claude Code や Codex 内部の subagent 相当機構の再実装
* role ごとの大規模 taxonomy の完成

---

## 3. Design Principles

### 3.1 Principles

v1.5 では以下を設計原則とする。

* 実際に使える dispatcher を優先する
* Runner は dumb のまま維持する
* 外部実行は単発ジョブを基本とする
* node の大分類は `PipeNode` と `ActionNode` とする
* `ActionNode` の継承軸は実現方法とする
* node の role は継承の第一軸にしない
* role と implementation は独立概念として扱う
* concrete node は役割の明確なものを優先する
* base class と concrete node の置き場所は分離する

### 3.2 Runner Prohibitions

dispatcher を実装するとき、Runner に制御を寄せたくなるが、v1.5 では次を **禁止**する（**§10** の単発ジョブ原則と併せて解釈する）。

* Runner は **routing policy** を持ってはならない
* Runner は **provider selection** を持ってはならない
* Runner は **node role を解釈して実行順を変えてはならない**
* dispatch の判断は **ActionNode** または **PipeNode** の構成に閉じ込める

---

## 4. Terminology

Part V における主要概念の関係は、次の一文ずつの定義で捉える。

* **dispatcher** は、task を適切な **node** および **executor** に振り分ける仕組み全体を指す。
* **executor** は、CLI プロセスや HTTP API など、node-flow の外で実際の処理を行う **外部実行器そのもの**を指す。
* **ActionNode** は、graph 上の **単一責務の処理単位**であり、dispatcher の中でロジックや external execution を担う。
* **external execution** は、**ActionNode** が **executor** を呼び出して結果を得る行為を指す。

### 4.1 Task

dispatcher が扱う仕事単位。  
例: review, implement, summarize, analyze

### 4.2 Executor

task を実行する外部実行器。  
例:

* Codex CLI
* Claude Code CLI
* OpenAI-compatible API client

### 4.3 PipeNode

複数の node を内部に持ち、それらを 1 つの node として扱う合成 node。

### 4.4 ActionNode

**ActionNode** は単一責務の処理 node である。その **role** は routing / transform / `exec` などでありうる。実装手段は **implementation kind**（`PythonActionNode` / `CliActionNode` / `ApiActionNode`）で表し、role とは独立に選ぶ（§6 参照）。

### 4.5 Role

node が何をするかを表す概念。  
例:

* `route_by_task_type`
* `summarize_result`
* `exec`

role は node の意味・契約を表すものであり、**継承の第一軸ではない**。

### 4.6 Implementation Kind

node が処理をどう実現するかを表す概念。  
v1.5 では少なくとも以下を扱う。

* `python`
* `cli`
* `api`

### 4.7 Common Result

外部実行系 `ActionNode` が **標準出力 port `execution_result`** に載せる payload の共通形。port 契約および `_meta.revision` は **§9** を参照。

---

## 5. Node Taxonomy

v1.5 における node class の大分類は **`PipeNode` と `ActionNode` のみ**とする（`BaseNode` 直下の top-level category はこの 2 つに限定する）。

* **`PipeNode`**: composite / subgraph を 1 node として扱うための class
* **`ActionNode`**: 単一責務の処理 node。subclass は **implementation kind** に基づいて定義する

```text
BaseNode
├─ PipeNode
└─ ActionNode
   ├─ PythonActionNode
   ├─ CliActionNode
   └─ ApiActionNode
```

### 5.1 Taxonomy のルール

* `PipeNode` と `ActionNode` 以外を top-level category に増やさない
* role は taxonomy に含めない（role は `ActionNode` の意味属性）
* implementation kind は `ActionNode` の継承軸とする

### 5.2 PythonActionNode / CliActionNode / ApiActionNode

* **PythonActionNode**: ローカル Python ロジックで処理を完結する
* **CliActionNode**: 外部 CLI を subprocess 等で呼び出す
* **ApiActionNode**: HTTP API を呼び出す

### 5.3 BaseNode との継承と execute 契約

* **`PipeNode` および `ActionNode`（その subclass を含む）はすべて `BaseNode` を継承する。**
* **`PipeNode.execute()` は `BaseNode.execute()` の共通テンプレート**（Part II の pre-limit → `run` → revision 付与・status 遷移等）に従う。`PipeNode` は **独自の execute 契約を定義しない**。
* **`PipeNode` 固有の責務は `run()` の内部**にあり、**内部 subgraph の実行**および §8 に従った **出力 dict の組み立て**を行う（§8.4 参照）。

---

## 6. Role and Implementation Model

v1.5 では、各 ActionNode は少なくとも以下の 2 つの側面を持ち、**仕様上完全に独立**とする。

* **role**: 何をする node か（契約・意味）
* **implementation kind**: どう実現する node か（継承軸）

継承は **implementation kind 側にのみ**置く。**role の表現規則は §6.1** に従う。**role ごとの abstract base class は導入しない**。

### 6.1 Rules

* role と implementation kind は独立概念として扱う
* 同じ role が複数の implementation kind を持ってよい
* 継承の骨格は implementation kind 側に置く
* **role は string 値として表現し、常に外向きの処理責務を表す**（内部実装名や略称を role 値に流用しない）
* **v1.5 では role の正本（source of truth）は class attribute**（例: `role = "exec"`）とする。spec や graph metadata にも同じ文字列を載せてよいが、**実装の参照は class attribute を優先**する
* v1.5 で **正式に用いる role 値**は少なくとも `route_by_task_type`, `summarize_result`, `exec` とする（将来、taxonomy を拡張してよい）
* **新しい role 値**を導入する場合は、既存値と意味が重複しないこと、および **外向きの処理責務**を表すことを条件とする
* role と implementation kind を **同一の継承木で表現しようとしてはならない**

### 6.2 external execution と role 名

v1.5 では外部実行系 ActionNode の **role メタデータ**は **`exec`** を用いる。将来、必要に応じて `call_cli` / `call_api` 等へ細分化してよい（予約的な拡張余地）。

**concrete クラス名**では外部呼び出しを **`*ExecNode`** に揃える（例: `CodexExecNode`, `ClaudeCodeExecNode`, `KimiExecNode`, `QwenExecNode`）。CLI / API の別は **implementation kind**（`CliActionNode` / `ApiActionNode`）で表す（§12.1）。

### 6.3 Prohibition

* role ごとの abstract base class は v1.5 では導入しない
* `RoutingNode` / `TransformNode` / `ExecutionNode` のような **role-based taxonomy** は採用しない
* role と implementation kind を同一の継承木で表現しようとしてはならない

### 6.4 例

以下の例は role と implementation kind の **独立性** を示すものであり、**role ごとの継承体系を意味しない**。

| Node                        | role                 | implementation kind |
| --------------------------- | -------------------- | ------------------- |
| `PythonRouteByTaskTypeNode` | `route_by_task_type` | `python`            |
| `PythonSummarizeResultNode` | `summarize_result`   | `python`            |
| `CodexExecNode`             | `exec`               | `cli`               |
| `ClaudeCodeExecNode`        | `exec`               | `cli`               |
| `KimiExecNode`              | `exec`               | `api`               |
| `QwenExecNode`              | `exec`               | `api`               |

---

## 7. Concrete Nodes in v1.5

### 7.1 PythonActionNode 配下

v1.5 で正式対象とする Python concrete は次の 2 つ。**クラス名・ファイル名は §12.1**。

#### PythonRouteByTaskTypeNode

role: `route_by_task_type`

役割:

* task metadata を見て次の executor / node を決める
* 自身では外部実行を行わない

入力例:

* `task_type`
* `needs_repo_write`
* `needs_shell`
* `cost_tier`
* `preferred_executor`（任意）
* `forbid_executor`（任意）

出力例:

* `route.executor`
* `route.reason`
* `route.next_node`

原則:

* v1.5 では deterministic な Python ロジックで実装する
* LLM を暗黙に使わない

#### PythonSummarizeResultNode

role: `summarize_result`

役割:

* 直前結果を後段に渡しやすい summary に整形する
* raw result を保持したまま summary を追加する

> 本 node は、**外部実行系 `ActionNode` が返す標準 port `execution_result` の payload** を入力として受け取ることを基本とする（§9）。

入力:

* `execution_result`

出力:

* `summary.short`
* `summary.key_findings`
* `summary.next_hint`

原則:

* v1.5 では Python ロジックで実装する
* LLM 要約は future scope とする

---

### 7.2 CliActionNode 配下

#### CodexExecNode

role: `exec`

役割:

* Codex CLI を単発ジョブとして呼び出す

原則:

* 対話 slash command (`/exit`, `/new`) に依存しない
* 単発実行完了でプロセス終了とする
* session 継続前提で dispatcher を組まない

#### ClaudeCodeExecNode

role: `exec`

役割:

* Claude Code を単発または非対話実行として呼び出す
* Sonnet / Opus を選択可能とする

原則:

* Claude Code 内部の subagent / skill / hook は Claude Code 側責務とする
* node-flow 側は開始・終了・結果取得のみを扱う

#### Future examples

* `CodexRouteByTaskTypeNode`
* `ClaudeCodeSummarizeResultNode`

これらは仕組み上は可能だが、v1.5 の正式対象には含めない。

---

### 7.3 ApiActionNode 配下

v1.5 では **provider ごとの concrete `ApiActionNode`** を正式対象とし、**OpenAI-compatible 等の横断 shared base クラスは仕様語彙に含めない**。複数 provider で重なる HTTP 処理は **§12.4 の推奨 helper**（または private モジュール）に切り出す。

#### KimiExecNode

role: `exec`

役割:

* Kimi（Moonshot）互換 HTTP API を **単発リクエスト**で呼び出す

原則:

* 出力は **§9** の `execution_result` port 契約に従う
* auth / endpoint / retry は **`run()` 実装**に置き、**timeout / retry 中の status 観測は §18.1**、retry 方針の詳細は **§11.3** に従う

#### QwenExecNode

role: `exec`

役割:

* Qwen 系 HTTP API を **単発リクエスト**で呼び出す

原則:

* **標準出力 port は `execution_result`**。payload は KimiExecNode と同様に **§9** の Common Result 契約に従う
* auth / endpoint / retry / **§18.1** / **§11.3** は KimiExecNode と同様

##### Future concrete examples

仕組み上は次のような **route / summarize の API concrete** を後から追加してよい（v1.5 の正式対象外）。

* `KimiRouteByTaskTypeNode`
* `KimiSummarizeResultNode`
* `QwenRouteByTaskTypeNode`
* `QwenSummarizeResultNode`

v1.5 では API-backed route / summarize は正式対象に含めず、まずは **§7.1 の Python 実装**を正式対象とする。

---

## 8. PipeNode Contract

### 8.1 定義

`PipeNode` は、複数 node の接続を内部に持つ composite node である。  
内部には `ActionNode` だけでなく `PipeNode` を含めてもよい。

**継承・execute:** `PipeNode` は **`BaseNode` の subclass**であり、**§5.3** に従い `BaseNode.execute()` のテンプレートを用いる。外部から見た「通常 node と同じ interface」とは、主にこの **execute / status / port 出力の契約**を指す。

**`PipelineNode` との対応:** Part I–IV の **`PipelineNode` は本 Part の `PipeNode` に相当する**（`BaseNode` 継承・`run` 内で Runner により子を直列実行する構造）。実装移行時は **execute 外殻を共通化し、子実行ロジックを `run()` に集約**すればよい。

### 8.2 Output contract（規範）

実装者が passthrough 専用と誤読しないよう、出力に関して次を **規範**とする。

1. `PipeNode` は、内部 graph の **final output をそのまま** 外部へ返してよい。
2. `PipeNode` は、**自身の外部 input / output contract** に従い、内部 node の出力を **再構成して** 返してよい。
3. **外部 caller は内部構造を意識しない。** caller は `PipeNode` を black box とし、内部の node 構成や最終子の id に依存した契約を前提としてはならない。

**注釈（final output）:** 上記 1 の **final output** は、`PipeNode.run()` が内部実行の結果として **外部に公開する最終出力**を指す。内部でどの子の出力を採用・集約するかは **`PipeNode` 実装の責務**であり、caller は black box として扱う。

### 8.3 Structural rules

* `PipeNode` は通常 node と同じ外部 interface（execute 契約など）を持つ
* `PipeNode` は明確な input / output contract を持たなければならない
* `PipeNode` の内部には node または `PipeNode` を任意に含めてよい
* ネスト深さに仕様上の制限は設けない

### 8.4 Execution rule

* **`PipeNode.run()`** は内部 subgraph の **実行責務**を持つ（子の `execute` 呼び出し順・接続解決・中間結果の受け渡しはすべて `run()` の実装詳細である）
* **`PipeNode.execute()`** は **`BaseNode.execute()`** に従い、**`run()` の前後**で status / limit / revision（Part II）を扱う
* `PipeNode` の内部 node 構成は **実装詳細**である
* 外部 caller は `PipeNode` を通常 node と同様に扱えることを期待してよい

### 8.5 Guidance

* 深いネストは許可する
* ただし、可読性・debug 性・再利用性との trade-off を考慮する
* 実運用では shallow で役割の明確な `PipeNode` が有利なことが多い

### 8.6 例

* `ReviewDispatchPipeNode`

  * `PythonRouteByTaskTypeNode`
  * `ClaudeCodeExecNode`
  * `PythonSummarizeResultNode`

* `ImplementDispatchPipeNode`

  * `PythonRouteByTaskTypeNode`
  * `CodexExecNode`
  * `PythonSummarizeResultNode`

---

## 9. Common Result

### 9.1 Port 名と Part II との整合

外部実行系 `ActionNode`（`CliActionNode` / `ApiActionNode` の **exec role** concrete 等）は、**標準出力 port 名として `execution_result` を用いる。** `run()` の戻り値（または `execute` が最終的に公開する output port の集合）は、少なくとも **`execution_result` というキー**を含み、その値が **Common Result の payload** である。

payload は **Part I–IV の port 契約に従い dict とし、`_meta.revision` を必須**とする（scalar port はない）。以下のフィールドは **`_meta` と同一階層**（payload 直下）に置く。

```python
{
    "execution_result": {
        "_meta": {"revision": "..."},
        "ok": bool,
        "executor": str,
        "provider": str,
        "model": str | None,
        "task_type": str | None,
        "summary": str | None,
        "stdout": str | None,
        "stderr": str | None,
        "raw_response": Any,
        "artifacts": list,
        "provider_meta": dict,
        "next_hint": str | None,
    }
}
```

上記は **ノード出力の一例**である。他 port を追加してよいが、**外部実行の共通契約として `execution_result` は必須**とする。

**命名:** `provider_meta` は **provider 固有の任意メタデータ**を格納する。Part II の **port-level `_meta.revision`** とは別概念であり、混同しないこと。

### 9.2 Payload フィールドの意味（Common Result 本体）

`execution_result` の dict 直下（`_meta` と兄弟）に最低限次を持つ。

* `ok`, `executor`, `provider`, `model`, `task_type`, `summary`, `stdout`, `stderr`, `raw_response`, `artifacts`, `provider_meta`, `next_hint` — 意味は従来どおり（`provider_meta` は旧称 `meta` に相当する provider 専用領域）

### 9.3 Rules

* **Common Result は `execution_result` port の payload として返す**
* raw output を捨ててはならない
* summary は raw を置き換えず追加情報として持つ
* provider 固有詳細は **`provider_meta`** に格納する
* **`provider_meta`** は **port-level の `_meta`（revision 等）とは別**である
* payload 直下にキー **`meta` は用いない**（旧案は `provider_meta` に統一する）
* 後段 node（例: `PythonSummarizeResultNode`）は可能な限り **`execution_result` payload** を前提に実装する

---

## 10. Single-Run Principle

**external execution を行う `ActionNode`（典型: `*ExecNode`）** について、原則として **1 task = 1 external run** とする。ここでいう **external run** は、**CLI プロセスの 1 回起動**、または **HTTP API の 1 回のリクエスト**を指す。

**`PipeNode.execute()`** は複数の子を含み得る **composite 実行**であり、**single-run 原則の直接の対象ではない**（各子の external run がそれぞれ 1 run として数えられる）。Runner に制御を寄せないことについては **§3.2** も参照する。

**retry:** single-run 原則は **対話セッション継続や暗黙の再利用を禁じる**ものであり、**transient 失敗に対する同一 `execute` 呼び出し内の有限 retry を禁止するものではない**（§18 参照）。

### Rules

* 対話 session 継続を前提にしない
* `/exit`, `/new` などの対話操作に依存しない
* 単発実行可能な実行器は単発実行を優先する
* 長期 session orchestration は v1.5 の primary scope 外とする

### Rationale

* task 境界を明確化できる
* session 汚染を防ぎやすい
* audit / rerun / retry がやりやすい
* CLI 実行と API 実行を統一的に扱いやすい

---

## 11. Base Class Responsibilities

### 11.1 PythonActionNode

共通責務:

* 入力解決
* ローカル Python ロジック実行
* 例外処理
* 共通出力形式への整形

### 11.2 CliActionNode

共通責務:

* subprocess 実行
* timeout 管理
* exit code 判定
* stdout / stderr 回収
* command 記録

### 11.3 ApiActionNode

共通責務:

* request 発行
* auth / endpoint 設定
* retry（**retry 中の status 観測は §18.1**）
* response 正規化

### 11.4 PipeNode

共通責務:

* 内部 node 構成の保持
* input / output contract の管理
* subgraph 実行の外部インターフェース化
* **`BaseNode.execute()` のテンプレートに従い**、status / limit / revision 等は **`BaseNode` 側の契約**に任せる（§5.3）。`PipeNode` は **独自の execute フックでこれらを迂回しない**

---

## 12. Inheritance and Composition Rules

### 12.1 Naming Rules

1. **Framework 固定名** — `BaseNode`, `PipeNode`, `ActionNode`, `PythonActionNode`, `CliActionNode`, `ApiActionNode` は本 Part の語彙として固定する。
2. **concrete `ActionNode`** — クラス名は **`<Scope><Role>Node`** とする。`Scope` は実装スコープ（例: `Python`, `OpenAI`, `Codex`, `ClaudeCode`, `Kimi`, `Qwen`）。**`Role` は必須**（外向き責務の PascalCase 語幹。例: `RouteByTaskType`, `SummarizeResult`, `Exec`）。**implementation kind** は継承で表し、**concrete 名では scope を省略しない**。
3. **concrete `PipeNode`** — **`<Purpose>PipeNode`**。外から見た目的のみを表す。
4. **外部実行系 concrete** — **`*ExecNode`**。role メタデータは **`exec`**（§6.2）。
5. **基底と concrete の同名禁止**。
6. **ファイル名** — クラス名の **snake_case**（例: `python_route_by_task_type.py`, `kimi_exec.py`）。

### 12.2 本 Part で用いる推奨クラス名

| 名前 | 種別 |
|------|------|
| `BaseNode` | 最上位基底 |
| `PipeNode`, `ActionNode`, `PythonActionNode`, `CliActionNode`, `ApiActionNode` | 本仕様で定義する基底 / 中間 |
| `PythonRouteByTaskTypeNode`, `PythonSummarizeResultNode` | Python concrete（`<Scope><Role>Node`） |
| `CodexExecNode`, `ClaudeCodeExecNode` | CLI 外部実行 concrete |
| `KimiExecNode`, `QwenExecNode` | API 外部実行 concrete（provider 別） |
| `KimiRouteByTaskTypeNode`, `KimiSummarizeResultNode`, `QwenRouteByTaskTypeNode`, `QwenSummarizeResultNode` | future concrete 例（§7.3） |
| `ReviewDispatchPipeNode`, `ImplementDispatchPipeNode` | `PipeNode` 例 |

### 12.3 継承・共通化の Rules

* base class は implementation kind ごとに持つ
* role ごとに巨大な base class を作らない
* provider 固有差分まで抽象親に押し込まない
* 必要に応じて helper / composition を使う

### 12.4 推奨 helper

* `CommandBuilder`
* `RequestBuilder`
* `ResultNormalizer`
* `ArtifactWriter`

**API exec 間のコード共有（仕様語彙に含めない）:** `KimiExecNode` / `QwenExecNode` 等が **HTTP リクエスト・auth・retry・レスポンス正規化**を重複実装しないため、**非 node のヘルパ**に切り出すことを推奨する。名前・配置の例（いずれも **class 名の固定語彙ではない**）:

* **仮称 `HttpApiCallHelper`**（モジュール `http_api_call.py` 等）。request 構築・auth・retry・正規化まで含む範囲に合わせて **`HttpApiExecHelper`** / **`ApiExecHelper`** 等の呼び方でもよい — **`nodes/base/helpers/`** または **`nodes/base/`** 直下に置き、concrete から import して用いる
* 同一ファイル内の **private 関数群**でもよいが、**複数 provider で共通化する場合は `nodes/base/` 側に寄せる**と配置が明確になる

---

## 13. What Remains Unchanged

v1.5 では以下は維持する。

* graph と node 接続を中心とする基本モデル
* Runner は dumb であるという方針（禁止事項は **§3.2**）
* pipeline / graph 記述方式の基本枠組み
* node を組み合わせて再利用するという flow の考え方

---

## 14. Implementation Order

### Step 1

* `PipeNode`
* `ActionNode`
* `PythonActionNode`
* `CliActionNode`
* `ApiActionNode`

### Step 2

* `PythonRouteByTaskTypeNode`
* `PythonSummarizeResultNode`
* `CodexExecNode`
* `ClaudeCodeExecNode`

### Step 3

* `KimiExecNode`
* `QwenExecNode`

### Step 4

* `ReviewDispatchPipeNode`
* `ImplementDispatchPipeNode`
* dispatcher example pipeline

---

## 15. Success Criteria

v1.5 完了時点で最低限以下が成立していること。

* `PipeNode` と `ActionNode` の区別が明確である
* base class が implementation kind ごとに整理されている
* role と implementation kind が独立概念として扱われている
* task_type に応じて executor を分岐できる
* Codex を単発ジョブとして呼べる
* Claude Code を単発ジョブとして呼べる
* Kimi / Qwen 等の HTTP API を **単発 exec** で呼べる concrete を持つ
* 共通結果形式で後段 node に渡せる
* `PipeNode` を通常 node と同様に扱える
* Runner に provider 固有ロジックを持ち込んでいない

---

## 16. Change Summary

### 16.1 削除・整理するもの

* `RoutingNode` / `TransformNode` / `ExecutionNode` のような role-based taxonomy
* role ごとの abstract base class
* abstract base と concrete node を曖昧にする命名（`Abstract…` 接頭辞への依存を含む）
* 仕様語彙としての **OpenAI-compatible 横断 shared base**（旧 `OpenAICompatible*` 系クラス名）
* file / folder 構成で implementation と role が混ざった配置
* Part V 本文での v1.4 系分類語彙（`DataNode` / `StructuralNode` 等）の参照

### 16.2 明確化するもの

* `PipeNode` / `ActionNode` を top-level taxonomy とする
* `ActionNode` の継承軸は implementation kind とする
* role は継承の第一軸にしない
* `PipeNode` は composite を正式に表す
* `PipeNode` は外部 contract に従って出力を再構成してよい
* `PipeNode` / `ActionNode` は **`BaseNode.execute()` テンプレート**に従う（§5.3、§8.4、§11.4）
* concrete `ActionNode` は **`<Scope><Role>Node`**（§12.1）
* Common Result の **port 名 `execution_result`** と **`provider_meta`**（§9）
* file / folder 配置は **role ベース**、継承は **implementation ベース**とする

### 16.3 追加するもの

* Naming Rules（§12.1）および推奨クラス名（§12.2）
* Runner Prohibitions（§3.2）
* `exec` role の将来細分化余地（§6.2）
* 用語間の関係説明（§4 冒頭）
* `PipeNode` の output / structural / execution に関する contract（§8.2–8.4）
* **§5.3** / **§18** — `BaseNode` との関係および Part II runtime との接続原則
* **§9** — `execution_result` port、`provider_meta`、`_meta.revision` との区別
* **§10** — external run 粒度と retry の関係

### 16.4 維持する考え方

**§13 What Remains Unchanged** に集約する（本節では列挙しない）。

---

## 17. Project Layout Guidance

**本節は normative な node 契約ではなく、project 構成の推奨である。** 実装時の一貫性のために明記する。

### 17.1 方針

* **abstract base を置く場所** と **concrete node を置く場所** は分ける
* **ファイル配置は role ベースに統一**する（`routing` / `transform` / `exec` 等）
* 継承は implementation kind ベースで読み、**ディレクトリで implementation を表現しなくてよい**
* `PipeNode` 系（`pipe/`）と concrete `ActionNode` 系（`action/`）を分ける

### 17.2 提案例

```text
nodeflow/
  nodes/
    base/
      node.py
      pipe.py
      action.py
      python_action.py
      cli_action.py
      api_action.py

    action/
      routing/
        python_route_by_task_type.py
        # future:
        # openai_route_by_task_type.py
        # kimi_route_by_task_type.py

      transform/
        python_summarize_result.py
        # future:
        # openai_summarize_result.py
        # qwen_summarize_result.py

      exec/
        codex_exec.py
        claude_code_exec.py
        kimi_exec.py
        qwen_exec.py

    pipe/
      review_dispatch_pipe.py
      implement_dispatch_pipe.py

  runner.py
  graph.py
```

### 17.3 この構成の意図

#### `nodes/base/`

* **abstract base class のみ**（および共通 helper）
* implementation kind ごとの共通親（`PythonActionNode` 等）

#### `nodes/action/`

* concrete ActionNode のみ
* **role ごとのサブディレクトリ**に揃える

#### `nodes/pipe/`

* reusable composite としての `PipeNode` 実装

### 17.4 ルール案

* `base/` には **abstract base class のみ**置く（concrete node を置かない）
* concrete ActionNode は **`action/<role>/`** に置く
* reusable composite は **`pipe/`** に置く
* 継承は implementation kind ベースでも、**ファイル配置は role ベースを優先**する
* implementation kind は **class hierarchy で読むもの**とし、ディレクトリで表現しなくてよい
* **§12.4** の API 用ヘルパは **`nodes/base/` 配下**（`helpers/` 推奨）に置き、**concrete の `action/exec/` には node クラス以外の共有モジュールを混在させない**（読み手が「node か helper か」を判別しやすくする）

---

## 18. Relation to Part II Runtime Extensions

Part II に記述される **execution primitives**（`execute` / status / limit / revision 等）は、**node taxonomy とは別層**であり、Part V の `PipeNode` / `ActionNode` 実装と **共有してよい**。

* **Part V** は **dispatcher 用の node model**（taxonomy・role・Common Result・Runner 禁止等）を定義する。
* **Part II** は **同一の `BaseNode.execute()` テンプレート**および status / limit / revision の意味を定義する。v1.5 の `PipeNode` / `ActionNode` はこれに従う（§5.3、§8.4、§11.4）。

**`DataNode` / `StructuralNode`** 等の v1.4 系分類語彙は **Part I–IV のみ**に残す。Part V 本文では用いない。

### 18.1 timeout / retry / status（v1.5 での接続原則）

timeout / retry / status の接続原則は **本節**に従う。

* **`CliActionNode`** の subprocess **timeout** は **`run()` 実装内**で扱う（Part II 本版の timeout 節が未整備でも、v1.5 の CLI 実行には事実上必須）。
* **`ApiActionNode`** の **retry** も **`run()` 実装内**で扱う。**retry 中に外部から観測される node の status は `executing`** とする（同一 `execute` 呼び出しの範囲内）。
* **single-run 原則（§10）** は **external run の粒度**を定義するものであり、**transient 向けの有限 retry を禁止しない**。
* **PauseSignal / resume** 等は Part II 側の拡張として **dispatcher の single-run 方針と両立させる**（詳細は Part II の該当節。本 Part では「対話セッション前提の dispatcher を組まない」§10 の趣旨と衝突しないよう設計する）。

### 18.2 usage / post-limit

**usage 集計・post-limit（token 等）** は Part II の runtime 拡張に属する。Part V の `ApiActionNode` / `LLM` 相当ノードは、**戻り値の `_usage` 等で Part II の契約に接続する**想定とする（本 Part では数値を固定しない）。

---

実行基盤の拡張（revision 完全実装、PauseSignal / resume、usage 等）は、**dispatcher model の節（§1–§17）と独立に**仕様化・実装してよい。同一リリースに束ねる必要はないが、最終的には **同一 Runner / graph 実行モデル**上で共存させる。

---

# Appendix A — v1.4.2 動作確認チェックリスト

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
[ ] ready 以外のノードを execute しようとした場合に RuntimeError が raise される（PipelineNode が fatal になる）
```d_error() で原因例外が取得できる
[ ] ready 以外のノードを execute しようとした場合に RuntimeError が raise される（PipelineNode が fatal になる）
```