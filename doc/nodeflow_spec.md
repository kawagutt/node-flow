# NodeFlow 仕様書（v1.7）

本書は NodeFlow v1.7 の public model を定義する。  
v1.7 は v1.6 の拡張ではなく、責務境界を正本化する breaking cleanup である。

## 結論（v1.7 正本原則）

```text
Pipe = wiring only
Runner = delivery / scan / call only
BaseNode = state guard / process lifecycle owner
ActionNode = domain execution owner
PipeNode = generic container
Node = state / readiness / validation / execution decision owner
```

```text
Runner は各 scan step で node.execute() を呼ぶ。
state による分岐は Runner ではなく BaseNode が行う。
```

各 node は非同期に動きうるため、Runner は **repeated rounds** で scan する。  
加えて次を明確化する。

```text
- file order は dependency order ではなく scan order である
- config の可視切り替えは round boundary のみである
```

---

## 目次

1. [Purpose](#1-purpose)
2. [Design Principles](#2-design-principles)
3. [Core Concepts](#3-core-concepts)
4. [Node Model](#4-node-model)
5. [BaseNode Contract](#5-basenode-contract)
6. [ActionNode Contract](#6-actionnode-contract)
7. [Generic PipeNode Contract](#7-generic-pipenode-contract)
8. [Port Contract](#8-port-contract)
9. [Static Node Contract](#9-static-node-contract)
10. [PipeSpec v1.7 Serialization](#10-pipespec-v17-serialization)
11. [Configuration Model](#11-configuration-model)
12. [PipeSpec Validation](#12-pipespec-validation)
13. [Runner Round Model](#13-runner-round-model)
14. [Runner Delivery Model](#14-runner-delivery-model)
15. [Fan-out Delivery](#15-fan-out-delivery)
16. [Mux / Conditional Behavior](#16-mux--conditional-behavior)
17. [Top-level and Nested Pipe Uniformity](#17-top-level-and-nested-pipe-uniformity)
18. [Project Layout](#18-project-layout)
19. [Conformance Checklist](#19-conformance-checklist)
20. [Migration from v1.6](#20-migration-from-v16)
21. [Forward Compatibility Notes for v1.8 Loop](#21-forward-compatibility-notes-for-v18-loop)

---

## 1. Purpose

v1.7 の目的は次の 3 点である。

- Pipe / Runner / Node の責務境界を public model として固定する
- PipeSpec を connection-only に正規化する
- 実行制御の意味判断を Runner から排除し、Node 所有に統一する

---

## 2. Design Principles

- すべては Node として表現する
- Runner は node readiness / node status を所有しない
- Runner は delivery / scan / call のみを行う
- Node は state / readiness / progress / validation / status / execution decision を所有する
- Pipe は wiring のみを担う
- PipeSpec は domain logic や制御構文（`if` / `else` / `skip` / `branch` 等）を持たない
- PipeNode は PipeSpec に基づく generic container とする
- workflow-specific な PipeNode subclass を許可しない
- Runner は payload の意味を解釈しない
- config の可視切り替えは round boundary のみで行う

---

## 3. Core Concepts

- `Node`: graph の実行単位。外部契約は port のみ
- `ActionNode`: 意味のある処理を行う node
- `PipeNode`: child graph を配線し pipe output port を filled にする generic container。child node の state は所有しない
- `Runner`: repeated scan round を実行する delivery / scan / call 実行器
- `BaseNode`: 全 Node 共通の `execute()` entrypoint と state / process lifecycle を提供する層
- `PipeSpec`: pipe node の default config を記述する形式
- `source`: `input.<pipe_input_port>` または `<node_id>.<output_port>`
- `fan-out`: 1 source を複数 child input target へ配送すること

---

## 4. Node Model

Node は `execute()` で 1 step 進む。`execute()` はすべての Node に共通の入口であり、具体は `BaseNode` が提供する（§5）。

Node / BaseNode 自身が次を決める。

- 現在の入力で実行可能か
- 現在の出力 occupancy で emit 可能か
- process / wait / no-op / fatal
- どの output port を emit するか
- `idle` / `executing` / `done` / `limit` / `fatal` への遷移

Runner はこれらを決めない。node state は BaseNode / Node が所有する。

### 4.1 state

v1.7 の node state は次の値を取る。

- `idle`
- `executing`
- `done`
- `limit`
- `fatal`

#### `idle`

- active process が存在しない
- terminal ではない
- input が揃っている、または実行可能である、という意味ではない

Runner が `execute()` を呼ぶ規則は state 値ではなく §14.1 の call rule で定義される。

#### `executing`

- active process が存在する
- `BaseNode.execute()` は新しい process を開始してはならない
- process 完了時に node は `idle` / `done` / `limit` / `fatal` のいずれかへ遷移しなければならない

#### terminal states

`done`:

- node は正常終了している
- `BaseNode.execute()` は新しい process を開始しない

`limit`:

- node は設定された制限に到達している
- `BaseNode.execute()` は新しい process を開始しない

`fatal`:

- node は継続不能な異常状態にある
- `BaseNode.execute()` は新しい process を開始しない

terminal state の意味の解釈・更新は Runner ではなく BaseNode / Node が行う。

`done` / `limit` / `fatal` は現在の execution episode における terminal である。terminal から `idle` へ戻す場合は、`reset` / `reconfigure` / `current_config` 更新境界などで明示的に行う。

Node が no-op / wait を選ぶ場合、process は開始されていない。  
この場合、state を `executing` にする必要はない。

state value は Runner の `execute()` 呼び出し可否には使われない。readiness / no-op / state guard の判断は BaseNode / Node が所有する。

---

## 5. BaseNode Contract

BaseNode はすべての Node に共通する `execute()` entrypoint を提供する。

BaseNode は次を所有する。

- state guard
- active process lifecycle
- `idle` / `executing` / `done` / `limit` / `fatal` の共通扱い
- process 開始前の atomic state transition
- terminal state の共通 no-op handling

`execute()` の実行形態は実装詳細とする。

- async 実装: process を開始して速やかに return する
- sync 実装: 短い処理を `execute()` 呼び出し内で実行して return する

どちらの場合も、`execute()` は unbounded な待機や無制限ループで Runner の round 進行を停止させてはならない。

### terminal state

terminal state（`done` / `limit` / `fatal`）では次に従う。

- 新しい process を開始しない
- `idle` / `executing` へ暗黙に遷移しない

（episode と `idle` への明示的な復帰は §4.1）。

`BaseNode.execute()` の契約は次のとおりである。

```text
BaseNode.execute():

  if state == executing:
      active process が残っている
      何も開始せず runner に戻る

  elif state == idle:
      input / output occupancy / node-local condition を確認する

      if node が実行可能:
          state = executing にする
          process を開始する
          process 内で node の処理を行う
          元の execute() は runner に戻る

      else:
          state は idle のまま
          runner に戻る

  elif state in {done, limit, fatal}:
      新しい process は開始しない
      idle / executing へ暗黙に遷移しない
      runner に戻る
```

重要なのは、**Runner は `executing` かどうかすら判断しない**ことである。`executing` の guard は BaseNode の責務にする。

ActionNode / PipeNode は BaseNode の共通 lifecycle の上で、自身の実行条件と処理内容を定義する。

Runner は BaseNode の外側から state を解釈しない。Runner は `node.execute()` を呼ぶだけである。

### 5.1 実装イメージ

仕様の正本は上記の契約である。実装の参考として、次のような構造が整合する。

```python
class BaseNode:
    def execute(self) -> None:
        if self.state == "executing":
            self._poll_or_refresh_process_state()
            return

        if self.state == "idle":
            if not self._can_start():
                return

            self.state = "executing"
            self._start_process()
            return

        if self.state in {"done", "limit", "fatal"}:
            self._refresh_terminal_state()
            return

        self.state = "fatal"
```

`_can_start()` の中で input 状況や output occupancy を見る。それは Runner ではなく Node 側の判断である。

---

## 6. ActionNode Contract

- `ActionNode` は domain semantics を持ち、§5 の BaseNode 共通 lifecycle の上で実行条件と処理内容を定義する
- `input_ports` は accepted ports であり required 判定そのものではない
- required 入力の最終判定は `ActionNode` が BaseNode lifecycle 上で提供する node-local の開始可否判定で行う。実装上は `_can_start()` / `_prepare_start()` 等に相当する。`ActionNode` は `BaseNode.execute()` の共通 state guard を迂回してはならない
- `output_ports` は static capability を表す

次の呼び出し関係を正本とする。

```text
BaseNode.execute()
  -> common state guard
  -> ActionNode local readiness
  -> ActionNode domain process
```

---

## 7. Generic PipeNode Contract

v1.7 の `PipeNode` は generic container に固定する。

Generic `PipeNode` は workflow-specific semantics を持たないが、container semantics として internal Runner を使って child graph を進める。

### 7.1 禁止事項

- workflow-specific `PipeNode` subclass
- `pipe_spec()` override
- PipeSpec 外で child の `config` を暗黙解決するための override
- workflow-specific な `PipeNode.run()` override

### 7.2 許可事項

- `PipeSpec` から構築される generic `PipeNode(spec)`
- pipe output port への書き込み

`PipeNode` は child node の state を所有・複製しない。  
`PipeNode` は自身の node state を持つが、その値は internal Runner / child graph / pipe output port の状態を観測して導出・反映される。  
`PipeNode` は wiring 以外の domain semantics を持たない。

### 7.3 Internal Runner と child graph

`PipeNode` の domain process は internal Runner を所有する。  
PipeNode の process は internal Runner を使って child graph を進める。  
外側 Runner は `PipeNode` を通常 node として扱い、`PipeNode` 内部の child node を直接 scan しない。

PipeNode は、1 回の `execute()` 呼び出しで bounded な child rounds だけを進めて return してよい。  
child graph がまだ完了していない場合、PipeNode は `executing` のままとし、次回以降の `execute()` で続きを進める。

`PipeNode.execute()` は unbounded な待機で外側 Runner の進行を停止させてはならない（§5）。

### 7.4 PipeNode state

PipeNode も BaseNode lifecycle に従う。

PipeNode の process は internal Runner を使って child graph を進める。  
process の状態更新は次の優先順位に従う。

1. child に `fatal` があれば `fatal`
2. child に `limit` があれば `limit`
3. child に `executing` があれば `executing`
4. `pipe.outputs` に定義された PipeNode output port がすべて `filled` なら `done`
5. それ以外は `idle`

---

## 8. Port Contract

- Node の外部 interface は port のみ
- `_state` / `_runtime` / `_usage` は observation field であり normal output port ではない
- observation field は delivery 対象にしてはならない
- output payload の意味解釈は Runner ではなく Node / downstream Node 側

### 8.1 Port occupancy

Port occupancy は次の 2 状態を取る。

- `empty`: payload を保持していない
- `filled`: payload を保持している

delivery は source output port が `filled` で、target input port が `empty` のときに行われる。  
clear は `filled` な source output port を `empty` に戻す操作である。

---

## 9. Static Node Contract

### 9.1 `input_ports`

`input_ports` は accepted input port names の宣言である。  
required inputs の宣言ではない。

### 9.2 `output_ports`

`output_ports` はその Node が emit しうる static output port names の宣言である。

### 9.3 所有境界

Node は次を所有する。

- readiness
- validation
- execution decision
- status transition
- output emission decision

---

## 10. PipeSpec v1.7 Serialization

PipeSpec v1.7 は pipe node の default config を書く形式である。  
v1.7 の最小 PipeSpec は connection-only とし、child が default_config 通りに使われる場合、child config を繰り返し書かない。

```json
{
  "kind": "pipe",
  "version": "1.7",
  "pipe": {
    "outputs": {
      "<pipe_output_port>": "<node_id>.<output_port>"
    }
  },
  "nodes": [
    {
      "id": "<node_id>",
      "path": "<node_or_pipe_json_path>",
      "inputs": {
        "<child_input_port>": "<source>"
      }
    }
  ]
}
```

### 10.1 source 形式

- `input.<pipe_input_port>`
- `<node_id>.<output_port>`

### 10.2 自動導出

pipe input ports は `input.*` 参照から自動導出する。

### 10.3 PipeSpec と pipe config

PipeSpec は pipe node の default config を表す。  
詳細な config ルール（`current_config` / `next_config`、更新境界、競合解決）は §11 に従う。

v1.7 の最小 node entry は次のみを持つ。

- `id`
- `path`
- `inputs`

必要に応じて、pipe 側でその child を既定以外の設定で使うための `config` を追加してよい。  
`config` は child の設定を階層的に指定できる。

これは次を意味する。

- `id`: この pipe 内での child 名
- `path`: child node または child pipe 定義の選択
- `inputs`: source を child input port にどう接続するか

child の default_config は `path` で選ばれた child definition から読む。
### 10.4 file order の意味

`nodes` list の順序は **stable scan order** である。dependency order ではない。

```text
Runner は各 round で nodes を file order に従って scan する。
nodes list は topological sort 済みである必要はない。
```

依存が逆順に並ぶ場合（例: `B` が `A.out` に依存し、file order は `B` -> `A`）、同一 round では `B` が no-op になりうる。  
その後 `A` が output を生成し、次 round で `B` が実行可能になる。これは repeated rounds 前提の正常挙動である。

### 10.5 v1.7 で定義されるキー

v1.7 PipeSpec で定義されるキーは次のみとする。

- root: `kind`, `version`, `pipe`, `nodes`
- `pipe`: `outputs`
- `nodes[*]`: `id`, `path`, `inputs`（必要時に `config` を追加可）

### 10.6 v1.7 JSON 例（valid）

#### 例 1: 線形配線

```json
{
  "kind": "pipe",
  "version": "1.7",
  "pipe": {
    "outputs": {
      "summary": "summarize.summary"
    }
  },
  "nodes": [
    {
      "id": "exec",
      "path": "nodes/codex_exec/node.json",
      "inputs": {
        "prompt": "input.task_prompt"
      }
    },
    {
      "id": "summarize",
      "path": "nodes/summarize_execution/node.json",
      "inputs": {
        "execution_output": "exec.execution_output"
      }
    }
  ]
}
```

#### 例 2: Fan-out

```json
{
  "kind": "pipe",
  "version": "1.7",
  "pipe": {
    "outputs": {
      "review_summary": "review_summary.summary",
      "artifact_summary": "artifact_summary.summary"
    }
  },
  "nodes": [
    {
      "id": "exec",
      "path": "nodes/codex_exec/node.json",
      "inputs": {
        "prompt": "input.task_prompt"
      }
    },
    {
      "id": "review_summary",
      "path": "nodes/summarize_review/node.json",
      "inputs": {
        "execution_output": "exec.execution_output"
      }
    },
    {
      "id": "artifact_summary",
      "path": "nodes/summarize_artifacts/node.json",
      "inputs": {
        "execution_output": "exec.execution_output"
      }
    }
  ]
}
```

#### 例 3: 条件分岐は Mux ActionNode

```json
{
  "kind": "pipe",
  "version": "1.7",
  "pipe": {
    "outputs": {
      "value": "mux.value"
    }
  },
  "nodes": [
    {
      "id": "source_a",
      "path": "nodes/value_provider_a/node.json",
      "inputs": {
        "value": "input.value"
      }
    },
    {
      "id": "source_b",
      "path": "nodes/value_provider_b/node.json",
      "inputs": {
        "value": "input.value"
      }
    },
    {
      "id": "mux",
      "path": "nodes/mux/node.json",
      "inputs": {
        "selector": "input.selector",
        "value_a": "source_a.value",
        "value_b": "source_b.value"
      }
    }
  ]
}
```

---

## 11. Configuration Model

public model における設定の表現は `config` のみとする（§10.5 の node entry `config` を含む）。

public model に残す config 用語は次のみとする。

- `config`
- `default_config`
- `current_config`
- `next_config`

### 11.1 PipeSpec と default_config

PipeSpec は pipe node の `default_config` を書く形式である。

PipeSpec は、その pipe の default wiring、`pipe.outputs` に定義される output ports、child の使い方を記述する。  
child を child 自身の `default_config` 通りに使う場合、PipeSpec は child config を繰り返し書かなくてよい。

v1.7 の最小 node entry は次だけを持つ。

- `id`
- `path`
- `inputs`

必要に応じて `config` を追加してよい。

### 11.2 runtime config

runtime では NodeFlow は次を保持する。

- `current_config`
- `next_config`

`current_config` は今 round で Runner が使う config、`next_config` は次 round で使う config である。

Runner は round 中 `current_config` のみを読む。  
Runner は round 中に config を切り替えてはならない。  
round boundary で、`next_config` を validation したうえで `current_config` になる。

### 11.3 config 更新の境界

Node は `next_config` のうち自分に対応する部分だけを更新してよい。  
PipeNode は自分の配下にある node の config を更新してよい。  
親と子が同じ config field を同一 round の更新で変更した場合、親側の更新を優先する。

更新要求のタイミング契約は次のとおり。

- Node / PipeNode は `execute()` 呼び出し中に `next_config` への更新要求を出してよい
- Node / PipeNode は自ノードの active process 完了イベント処理中にも更新要求を出してよい
- いずれの更新要求も反映先は `next_config` のみであり、同一 round の `current_config` には反映してはならない

`next_config` をどう計算するかを含む内部メカニズムは実装詳細である。

### 11.4 Runtime config と graph structure

Runtime config は graph structure を含む。

次は config の一部である。

- `nodes` list
- node id
- `path`
- `inputs` wiring
- `pipe.outputs`
- scan order
- node-local config

Runner は round 中 `current_config` のみを使う。  
`current_config` は round 中 immutable である。

`next_config` は round boundary で validation された後、`current_config` になる。

---

## 12. PipeSpec Validation

### 12.1 path に応じた static ports の導出

validation は child の accepted input ports と static output ports を参照する。`path` の指す先に応じて次により導出する。

`path` が ActionNode definition を指す場合:

- accepted input ports はその ActionNode definition の `input_ports`
- static output ports はその ActionNode definition の `output_ports`

`path` が PipeSpec（child pipe definition）を指す場合:

- accepted input ports は PipeSpec 内の `input.*` references から導出される pipe input ports
- static output ports は `pipe.outputs` の keys から導出される PipeNode output ports

以降の検証ルールにおける child の accepted `input_ports` / static `output_ports` は、上記の導出結果を指す。

以下を必須 reject とする。

- `kind != "pipe"`
- `version != "1.7"`
- unknown root key
- unknown pipe key
- unknown node entry key
- `nodes` が list ではない
- `nodes` が空 list
- duplicate node id
- node id が invalid
- `pipe.outputs` が欠落または object でない
- `pipe.outputs` が空 object（キーが一つもない）
- node `inputs` が欠落または object でない
- node `config` が存在する場合に object でない
- path が空または invalid
- path が node definition / pipe definition のいずれにも解決できない
- source 形式が invalid
- source node id が存在しない
- source output port が child static `output_ports` に存在しない
- target input port が child accepted `input_ports` に存在しない
- `pipe.outputs` の source が child static `output_ports` に存在しない
- invalid input port name
- invalid output port name
- source に `_state` / `_runtime` / `_usage` を使用
- source に `output.<pipe_output_port>` を使用
- 同一 target input port に複数 source（fan-in）を割当
- node entry に制御構文キー（`if`, `else`, `skip`, `branch` など）を含む
- cyclic wiring

以下は valid。

- fan-out（1 source -> 複数 child input target）
- topological sort されていない nodes list

注記: cyclic wiring は v1.7 の validation で reject する。これは v1.7 固有の制約であり、将来バージョンで緩和してもよい。

---

## 13. Runner Round Model

Runner は repeated rounds で scan する。  
各 round では `current_config` を使い、`nodes` list を file order で scan する。

各 node scan step では次を行う。

1. `current_config` の wiring に基づき（§11.4）、その node に配送可能な input を delivery する。
2. `node.execute()` を呼ぶ

Runner は node state に基づいて `execute()` 呼び出し可否を判断しない。  
state guard / readiness / no-op / process 開始 / terminal handling は BaseNode / Node が所有する。

異常系は必要に応じて Node / `PipeNode` が `fatal` / `limit` に落とす。  
v1.7 では凝った停止条件を仕様化しない。

---

## 14. Runner Delivery Model

Runner の責務は次のみである。

- repeated round の実行（round 手順は §13）
- output を downstream input へ delivery
- fan-out 先の全 consumer への delivery
- delivery bookkeeping の保持
- scan した各 node に対する `node.execute()` の呼び出し

Runner は node output から即時に下流へ delivery する特別 step を持たない。  
output は source として利用可能になり、downstream input はその downstream node 自身の scan step で delivery される。

### 14.1 call rule

Runner は scan した node に対して `node.execute()` を呼ぶ。

これは readiness 判定ではない。  
Runner は node が実行可能かどうかを判断しない。  
Runner は node state に基づいて呼び出しを skip しない。

`execute()` が実際に process / wait / no-op / state correction / `fatal` / `limit` のどれを行うかは BaseNode / Node が判断する。  
v1.7 はそれ以上の細かい停止・再実行規則を仕様化しない。

Node が実際に process を開始した場合のみ、state は `executing` へ遷移する。

### 14.2 Runner がしないこと

- node state に基づく `execute()` の skip
- required input の充足判定
- node readiness の決定
- node status の書き換え
- mux / skip / branch の特別扱い
- selector 値の解釈
- payload 意味解釈による分岐
- caller 側の停止判断の代行

---

## 15. Fan-out Delivery

v1.7 では fan-out を first-class に許可する。

```text
source output
  consumers: set[input_ref]
  delivered_to: set[input_ref]
```

delivery 規則:

- source occupancy が filled で delivery 開始
- empty target ごとに delivery 成功
- `delivered_to` を更新
- `delivered_to == consumers` になるまで source は filled のまま
- 全 consumer への delivery 完了後に source を empty へ遷移

### 15.1 fan-out consumer の範囲

- fan-out consumer は child node input target のみ
- PipeNode output port は fan-out consumer ではない

### 15.2 PipeNode output port と source clear

`pipe.outputs` は PipeNode output port と child output source の対応を定義する。

`pipe.outputs` から参照される child output source は、対応する PipeNode output port が `filled` になるまで clear してはならない。

PipeNode output port は fan-out consumer ではない。  
そのため、PipeNode output port への書き込みは、fan-out source の delivery 完了判定（fan-out consumer に対する到達のカウント）に含めない。

### 15.3 内部 feedback source

PipeNode output port は内部 feedback source ではない。内部 feedback が必要な場合は child output source を直接参照する。

---

## 16. Mux / Conditional Behavior

v1.7 では PipeSpec に `if` / `else` / `skip` / `branch` 構文を追加しない。

条件分岐は通常 ActionNode で表現する。

```text
MuxNode inputs:
  selector
  value_a
  value_b

MuxNode outputs:
  value
```

- Runner は branch / mux を特別扱いしない
- mux の入力充足条件は MuxNode が決める
- MuxNode は selector と、selector が選んだ入力だけが揃えば emit してよい
- 選ばれていない入力を待つ必要はない
- 選ばれていない入力の受け取り可否・保持・破棄ポリシーは MuxNode が決める
- Runner は selector 値を解釈しない

---

## 17. Top-level and Nested Pipe Uniformity

top-level pipe と nested pipe は同一モデルで扱う。

```text
pipe path
  -> PipeSpec
  -> Generic PipeNode
  -> Runner
```

top-level / nested で Runner 規則を変えてはならない。

---

## 18. Project Layout

推奨レイアウト:

- `core/`: BaseNode / ActionNode / PipeNode / Runner / loader / validator
- `nodes/`: reusable ActionNode 群（exec, summarize, mux など）
- `workflows/`: PipeSpec 群（wiring 定義）

重要な分離:

- semantics は `nodes/` 側
- wiring は `workflows/` 側
- Runner は `core/` 側で delivery / scan / call のみ

---

## 19. Conformance Checklist

### 19.1 Runtime 実装チェック（Node / Runner / PipeNode）

- BaseNode は §5 に従い `execute()` entrypoint と state guard / active process lifecycle を提供する
- `execute()` は unbounded wait で Runner を停止させない（§5）
- terminal state での規則（新 process 開始禁止、`idle` / `executing` への暗黙遷移禁止）は §4.1 / §5 に従う
- ActionNode の readiness / domain 実行は §6 の呼び出し関係に従う（`BaseNode.execute()` の state guard を迂回しない）
- workflow-specific `PipeNode` を置かない（§2 / §7）
- `PipeNode` は child node の state を所有・複製しない（§7）
- PipeNode の child graph は §7.3 の internal Runner で進める。外側 Runner は child を直接 scan しない
- PipeNode state 導出は §7.4 の優先順位に従う
- graph structure を含む runtime config と round boundary の validation・適用は §11.2 / §11.4 に従う（round 中は `current_config` が immutable）
- `next_config` 更新要求は §11.3 の許可タイミングに従い、同一 round の `current_config` へ反映しない
- Runner は repeated scan rounds を使う
- Runner は scan した node の `execute()` を呼ぶ
- Runner は node state を解釈して `execute()` を skip しない（§14.1）
- Runner は state を readiness 判定に使わない
- downstream への delivery は downstream node 自身の scan step で行う
- fan-out source は全 consumer 配送完了後にのみ empty になる
- `pipe.outputs` から参照される source clear は §15.2 の規則に従う
- Runner は selector 値を解釈しない

### 19.2 PipeSpec / 定義チェック（loader / validator / author）

- PipeSpec version is `1.7`
- PipeSpec root は `kind / version / pipe / nodes` のみ
- `pipe` は `outputs` のみを持つ
- `nodes` は object ではなく list
- node entry の許可キーは §10.5 に従い、`id` / `path` / `inputs` を必須とし必要時のみ `config` を追加できる
- file order は dependency order ではなく scan order
- cyclic wiring は v1.7 で reject される（§12）
- すべての設定は `config` として表現される（§10.5）
- PipeSpec は pipe node の `default_config` 記述形式である
- NodeFlow は `current_config` と `next_config` を管理する
- Runner は round 中 `current_config` のみを使う
- Runner は round 中に config を切り替えない
- `next_config` は round boundary で validation したうえで `current_config` になる
- `next_config` の内部計算メカニズムは実装詳細である
- pipe config は child config を階層的に持てる
- child が `default_config` 通りなら PipeSpec に child config を繰り返し書かない
- fan-out consumer は child node input のみ
- `pipe.outputs` は PipeNode output port と child output source の対応を表し、delivery consumer ではない
- MuxNode は非選択入力を待つ必要がない
- PipeSpec に `if / else / skip / branch` 構文を持たない

---

## 20. Migration from v1.6

v1.7 は後方互換に旧キーを残さない。loader / validator は旧形式を reject し、silent ignore や best-effort 変換をしてはならない。

### 20.1 Remove obsolete node entry fields and pipe keys

PipeSpec から次を除き、残りは §10.5 の構造へ移す。子 node / child pipe の契約は `path` で選ばれた定義（必要なら entry の `config`）から得る。

- `node.type`
- `node.input_ports` / `node.output_ports`
- `pipe.input_ports`

### 20.2 Convert nodes object to nodes list

`nodes` は id 付き list が正本。list order は scan order として使う。

### 20.3 Derive pipe inputs from input.* references

pipe input ports は `nodes[*].inputs` 内の `input.<name>` 参照から集合として導出する。

### 20.4 Replace copy/split fan-out workaround with native fan-out

同一 `<node_id>.<output_port>` を複数 `inputs` 参照先へ配線してよい。

### 20.5 Replace if/skip/branch syntax with mux ActionNode

制御構文は PipeSpec に置かず、Mux 等の通常 ActionNode で表現する。

### 20.6 推奨実装順序（参考）

1. v1.6 前提の監査（コード・fixture・docs）
2. v1.7 principles / 責務境界の固定
3. PipeSpec v1.7 schema と validator（旧キー reject）
4. Generic PipeNode 一本化
5. Runner delivery / round model 実装
6. Configuration Model 実装（`current_config` / `next_config`、round boundary 適用）
7. static node contract、mux、conformance / tests / examples 更新

### 20.7 Node state の表記（参考）

v1.7 より前の実装で `_state.value` に `ready` が出ていたものは、`idle` に読み替えるまたはリネームする（§4.1）。

---

## 21. Forward Compatibility Notes for v1.8 Loop

将来バージョンで cyclic wiring を許可する場合も、基本的には同一の repeated rounds + file-order scan で表現できる想定とする。

将来課題（v1.8）として、loop 対応時に repeated rounds の停止規則を定義する。  
v1.7 では no-progress / mechanical progress を public model に含めない。
