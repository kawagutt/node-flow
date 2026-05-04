# NodeFlow 仕様書（v1.6）

本書は **NodeFlow v1.6 の public model**を定義する。

文書の中心は次の五つである。

* **node model**（`BaseNode` と port・観測可能な出力の境界）
* **taxonomy**（`PipeNode` / `ActionNode` と implementation kind）
* **port contract**（外部 interface・予約キー・`execute()` の観測境界）
* **role contract**（`ActionNode` と role）
* **Common Output** と **single-run execution model**（外部実行の粒度と Runner の dumb 性）

---

## 目次

1. [Purpose](#1-purpose)
2. [Scope and Non-Goals](#2-scope-and-non-goals)
3. [Design Principles](#3-design-principles)
4. [Core Concepts and Terminology](#4-core-concepts-and-terminology)
5. [BaseNode and the Node Model](#5-basenode-and-the-node-model)
6. [Node Taxonomy](#6-node-taxonomy)
7. [Port Contract](#7-port-contract)
8. [Common Output](#8-common-output)
9. [Single-Run Principle](#9-single-run-principle)
10. [Runner and Pipe Execution Model](#10-runner-and-pipe-execution-model)
11. [Role and Implementation Model](#11-role-and-implementation-model)
12. [Built-in Role Contracts](#12-built-in-role-contracts)
13. [PipeNode Contract](#13-pipenode-contract)
14. [Base Class Responsibilities](#14-base-class-responsibilities)
15. [Naming and Inheritance Rules](#15-naming-and-inheritance-rules)
16. [Project Layout Guidance](#16-project-layout-guidance)
17. [Conformance Checklist](#17-conformance-checklist)

---

## 1. Purpose

NodeFlow v1.6 は **graph-oriented node model** である。

* task を **external executor** に渡し、外部実行を **単発 execution** として扱い、その output を **共通形式（Common Output）** として後段へ渡す。
* 汎用 workflow engine の抽象性を最大化することより、**実際に使える graph execution model** と **明確な contract** を優先する。
* node の **再利用**と **composite（`PipeNode`）** は維持する。`PipeNode` は **子 node を接続し 1 node として公開する**だけであり、**意味のある処理は child の `ActionNode` に委ねる**（[§13](#13-pipenode-contract)）。
* **Runner** は graph を愚直に進めるだけとし、output の意味解釈や provider 選択を持たない（[§3](#3-design-principles)）。

---

## 2. Scope and Non-Goals

### 2.1 Scope

v1.6 が primary scope とするもの。

* input payload に基づく output port 選択（`ActionNode` 側で実施）
* **external execution**（CLI / API による単発実行）
* **summarization / 整形**（**built-in role `summarize_output` 等**）
* **composite（`PipeNode`）** による subgraph の **配線と公開**（処理そのものは「output 選択 / summarize」とは別層で、**child に置く**）

**アプリ固有の concrete `PipeNode`** は許容されるが、**本仕様が必須とする concrete として固定しない**。

### 2.2 Non-Goals

次を primary scope に含めない。

* 複雑な **scheduler / queue / worker system** そのもの
* **長期 session** を前提とした orchestration
* **multi-agent negotiation** の一般解
* provider 横断の巨大 **plugin framework**
* examples 的 **reusable pipe の標準カタログ化**（個別 pipe の内部フィールド列挙の固定）

---

## 3. Design Principles

* **Runner is dumb** — 実行順の賢い最適化・output selection policy・provider 選択は Runner に置かない（責務の列挙は **§10.1** — **§10** 章）。
* **external execution is single-run by default** — 1 task に対する外部 execution の粒度は [§9](#9-single-run-principle)。
* **top-level taxonomy** は **`PipeNode` / `ActionNode`** のみ。
* **`PipeNode` は処理しない** — 配線・接続・graph 境界の所有のみ（[§13](#13-pipenode-contract)）。選択・振り分け・要約・変換などは **child `ActionNode`**（[§12](#12-built-in-role-contracts)）。
* **role** と **implementation kind** は独立（[§11](#11-role-and-implementation-model)）。
* **node interface は port のみ**（内部状態・内部 subgraph は契約外）。
* **予約 observation field** は最小限（`_state` / `_runtime` / `_usage` のみ、[§7](#7-port-contract)）。
* **base class** と **concrete node** の責務を分離する。
* 本仕様は **built-in role contract** を定義し、個別 concrete の局所構造は過剰に固定しない。
* **便利さより contract の明確さ**、**実装の楽さより public model のきれいさ**を優先する。

---

## 4. Core Concepts and Terminology

後続章で用いる語を短く定義する（詳細 rule は各章に従う）。**§番号は目次の章番号と対応**する。

| 語 | 意味 |
|----|------|
| **node** | graph 上の実行単位。外部から見えるのは **port のみ**（[§5](#5-basenode-and-the-node-model)、[§7](#7-port-contract)）。 |
| **task** | node-flow が扱う仕事単位（例: review, implement, summarize）。語としての **task** と、[§8](#8-common-output) で `execution_output` に載るキー **`task_type`** は **別**。 |
| **external executor** | CLI プロセスや HTTP クライアント等、**node-flow 外**で実処理を行う外部実行器。 |
| **external execution** | `ActionNode` が external executor を呼び出して output を得る行為。 |
| **state** | node の論理状態。詳細は §5 / §7。 |
| **`ActionNode`** | **意味のある処理**（条件付き output 生成 / exec / summarize 等）を担う単一責務 node。実装手段は **implementation kind**。 |
| **`PipeNode`** | child graph を **接続**し **1 node として公開**する composite。**domain-level の処理は行わない**（[§13](#13-pipenode-contract)）。 |
| **role** | **`ActionNode`** の意味・契約（何をするか）。`PipeNode` は role による domain semantics を持たない。 |
| **implementation kind** | 処理の **実現方法**（`PythonActionNode` / `CliActionNode` / `ApiActionNode`）。 |
| **public port contract** | **caller** が依存してよい **port 名と payload 形**の宣言。 |
| **caller** | `PipeNode.execute()` / `ActionNode.execute()` を呼ぶ主体。 |
| **input port / output port / payload shape** | role が宣言する入出力契約。 |
| **runtime metadata** | 実行テンプレートが付与する **`_runtime`** の内容。`_usage` は §7 と同様 **accounting observation field** とし、`runtime metadata` と混同しない。 |
| **Common Output** | `exec` role の標準出力 port **`execution_output`** の共有 payload 形（[§8](#8-common-output)）。 |
| **`PipeSpec`** | Runner が実行する PipeNode の静的定義。`pipe` / `nodes` を持つ。 |
| **source** | input port が payload を受け取る元、または pipe output port が payload を公開する元を表す文字列。形式は `input.<pipe_input_port>` または `<node_id>.<output_port>`。 |
| **`_state`** | node の read-only **state snapshot**（`_state.value` が論理 state）。output port ではなく観測用フィールド。 |
| **`pipe.input_ports`** | PipeNode が受け取れる public input port 名の集合。詳細は §16.3。 |
| **`pipe.output_ports`** | PipeNode の public output port から source への mapping。形は `<pipe_output_port> -> <source>`。詳細は §16.3。 |
| **port occupancy** | port が `filled` か `empty` かを表す delivery 用状態。詳細は §7 / §10。 |
| **pipe output buffer** | `output.<pipe_output_port>` に対応する PipeNode 側 buffer。詳細は §10 / §13。 |
| **ready** | state の一つ。詳細は §5。 |

---

## 5. BaseNode and the Node Model

### 5.1 Node の定義

* **Node** は graph 上の **実行単位**である。
* Node の **外部 interface** は **port のみ**である（内部状態や subgraph の形は契約外、[§7](#7-port-contract)）。
* Node は **`execute()`** により 1 step 進む。共通の **execution template** は **`BaseNode`** が担う。
* 観測出力は **normal output ports** と **reserved observation fields**（`_state` / `_runtime` / `_usage`）からなる（詳細は [§7](#7-port-contract)）。

### 5.2 `BaseNode`

* **`BaseNode`** は **すべての node の最上位基底**である。
* **execution template** と **state / limit / error の共通 mechanism** を提供する。各 node の state はその node instance が所有し、Runner は state を直接所有しない。
* public execution step は **`execute()` のみ**とする。内部 helper 名は public contract に含めない。本リポジトリの **transitional** な呼び出し形（`execute` が `inputs` を受け取る）は [§14.1](#141-basenode) に従う。
* v1.6 の論理 **state**（`_state.value`）は `ready` / `executing` / `done` / `limit` / `fatal`。`done` と `ready` は主に **port occupancy** と突き合わせて読む（[§7.3](#73-_state--_runtime)）。

## 6. Node Taxonomy

### 6.1 階層

v1.6 の **top-level category** は **`PipeNode` と `ActionNode` のみ**（`BaseNode` 直下はこの 2 つに限定）。

```text
BaseNode
├─ PipeNode
└─ ActionNode
   ├─ PythonActionNode
   ├─ CliActionNode
   └─ ApiActionNode
```

### 6.2 ルール

* **role は taxonomy に含めない**（`ActionNode` の意味属性として別管理）。
* **implementation kind** は **`ActionNode` の継承軸**のみ。
* **`PipeNode` / `ActionNode`** はいずれも **`BaseNode` を継承**する。
* **`execute()`** の共通構造は **`BaseNode` のテンプレート**に従う（[§5](#5-basenode-and-the-node-model)、[§14](#14-base-class-responsibilities)）。

### 6.3 各クラスの意味（概要）

* **`PipeNode`** — child を **接続**し graph を **1 node として公開**する。**semantics / domain logic は持たない**（[§13](#13-pipenode-contract)）。
* **`PythonActionNode`** — ローカル Python による **処理**。
* **`CliActionNode`** — subprocess 等による **外部 CLI 実行**。
* **`ApiActionNode`** — HTTP API **呼び出しと結果の正規化**。

---

## 7. Port Contract

本章は v1.6 の **中心規範**である。

### 7.1 Node interface is defined only by ports

* node の **外部 interface** は **port のみ**で定義される。
* **internal state**、**internal subgraph**、**implementation detail** は interface に含めない。
* caller と下流 node は **公開 port contract** にのみ依存する。

### 7.2 Port payload must be dict

* **input / output** の port payload は **いずれも dict**。
* scalar / list / string / number を **port payload そのもの**として用いてはならない（単一値も dict でラップする）。

観測可能な node 出力は **`output_port_name -> dict payload`** のマッピングとして表現される。

### 7.2.1 Node input format

Node input は **input port name から dict payload への mapping** である。

```python
{
    "<input_port_name>": {
        "...": "payload"
    },
    "<another_input_port_name>": {
        "...": "payload"
    }
}
```

top-level key は input port name のみとする。
`_state` / `_runtime` / `_usage` は input に含めない（reserved observation は input port として扱わない）。

理想的な呼び方では、caller または Runner が **`set_input`** 等で input port occupancy を filled にしたうえで `execute()` を開始する（Runner の subgraph では Phase A の delivery がこれに相当する）。

本リポジトリ実装の **v1.6 transitional API** では、`BaseNode.execute(inputs, params)` が **同一呼び出し内**で port mapping を受け取り、テンプレートが occupancy を整えてから `run()` に渡す（詳細は [§14.1](#141-basenode)）。graph 境界の **意味論上の入力**は引き続き port の dict payload のみである。

PipeNode の場合、caller が渡した input port mapping は、child PipeSpec 内の `input.<pipe_input_port>` source として Runner により扱われる。

Runner は subgraph execution 開始時に、PipeNode input のうち payload が存在する port に対応する `input.<pipe_input_port>` source occupancy を filled にする。

### 7.2.2 Node output format（observable）

観測可能な出力は **output port mapping と reserved observation field** を合わせた top-level mapping である。

**`_state` / `_runtime` / `_usage` はすべて `execute()` の observable output の top-level に必ず現れる**。`_usage` に記録する項目がなくても **キー `_usage` 自体と値としての空 dict `{}` は必須**。`_runtime` は少なくとも **`{"ports": {}}`** の形でなければならない（[§7.3](#73-_state--_runtime)）。

```python
{
    "<output_port_name>": {
        "...": "payload"
    },
    "_state": {
        "value": "done",
        "error": None
    },
    "_runtime": {
        "ports": {
            "<output_port_name>": {
                "revision": "..."
            }
        }
    },
    "_usage": {
        "<metric_name>": 0
    }
}
```

`_state` / `_runtime` / `_usage` は **output port ではない**。Runner の delivery 対象にしてはならない。

### 7.3 `_state` / `_runtime`

* **`_state`** は node の read-only state snapshot である。
* 必須最小形状は次のとおり。

```python
{
    "value": "ready" | "executing" | "done" | "limit" | "fatal",
    "error": None | {
        "type": "...",
        "message": "...",
    },
}
```

* `_state` は downstream に配送しない。状態依存の制御を data として流したい場合は、明示的な normal output port に変換して emit する。

**論理 state と port occupancy（通常 node）**

Runner は `_state.value` の **意味解釈や充足判定をしない**。以下は node が `execute()` とともに整える **規約的な対応**である。

* **`fatal`** — 継続不能な失敗がある。
* **`limit`** — 実行制約（token / 回数など）に達している。
* **`executing`** — node が execution を開始済みであり、同期的な `execute()` 呼び出しだけではまだ `done` / `limit` / `fatal` に整理できない状態。
  `ActionNode` では、外部 execution / 内部処理が完了待ちである状態を表す。
  `PipeNode` では、内部 child のどれかが非同期 execution 中であり、その child の完了が `execute()` 呼び出し外で起きるのを待っている状態を表す。
  PipeNode 内で同期的に進められる child が残っているだけの状態は、caller-visible な `executing` にはしない。
* **`done`** — `fatal` / `limit` / `executing` でなく、**delivery により下流へ渡せるように output port occupancy が filled** である状態。「永久に完了した」ことを意味しない。
* **`ready`** — `fatal` / `limit` / `executing` でなく、上記の意味での **output が empty** である状態。入力待ち・実行条件未達・`execute()` が no-op のいずれもあり得る。**error ではない。**

input が filled かどうかは Runner の判断対象外である。`ready` で `execute()` が呼ばれたとき、node は input / output / precondition を見て **開始・継続・完了・no-op** を決める。

複数 output port を持つ node は、**どれを `done` に相当する「filled」とみなすか**（単一代表作 port か、`required` な emit の集合か）をその node の public contract で定めてよい。

* **`_runtime` は canonical** な実行層メタデータ用の予約領域である。`execute()` の observable output では **常に key `_runtime` を含め**、少なくとも **`{"ports": {}}` と同義の構造を持つ**（`ports` が空でもよく、他の許可フィールド追加はしない）。
* **normative な木構造**として、**`_runtime["ports"][port_name][...]`** を用いる（port-centric）。
* **revision** 等、実行テンプレートが定義する **port 単位の runtime 属性**はこの **`ports` 木**に置く（例: `_runtime["ports"]["execution_output"]["revision"]`）。
* **`_runtime` は concrete node の任意拡張バケットではない**（provider 固有の任意データは domain payload の **`provider_meta`** 等へ、[§8](#8-common-output)）。
* `_runtime` は `execute()` の observable output で常に付与される。

### 7.4 `_usage`

`_usage` は accounting observation field（例: token 数）であり、**output port ではない**。**`execute()` の observable output には `_usage` キーを常に含め**、計上項目がなくても **値は `{}` とする**。Runner の delivery 対象にしてはならない。

`_usage` の最小形は空 dict である。

```python
{
    "_usage": {}
}
```

使用量を記録する場合は、任意の metric を含めてよい。

```python
{
    "_usage": {
        "input_tokens": 120,
        "output_tokens": 30,
        "total_tokens": 150
    }
}
```

* `_usage` の field は完全に optional である
* ただし `_usage` key 自体は `execute()` の observable output に必ず含める
* provider 固有の追加フィールドを入れてよいが、`_usage` を provider meta のダンプに使ってはならない（→ `provider_meta`、[§8](#8-common-output)）
* `_usage` の生成・正規化方式は実装詳細として固定しない。public contract としては `execute()` の observable output に `_usage` キーが常に存在することのみを要求する。

### 7.5 `execute()` と入力充足

`execute()` は、node が自分の input / output / state を見て **1 step 進める**唯一の **public execution step** である。

Node は `execute()` 内で次を **自分で**判断する。

* input が実行に足りるか
* output を置けるか（port occupancy が受け付けるか）
* 内部 precondition が満たされるか
* 実行を開始するか、no-op で戻るか
* `_state.value` と port occupancy をどう更新するか

実行できない場合、`execute()` は **no-op** で戻ってよい。その場合も §7 の observable output 形式は満たす。

実行が開始され、まだ完了していない場合、node は `_state.value` を **`executing`** にする。
正常に完了し output を filled にした場合、node は `_state.value` を **`done`** にする。
`fatal` / `limit` / `executing` でなく、done とみなす output が filled でない場合、node は `_state.value` を **`ready`** にする。

Runner は `ready` または `done` の node に対して `execute()` を呼びうる。input / output の **意味**、required input の充足、required output の空き、provider 選択を Runner は判断しない。

入力の必須条件は node の public contract と `execute()` 実装で定義する。Runner はその意味を解釈しない。

---

## 8. Common Output

### 8.1 Port 名

* **`exec` role** の concrete `ActionNode` は、標準 output port 名として **`execution_output`** を用いる。
* `exec` role の `execute()` observable output は、`execution_output` payload を含む。

### 8.2 Payload 形状（output payload）

`execution_output` の dict 直下で、必須フィールドと任意フィールドを次で定義する。

| 種別 | フィールド | 意味 |
|------|------------|------|
| required | `ok` | domain-level の成否（transport success そのものを意味しない。外部が応答した意味的失敗は通常 `false`） |
| required | `external_executor` | 論理 external executor 名 |
| required | `raw_output` | 捨てない raw |
| optional | `provider` | プロバイダ識別子 |
| optional | `model` | 利用モデル |
| optional | `task_type` | task 分類 |
| optional | `summary` | 短い要約（raw の置換ではない） |
| optional | `stdout` / `stderr` | CLI 時の回収 |
| optional | `artifacts` | 成果物リスト等 |
| optional | `provider_meta` | **provider 固有**の任意 dict（**`_runtime` と混同しない**） |
| optional | `next_hint` | 後段向けヒント |

**revision** は output payload 直下ではなく **`_runtime["ports"]["execution_output"]`** に置く（[§7.3](#73-_state--_runtime)）。

### 8.3 観測例（`execute()` 後）

`execute()` 完了後に観測しうる出力の一例。

```python
{
    "execution_output": {
        "ok": True,
        "external_executor": "…",
        "provider": "…",
        "model": None,
        "task_type": None,
        "summary": None,
        "stdout": None,
        "stderr": None,
        "raw_output": {},
        "artifacts": [],
        "provider_meta": {},
        "next_hint": None,
    },
    "_state": {
        "value": "done",
        "error": None,
    },
    "_runtime": {
        "ports": {
            "execution_output": {
                "revision": "…",
            },
        },
    },
    "_usage": {
        "input_tokens": 120,
        "output_tokens": 30,
        "total_tokens": 150,
    },
}
```

### 8.4 Rules

* **raw を捨てない。**
* **summary** は **追加情報**であり、raw の **置換ではない**。
* **provider 固有**は **`provider_meta`**。**`_runtime["ports"]` の revision 等とは別概念**。
* output payload 直下に **`meta`** という名前を **`provider_meta` の代替として用いない**（統一は `provider_meta`）。

### 8.5 Failure semantics

* 外部呼び出しが **意味のある失敗応答**を返した場合は、通常 **`ok = false`** とする。
* **precondition 不足**や **実行層の失敗**（接続不能等）は **node の fatal / limit** として扱ってよい（[§14](#14-base-class-responsibilities)）。

---

## 9. Single-Run Principle

**external execution を行う `ActionNode`（典型: `*ExecNode`）** について、原則として **1 task = 1 external execution** とする。

* **CLI** なら **1 プロセス起動**。
* **API** なら **1 リクエスト**。
* **session 継続**を前提にしない。
* `/new`, `/exit` 等の **対話操作への依存**を要求しない。

**retry:** single-run は **対話セッション継続や暗黙の再利用**を禁じるものであり、**同一 `execute` 呼び出し内の finite retry**（瞬断等）を **禁止しない**。retry 中に外部から観測される node の **_state.value は `executing`** とする（`ApiActionNode` の実装責務）。

**`PipeNode`:** composite 全体を「1 external execution」とは限らない（子ごとに [上記](#9-single-run-principle) を数える）。

---

## 10. Runner and Pipe Execution Model

本章は Runner の責務、`PipeSpec` の形、source に基づく配送規則を定義する。

### 10.1 Runner / Node responsibility boundary

Runner と Node の責務境界を次で固定する。

**Runner の責務:**

* **`PipeSpec` の source 宣言**（`nodes[*].input_ports` と `pipe.output_ports`）に沿って **port occupancy** だけを見て payload を転送する。手続の詳細は **§10.2** / **§10.5**。
* **進捗（progress）を判断して停止しない**。Runner は「進んだかどうか」を解釈しない。
* source 宣言に従う delivery のために source occupancy と target port occupancy を読む。
* delivery 成功時に target port occupancy を **filled** にする。
* delivery 成功時に source occupancy を **empty** にする（copy 失敗時は source を **filled のまま**残す）。
* `_state.value` を読み、`ready` または `done` なら `node.execute()` を呼ぶ。`executing` / `limit` / `fatal` なら `execute()` を呼ばない。
* `executing` node の完了判定、polling、非同期 process 管理は行わない。
* `_state.value` の **決定・代入は node** のみ。Runner は `done` / `limit` / `fatal` / `ready` / `executing` を **書き込まない**。
* **caller が直接 `execute()` した `PipeNode` の停止**は、その `_state.value` を caller が見て行う（`done` / `limit` / `fatal` で打ち切り）。**graph 専用の独立 state を Runner は定義しない**（§10.4）。
* source declaration / **port occupancy** では、source / target の **filled / empty のみ**を見る。payload の **domain 意味や provider 選択**は行わない。

**Node の責務:**

* 自分の `_state.value` の所有と更新
* `execute()` による input 消費と output 生成
* `execute()` 内で、自分の input / output occupancy を見て実行可否を判断する
* `fatal` / `limit` / `executing` / `done` / `ready` の決定（§7.3 の port-oriented 規約に沿う）
* 正常完了時に、自分が emit する output port occupancy を `filled` にする
* 非同期 execution を持つ場合は、内部機構で完了を検知し、`executing -> done | limit | fatal` と自分の output occupancy を更新する
* output port の選択と emit

**Runner の禁止事項:**

* input / output payload の意味を解釈して emit port や実行順を決める
* action 名を if 文で解釈して分岐する
* required input が filled か、required output が empty かを最終判定する
* node の代わりに `done` / `limit` / `fatal` を決める
* node の state を直接変更する
* provider を選択する

---

### 10.2 Runner loop

Runner は PipeNode の child PipeSpec に含まれる node を巡回する。
Runner step は、Phase A と Phase B をそれぞれ 1 回ずつ実行する 1 巡回である。

各巡回では、次の順序で処理する。

#### Phase A: delivery

Runner は PipeSpec の source 宣言に従って、filled source から empty target port への配送を試みる。

* target が empty の場合、payload を target に copy する
* copy に成功した場合、source occupancy を empty にする
* target port が filled の場合、配送せず source occupancy は filled のまま残す
* observation field（`_state` / `_runtime` / `_usage`）は配送対象にしない

#### Phase B: node execution step

Runner は各 node の state を読む。

* `ready` または `done` なら `node.execute()` を呼ぶ
* `executing` / `limit` / `fatal` なら何もしない

Runner は input payload の意味、output payload の意味、required input の充足、required output の空き、provider selection を判断しない。

PipeNode の場合、`execute()` は child PipeSpec の Runner loop を synchronous progress が尽きるまで内部で回す。
そのため、外側の Runner から見ると、`PipeNode.execute()` の 1 回の呼び出しで、その PipeNode 内で同期的に可能な delivery と child execution は完了している。
外側の Runner は PipeNode 内部の途中状態を scheduling しない。

Runner は graph completion 判定として progress を使わない。
ただし `PipeNode.execute()` の内部実装は、1 回の execute 呼び出し内で同期的に進められる処理を尽くすために progress を内部 loop の停止条件として使ってよい。

### 10.2.1 Executing state and asynchronous completion

Runner は `executing` state の node に対して `execute()` を呼ばない。

Runner は `executing` node の完了判定、polling、非同期 process 管理を行わない。

`executing` node が同期処理中か非同期処理中かは node の内部実装である。

非同期 execution を行う node は、自分の内部機構により完了を検知し、自分の state と output occupancy を更新する責務を持つ。

完了時の state 遷移は次のいずれかである。

* 正常完了: `executing` -> `done`
* 実行制約到達: `executing` -> `limit`
* 継続不能な失敗: `executing` -> `fatal`

Runner はこの遷移を直接実行しない。

Node state は node が所有するため、非同期 node は `execute()` 呼び出し外で自分の state を更新してよい。
ただし、その更新は node 自身の内部 execution に由来しなければならず、Runner が代入してはならない。

PipeNode が `executing` になるのは、内部 child のどれかがこの意味で非同期 execution 中であり、同期的な child PipeSpec execution が尽きた場合だけである。

child の非同期完了により PipeNode 内部の状態が変わった場合、PipeNode は自身の `_state.value` を再評価できる。Runner が PipeNode の state を直接代入してはならない。

caller は直接 `execute()` した `PipeNode` の `_state.value` を観測し、`done` / `limit` / `fatal` のいずれかになるまで execution step を継続する。`PipeNode.execute()` は内部で child PipeSpec 用の Runner step を呼び、その結果から自身の `_state.value` を整える（§13）。

v1.6 は **graph レベルでの repeated domain execution** を規定しない。`execute()` が no-op か再開かは node の責務である。

### 10.2.2 同一 Runner インスタンス内の idempotency（実装詳細）

本リポジトリの Runner は、**同一 `Runner` インスタンス**の寿命のあいだ、次のような **重複 `node.execute()` の抑止**を行ってよい。これは **input / output payload の意味解釈ではなく**、delivery 後に source が empty になる **1:1 occupancy モデル**のもとで、**同じ進行を何度も繰り返さない**ための **idempotency guard** である。

* 宣言 input port が **一つも無い** node が、すでに **runner 可視の status / port occupancy に material な変化**を起こした後は、その Runner インスタンス上で **再度 `execute()` しない**。
* **`done`** で **filled な output port が無い**（idle `done`）node への `execute()` は、その Runner インスタンス上で **高々一度**に制限してよい。
* **`done`** かつ **output snapshot が空でない** node は、Phase B で **`execute()` を呼ばない**（`BaseNode` テンプレート側が **再入 duplicate 実行**を避ける想定と整合する）。

これらは **normative な graph 意味論の一部ではなく**、参照実装の挙動として許容される。payload の充足判定や provider 選択を Runner が行うことにはならない（[§10.1](#101-runner--node-responsibility-boundary)）。

### 10.3 PipeSpec in v1.6

v1.6 の `PipeSpec` は少なくとも次を持つ。

* `pipe`
* `nodes`

`PipeSpec` は Runner が実行する in-memory 表現であり、外部 JSON も同じ形を持つ（§16.3）。

single final-node completion rule は v1.6 の public model では採用しない。

Runner の反復 step は **§10.2** とし、**progress による停止は行わない**。execution の打ち切りは **caller が直接 `execute()` した `PipeNode` の state** で行う。

cycle / repeated execution / done から ready への復帰は v1.6 の public model では graph 機能として support しない。
cycle を含む `PipeSpec` は **invalid** である。
JSON loader または `PipeSpec` validator は cycle を検出したら **reject しなければならない**。
Runner が実行するのは **validation 済みの acyclic `PipeSpec` object のみ**とする。

source 文字列は次のみを許可する。

* `input.<pipe_input_port>`
* `<node_id>.<output_port>`

`node_id` と `port_name` は次の形式に限定する。

* 正規表現: `[A-Za-z][A-Za-z0-9_]*`
* `.` は含めない
* `node_id` に `input` / `output` は使えない
* `port_name` に `_state` / `_runtime` / `_usage` は使えない

次は禁止する。

* `pipe.output_ports` の source に `input.*` を直接指定
* `_state` / `_runtime` / `_usage` を source に指定

v1.6 では source 参照制約を次で固定する。

* 1 source -> 最大 1 target port
* 1 target input port <- 最大 1 source

v1.6 では fan-out は support しない。

同じ payload を複数 downstream node に渡したい場合は、明示的な copy / split 用 ActionNode を置く。

理由は、v1.6 の delivery model が source occupancy を delivery 成功時に empty にする 1:1 delivery model だからである。

#### PipeSpec validation（必須 reject）

validation（JSON loader またはそれに準ずる層）は、少なくとも次を検出したら **reject**（Runner は検証済み object のみを受け取る）しなければならない。

* **cycle** が source 参照から導出される graph に含まれる
* **未知の node id**：source に現れる `<node_id>` が `nodes` に存在しない
* **ノードエントリの妥当性**：`type` が欠ける／空文字、または registry が解決できない unknown node type
* **`params` が JSON object でない**
* source 文字列が許可される形式に合致しない
* `_state` / `_runtime` / `_usage` を source に指定
* **同一 source の重複**：同じ source が複数 target port に参照される（fan-out）
* **同一 target input port の重複**：同じ `<node>.<input_port>` に複数 source が割り当てられる

（JSON が **許可しない root object / node entry の追加キーを含む**場合も invalid。細則は [§16.3](#163-pipespec-serializationv16)。）

### 10.4 PipeNode state in v1.6

`PipeNode.execute()` は、内部 Runner loop を synchronous progress が尽きるまで回してから戻る。

ここで synchronous progress とは、同一 `PipeNode.execute()` 呼び出し内で可能な delivery、child `execute()` 呼び出し、pipe output buffer 更新、child state 更新を指す。

PipeNode の `_state.value` は、内部 loop 終了時点の child state と pipe output buffer から次の優先順位で決める。

1. いずれかの direct child が `fatal`
   -> PipeNode は `fatal`

2. いずれかの direct child が `limit`
   -> PipeNode は `limit`

3. `pipe.output_ports` に宣言された output port（すなわち §16.3 の `pipe.output_ports` の key 群）がすべて pipe output buffer で `filled`
   -> PipeNode は `done`

4. いずれかの direct child が genuinely `executing`
   -> PipeNode は `executing`

5. それ以外
   -> PipeNode は `ready`

[4] の genuinely `executing` とは、child が非同期 execution 中であり、`execute()` 呼び出し外で完了を待っている状態を指す。

[5] は、pipe output はまだ filled ではないが、`fatal` / `limit` / async executing のいずれでもない状態である。
これは入力待ち、branch 未到達、または現在の input では required pipe output が生成されない状態を表す。
error ではない。

PipeNode の state は、Runner step の各巡回後、または child node が非同期完了により `_state.value` / output occupancy を更新した後に再評価してよい。

### 10.4.1 Caller-visible stop rule

caller から見た execution は、caller が直接 `execute()` した PipeNode の state により停止を判断する。

* `done` なら正常停止
* `limit` なら limit 停止
* `fatal` なら fatal 停止
* `executing` なら継続中
* `ready` なら入力待ち、またはまだ pipe output が filled でない状態

Runner は progress の有無を graph completion 判定として使わない。

### 10.5 Source-based delivery rules

**§10.2** の配送手続と同義の詳細。Runner は PipeSpec の source 宣言に従い、source occupancy が filled かつ target port occupancy が empty のときだけ配送する。
Delivery に成功したら、Runner は **source occupancy を必ず empty にする**。target が `pipe.output_ports` の port の場合も同様であり、payload を **`PipeNode` の pipe output buffer に write したうえで** source occupancy は **empty** にする。

**clear** は **delivery に使う source occupancy**（filled / empty）のみを **`empty`** に戻す操作であり、過去に `execute()` が返した **observable output の dict に対して破壊的に mutation を加えることを意味しない**。
Runner は `execute()` が返したオブジェクトから **payload を読み delivery キューまたは buffer に写す**のみとし、その後の clear は **別レイヤーの port occupancy** に対して行う。**観測済み output snapshot を後から Runner が書き換えてよいモデルにはしない**。

source が `input.<pipe_input_port>` の場合も、clear されるのは Runner が subgraph execution 用に管理する source occupancy であり、PipeNode input payload そのものではない。

* **Observable output**：呼び出し時点で `execute()` が返した **不変またはコピーのスナップショット**として扱う（実装は snapshot / copy でよい）。
* **Port occupancy**：Runner が subgraph 実行中にのみ管理する filled / empty。clear はここだけを更新する。

`_state` / `_runtime` / `_usage` は observation field であり、delivery の対象にしてはならない。
payload 変換・分岐・集約が必要な場合は、Runner ではなく `ActionNode` を置く。

`PipeNode` の `pipe.output_ports` に対応する output port への書き込みで当該 **pipe output buffer** が **filled** になる。

Runner は PipeSpec に現れる source のみを delivery の対象にする。

* **source として参照されない** filled output は **delivery には使わない** が、当該 **node の observable output** に残ってよい。
* source として参照されない output は **`pipe.output_ports` の充足にも寄与しない**（明示的な `output.*` に届かない emission は contract 上不十分）。

---

## 11. Role and Implementation Model

### 11.1 分離

* **role** — **semantic contract**（何をするか、期待する入出力）。
* **implementation kind** — **実装手段**（Python / CLI / API）。

**role は継承木に載せない。** 継承の骨格は **implementation kind** 側（`PythonActionNode` 等）に置く。

### 11.2 role が定義する最低限

`ActionNode` の role は semantic contract であり、少なくとも次を定める。

* input ports: どの input port をどの意味で受け取るか
* output ports: どの output port を emit しうるか
* payload shape: 各 output payload の形

具象 node の入力契約は補助宣言してよいが、Runner の実行可否判定には使わない。実行可否の最終判断は node の `execute()` 内で行う。

### 11.3 禁止事項

* **role-based taxonomy**（例: `RoutingNode` を top-level に置く）を作らない。
* **role ごとの abstract base class** を設けない（framework 固定の implementation kind 基底のみ）。
* **role と implementation kind を同一継承木で同一視**してはならない。

### 11.4 role の表現

* v1.6 では **role の source of truth は `ActionNode` class attribute**（例: `role = "exec"`）とする。
* v1.6 が定義する built-in role は **`exec`** と **`summarize_output`** である。その他の role は concrete / application-specific role として追加してよいが、本仕様の built-in role contract ではない。

---

## 12. Built-in Role Contracts

### 12.1 Spec boundary と semantics の所在

* 本仕様が定義するのは **built-in role contract** に限る。処理意味論は built-in と [§8](#8-common-output) に収まる。アプリ固有 role は追加してよいが本仕様の必須 contract ではない。
* **`PipeNode` はこれらの意味を実装しない**。必要なら **child `ActionNode`** を置く（[§13](#13-pipenode-contract)）。
* `ActionNode` は入力に応じて emit する output port の subset を選んでよい。Runner はその意味を解釈しない。

### 12.2 `summarize_output`

* **入力:** 直前結果の port — 多くは **`execution_output`**（[§8](#8-common-output)）。
* **produced output port:** **`summary`**
* **minimum 意味:** raw を捨てず、要約・要点・後段向けヒントなど追加の視点を載せる（フィールド名は concrete に委ねる）。

### 12.3 `exec`

* **role `exec`** の concrete は **標準 output port `execution_output`** を返す（[§8](#8-common-output)）。
* exact input set は external executor ごとに異なってよい（CLI 引数、API ボディ等）。
* output の意味論は **Common Output** に従い、implementation kind はその意味を変えない。

アプリ固有の concrete（開発ループ向け stage nodes 等）は本仕様の built-in role contract ではなく、taxonomy / port contract に従って `nodes/` / `workflows/` に配置する（[§16](#16-project-layout-guidance)）。

---

## 13. PipeNode Contract

### 13.1 定義と責務

* **`PipeNode`** は **child node を接続**し、**外部から 1 node として**公開する。
* **`PipeNode` は domain semantics を持たない**（output 選択 / summarize / exec の意味は **child `ActionNode`**、[§12](#12-built-in-role-contracts)）。
* **`BaseNode` の subclass** であり、外部からは通常 node と同様に `execute()` される。
* **`PipeNode` は `pipe_spec()` により child PipeSpec を宣言する。**
* child PipeSpec の実行順序・接続・中間 payload の受け渡しは、`PipeNode` の execution path が Runner に `PipeSpec` を実行させることで行う。
* subgraph の **PipeNode が `done` になるための条件**は、その `PipeSpec` の `pipe.output_ports` に宣言された public output が pipe output buffer で filled になることである（詳細 **§10.4**）。
* PipeNode の public input contract は `pipe_spec()` が返す `PipeSpec` の `pipe.input_ports` で宣言する。
* PipeNode の public output contract は同じく `pipe.output_ports` で宣言する。
* v1.6 では class attribute による `required_input_ports` / `required_output_ports` の PipeNode contract は定義しない。
* `pipe.output_ports` に載る名前は、当該 `PipeNode` の public normal output port と整合していなければならない。`pipe.output_ports` が未充足であること自体は fatal ではない。PipeNode state は §10.4 の優先順位に従う。
* concrete `PipeNode` は `pipe_spec()` が返す PipeSpec の `pipe.input_ports` / `pipe.output_ports` で公開入出力を宣言する：

```python
class DevelopmentFlowPipeNode(PipeNode):
    def pipe_spec(self) -> PipeSpec:
        ...
```

### 13.2 境界

* **child payload の意味（domain semantics）を、pipe 側の都合で再解釈したり書き換えたりしてはならない**（*Must not reinterpret or alter the semantic meaning of child payloads.*）。配線として **そのまま渡す／出口で公開する**ことはよい。
* **caller**（定義 §4）は内部 subgraph に依存せず **公開 port contract** と観察可能な契約のみに依存する（black box）。意味の載せ替えは **child の `ActionNode`** に寄せる（§13.1）。

### 13.3 `execute()` と single-run

* **`PipeNode.execute()`** は **基底テンプレート**に従う。内部では、`pipe_spec()` が返す child PipeSpec を Runner に渡し、synchronous progress が尽きるまで内部 Runner loop を実行する。
* その後、PipeNode は §10.4 の優先順位に従って自身の `_state.value` を整える。
* concrete `PipeNode` は child node を直接 `execute()` してはならない。child execution は Runner が PipeSpec に従って行う。
* single-run の粒度は [§9](#9-single-run-principle) を参照。
* **child ランタイムのリセット:** 各 **外側**の `PipeNode.execute()` 呼び出しの開始時に、child node 集合の実行時状態は **リセット**され、その呼び出し専用の **fresh な内部 Runner** 用状態から始まる（実装では child の `reset_status` 等に相当する）。
* **partial progress と外側呼び出し:** **別々の外側** `PipeNode.execute()` 呼び出しのあいだでは、子ノードの partial progress は **保持しない**。1 回の外側呼び出しの内側で、同期 Runner loop が尽きるまで delivery / child `execute` を進めるモデルである。

### 13.4 `pipe_spec()` contract

`PipeNode` は **`pipe_spec()` をオーバーライドして `PipeSpec` を返す**。

```python
def pipe_spec(self) -> PipeSpec:
    ...
```

* 戻り値は `PipeSpec`（`pipe` / `nodes` を持つ）
* `pipe_spec()` が pure であるとは、同一 PipeNode instance に対して副作用なく同じ PipeSpec 構造を返す、という意味である
* constructor params を参照して PipeSpec を構築してよい
* ただし、`pipe_spec()` 呼び出しごとに外部状態、時刻、乱数、I/O により構造が変わってはならない
* `pipe_spec()` は `PipeNode` execution path から参照される。
* concrete `PipeNode` は public `execute()` contract を破る独自実行 API を公開してはならない。
* concrete `PipeNode` は **child node を直接 `execute()` してはならない**（実行は Runner が `PipeSpec` に従って行う）

---

## 14. Base Class Responsibilities

本章は **contract レベルの責務**に限る。細かい **helper の具体名**や **child 出力の選別ロジック**は仕様に固定しない。

### 14.1 `BaseNode`

* `BaseNode` は `execute()` の共通 contract を提供する。
* **本リポジトリ v1.6.x（transitional）**では、公開シグネチャは次の形である: `execute(self, inputs: Mapping[str, Any], params: Mapping[str, Any]) -> dict`。`inputs` は **input port name → dict payload** の mapping（[§7.2.1](#721-node-input-format)）。テンプレートはこの mapping から port occupancy を整え、`run(inputs, frozen_params, context)` を呼ぶ。戻り値は必ず observable output とし、形式は §7 に従う。
* **target direction（非 transitional）:** 将来バージョンでは、graph 上の caller は **`set_input`（または Runner の delivery）のみ**で occupancy を埋め、`execute()` は **input payload 引数なし**（または `params` のみ）へ寄せることを想定する。子ノードへの配送は引き続き **`set_input`** 経路のみとする（Runner は payload の意味を解釈しない; [§10.1](#101-runner--node-responsibility-boundary)）。
* Runner は node state を直接変更しない。`_state.value` と port occupancy の整合は node が所有し、同期処理では `execute()` 内、非同期処理では node 内部完了検知でも更新しうる。
* `execute()` は処理を進められない場合 no-op で戻ってよい。その場合も `_state` / `_runtime` / `_usage` を返す。
* 正常完了時は node が input を consume し、該当 output の occupancy を filled にし、§7.3 に従って **`done`** としうる。
* 実行制約に達した場合は **`limit`**、継続不能な失敗は **`fatal`**、処理途中は **`executing`** としうる。

### 14.2 `PipeNode`

* [§13](#13-pipenode-contract) のとおり **配線・公開・graph boundary の所有**のみ。**semantics は持たない**。
* **`PipeNode` は `pipe_spec()` を返し**、Runner が child `PipeSpec` を実行する。

### 14.3 `ActionNode` 系（`PythonActionNode` / `CliActionNode` / `ApiActionNode`）

* **処理・外部実行・変換・正規化**を担う側である（**semantics ownership** は [§11](#11-role-and-implementation-model)、[§12](#12-built-in-role-contracts)）。
* **`PythonActionNode`** — ローカル Python による入力解決と出力 dict の組み立て。
* **`CliActionNode`** — subprocess、timeout、exit code、stdout/stderr 回収。
* **`ApiActionNode`** — HTTP request、auth、URL、retry（[§9](#9-single-run-principle)）、Common Output への正規化。

---

## 15. Naming and Inheritance Rules

### 15.1 フレームワーク固定名

`BaseNode`, `PipeNode`, `ActionNode`, `PythonActionNode`, `CliActionNode`, `ApiActionNode` は **本仕様の固定語彙**とする。

### 15.2 concrete 命名

**concrete `ActionNode`:** **`<Qualifier><Role>Node`** の形式とする。

`<Qualifier>` は以下のいずれかを使う:

| Qualifier の種類 | 例 |
|---|---|
| implementation kind の略称 | `Python`, `Cli`, `Api` |
| プロバイダ名 | `Kimi`, `Claude`, `Gemini` |
| アプリ固有のスコープ | `Review`, `Implement` |

複数の Qualifier を組み合わせてよい（例: `KimiExecNode`, `PythonSelectTargetNode`）。
基底クラス名（`ActionNode`, `PipeNode` 等）と同名にしてはならない。

* **concrete `PipeNode`:** **`<Purpose>PipeNode`**（例: アプリ固有の合成；**配線専用**であることが名前から読めてもよい）。
* **外部実行系:** クラス名は **`*ExecNode`** に揃え、role は **`exec`**（[§11](#11-role-and-implementation-model)）。
* **基底クラス名と concrete の同名**は禁止。
* **ファイル名**は **snake_case**（例: `python_select_target.py`, `kimi_exec.py`）。

### 15.3 禁止・推奨

* **role ベースの abstract class** は設けない。
* **巨大な cross-provider 抽象**を基底に押し込まない。
* 重複は **非 node の private モジュール**に切り出してよいが、**node contract を隠すために使ってはならない**。
* helper の名前や構成は仕様で固定しない。

---

## 16. Project Layout Guidance

本節は主にレイアウト推奨を述べる。規範は [§16.3](#163-pipespec-serializationv16) の PipeSpec JSON 形状。

### 16.1 方針

* **`core/`** — **`BaseNode`**、**taxonomy**（`PipeNode` / `ActionNode` と implementation kind）、**`Runner`**、**registry**。
* **`nodes/`** — **再利用可能な building-block concrete のみ**。第一分類軸は **role または purpose**（`summarize/`、`exec/`、`transform/` 等）。
* **`workflows/`** — **ユーザー向けの複合 workflow**（例: `development_flow`、固定プロバイダの合成 Pipe）。`nodes/` に混在させない。
* **`Action/` と `Pipe/` をディレクトリ分類の第一軸にしない**。
* **特定の concrete クラス名**を **標準ディレクトリ構成の前提**にしない（説明用・実験用の合成 node に依存したレイアウトを強制しない）。
* **`PipeNode` は special concrete を仕様上要求しない**。

### 16.2 例（推奨のイメージ）

taxonomy は **`core/node_kinds/` のようなパッケージ**に分けると、`PipeNode` と `ActionNode` 系の **別ファイル・責務分離**がしやすい（**単一の `node_kinds.py` に全部詰め込む**より推奨）。

```text
nodeflow/
  core/
    base_node.py
    runner.py
    registry.py
    loader.py
    run.py
    config.py
    node_kinds/
      __init__.py
      action.py
      pipe.py

  nodes/
    summarize/
    exec/
    transform/

  workflows/
    development_flow/
    review_with_claude/
    implement_with_codex/
```

**`core/loader.py` / `core/run.py` / `core/config.py`** — **`core/runner.py`**（pipe の step 実行）とは別に、**pipeline 読み込み・組み立て・ルート `execute` のキック**と JSON PipeSpec IO を置く。`loader` / `run` / `config` の役割分担はリポジトリ任せ。

### 16.3 PipeSpec serialization（v1.6）

v1.6 の `PipeSpec` 表現は次で固定する。

1. **in-memory 表現**: `PipeSpec(pipe, nodes)` object — Runner が実行する
2. **外部表現**: **JSON のみ**（node-centered format）— ファイル・永続化・ユーザー記述・テスト fixture

Runner は JSON を直接実行しない。loader は外部 JSON を parse / validation し、Runner が参照しやすい in-memory `PipeSpec(pipe, nodes)` に正規化してから渡す。

#### JSON PipeSpec（normative shape）

外部 JSON は厳密に次の JSON root object shape を持つ。

```json
{
  "pipe": {
    "input_ports": [
      "<pipe_input_port>"
    ],
    "output_ports": {
      "<pipe_output_port>": "<source>"
    }
  },
  "nodes": {
    "<node_id>": {
      "type": "<node_type>",
      "params": {},
      "input_ports": {
        "<input_port_name>": "<source>"
      },
      "output_ports": [
        "<output_port_name>"
      ]
    }
  }
}
```

JSON root object に `pipe` と `nodes` 以外の key を含めてはならない（未知 key は invalid）。

**重複 object key:** JSON テキスト内で **同一 object に同じ key が二度出現する**ことは RFC に反するが、Python 標準の `json.loads` は **後勝ちで黙って解釈する**ため、本リポジトリの PipeSpec loader は **その違反を検出・拒否しない**。厳密に弾きたい場合は、編集時検証・`jq`・JSON Schema 等の **外部ツール**に委ねる。

#### Root keys

| key | required | JSON type | 意味 |
|---|---:|---|---|
| `pipe` | yes | object | この PipeSpec を公開する PipeNode の public input / output 宣言。 |
| `nodes` | yes | object | `<node_id>` を key とする node spec mapping。 |

#### Pipe spec（`pipe`）

| key | required | JSON type | 意味 |
|---|---:|---|---|
| `input_ports` | yes | array[string] | 許可される `input.<pipe_input_port>` の集合。 |
| `output_ports` | yes | object | `<pipe_output_port> -> <source>` の mapping。`source` は `input.<pipe_input_port>` または `<node_id>.<output_port>` 形式。 |

追加 validation:

* `pipe.input_ports` の要素は unique でなければならない
* `pipe.output_ports` の key は §10.3 の `port_name` regex に従わなければならない
* `pipe.output_ports` の source は §10.3 の valid source 形式でなければならない
* `pipe.input_ports` に宣言されていない `input.<name>` を source に使ってはならない
* `pipe.output_ports` の source に `input.<pipe_input_port>` を直接指定してはならない。PipeNode に input pass-through semantics を持たせないためであり、input をそのまま output に公開したい場合も明示的な child `ActionNode` を置き、その output を source にする

#### Node spec（`nodes.<node_id>` の value）

各 node entry は次の key のみを持つ（未知 key は invalid）。

| key | required | JSON type | 意味 |
|---|---:|---|---|
| `type` | yes | string | registry で解決される node type。空文字は禁止。 |
| `params` | yes | object | node constructor 引数。省略不可（空でも `{}` を明示）。 |
| `input_ports` | yes | object | `<input_port_name> -> <source>` mapping。`source` は `input.<pipe_input_port>` または `<node_id>.<output_port>`。 |
| `output_ports` | yes | array[string] | node が emit しうる output port 名の宣言。compile 時 validation に用いる。 |

追加 validation:

* `node.input_ports` の key は §10.3 の `port_name` regex に従わなければならない
* `node.output_ports` の要素は unique でなければならない
* `node.output_ports` に宣言されていない `<node_id>.<output_port>` を source に使ってはならない

`params` が JSON object でない（例: `null` / array）場合は invalid。
loader は `params` が JSON object であることを検証する。
`params` の中身の意味・必須 field・型の検証は、対応する node type の constructor または node-specific validator の責務である。

loader は `type` を registry で解決し、`params` を渡して node instance を構築する。registry が `type` を解決できないとき、または `type` が空文字のときは invalid。

#### Source normalization rule（external JSON -> Runner inputs）

loader は外部 JSON を、Runner が参照しやすい in-memory `PipeSpec` と source 宣言へ正規化する。

展開後の結果は §10.3 の source 形式・重複制約・cycle 禁止・observation source 禁止を満たさなければならない。

```json
{
  "pipe": {
    "input_ports": ["request"],
    "output_ports": {
      "flow_output": "b.result"
    }
  },
  "nodes": {
    "a": {
      "type": "A",
      "params": {},
      "input_ports": {
        "request": "input.request"
      },
      "output_ports": ["result"]
    },
    "b": {
      "type": "B",
      "params": {},
      "input_ports": {
        "request": "a.result"
      },
      "output_ports": ["result"]
    }
  }
}
```

#### unsupported 形式

YAML、TOML、Python literal dict ファイル、ad-hoc な独自 graph 記法は v1.6 の **public model に含めない**。それらのみを読む loader を **v1.6 準拠**として扱ってはならない。将来バージョンで別途仕様化してから検討する。

### 16.4 ルール

* **taxonomy（抽象の node kind）** は **`core/`**（上記 `node_kinds/` 等）。**building-block concrete** は **`nodes/`**、**パッケージされた複合 workflow** は **`workflows/`**。
* **`core/`** に **アプリ concrete** を置かない。
* **built-in registration** は **一箇所に集約**する。
* **直列 subgraph** は **`PipeNode` の能力**として表現し、**特別な concrete 名を仕様が要求しない**。

---

## 17. Conformance Checklist

実装が v1.6 準拠かどうかは以下で確認する。

| # | 確認項目 | 種別 | 判定方法 |
|---|---|---|---|
| 1 | `BaseNode` 直下の subclass が `PipeNode` / `ActionNode` のみ | automated | 継承木を検査 |
| 2 | `PipeNode` が domain semantics を持たない | review | `pipe_spec()` / child `ActionNode` をレビュー |
| 3 | `exec` role の node が `execution_output` を返す | automated | port 名を検査 |
| 4 | `execution_output` に `ok` / `raw_output` / `external_executor` が存在する | automated | payload を検査 |
| 5 | すべての `execute()` が **observable output の top-level に `_state` / `_runtime`（少なくとも `ports` を含む） / `_usage`（空でも `{}`）を含む** | automated | `execute()` 戻り値を検査 |
| 6 | observation field が Runner delivery の対象にならない | automated / static | Runner 実装を検査 |
| 7 | Runner が output payload の内容で分岐しない | review / static | Runner 実装をレビュー |
| 8 | `exec` 系 node の外部呼び出しが 1 task = 1 external execution | review / runtime | 外部呼び出し回数を検査 |
| 9 | 外部 JSON が in-memory `PipeSpec(pipe, nodes)` と source 宣言に正規化される | automated | loader の正規化結果を検査 |
| 10 | Runner が node の state を直接変更しない | review / static | Runner 実装をレビュー |
| 11 | Node が `execute()` 内で input/output/precondition に基づく実行可否を自分で判断する | review | Node 実装をレビュー |
| 12 | cycle を含む `PipeSpec` は validation で reject（Runner は acyclic のみ実行） | automated | loader / validator / Runner を検査 |
| 13 | concrete `PipeNode` が **`pipe_spec()` を実装している** | automated / review | `PipeNode` 実装を検査 |
| 14 | PipeSpec の外部表現が JSON のみ | review | loader / docs を検査 |
| 15 | no-op を含むすべての `execute()` 返却で **§7.2.2 の必須 observation フィールドを含む** | automated | `execute()` 戻り値を検査 |
| 16 | external PipeSpec JSON が **[§16.3](#163-pipespec-serializationv16) の厳密 shape（`pipe` / `nodes`、`input_ports` / `output_ports`、`params` 明示）に適合する** | automated | JSON schema / validator を検査 |
| 17 | concrete `PipeNode` が **child を直接 `execute()` していない** | review / static | `PipeNode` 実装をレビュー |
| 18 | Runner が **progress／進捗の有無**だけを理由に subgraph を終了しない | review / static | Runner 実装をレビュー |
| 19 | caller-visible execution の打切りが、caller が直接 `execute()` した PipeNode の state（`done` / `limit` / `fatal`）に依拠する | review / runtime | caller / Runner 統合を検査 |
| 20 | PipeNode state が §10.4 の優先順位（1〜6）に従う | automated / runtime | child state と pipe output buffer を検査 |
