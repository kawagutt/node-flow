# NodeFlow 仕様書（v1.5）

本書は **NodeFlow v1.5 の public model の正本**である。

* 他リリースとの差分や互換メモは含めない。
* 現在 support する **contract / taxonomy / runtime boundary** だけを書く。
* examples 的な reusable concrete pipe を本文の必須要素として固定しない。
* 具体 node の列挙は **built-in role を支える最小限**に留める。

文書の中心は次の五つである。

* **node model**（`BaseNode` と port・観測可能な出力の境界）
* **taxonomy**（`PipeNode` / `ActionNode` と implementation kind）
* **port contract**（外部 interface・予約キー・`run()` / `execute()` の境界）
* **role contract**（意味と built-in role；**処理の意味論はここに置く**）
* **Common Result** と **single-run dispatcher model**（外部実行の粒度と Runner の dumb 性）

**読み方:** まず [§5](#5-basenode-and-the-node-model) で node の共通本質を押さえ、[§6](#6-node-taxonomy)・[§7](#7-port-contract) で形を固定し、[§9](#9-built-in-role-contracts) が **意味処理の正本**、`PipeNode` は [§10](#10-pipenode-contract) のとおり **配線のみ**である。

---

## 目次

1. [Purpose](#1-purpose)
2. [Scope and Non-Goals](#2-scope-and-non-goals)
3. [Design Principles](#3-design-principles)
4. [Core Concepts and Terminology](#4-core-concepts-and-terminology)
5. [BaseNode and the Node Model](#5-basenode-and-the-node-model)
6. [Node Taxonomy](#6-node-taxonomy)
7. [Port Contract](#7-port-contract)
8. [Role and Implementation Model](#8-role-and-implementation-model)
9. [Built-in Role Contracts](#9-built-in-role-contracts)
10. [PipeNode Contract](#10-pipenode-contract)
11. [Common Result](#11-common-result)
12. [Single-Run Principle](#12-single-run-principle)
13. [Base Class Responsibilities](#13-base-class-responsibilities)
14. [Naming and Inheritance Rules](#14-naming-and-inheritance-rules)
15. [Project Layout Guidance](#15-project-layout-guidance)
16. [Success Criteria](#16-success-criteria)

---

## 1. Purpose

NodeFlow v1.5 は **dispatcher-oriented node model** である。

* task を適切な **executor**（CLI・HTTP API 等の外部実行器）に振り分け、外部実行を **単発 run** として扱い、結果を **共通形式（Common Result）** で後段へ渡す。
* 汎用 workflow engine の抽象性を最大化することより、**実際に使える dispatcher** と **明確な contract** を優先する。
* node の **再利用**と **composite（`PipeNode`）** は維持する。`PipeNode` は **子 node を接続し 1 node として公開する**だけであり、**意味のある処理は child の `ActionNode` に委ねる**（[§10](#10-pipenode-contract)）。
* **Runner** は graph を愚直に進めるだけとし、routing や provider 選択を持たない（[§3](#3-design-principles)）。

---

## 2. Scope and Non-Goals

### 2.1 Scope

v1.5 が primary scope とするもの。

* **task routing**（task メタデータに基づく executor / 次段の決定 — **built-in role `route_by_task_type` 等、child `ActionNode` で表現**）
* **external execution**（CLI / API による単発実行）
* **summarization / 整形**（**built-in role `summarize_result` 等**）
* **composite（`PipeNode`）** による subgraph の **配線と公開**（処理そのものは scope の「routing / summarize」とは別層で、**child に置く**）

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

* **Runner is dumb** — 実行順の賢い最適化・routing policy・provider 選択は Runner に置かない（[§3.1](#31-runner-prohibitions)）。
* **external execution is single-run by default** — 1 task に対する外部 run の粒度は [§12](#12-single-run-principle)。
* **top-level taxonomy** は **`PipeNode` / `ActionNode`** のみ。
* **`PipeNode` は処理しない** — 配線・接続・graph 境界の所有のみ（[§10](#10-pipenode-contract)）。選択・振り分け・要約・変換などは **child `ActionNode`**（[§9](#9-built-in-role-contracts)）。
* **role** と **implementation kind** は独立（[§8](#8-role-and-implementation-model)）。
* **node interface は port のみ**（内部状態・内部 subgraph は契約外）。
* **予約トップレベル runtime key** は最小限（`_runtime` / `_usage` のみ、[§7](#7-port-contract)）。
* **base class** と **concrete node** の責務を分離する。
* 本仕様は **built-in role contract** を定義し、個別 concrete の局所構造は過剰に固定しない。
* **便利さより contract の明確さ**、**実装の楽さより public model のきれいさ**を優先する。

### 3.1 Runner Prohibitions

dispatcher を実装するとき、次を **禁止**する（[§12](#12-single-run-principle) と併せて解釈する）。

* Runner は **routing policy** を持ってはならない。
* Runner は **provider selection** を持ってはならない。
* Runner は **node role を解釈して実行順を変えてはならない**。
* 上記の判断は **`ActionNode` の実装**および **`PipeNode` が配線した subgraph 内の node** に閉じ込める（`PipeNode` 自体は **意味を解釈しない**、[§10](#10-pipenode-contract)）。

---

## 4. Core Concepts and Terminology

後続章で用いる語を短く定義する（詳細 rule は各章に従う）。

| 語 | 意味 |
|----|------|
| **node** | graph 上の実行単位。外部から見えるのは **port のみ**（[§5](#5-basenode-and-the-node-model)、[§7](#7-port-contract)）。 |
| **task** | dispatcher が扱う仕事単位（例: review, implement, summarize）。 |
| **executor** | CLI プロセスや HTTP クライアント等、**node-flow 外**で実処理を行う外部実行器。 |
| **external execution** | `ActionNode` が executor を呼び出して結果を得る行為。 |
| **`ActionNode`** | **意味のある処理**（routing / exec / summarize 等）を担う単一責務 node。実装手段は **implementation kind**。 |
| **`PipeNode`** | child graph を **接続**し **1 node として公開**する composite。**domain-level の処理は行わない**（[§10](#10-pipenode-contract)）。 |
| **role** | node の **意味・契約**（何をするか）。**NodeFlow が support する処理意味論は built-in role で表す**（[§9](#9-built-in-role-contracts)）。 |
| **implementation kind** | 処理の **実現方法**（`PythonActionNode` / `CliActionNode` / `ApiActionNode`）。 |
| **public port contract** | caller が依存してよい **port 名と payload 形**の宣言。 |
| **domain output** | **domain port** に載る意味あるデータ（`_runtime` / `_usage` 以外の output port）。 |
| **runtime metadata** | 実行テンプレートが付与する **`_runtime`** 等の観測用メタデータ。 |
| **Common Result** | `exec` role の標準出力 port **`execution_result`** の共有 payload 形（[§11](#11-common-result)）。 |

---

## 5. BaseNode and the Node Model

### 5.1 Node の定義

* **Node** は graph 上の **実行単位**である。
* Node の **外部 interface** は **port のみ**である（内部状態や subgraph の形は契約外、[§7](#7-port-contract)）。
* Node は **`execute()`** により実行される。共通の **execution template** は **`BaseNode`** が担う。
* 返却値は **domain port** と、テンプレートが整える **`_runtime` / `_usage`** に分けて考える（[§7](#7-port-contract)）。

### 5.2 `BaseNode`

* **`BaseNode`** は **すべての node の最上位基底**である。
* **execution template** および **status / limit / error** を担う。**`_runtime` / `_usage` の意味、`run()` が返してよいもの、観測形**は **§7.5 / §7.6** を正本とする。
* **`run()`** は subclass が実装し、**domain contract** は **domain port** に限る（[§7](#7-port-contract)）。

### 5.3 Node の本質

Node は、**input port を受け取り**、**ひとまとまりの境界のある仕事**を行い、**output port を返し**、その **execution state** は **`BaseNode` の共通テンプレート**で観測される単位である。

このうち **「仕事」の意味内容**（routing・exec・要約など）は **`ActionNode` と role** が担い、**`PipeNode` は子に仕事を割り当てる配線に徹する**（[§10](#10-pipenode-contract)）。

---

## 6. Node Taxonomy

### 6.1 階層

v1.5 の **top-level category** は **`PipeNode` と `ActionNode` のみ**（`BaseNode` 直下はこの 2 つに限定）。

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
* **`execute()`** の共通構造は **`BaseNode` のテンプレート**に従う（[§5](#5-basenode-and-the-node-model)、[§13](#13-base-class-responsibilities)）。

### 6.3 各クラスの意味（概要）

* **`PipeNode`** — child を **接続**し graph を **1 node として公開**する。**semantics / domain logic は持たない**（[§10](#10-pipenode-contract)）。
* **`PythonActionNode`** — ローカル Python による **処理**。
* **`CliActionNode`** — subprocess 等による **外部 CLI 実行**。
* **`ApiActionNode`** — HTTP API **呼び出しと結果の正規化**。

---

## 7. Port Contract

本章は v1.5 の **中心規範**である。

### 7.1 Node interface is defined only by ports

* node の **外部 interface** は **port のみ**で定義される。
* **internal state**、**internal subgraph**、**implementation detail** は interface に含めない。
* caller と下流 node は **公開 port contract** にのみ依存する。

### 7.2 Port payload must be dict

* **input / output** の port payload は **いずれも dict**。
* scalar / list / string / number を **port payload そのもの**として用いてはならない（単一値も dict でラップする）。

観測可能な node 出力は **`output_port_name -> dict payload`** のマッピングとして表現される。

### 7.3 Reserved top-level runtime keys

node の出力トップレベルにおいて、**予約される runtime 名**は次の **2 つのみ**とする。

* **`_runtime`**
* **`_usage`**

それ以外の **特別なトップレベルキー**は **public contract に含めない**（caller は依存してはならない）。

**理由（要約）:** domain port を安定させ、下流が依存する **予約 surface を最小**に保ち、**ad-hoc なトップレベル名前空間**が増殖するのを防ぐ。

### 7.4 `_runtime`

* **`_runtime` は canonical** な実行層メタデータ用の予約領域である。
* **normative な木構造**として、**`_runtime["ports"][port_name][...]`** を用いる（port-centric）。
* **revision** 等、実行テンプレートが定義する **port 単位の runtime 属性**はこの **`ports` 木**に置く（例: `_runtime["ports"]["execution_result"]["revision"]`）。
* **`_runtime` は concrete node の任意拡張バケットではない**（provider 固有の任意データは domain payload の **`provider_meta`** 等へ、[§11](#11-common-result)）。
* **`run()` は通常 `_runtime` を返さない**（domain のみ返す、[§7.6](#76-run-vs-execute)）。観測時は実行テンプレートが付与する。

### 7.5 `_usage`

* **`_usage`** は **NodeFlow runtime が解釈する予約 channel**（usage / accounting / execution-consumption。例: トークン数）であり、**semantic output ではない**。
* **domain contract に含めない**。**domain output port でもない**。**provider や任意メタデータのダンプに使ってはならない**。
* concrete node は **この protocol に従って返すだけ**とし、**「特別な意味付きの戻り」ではない**と心得る。
* `run()` の戻りに **`_usage` キーを含めてよい**のは **template が消費するため**に限る。テンプレートは **`_apply_usage` 等で `result` から除去**し limit state を更新する。**caller が観測する `_usage`（ある場合）もテンプレートが整えたもの**であり、domain port の延長ではない。

### 7.6 `run()` vs `execute()`

* **`run()`** — **domain port** の **`port_name -> dict payload`**。node author の **domain contract** はここに限る。
* **`_runtime`** — `run()` の契約に含めない。execution template が付与する（[§7.4](#74-_runtime)）。
* **`_usage`** — domain ではない（[§7.5](#75-_usage)）。
* **`execute()`** — `run()` の結果に対し、テンプレートが **`_runtime` / `_usage`** を含む **観測可能な出力**を組み立てる。
* portable に依存してよいのは **domain port** と **`_runtime` / `_usage` の予約意味**に限る。

---

## 8. Role and Implementation Model

### 8.1 分離

* **role** — **semantic contract**（何をするか、期待する入出力）。**意味のある処理の正本**はここおよび [§9](#9-built-in-role-contracts) に置く。
* **implementation kind** — **実装手段**（Python / CLI / API）。

**role は継承木に載せない。** 継承の骨格は **implementation kind** 側（`PythonActionNode` 等）に置く。

### 8.2 role が定義する最低限

少なくとも次を定義する。

* **expected input ports**（意味のあるもの）
* **produced output ports**
* **output payload shape**（built-in は [§9](#9-built-in-role-contracts)、exec は [§11](#11-common-result)）

### 8.3 禁止事項

* **role-based taxonomy**（例: `RoutingNode` を top-level に置く）を作らない。
* **role ごとの abstract base class** を設けない（framework 固定の implementation kind 基底のみ）。
* **role と implementation kind を同一継承木で同一視**してはならない。

### 8.4 role の表現

* v1.5 では **role の source of truth は class attribute**（例: `role = "exec"`）とする。
* 正式に用いる built-in role 値の例: **`route_by_task_type`**, **`summarize_result`**, **`exec`**（将来、重複のない範囲で追加してよい）。

---

## 9. Built-in Role Contracts

### 9.1 Spec boundary と semantics の所在

* 本仕様が定義するのは **built-in role contract** に限る。**NodeFlow が support する処理意味論**は、**この章の role** と **[§11](#11-common-result)** で表現する。
* **`PipeNode` はこれらの意味を実装しない**。必要なら **child として `ActionNode`** を置く（[§10](#10-pipenode-contract)）。
* **concrete reusable pipe** の内部配線や局所フィールド一覧は **過剰に固定しない**（[§7](#7-port-contract)、[§10](#10-pipenode-contract) を満たせばよい）。

### 9.2 `route_by_task_type`

* **役割:** task メタデータに基づき **次の executor または接続先方針**を決める（外部実行を必須としない）。
* **入力:** **task routing に必要な情報**を受け取る。必須キー名の集合は concrete に委ねる。
* **produced output port:** **`route`**
* **minimum 意味（`route` payload）:** 次を表す情報を含まなければならない（**キー名は spec で固定しない**）。

  * **next executor / route target** の識別
  * **rationale**（判断理由）
  * **follow-up hint**（次段接続や継続の示唆）

* **原則:** built-in では **決定的なロジック**を想定し、暗黙の LLM routing を要求しない。

### 9.3 `summarize_result`

* **入力:** 直前結果の port — 多くは **`execution_result`**（[§11](#11-common-result)）。
* **produced output port:** **`summary`**
* **minimum 意味:** **raw を捨てず**、要約・要点・後段向けヒントなど **追加の視点**を載せる（フィールド名は concrete に委ねる）。**summary と raw の関係**は [§11](#11-common-result) に従う。

### 9.4 `exec`

* **role `exec`** の concrete は **domain port `execution_result`** を返す（[§11](#11-common-result)）。
* **exact input set** は executor ごとに異なってよい（CLI 引数、API ボディ等）。
* **output の意味論**は **Common Result** に従い、implementation kind はその意味を変えない。

### 9.5 Development flow built-ins（P0/P2 profile）

本仕様の taxonomy / port contract に従う concrete として、開発ループ向け built-in nodes（`workflows.development_flow.spec_plan` / `workflows.development_flow.implement` / `workflows.development_flow.review` / `workflows.development_flow`）を追加してよい。

* stage nodes（`workflows.development_flow.spec_plan` / `workflows.development_flow.implement` / `workflows.development_flow.review`）は、単発実行の `stage_result` を返し、checkpoint artifact を生成する。
* top-level `workflows.development_flow` は、`start` / `approve` / `rework` / `revise_spec` / `merge` の action で checkpoint/resume を外部運用する。
* `flow_result.ok` は stage 成功を示す値であり、merge 許可そのものは `flow_result.merge_ready` および `allowed_actions` / `next_action` で判定する。
* これらは concrete の contract であり、`PipeNode` 本体の責務（配線・公開）を広げるものではない（[§10](#10-pipenode-contract)）。

---

## 10. PipeNode Contract

### 10.1 定義と責務

* **`PipeNode`** は **child graph を接続**し、**外部から 1 node として**公開する。
* **`PipeNode` は domain semantics を持たない**（routing / summarize / exec の意味は **child `ActionNode`**、[§9](#9-built-in-role-contracts)）。
* **semantics が必要なら child に置く** — pipe を **配線以外に複雑にしない**。

* **`BaseNode` の subclass** で **`BaseNode.execute()` テンプレート**に従う。 **`PipeNode.run()`** が subgraph の実行（子の `execute` 順序・接続・中間 dict の受け渡し）を **所有**する。

### 10.2 境界

* **child payload の意味（domain semantics）を、pipe 側の都合で再解釈したり書き換えたりしてはならない**（*Must not reinterpret or alter the semantic meaning of child payloads.*）。配線として **そのまま渡す／出口で公開する**ことはよい。
* **caller** は内部 graph に依存せず **公開 port contract** のみに依存する（black box）。

### 10.3 `execute()` と single-run

* **`PipeNode.execute()`** は **基底テンプレート**に従う（[§13.1](#131-basenode)）。
* **composite** なので **single-run の単位は各 `exec` 系 child**（[§12](#12-single-run-principle)）。

---

## 11. Common Result

### 11.1 Port 名

* **`exec` role** の外部実行系 concrete（典型: `*ExecNode`）は、**標準 domain 出力 port 名**として **`execution_result`** を用いる。
* **`run()`** は少なくとも **`execution_result` キー**とその **dict payload** を返す（`_runtime` / `_usage` は [§7](#7-port-contract)）。

### 11.2 Payload 形状（domain）

`execution_result` の dict 直下に、少なくとも次の **意味**を持つフィールドを含める（型は実装で拡張可）。

| フィールド | 意味 |
|------------|------|
| `ok` | 意味のある成否（外部が応答した失敗は通常 `false`） |
| `executor` | 論理 executor 名 |
| `provider` | プロバイダ識別子 |
| `model` | 利用モデル（任意） |
| `task_type` | task 分類（任意） |
| `summary` | 短い要約（任意、raw の置換ではない） |
| `stdout` / `stderr` | CLI 時の回収（任意） |
| `raw_response` | 捨てない raw |
| `artifacts` | 成果物リスト等 |
| `provider_meta` | **provider 固有**の任意 dict（**`_runtime` と混同しない**） |
| `next_hint` | 後段向けヒント（任意） |

**revision** は **domain payload 直下ではなく** **`_runtime["ports"]["execution_result"]`** に置く（[§7.4](#74-_runtime)）。

### 11.3 観測例（`execute()` 後）

`execute()` 完了後に観測しうる出力の一例（`_usage` は省略可）。

```python
{
    "execution_result": {
        "ok": True,
        "executor": "…",
        "provider": "…",
        "model": None,
        "task_type": None,
        "summary": None,
        "stdout": None,
        "stderr": None,
        "raw_response": None,
        "artifacts": [],
        "provider_meta": {},
        "next_hint": None,
    },
    "_runtime": {
        "ports": {
            "execution_result": {
                "revision": "…",
            },
        },
    },
}
```

### 11.4 Rules

* **raw を捨てない。**
* **summary** は **追加情報**であり、raw の **置換ではない**。
* **provider 固有**は **`provider_meta`**。**`_runtime["ports"]` の revision 等とは別概念**。
* domain payload 直下に **`meta`** という名前を **`provider_meta` の代替として用いない**（統一は `provider_meta`）。

### 11.5 Failure semantics

* 外部呼び出しが **意味のある失敗応答**を返した場合は、通常 **`ok = false`** とする。
* **precondition 不足**や **実行層の失敗**（接続不能等）は **node の fatal / limit** として扱ってよい（[§13](#13-base-class-responsibilities)）。

---

## 12. Single-Run Principle

**external execution を行う `ActionNode`（典型: `*ExecNode`）** について、原則として **1 task = 1 external run** とする。

* **CLI** なら **1 プロセス起動**。
* **API** なら **1 リクエスト**。
* **session 継続**を前提にしない。
* `/new`, `/exit` 等の **対話操作への依存**を要求しない。

**retry:** single-run は **対話セッション継続や暗黙の再利用**を禁じるものであり、**同一 `execute` 呼び出し内の有限 retry**（瞬断等）を **禁止しない**。retry 中に外部から観測される node の **status は `executing`** とする（`ApiActionNode` の実装責務）。

**`PipeNode`:** composite 全体を「1 external run」とは限らない（子ごとに [上記](#12-single-run-principle) を数える）。

---

## 13. Base Class Responsibilities

本章は **contract レベルの責務**に限る。細かい **helper の具体名**や **child 出力の選別ロジック**は仕様に固定しない。

### 13.1 `BaseNode`

* **共通実行テンプレート** — 参照実装 `BaseNode.execute` と整合させる。正常系の概略は次のとおりである。

  1. **pre-limit** — 条件を満たさない場合は **`run()` を呼ばず `{}` を返す**（revision / `_usage` 消費も行わない）。
  2. **status = executing**、`ExecutionContext` 生成、**`run()`**。
  3. **`_apply_usage(result)`** — `result` から **`_usage` を除去**し、token 等を **limit state** に反映する。
  4. **revision / runtime 付与**（参照実装では `_attach_revision` 等。仕様上の `_runtime` 木と整合させる）。
  5. **post-limit** — 条件を満たせば **status = limit**、そうでなければ **done**（いずれも **この時点で返す dict** は上記までに組み立てたもの）。
  6. **dict 返却**。

* **pre-limit** と **post-limit** を混同しない — **max_calls 超過など実行前の判定**は **pre-limit**（`{}` で早期 return）、**max_tokens 等の実行後判定**は **post-limit**。
* **status / limit / error** の観測モデル（`read_status`, `read_error`, `reset_status` 等）。
* **`run()`** はサブクラスが実装し、**domain port の dict** を返す（`_usage` の扱いは [§7.5](#75-_usage)）。
* **制御例外** — `NodeExecutionLimit`, `NodeExecutionFailure` 等（実装は core に従う）。

### 13.2 `PipeNode`

* [§10](#10-pipenode-contract) のとおり **配線・公開・subgraph 実行の所有**のみ。**semantics は持たない**。

### 13.3 `ActionNode` 系（`PythonActionNode` / `CliActionNode` / `ApiActionNode`）

* **処理・外部実行・変換・正規化**を担う側である（**semantics ownership** は [§8](#8-role-and-implementation-model)、[§9](#9-built-in-role-contracts)）。
* **`PythonActionNode`** — ローカル Python による入力解決と出力 dict の組み立て。
* **`CliActionNode`** — subprocess、timeout、exit code、stdout/stderr 回収。
* **`ApiActionNode`** — HTTP request、auth、endpoint、retry（[§12](#12-single-run-principle)）、Common Result への正規化。

---

## 14. Naming and Inheritance Rules

### 14.1 フレームワーク固定名

`BaseNode`, `PipeNode`, `ActionNode`, `PythonActionNode`, `CliActionNode`, `ApiActionNode` は **本仕様の固定語彙**とする。

### 14.2 concrete 命名

* **concrete `ActionNode`:** **`<Scope><Role>Node`**（例: `PythonRouteByTaskTypeNode`, `KimiExecNode`）。
* **concrete `PipeNode`:** **`<Purpose>PipeNode`**（例: アプリ固有の合成；**配線専用**であることが名前から読めてもよい）。
* **外部実行系:** クラス名は **`*ExecNode`** に揃え、role は **`exec`**（[§8](#8-role-and-implementation-model)）。
* **基底クラス名と concrete の同名**は禁止。
* **ファイル名**は **snake_case**（例: `python_route_by_task_type.py`, `kimi_exec.py`）。

### 14.3 禁止・推奨

* **role ベースの abstract class** は設けない。
* **巨大な cross-provider 抽象**を基底に押し込まない。
* 重複は **非 node の private モジュール**に切り出してよいが、**node contract を隠すために使ってはならない**。
* helper の名前や構成は仕様で固定しない。

---

## 15. Project Layout Guidance

本節は **normative な port 契約ではなく**、リポジトリ整理の **推奨**である。

### 15.1 方針

* **`core/`** — **`BaseNode`**、**taxonomy**（`PipeNode` / `ActionNode` と implementation kind）、**`Runner`**、**registry**。
* **`nodes/`** — **再利用可能な building-block concrete のみ**。第一分類軸は **role または purpose**（`routing/`、`summarize/`、`exec/` 等）。
* **`workflows/`** — **ユーザー向けの複合 workflow**（例: `development_flow`、固定プロバイダの合成 Pipe）。`nodes/` に混在させない。
* **`Action/` と `Pipe/` をディレクトリ分類の第一軸にしない**。
* **特定の concrete クラス名**を **標準ディレクトリ構成の前提**にしない（説明用・実験用の合成 node に依存したレイアウトを強制しない）。
* **`PipeNode` は special concrete を仕様上要求しない**。

### 15.2 例（推奨のイメージ）

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
    routing/
    summarize/
    exec/

  workflows/
    development_flow/
    review_with_claude/
    implement_with_codex/
```

**`core/loader.py` / `core/run.py` / `core/config.py`** — **`core/runner.py`**（graph の step 実行）とは別に、**pipeline 読み込み・組み立て・ルート `execute` のキック**と YAML ファイル IO を置く。`loader` / `run` / `config` の役割分担はリポジトリ任せ。

### 15.3 ルール

* **taxonomy（抽象の node kind）** は **`core/`**（上記 `node_kinds/` 等）。**building-block concrete** は **`nodes/`**、**パッケージされた複合 workflow** は **`workflows/`**。
* **`core/`** に **アプリ concrete** を置かない。
* **built-in registration** は **一箇所に集約**する。
* **直列 subgraph** は **`PipeNode` の能力**として表現し、**特別な concrete 名を仕様が要求しない**。

---

## 16. Success Criteria

v1.5 として次が成立していればよい。

* **node model** が先に説明できる（[§5](#5-basenode-and-the-node-model)）。
* **taxonomy** が明確（`PipeNode` / `ActionNode` と implementation kind）。
* **`PipeNode` は配線・公開のみ**で、**domain-level の処理は child `ActionNode`** に置ける（[§10](#10-pipenode-contract)）。
* **意味処理の正本**が **built-in role contract**（[§9](#9-built-in-role-contracts)）として読める。
* **role** と **implementation kind** が独立して説明・実装できる。
* **port contract** が明確（dict payload、`_runtime` / `_usage`、**`run()` vs `execute()`**）。
* **`_runtime` / `_usage`** の境界が明確。
* **`execution_result` Common Result** が確立している。
* **`PipeNode`** を **通常 node と同様**に graph 上で扱える。
* **Runner** が dumb のまま（[§3.1](#31-runner-prohibitions)）。
* **single-run** の外部実行が守られている（[§12](#12-single-run-principle)）。
* **taxonomy は `core/`、concrete は `nodes/`** の分離が説明できる（[§15](#15-project-layout-guidance)）。
* **concrete node** を追加しやすい命名・配置になっている（[§14](#14-naming-and-inheritance-rules)、[§15](#15-project-layout-guidance)）。

**実装の目安（必須ではない）:** まず **`BaseNode` と execution template**、次に **taxonomy（`PipeNode` / `ActionNode` と implementation kind）**、続けて **concrete `ActionNode`**、最後に必要に応じて **concrete `PipeNode`** とすると、contract の下から積み上げやすい。
