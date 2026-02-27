# NodeFlow v1.30 仕様

---

# Part I — Core Model（Strict Version）

本章は NodeFlow の抽象計算モデルを定義する。
構文と意味論を分離し、Execution 概念は含めない。

---

# 0. Scope

## 0.1 Core Principles

Nodes do not have access to graph topology. They only receive resolved input ports. Connection information is exclusively managed by StructuralNode.

---

Core Model は以下のみを定義する：

* 構文（Node, Graph）
* 意味論（Graph の評価）
* Loop 演算子

以下は定義しない：

* 実行順序
* スケジューリング
* 並列性
* 制限（limit）
* 停止制御
* pause / status
* revision
* 実装クラス
* Runner

Core Model はさらに以下を定義しない：

* invalidation（無効化・部分再評価の状態リセット）
* 部分再評価（partial re-evaluation）
* execution cursor（実行カーソル）
* pause / resume（停止・再開制御）

Core は純粋な計算モデルである。

---

# 1. Node

## 1.1 Syntax

```
Node = (S, f)
```

* S : 状態空間
* f : 遷移関数

入力空間を X、出力空間を Y とすると：

```
f : (X × S) → (Y × S)
```

Node は状態付き遷移器である。

---

## 1.2 Properties

* Node は black-box
* 他 Node の存在を知らない
* Graph 構造を知らない
* 自身の state と inputs のみ参照可能

Core は Node の内部構造を規定しない。

---

## 1.3 Port structure（Core 契約）

**Each output port MUST be a dictionary.**

Core は port の具象構造を規定しない。具象的な port 構造（`_meta`、`revision` 等）は Execution 層で定義する（Part II §5 等）。

If a Node returns a non-dict port value, the behavior is undefined and treated as fatal.

---

# 2. Graph

## 2.1 Syntax

```
Graph = (Nodes, Edges)
```

* Nodes : Node の有限集合
* Edges : Node の出力ポートから入力ポートへの有向接続集合

Edges は次の形を持つ：

```
u.output_port → v.input_port
```

循環は許可される。

---

## 2.2 Boundary

Graph は外部との入出力境界を持つ。

* Graph 入力空間を X_G
* Graph 出力空間を Y_G

境界は Graph 構造によって定まる。

---

# 3. Graph Semantics

Graph は構造であり、遷移器ではない。
Graph を遷移器として解釈するために評価意味論を定義する。

---

## 3.1 Evaluation Function

```
⟦ · ⟧ : Graph → Node
```

任意の Graph G に対し：

```
⟦G⟧ = (S_G, f_G)
```

が定義される。

---

## 3.2 Induced State Space

```
S_G = ∏_{n ∈ Nodes} S_n
```

各 Node の状態の直積である。

---

## 3.3 Induced Transition

```
f_G : (X_G × S_G) → (Y_G × S_G)
```

f_G は Graph 構造 (Nodes, Edges) と整合的な遷移関数として定義される。

評価順序や適用戦略は規定しない。**Core は戦略独立な意味論を前提とする。** ただしこの前提は Node の遷移関数が決定的であることに依存する。すなわち Graph の意味論 ⟦G⟧ は評価戦略に依存しない。戦略独立とは、Graph 構造と Node の遷移関数が決定的である限り、評価順序の違いが f_G の結果に影響しないことを意味する。戦略独立性は、各 Node の遷移関数が決定的であり、Graph 構造が評価順序に依存しない設計であることを前提とする。

**Scope of strategy independence**  
The strategy independence property stated above holds for well-formed acyclic Graphs. For Graphs containing cycles, the semantics ⟦G⟧ may depend on the evaluation strategy (i.e., the fixed point reached may vary). In such cases, the semantics is defined only partially, and the Execution Layer is responsible for ensuring deterministic behavior through scheduling policy (§7.1.1) and initial value injection (§8).

ただし意味論 ⟦G⟧ は：

* Graph 構造のみに依存する
* 外部状態や時間に依存しない
* 同一の Graph に対して同一に定まる

ものとする。

---

## 3.4 Determinism

Graph G が well-formed であるとは、構造と Node の遷移関数が決定的であり、かつ G が **非循環** である（または循環に対して初期値が明示的に与えられている）とき、意味論 ⟦G⟧ = (S_G, f_G) が一意に定まることをいう。循環グラフの意味論は部分的にのみ定義される。Execution Layer における循環の扱いは §8 に従う。

Core Model は決定的意味論を前提とする。**Core の determinism は、Node の遷移関数 f が決定的である場合に限り成り立つ。**

---

# 4. Loop

Loop は Graph 上に定義される高階演算子である。

---

## 4.1 Definition

```
Loop = (G, P)
```

* G : Graph
* P : Y_G → Bool

ただし ⟦G⟧ = (S_G, f_G) とする。

---

## 4.2 Well-formedness Condition

Loop が定義可能であるためには：

```
X_G = Y_G
```

でなければならない。

すなわち：

```
f_G : (X_G × S_G) → (X_G × S_G)
```

である。

---

## 4.3 Semantics

初期入力 (x₀, s₀) ∈ X_G × S_G に対し：

```
(xₙ₊₁, sₙ₊₁) = f_G(xₙ, sₙ)
```

停止条件：

```
P(xₙ₊₁) = true
```

Loop は収束を保証しない。

---

# 5. Core Invariants

* Node は状態付き遷移器である
* Graph は Node の接続構造である
* 任意の well-formed Graph G に対し、意味論 ⟦G⟧ は一意に定まる
* Loop は Graph の意味論上で定義される

Core は：

* 実行順序
* 時間
* 制限
* 実装構造

を規定しない。

---

# 6. Structural Closure

Graph の意味論 ⟦G⟧ は Node である。

したがって、Graph はその意味論を通じて Node として扱うことができる。

この意味で、Node と Graph の構造は再帰的に閉じている。

Core はこの構造的閉包性を前提とする。

---

# 7. What Is Not Defined

Core は以下を定義しない：

* 実行順序
* 時間
* 並列性
* 実行過程の決定性（評価順序や時間依存性）
* 停止保証
* 制限
* エラー処理
* 実装モデル

意味論 ⟦G⟧ の一意性のみを前提とする。

---

# 8. Core と Implementation

Core Model は抽象計算モデルである。

実装層は：

* Node を具体的に実装する
* Graph の意味論 ⟦·⟧ を具体的実行機構として実現する

Core は実装形式を制限しない。

---

# Part II — NodeFlow Execution Layer v1.2

本章は Core Model の具体実装仕様である。BaseNode、execute/run、status 種類、pause、limit、revision、usage、Runner、YAML、resume 等はすべて Core の上に積んだ制御層として定義する。

---

## 0. Execution Scope（形式定義）

**Execution Scope** とは、**トップレベル StructuralNode の execute の 1 回の呼び出しのライフタイム**（通常は PipelineNode；LoopNode をトップレベルとして kick することも可能である；その呼び出しに含まれるすべてのネストした StructuralNode の実行を含む）を指す。

```
Execution Scope :=
    the lifetime of a single top-level StructuralNode.execute call
    (typically PipelineNode), including all nested StructuralNodes.

Within one Execution Scope:
- Node instances are reused
- Context is preserved
- revision comparison is valid

Outside this scope:
- revision comparison is undefined
- Node state is discarded
```

分散実行設計では、Execution Scope を超えた revision の比較を想定しないこと。

**空 dict `{}` の扱い（保存しない）**

If `execute()` returns an empty dictionary (`{}`), the Runner MUST NOT update latest_output for that node.

StructuralNode MUST return `{}` if the final node returns `{}`. No implicit substitution or fallback is allowed.

---

# 1. 設計原則

## 1.1 基本思想

本仕様は **2 層構造**を採用する：(1) **Core Model**（Part I）(2) **Execution Layer v1.2**（Part II）。以下は Execution Layer の設計原則である。

**Execution Layer では、実際の Node 実装が決定的である保証はない。**（LLMNode 等は非決定的でもよい。Core の determinism は抽象モデルの前提であり、実装層の契約ではない。）

* Everything is a Node
* PipelineNode は Graph を 1-shot 実行する Node。LoopNode は Graph を反復実行する Node。
* **Loop も Node**（LoopNode として表現）
* Runner is dumb
* グローバル Context は廃止
* データは input / output のみで流れる
* 循環は許可
* 終了は final ノード（graph で明示指定）による
* 例外は Node が吸収する
* Retry / 分岐 / Loop も通常 Node で表現する
* **revision は I/O 契約**（Node 間の契約であり、Runner は解釈しない）
* **状態は Node 内部に持つ**
* **graceful stop を基本とする**
* **kill / interrupt は持たない**

---

## 1.2 全体像

本仕様の構成は次の順序とする。

* **Part I — Core Model**：構文・意味論（Node / Graph / ⟦·⟧ / Loop）。実行戦略・Scheduler は定義しない。
* **Part II — NodeFlow Execution Layer v1.2**：設計原則、Node（Execution）、入出力、Param、Revision、BaseNode、Runner、循環、例外、スコープ、定義ファイル（§1〜§11）
* **Part III — Concrete Nodes**：各種 DataNode・StructuralNode（§12）
* **Part IV — Invariants**：不変条件（§19）

まず Part I で Core を把握し、Part II で Execution Layer の詳細に進む。**不変条件は §19 にまとめる。先に §19 を一読することを推奨する。**

---

## 1.3 Node 分類とクラス階層

Node は **DataNode** と **StructuralNode** に分類する。曖昧さの排除・実装ブレ防止・将来拡張のため、クラス階層を明示する。

```text
Node (abstract)
 ├── DataNode (abstract)
 │       ├── LLMNode
 │       ├── ScriptNode
 │       └── ...
 │
 └── StructuralNode (abstract)
         ├── PipelineNode
         └── LoopNode
```

| 分類 | 役割 | 詳細 |
|------|------|------|
| **DataNode** | run() 実装、usage 更新、limit pre/post、status 設定（revision は BaseNode が content-hash で付与） | §2, §6, §12 の「BaseNode を継承する単体 Node」に相当。LLMNode、ScriptNode など |
| **StructuralNode** | children 管理、usage 集約、status 集約、termination 判定 | PipelineNode、LoopNode。子グラフを持ち、子の状態を集約する |

共通の execute モデル・status・入出力は §2〜§4 に、DataNode 固有は §6 と §12.1 各種 Leaf Node に、StructuralNode 固有は §12.2 各種 StructuralNode（§12.2.1 PipelineNode、§12.2.2 LoopNode）に記述する。

---

# 2. Node（ノードそのもの）

## 2.1 実行インターフェース

すべての Node は以下を公開する：

```python
execute(inputs: dict, params: dict) -> dict
```

* inputs: 他ノードから渡された JSON データ
* params: 静的実行設定（immutable）
* return: 必ず dict（JSON）

**execute の戻り値契約は本節に集約する。** 他節では「§2.1 参照」とし、重複説明を避ける。

**execute は常に dict を返さなければならない。** None や非 dict 型は仕様違反とする。この制約によりエンジンの安定性が上がる。成功時は output port を持つ dict、停止系では `{}` 可（詳細は §2.2, §6.4）。

**重要**：成功時は output port を持つ dict を返す。停止系（limit pre / PauseSignal / fatal、および limit post で run が成功していない場合）では、execute は空 dict `{}` を返してよい（§2.2.1, §2.2.2）。limit post で run が成功している場合は、その出力を返す（§2.2.2）。

---

## 2.2 execute / run 分離

### 2.2.1 成功時

run が正常終了した場合：

* dict を返す（各 output port を持つ）
* 各 output port に `_meta.revision` を付与（BaseNode が自動補完）
* status = done

### 2.2.2 停止系

以下のケースでは **execute は空 dict `{}` を返してよい**：

* limit（limit post 以外。limit post の扱いは次項）
* PauseSignal
* fatal
* limit post（**run が成功していない場合のみ** `{}`。run が成功している場合はその出力を返す）

**limit post の明確化**：limit post には (1) run 成功後の limit 超過 と (2) run 未実行での limit 超過 がある。run が成功している場合は、その出力を返す。run が成功していない場合のみ `{}` を返してよい。**StructuralNode における「run が成功した」の定義は §6.7 に記載する。**

```python
return {}
```

status はそれぞれ：

* limit
* pause
* fatal

に設定される。いずれの停止系でも execute の返り値は `{}` でよい。

**execute 共通仕様**：execute は **ready または done のときに通常経路で呼べる**（done の Node は再実行可能。pause は resume 経由のみ。§2.3.2）。共通処理の流れは「ready → executing → _execute_impl() 相当（run 呼び出し＋limit pre/post）→ 例外を status に変換 → dict を返す」。

**例外 → status 変換**（run 内の例外および BaseNode の limit 検出）：

| 原因 | status |
|------|--------|
| PauseSignal | pause |
| LimitSignal（run 内から raise）または limit pre/post 検出 | limit |
| その他 Exception | fatal |

execute は常に dict を返す。LimitSignal は BaseNode が提供する組み込み例外。run 内から raise することで limit を宣言できる（limit pre/post で検出する従来方式と併用可能）。詳細は §6.3。

---

BaseNode は execute を共通実装し、サブクラスは run のみ実装する（詳細は §6）。**execute の流れ・フロー図は本節（§2.2）に完全に集約する。** §6.3 は BaseNode 実装上の補足のみとする。

**実行の流れ**

1. 最初に **status = executing** にする。
2. **実行中**、次のいずれかで終了する：
   * **問題（例外）** → status を **fatal** にして終了（返り値は `{}` 可）
   * **limit pre でひっかかった** → status を **limit** にして終了（返り値は `{}` 可）
   * **limit post でひっかかった** → status を **limit** にして終了（run 成功時はその dict を返す、run 未成功時は `{}` 可）
   * **run が PauseSignal を raise した** → status を **pause** にして終了（返り値は `{}` 可）
   * **run が LimitSignal を raise した** → status を **limit** にして終了（返り値は `{}` 可）
   * **最後まで問題なく動いた** → status を **done** にして終了（dict を返す）
3. status は **read_status()** で読むだけ（制御ではない）。execute の返り値には含めない。

**PauseSignal**：BaseNode が提供する組み込み例外。run 内から raise することで pause を宣言する。PauseSignal 以外の例外はすべて fatal として扱う。run の戻り値による pause 表現は採用しない（戻り値は常に dict）。

**pause の性質**

* Node は内部状態（LLM セッション履歴など）を保持したまま停止する。pause は「実行未完了」ではなく**中断状態で安定している**状態である。
* execute の返り値は `{}`
* Runner はそのノードを再スケジュールしない
* 再開は外部から PipelineNode の `resume()` を呼ぶことでのみ行う（§12.2.1.5）。resume_inputs_schema は外部に「何を渡せばよいか」を伝える参考情報であり、エンジンは解釈しない。

```python
# BaseNode が提供する組み込み例外
class PauseSignal(Exception):
    """
    run() 内から raise することで pause を宣言する。
    Node は内部状態を保持したまま停止する。
    resume_inputs_schema は外部に「何を渡せばよいか」を伝える参考情報。
    エンジンはこれを解釈しない。
    """
    def __init__(self, reason: str = "", resume_inputs_schema: dict = None):
        self.reason = reason
        self.resume_inputs_schema = resume_inputs_schema or {}

class LimitSignal(Exception):
    """run() 内から raise することで limit を宣言する。limit pre/post による検出と併用可能。"""
    def __init__(self, reason: str = ""):
        self.reason = reason
```

```
execute(inputs, params)
  ├ params = _freeze(params)   # shallow freeze（§4.4）
  ├ status = executing
  ├ limit pre          → 超過なら status = limit, return {} 可
  ├ run(inputs, params)
  │   ├ PauseSignal   → status = pause, return {} 可
  │   ├ LimitSignal   → status = limit, return {} 可（revision 補完・limit post をスキップ。§6.3 ②）
  │   ├ その他例外    → status = fatal, return {} 可
  │   └ dict 返却    → 正常継続
  ├ revision 補完
  ├ limit post         → 超過なら status = limit; run 成功時は revision 補完済み dict を返却, run 未成功時は return {} 可
  ├ status = done
  └ return dict
```

* limit pre: 実行前の limit チェック。超過時は run を呼ばず status = limit、return `{}` 可。
* limit post: 実行後の limit チェック。詳細は §6.3 ④ 参照。
* run: ノード固有処理。PauseSignal を raise すれば status = pause。LimitSignal を raise すれば status = limit（revision 補完・limit post はスキップ）。その他の例外は execute が吸収し status = fatal。
* revision 補完: 欠落時は BaseNode が自動付与。

Runner は execute のみ呼ぶ。

---

## 2.3 Node 状態モデル（確定）

### 2.3.1 Node が持つ状態

Node は内部に **status**（予約フィールドではない。内部状態）を持つ。状態集合は **ready, executing, done, pause, limit, fatal** の 6 値とする。**内部実装では Enum を使用することを推奨する。** 外部 API（read_status() の戻り値など）は string とし、"done" / "Done" / "completed" などの表記ゆれを防ぐ。

**状態遷移（FSM）**

**DataNode**（単体 Node）：

```
ready → executing
executing → done
executing → pause
executing → limit
executing → fatal
pause → executing（resume 経由のみ。§12.2.1.5）
```

**StructuralNode**：同様だが、done は子の状態集約で決定する（§6.7）。

```
ready → executing → done
                     ↘ fatal
                     ↘ pause → executing（resume 経由。§12.2.1.5）
                     ↘ limit
```

| status    | 意味                   |
| --------- | ---------------------- |
| ready     | 実行待ち               |
| executing | 実行中                 |
| done      | 実行完了（再実行可能） |
| pause     | 一時停止要求           |
| limit     | limit 到達             |
| fatal     | 異常終了               |

**status の制約**

* status は Node 自身のみが変更可能
* StructuralNode は子の status を直接変更してはならない（子は black-box）
* done / limit / fatal → ready の遷移は禁止（reset しない）
* pause → executing は resume 経由のみ

### 2.3.2 重要な性質

* **done は終端状態ではない** — execute は **ready・done・pause（resume 経由のみ。§12.2.1.5）** のときに呼べる。**通常スケジューリングでは ready / done のみが execute される。pause は resume 経由のみ。**
* execute 呼び出し時に status は executing になる
* **reset は不要**
* **execution_id は不要**
* Node は再実行可能であり、done は再実行可能な状態である

**Re-execution Semantics**：`execute(node, inputs, params)` は、inputs が同一でも**内部状態に応じて異なる出力**を返し得る。Core Model は referential transparency を保証しない。

### 2.3.3 read_status()

* status は出力ではない — execute の戻り値には含まれず、内部状態である
* read_status() で読むだけ（制御ではない。単に status を取得する API）
* 状態はイベントではなく「状態」として保持される

Runner は read_status() で status を読む。制御は StructuralNode（§6.7）が行う。

---

# 3. 入出力モデル

## 3.1 inputs

Graph 定義（graph / node_pipeline.yaml）でバインディングされる。

```yaml
inputs:
  key: ${node_id.port}
```

参照形式：

* `${node_id.port}` — 他ノードの output port を参照
* `${inputs.port}` — 親 StructuralNode（または外部から渡された）inputs の port を参照（§10.2）
* `${params.<param_name>}` — 親 StructuralNode（PipelineNode / LoopNode）の params を参照（§4, §12.2.1.2）

制約：

* port 省略禁止
* **未定義参照は例外を投げない。当該ノードは「実行不能」として扱う。fatal とはしない。**（Runner は例外処理をしないため。max_idle_sec や limit に委ねる。§7.5）
* Node は inputs / params 以外を参照してはならない（詳細は §10 スコープルール）

---

## 3.2 outputs

Node は必ず dict を返す（§2.1）。

各 key が output port である。

各 port の値は `_meta.revision` を内部に持つ。

例：

```json
{
  "artifact": {
    "_meta": { "revision": 1 },
    "data": {...}
  }
}
```

仕様：

* すべての output port は `_meta.revision` を持つ（BaseNode が content-hash で自動付与。§5.7）
* `_meta` は予約キー
* `_meta.revision` は content-hash（SHA-256 の hex 文字列、64 文字。§5.10）
* revision は同一内容→同一値（順序・単調増加は表さない。§5.1.1）

---

## 3.3 出力保持ルール

Runner は：

* 各 node の最新出力のみ保持
* 履歴は保持しない

**Runner の出力保存ルール（確定）**

Runner は node.execute() の返り値を受け取るが：

### 🔴 空 dict は保存しない

```python
output = node.execute(...)
if output != {}:
    latest_output[node_id] = output
```

**意味**

* `{}` は「出力なし」を意味する
* 既存の latest_output を上書きしない
* 出力は消えない

**仕様としての一文**

> BaseNode.execute が空 dict `{}` を返した場合、Runner は当該ノードの最新出力を更新しない。

**limit post で出力が返ってきた場合**：Runner はその出力を通常どおり保存する。limit pre / limit post の扱いの詳細は §6.3 ④ 参照。

履歴が必要な場合：

* Node 自身が出力する
* バッファ専用 Node を用いる

---

## 3.4 出力更新の意味と「出力がない」状態

**出力が更新されるのは**：run が成功し、新しい output port が生成された場合のみ。停止状態（pause / limit / fatal）は出力の更新を伴わない制御状態である。

**`{}` の意味**：新しい出力が生成されなかったことを表す。データ削除ではない。no-op と同義。**空 dict の保存ルールは §3.3 参照。**

---

## 3.5 status と Flow 制御

status の定義・kind の意味・状態遷移は **§2.3 Node 状態モデル** にまとめる。status は予約フィールドではなく、Node の内部状態である。read_status() で読む。execute の返り値ではない。

* **Runner は status の意味（制御）は解釈しない。** ただし実行可能判定のため、状態値は参照する。制御は StructuralNode が read_status() で status を読み行う（§6.7, §7.3）。

設計上の効果：出力（output port）と分離し、revision モデルを汚さず、error と control を統一的に扱える。

---

## 3.6 制御とデータの完全分離（この設計の性質）

✅ last_output を Node が持たない  
✅ 出力消失が起きない  
✅ revision 契約を破らない  
✅ Loop の condition と衝突しない  
✅ Runner は dumb のまま  
✅ 状態とデータが完全分離  

| 種類     | 表現                   |
| -------- | ---------------------- |
| データ   | execute の戻り dict    |
| 制御     | read_status()          |
| 停止     | status                 |
| 出力更新 | output != {} のときのみ |

これにより v1.2 は **一貫した I/O モデル** となる。

---

## 3.7 usage 仕様

**usage の定義・所有者・読み書き主体・累積ルールは §3.9.2 にまとめる。** 本節は API と集約ルールのみとする。

**型（参考）**：

```python
class Usage:
    add(metric, value)
    merge(other)
    snapshot()
```

**更新粒度**：run 単位、またはそれ以下の粒度。消費時点で加算。execute 単位ではない。

**累積**：usage は累積値。reset しない。iteration 単位の値は snapshot 差分で取得する。

**merge の意味論**：`Usage.merge(other)` は以下の規則で集約する。同一 metric が両方に存在する場合は加算する。片方にしか存在しない metric はそのまま引き継ぐ。metric 名は文字列で識別する。

**集約**：StructuralNode は `child.read_usage()` を集約する。Runner は usage を触らない（§7.2）。**state / usage / Context の厳密な定義は §3.9 実行状態モデルにまとめる。**

---

## 3.8 node_calls の定義

**node_calls** とは、`BaseNode.execute()` が呼び出された回数を指す。

定義：

```
node_calls := number of times BaseNode.execute() is invoked,
              across all Nodes within the Execution Scope.
```

**カウントの規則**

* **インクリメントタイミング**：node_calls は execute() の**入場時**（limit pre 評価の前）に 1 加算する。このため limit pre で即 `{}` を返した場合も 1 call としてカウントされ、max_total_node_calls による保護が正確に機能する。
* DataNode の execute() 呼び出し 1 回 = 1 call
* StructuralNode の execute() 呼び出し 1 回 = 1 call（内部の子 Node の call とは別にカウント）
* resume() 経由で呼ばれた execute() もカウント対象
* limit pre で即 `{}` を返した execute() もカウント対象（execute が呼ばれた事実をカウントする）

**集約**

StructuralNode は配下の全 Node（自身を含む）の node_calls を集約する。集約は `child.read_node_calls()` により行う。Runner は node_calls を管理しない。

**`max_total_node_calls`**（§12.2.1.9）は、当該 Execution Scope 内の集約 node_calls がこの値に達した時点で status = limit とする。

---

## 3.9 実行状態モデル（State / Usage / Context）

NodeFlow v1.2 では、実行中の可変情報を **state / usage / Context** の 3 種類に分類する。それぞれの責務・所有者・可視範囲を厳密に分離する。

> **要約**：state は因果情報、usage は観測情報、Context は実行環境情報である。

---

### 3.9.1 state（Node 内部状態）

**定義**

state とは、**Node の実行ロジックに影響する内部可変状態**を指す。

例：LLM セッション履歴、前回処理した revision、iteration カウンタ、human review 待機フラグ、キャッシュ。

state には、実行ロジックに影響する可変状態のほか、**診断目的の実行結果情報（fatal 原因例外等）**を含めてよい。診断情報は実行ロジックを変更しないが、Node インスタンスのライフタイムと同一のスコープで保持される点で state と同一の性質を持つ。外部からの読み取りは `read_status()` および `read_error()` という読み取り専用 API に限る（§9.1）。

| 観点 | 内容 |
|------|------|
| **所有者** | Node インスタンス |
| **書き込み主体** | Node 自身のみ（run() 内） |
| **読み取り主体** | Node 自身。外部は `read_status()` および `read_error()` で参照可能（status および fatal 原因は state の一部） |
| **ライフタイム** | Node インスタンスと同一。Loop iteration 間で保持される。当該実行終了時に破棄される |

**禁止事項**

* 他 Node が state を参照してはならない
* StructuralNode は子 Node の state を直接変更してはならない
* state を Context に格納してはならない

---

### 3.9.2 usage（実行メトリクス）

**定義**

usage とは、**実行コストや消費量を表す観測メトリクス**を指す。

例：prompt_tokens、completion_tokens、api_calls、tool_calls。

**特徴**：usage はロジックの振る舞いを決定しない。usage は revision 制御に関与しない。**usage は state の一部ではない。state と usage は意味論的に異なる層である。** 例外として、usage を limit 判定に使う場合がある（§6.3 ④ limit post）。ただしこれは BaseNode が行うのであり、run() が usage を直接参照してロジックの分岐に使ってはならない。**run 内で usage を参照して LimitSignal を raise することは許可されるが、出力ロジックを usage に依存させてはならない。**

| 観点 | 内容 |
|------|------|
| **所有者** | Node |
| **書き込み主体** | Node 自身（消費時点で add） |
| **読み取り主体** | StructuralNode（集約のため）、外部監視機構（snapshot 経由）。**Runner は usage を参照しない。** |
| **累積ルール** | usage は累積値。reset しない。iteration 単位の値は snapshot 差分で取得する（§3.7） |

---

### 3.9.3 Context（実行スコープ管理情報）

**定義**

Context とは、**Execution Scope 内のデータフロー状態管理情報**を指す。グローバル Context は廃止（§1.1）。配置・役割は本節で定義する。

**含むもの**：latest_output、入力解決結果、binding 状態、graph 実行管理情報。

**含まないもの**：Node state、usage、revision 制御ロジック。

| 観点 | 内容 |
|------|------|
| **所有者** | StructuralNode（PipelineNode） |
| **書き込み主体** | Runner、PipelineNode |
| **読み取り主体** | Runner（inputs 解決）、StructuralNode（終了判定。§6.7）。**Node は Context を直接参照してはならない。** |
| **スコープ** | グローバルではない。Execution Scope 単位。独立 1-shot の場合は実行終了時に破棄される。LoopNode 内で PipelineNode が再実行される場合は iteration 間で保持される（§12.2.1.4）。|

---

### 3.9.4 3者の分離原則（不変条件）

以下を常に満たさなければならない。

1. **state は Node のみが変更可能。** 外部からの読み取りは `read_status()` および `read_error()` に限る（§3.9.1）。
2. **usage は Node が記録し、StructuralNode が集約する**
3. **Context は StructuralNode が管理し、Node は直接参照しない**
4. state を Context に移してはならない
5. usage を Context に移してはならない
6. Context を Node から参照してはならない

---

# 4. Param モデル

## 4.1 params の役割

params は：

> Node の静的実行設定

である。

* データフローとは独立
* 実行中に変化しない
* shallow immutable で十分

---

## 4.2 limit は params の一部

limit は params 内に含まれる。

```yaml
params:
  limit:
    max_calls: 10
    max_wall_time_sec: 30
```

仕様：

* limit キーは v1.2 では固定しない
* Node ごとに自由に定義してよい
* BaseNode は解釈できるキーのみ解釈する
* Runner は limit を解釈しない

---

## 4.3 Param伝搬

* Param 継承ルールは仕様で固定しない
* Param 伝搬は PipelineNode の責務
* 暗黙継承は禁止
* 必要な場合は PipelineNode が明示的にマッピングする

---

## 4.4 params は immutable

**params は実装者が変更してはならない（仕様上の制約）。**

BaseNode.execute は params を **shallow freeze** してから run に渡す。`_freeze` の実装は `types.MappingProxyType(params)` など処理系依存でよい。深いネストの freeze は v1.2 では行わない（§4.4 の「仕様上の制約で足りる」方針を維持）。run 内で `params["key"] = value` のような直接代入を試みた場合は TypeError が発生する。

---

# 5. Revision モデル

revision は **Core Model には存在しない概念**である。Execution Layer における**内容識別子（content-based identifier）**として定義する。

## 5.1 定義

revision は：

> 出力内容の **内容識別子（content-based identifier）**

である。

```text
revision = HASH( CanonicalJSON(output_without_meta) )
```

* **revision は port に属する** — `_meta.revision` に格納
* Node 間の I/O 契約
* Runner は解釈しない
* **BaseNode が自動生成する**（§5.7）。Node 実装者は revision を生成してはならない

### 5.1.1 性質

✔ revision は opaque identifier  
✔ revision は deterministic  
✔ 同一内容 → 同一 revision（ただし `_meta.hash_skip: true` が指定された場合はこの限りではない。§12.4.3）  
✔ 異なる内容 → 異なる revision（ハッシュ衝突が発生した場合の挙動は未定義。Execution Layer は SHA-256 の衝突耐性を前提とする）  
✔ revision は Node state ではない  
✔ revision は BaseNode が自動生成する  
✔ Node 実装者は revision を生成してはならない  
✔ **revision は Execution Scope 内でのみ意味を持つ識別子である**（別 Execution Scope・別 Runner・別環境では revision の比較は保証されない）  
✔ **revision はグローバル一意性を保証しない**（SHA-256 の衝突可能性は理論上存在する）  
✔ **revision は ID ではなく内容識別子（content-based identifier）である**  

revision は **順序や回数を表さない**。

**重要**：**revision はグローバル ID ではない。** revision は同一 Execution Scope 内で deterministic であることを前提とする。異なる Execution Scope 間での revision 一致は保証しない。Execution Scope 内でのみ意味を持ち、分散実行設計ではこのスコープを超えた revision の比較を想定しないこと。グローバル ID と誤解すると分散実行で事故の原因となる。

**補足**：revision は内容ベースのみである。Node の出力が内部状態に依存する場合でも、revision が反映するのは**生成された出力内容のみ**であり、因果履歴ではない。同一の出力 JSON を返す 2 回の実行は、内部状態が異なっていても**同一の revision を生成する**。

---

## 5.2 出力側責務

**Node は revision を生成しない。** Node は単に output port の内容を返す。

BaseNode が以下を行う（§5.7）：

1. `_meta` を除いた出力を canonical 化（§5.9）
2. ハッシュを計算（§5.10）
3. `_meta.revision` に格納

---

## 5.3 入力側責務

**入力側の revision 確認は任意とする。** Node が revision を参照する場合、許可されるのは等価比較のみである。

```python
if current_revision == last_revision:
    no_op()
```

**順序比較は禁止**（revision は content-hash であり順序を持たない）。

| 確認する | 確認しない |
|----------|-------------|
| 前回と revision が同じなら no-op 可能 | 常に実行される前提で設計する |
| 不要な再実行を減らせる | 実装がシンプルになる |

**Loop の終了は revision に依存しない。** Loop は condition のみで終了判定する（§12.2）。

**no-op を実装する Node は、前回処理した revision を内部状態として保持する責務を負う。** Runner はこの保持を管理しない。

---

## 5.4 動作例（revision の流れ）

implement → review の例：implement が出力 A を返す → BaseNode が content-hash を計算し `_meta.revision` に格納 → review が処理。implement の出力が同じ内容なら同一 revision、内容が変われば異なる revision となる。review が revision を確認する場合、同一 revision なら no-op 可能。revision は Node 間の I/O 契約であり、**Loop 制御とは無関係**（§12.2）。

---

## 5.5 Revision と循環

revision は再実行最適化のための I/O 契約（content-hash により同一出力は同一 revision）。循環グラフ内では、変化が伝播したときのみ再実行が発生し、不必要な再実行が減る。**Loop の終了条件には関与しない**（§12.2）。

---

## 5.6 revision の設計原則

✔ revision は port 単位  
✔ すべての Node（output port）が revision を持つ  
✔ revision は Node 出力の一部（BaseNode が付与）  
✔ Runner は解釈しない  
✔ revision は実行制御の補助情報（等価比較のみ）  
✔ revision は Node 間の契約  
✔ revision は内容から決定的に計算される（content-hash）  

---

## 5.7 revision 自動生成

**BaseNode.execute** は run() の戻り値に対し、各 output port ごとに以下を実行する：

1. 当該 port の値から `_meta` を除く
2. Canonical JSON に正規化する（§5.9）
3. SHA-256 でハッシュを計算する（§5.10）
4. `port_value["_meta"]["revision"]` に格納する

```text
canonical = CanonicalJSON(port_value_without_meta)
revision = SHA256(canonical)
port_value["_meta"]["revision"] = revision  # hex string, 64 chars
```

**注意**：status は output ではないので revision を付与しない。revision は output port のみ。`{}` を返した場合は revision は存在しない（§5.8）。

**hash_skip**：巨大出力等の例外的ケースでは `_meta.hash_skip: true` を指定し、revision を UUID 等で代替できる（§12.4.3）。

---

## 5.8 revision 契約と停止時の整合

* revision は output port のみに存在する
* execute が `{}` を返した場合、revision は存在しない（出力がないため）
* revision 契約は「出力があるときのみ」適用される
* Runner の revision・空 dict の扱いは §5.1, §3.3 参照

---

## 5.9 Canonical JSON（revision 計算用）

revision 計算に使用する **Canonical JSON** は以下を満たす。**Canonical JSON は JSON 互換型（object, array, string, number, boolean, null）のみを対象とする。**

**RFC 8785 準拠**：Canonical JSON は **RFC 8785 (JSON Canonicalization Scheme)** に準拠しなければならない（MUST）。実装は RFC 8785 準拠ライブラリを使用すること。分散環境での revision 一致を保証するため、独自実装する場合は同一出力が cross-language で同一ハッシュになることを保証しなければならない。

### 5.9.1 基本規則

1. UTF-8 エンコード
2. キーは辞書順（sort_keys=True）
3. separators=(',', ':')
4. ensure_ascii=False
5. 不要な空白を含まない
6. list の順序は保持する

**非 JSON 型の扱い（確定）**

Canonical JSON 化の対象に JSON 非互換型（`datetime`、`bytes`、カスタムオブジェクト等）が含まれていた場合、BaseNode は **TypeError を raise し、当該 Node の status を fatal とする**。

暗黙の型変換（`str()`、`repr()` 等）は **禁止**する。暗黙変換を許可すると、変換結果が処理系・バージョン依存になり、revision の決定性が破壊される。

Node 実装者は run() の戻り値が JSON 互換型のみで構成されることを保証する責任を負う。

### 5.9.2 浮動小数点の扱い

浮動小数点の正規化は **RFC 8785** に従う。**NaN / Infinity は禁止**（fatal）。その他の細則は RFC 8785 および §5.9.1 に委ねる。

### 5.9.3 null / boolean / int

標準 JSON 表現に従う。

### 5.9.4 _meta の扱い

revision 計算時、**すべての階層における `_meta` フィールドは除外する**（再帰的除外）。これは出力 JSON のいかなるネスト深度においても `_meta` キーを持つオブジェクトから `_meta` を取り除いた上でハッシュ計算を行うことを意味する。

形式的には：

```
strip_meta(value):
  if value is object:
    return { k: strip_meta(v) for k, v in value.items() if k != "_meta" }
  if value is array:
    return [ strip_meta(v) for v in value ]
  return value

canonical = CanonicalJSON(strip_meta(port_value))
revision  = SHA256(canonical)
```

**理由**：ネスト内の `_meta.revision` は別ノードの出力が埋め込まれた場合に発生し得る。それを revision 計算に含めると、内容が変わっていないのに参照先の revision 変更によって自身の revision が変わるという「revision の連鎖伝播」が発生し、決定性が損なわれる。_meta 内の他フィールド（将来拡張）も revision 計算には含めない。

---

## 5.10 ハッシュアルゴリズム

使用するハッシュ関数：**SHA-256**。

出力形式：**hex string（64 文字）**。

例：`"e3b0c44298fc1c149afbf4c8996fb924..."`

---

# 6. BaseNode

## 6.1 役割

BaseNode は：

> すべての Node が継承する基底クラス（Core Node の実装テンプレート）

である。

BaseNode は NodeFlow の実行契約を強制する。**外部 I/O を含む Node は timeout を実装しなければならない（MUST）。** これに違反すると executing 中のノードが永久待機となり、max_idle_sec 等の limit が機能しない（§7.4）。

**Runner は通常のスケジューリングで `node.execute()` のみを呼ぶ。** pause 再開時は StructuralNode が直接 execute を呼ぶ（§12.2.1.5）。

---

## 6.2 公開インターフェース

```python
execute(inputs: dict, params: dict) -> dict
```

すべての Node はこのシグネチャを持つ。

サブクラスは `run()` のみを実装する。

---

## 6.3 execute の責務

**execute の流れ・フロー図は §2.2 に完全に集約する。** 本節は BaseNode 実装上の補足（limit pre / run / revision 補完 / limit post / 例外吸収）のみとする。

---

#### ① limit pre

* `params.limit` を参照し、解釈可能な limit をチェック
* **超過時は run を呼ばず、status = limit にして return**

**形式仕様**：If limit is exceeded before run(), the node MUST return `{}`. No output is produced.

---

#### ② run 呼び出し

```python
def run(self, inputs, params) -> dict:
```

* ノード固有処理
* dict を返すこと
* **PauseSignal を raise した場合**、BaseNode は status = pause にして return する。**LimitSignal を raise した場合**、BaseNode は status = limit にして return する。上記以外の例外はすべて fatal として扱う。run の戻り値による pause/limit 表現は採用しない（戻り値は常に dict）。

**LimitSignal を raise した場合**、revision 補完および limit post をスキップして status = limit、`{}` を返す。（limit post で検出される limit は ④ 参照。）

---

#### ③ revision 自動補完

run の戻り値に対して、各 output port ごとに：

* `_meta` を除いた port 値を Canonical JSON 化（§5.9）
* SHA-256 でハッシュを計算し `_meta.revision` に格納（§5.7, §5.10）
* status（read_status() で読むもの）には付与しない

`_meta.hash_skip` が true の場合は UUID 等の一意値を用いる（§12.4.3）。これにより revision 契約を保証する（§5.7）。

---

#### ④ limit post

* 呼び出し回数などの更新。**超過したら status = limit にして return**
* **limit post が発生しても、run が成功している場合は revision 補完済み出力を返す。** run が成功していない場合のみ `{}` を返してよい。
* 必要に応じて内部状態更新

**形式仕様**：If limit is exceeded after run(), the node MUST:
- Preserve and return the output of run()
- Set status = limit
- Prevent further scheduling in the current StructuralNode

---

#### ⑤ 例外吸収

run が例外を投げた場合、§2.2 の例外→status 変換に従う。PauseSignal → pause、LimitSignal → limit、その他 → fatal。Runner は例外を扱わない。

---

## 6.4 出力制約

BaseNode は以下を保証する（戻り値契約は §2.1 参照）：

* 必ず dict を返す（成功時は output port を持つ dict、停止系では `{}` を返してよい）
* execute の返り値は output port の dict のみ。status は含まない。
* **成功時**の output port には `_meta.revision` が存在する（`{}` を返した場合は revision は存在しない。§5.8）

---

## 6.5 状態（state）

Node は内部状態を保持してよい。**state の定義・所有者・読み書き主体・禁止事項は §3.9.1 にまとめる。**

* 状態保持は許可される
* ただし再実行時に初期化可能であることが望ましい
* NodeFlow v1.2 は deterministic 実行を保証しない

---

## 6.6 DataNode の責任（§1.3 分類との対応）

DataNode（LLMNode、ScriptNode など、単体で run を実装する Node）の責任は以下とする。

* run() の実装
* revision の付与（出力生成時のみ存在する。BaseNode が content-hash で付与。§5.2）
* usage の更新（§3.7）
* limit pre / limit post の扱い（§6.3 ①④）
* status の設定（done / limit / pause / fatal）

**revision**：DataNode は revision を生成しない。BaseNode が content-hash で自動付与する（§5.7）。StructuralNode の revision 扱いは §6.7 に従う。

---

## 6.7 StructuralNode の責任（§1.3 分類との対応）

**StructuralNode は Graph を内部に保持する Node である。** StructuralNode は Graph 構造そのものではない。Graph は宣言的構造であり、StructuralNode はその実行的実体である。StructuralNode.execute は Core における ⟦G⟧ の具体化である。

BaseNode.execute を完全に共有し、run() の中で subgraph（内部 Graph）を実行する。Core は Graph とその意味論 ⟦·⟧ を定義する（Part I §3）。StructuralNode の execute 呼び出しによって、その Graph 構造が動作する。これにより limit pre/post、PauseSignal、LimitSignal、fatal、revision 補完が DataNode と同一のテンプレートで扱われ、設計が統一される。

**StructuralNode.run のアルゴリズム**：StructuralNode は共通で **subgraph を 1-shot 実行する**抽象操作を持つ。これを仕様上 `_execute_subgraph()` と表す。PipelineNode はこれを 1 回呼び出して終了する。LoopNode は condition が true になるまで反復して呼び出す。実装の自由度を絞るため、以下の疑似コードで run の骨格を規定する。

**PipelineNode.run の疑似コード**（§12.2.1 の実体）：

```python
def run(self, inputs, params):
    self._init_context_if_needed()
    while True:
        progressed = self._runner.step()  # 実行可能ノードを 1 つ以上 execute
        if not progressed:
            break
        if self._check_limit():
            raise LimitSignal()
        if self._should_terminate():  # final_node.status == done 等
            break
    return self._get_final_output()
```

**LoopNode.run の疑似コード**（§12.2.2 の実体）：

```python
def run(self, inputs, params):
    while True:
        self._execute_subgraph()  # subgraph を 1-shot 実行（内部で Runner 相当のループ）
        if self._check_limit():
            raise LimitSignal()
        if final_node.status != "done":
            break  # pause / fatal / limit 等を伝播
        if self._evaluate_condition():  # final_node.status == done のときのみ評価（§12.2.2.5）
            break
    return self._get_final_output()
```

PipelineNode では `_execute_subgraph()` に相当する 1-shot 実行が run 内の while + _runner.step() で実現されている。LoopNode は「`_execute_subgraph()` を反復し、各反復後に condition を評価する」構造である。

StructuralNode（PipelineNode、LoopNode）の責任は以下とする。

* children（子グラフ）の管理
* usage の集約（§3.7）
* status の集約（子の状態から自身の done / limit / pause / fatal を決定）
* scope limit（params.limit の解釈）
* termination 判定（final ノードの done 確定条件）

**終了判定**：StructuralNode は必ず 1 つの **final ノード**を持つ。graph で明示的に指定する（§12.2.1.2, §12.2.2.2）。

終了条件は次のとおりである。

* **done で終了する場合**：final ノードの status が done であり、かつ **他の sub ノードに fatal / limit / pause が 1 つもいない**こと。さらに **executing の sub ノードが 1 つもいない**こと。executing が 1 つでもいるときは StructuralNode は executing のまま継続する。
* **fatal / limit / pause で終了する場合**：いずれかの sub ノードが fatal, limit, pause になったら、StructuralNode の status もその優先順位で集約し終了する。優先順位：**fatal > limit > pause**。

StructuralNode.status は**子ノード status の優先順位付き集約**である。優先順位（Priority order）：**fatal > limit > pause > executing > done > ready**。すなわち、子のいずれかが executing のあいだは集約結果も executing（done より executing を優先）。形式的に：

```
StructuralNode.status := argmax_priority(child_statuses ∪ {final_status})
```

**StructuralNode の最終 status は、子ノード status の優先順位付き集約結果に等しい。** **Single source of truth**：BaseNode が持つ status フィールドは **local**（実行中は executing 等の表示用）である。**終了時には必ず子の集約結果で上書きする。** 集約は self.status を更新するための内部計算であり、**read_status() は常に self.status を返す**（集約結果そのものを返す関数ではない）。実装では「local の一時状態」と「集約で更新された status」を混同せず、終了判定・伝播には常に集約結果を用いる。

**StructuralNode.read_status()**：self.status を返す。self.status は上記のとおり集約結果で更新される。返しうる値は上記優先順位で決まる。**終了判定（継続するか・止めるか）では**、read_status() の戻り値ではなく **集約結果（argmax_priority(child_statuses)）を直接参照すること**。これにより「子が done なのに self.status がまだ executing のまま」のような更新遅れで終了が遅れるバグを防ぐ。疑似コード：

```python
def read_status():
    return self.status  # 集約は self.status を更新するための内部計算。外部は read_status() のみ参照する。
# self.status の更新：実行中は "executing"、それ以外は argmax_priority(child_statuses) で上書きする。
```

終了時に StructuralNode 自身の status を上記優先順位に従って設定する。**graceful stop**：pause / limit / fatal 時は **実行可能判定を無効化し、executing 中のノードの完了待ちのみ行う。** 新規 execute を開始してはならない。同期／非同期の実装戦略は自由だが、この制約は満たすこと。その時点の latest_output は保持される（§3.3）。

**終了判定の疑似コード**（PipelineNode / LoopNode 等で共通利用）：

```python
# 終了判定は全子ノードの status を優先順位で集約した結果に基づく。
# いずれかが fatal/limit/pause ならその優先順位で終了。いずれかが executing なら executing のまま継続。
# done で終了するのは、全子が done（または ready）で final が done のときのみ。
status = argmax_priority([c.read_status() for c in children])
if status == "fatal":
    return "fatal"
if status == "limit":
    return "limit"
if status == "pause":
    return "pause"
if status == "done":
    return "done"
# executing / ready の場合は継続
return None
```

**status 更新タイミングの規則（確定）**

StructuralNode は以下のタイミングで `self.status` を集約結果で更新しなければならない。

1. **各子 Node の execute() 完了直後、終了判定の評価前**（同期・非同期いずれも。MUST be updated immediately after each child execute completes, before termination checks are evaluated).
2. **resume() 完了後**

「while ループ末尾でまとめて更新する」実装は許可しない。これにより、外部から read_status() を呼んだ時点での値は「直近の子 execute 完了時点での集約結果」を反映する。

形式的には：

```
after each child_node.execute() returns:
    self.status = argmax_priority([c.read_status() for c in children])
```

ただし、実行中（executing 中の子が存在する）は `argmax_priority` の結果が executing を返すため、`self.status` は executing のままになる（意図的）。

**注意**：上記疑似コードのとおり、StructuralNode は全子ノードの status を **fatal > limit > pause > executing > done > ready** の優先順位で集約し、その結果で自身の status を決定する。例：final ノードが done でも他の子ノードが fatal なら StructuralNode は fatal。いずれかの子が executing なら StructuralNode は executing のまま（done とはならない）。

**StructuralNode.execute の戻り値**：§2.1 の契約に従う。**done 時は final ノードの出力をそのまま返す**（revision 含め §6.7 に従う）。停止系では下表のとおり。

| 終了理由                | execute の戻り値           |
|-------------------------|----------------------------|
| done                    | final ノードの出力（そのまま） |
| limit post（run 成功）  | その出力 dict              |
| limit post（run 未成功）| `{}`                       |
| limit pre / pause / fatal | `{}`                     |

**StructuralNode における「run が成功した」**：**subgraph 実行により final ノードの出力 dict が生成されたこと**を指す。status が done であることとは独立である。run 成功 + status=limit（limit post）や run 成功 + status=done はいずれもあり得る。limit post で「出力を返す」とは、run が出力を生成したがその後に limit に達した場合に、その出力を返しつつ status=limit で終了することを意味する。

**子の status に対する挙動**：ready / executing → 継続。done → 終了判定へ（PipelineNode は終了、LoopNode は condition 評価）。fatal / limit / pause → 停止（graceful stop）。

**子が limit になったときの StructuralNode の義務**：If any child node enters status = limit, StructuralNode MUST:
- Set its own status = limit
- Stop scheduling further nodes
- Return the final node output only if the final node produced output

**pre/post limit**：StructuralNode も limit を持つ。**limit チェックのタイミング**は (1) execute 開始直後 (2) 各子ノードの execute 完了直後の 2 つのみ。終了は (2) の limit チェックの後に行う。超過時は status = limit で graceful stop。終了判定では limit が done より優先される。

**revision**：StructuralNode は revision を変更しない。final ノードの出力をそのまま返し、revision は final ノードが生成した値を保持する。StructuralNode はデータを生成・変換しない制御ノードであり、revision 契約には関与しない。（本段落が StructuralNode の revision の正の定義。）

**resume の共通仕様**：**API** は `resume(resume_inputs: dict) -> dict`。PipelineNode と LoopNode で同一である。**node_id の方針**：resume の**呼び出し引数**（どの node を resume するかの指定）には node_id を使わない。内部構造を API に漏らさないという意味では「対象指定に node_id を用いない」である。戻り値に node_id を含めることは許容し、実装・監視の便宜に供する。

**呼び出し条件**：StructuralNode の status が `pause` のときのみ呼べる。pause ノードが 1 つ以上存在すること。違反時は **InvalidStateError** を raise する。StructuralNode が pause になるのは、子ノードのいずれかが pause になった場合のみである（§6.7 子の status 集約）。したがって StructuralNode.status == pause の場合、常に 1 つ以上の pause 子ノードが存在する。

**動作**：(1) pause 状態のノードをすべて列挙する。列挙順は graph.nodes の記述順に従う（§7.1.1 と同一の決定性ルール）。(2) 各ノードに `execute(resume_inputs, params)` を呼ぶ（params は通常スケジューリング時と同一。StructuralNode が graph 定義から解決する）。(3) 出力が `{}` でなければ保存する（§3.3）。(4) **resume 呼び出し中に fatal が発生した場合は、それ以降の resume は実行せず即座に停止する。** (5) すべての resume 呼び出し完了後、通常スケジューリングに戻り、termination 判定を再評価する。StructuralNode は一時的に status を executing に戻し、終了判定に従って最終 status を再設定する。

**resume 時の入力**：resume 時には **resume_inputs のみ**が pause ノードに渡される。通常の inputs は再解決しない。必要な状態は Node が内部で保持する責任がある。

**ネスト構造での resume**：StructuralNode 内の子ノードが StructuralNode（PipelineNode / LoopNode）である場合、その子 StructuralNode が pause 状態であれば、親は子 StructuralNode の `resume()` を呼ぶ。親が孫ノード以下を直接 resume することは禁止する。resume はネストの外側から順に呼び出される。例：外側 PipelineNode → 内側 LoopNode が pause の場合、`outer_pipeline.resume(inputs)` において outer は `inner_loop.resume(inputs)` を呼び、inner_loop は pause 中の子ノードに execute を呼ぶ。外側から直接 inner_loop 内の子ノードを resume することはない。

**戻り値**：`{"resumed": [node_id, ...], "statuses": { node_id: "done" | "pause" | "limit" | "fatal" }}`。resume したノード ID のリストと、各ノードの resume 後の status を返す（node_id は戻り値には含めてよい。上記「node_id の方針」参照）。呼び出し元が後続処理を判断できる。戻り値は resume 呼び出し中の各 Node の直後状態を示すものであり、最終的な StructuralNode.status は別途 read_status() により確認すること。

**resume_inputs**：単一の dict を全 pause ノードに渡す。各 Node は自分が必要なキーだけ読む（他は無視）。Node は black-box であり、内部構造を API に漏らさない。resume_inputs_schema は外部に「何を渡せばよいか」を伝える参考情報であり、エンジンは解釈しない。**resume_inputs のキー衝突は未定義動作であり、パイプライン設計者の責任とする。** 静的診断ツールは、同一 Graph 内で複数 Node が pause になり得る場合に resume_inputs_schema のキーが衝突していないかを警告することが推奨される（SHOULD warn）。

**実行不能状態**：§7.5 に従う。**状態保持**：StructuralNode も内部状態を持ってよい。**limit の例**：params.limit で max_total_node_calls / max_idle_sec / max_iterations 等を指定する。詳細は各 Node（§12.2.1.9, §12.2.2.10）参照。

**StructuralNode.run の責務境界（MUST / MUST NOT）**

StructuralNode.run **MUST NOT**：

* 子ノードの状態を直接変更する
* BaseNode.execute の流れをバイパスする（子の実行は必ず `child_node.execute()` 経由）
* revision を変更する
* fatal を status 伝播せずに握りつぶす

StructuralNode.run **MUST**：

* 子の実行に `child_node.execute()` を用いる
* 子の status を集約する
* 終了判定を final ノードおよび子の status のみに基づいて行う
* BaseNode の limit pre/post の意味論を尊重する

---

# 7. StructuralNode と Runner（Execution Mechanism）

**FlowRunner という概念は存在しない。** 実行主体は StructuralNode であり、Runner はその内部機構である。

**Runner の定義**：Runner は StructuralNode 内部で、Graph 構造に従い子 Node の execute を呼び出す補助機構である。Graph の意味論を実現する主体は StructuralNode である。Runner は inputs 解決・実行可能判定・execute 呼び出し・最新出力保存を行う。**Runner は dumb**（詳細は §7.2）。

**実行開始**：トップレベル StructuralNode の execute 呼び出しによって、内部 Graph の意味論が具体実行される。通常の実装ではトップレベルは PipelineNode であるが、これは StructuralNode の一種である。

**Kick** とは、当該実行を開始することを指す。定義：トップレベル StructuralNode の `execute(inputs, params)` が kick である（通常は PipelineNode）。

**Runner の位置づけ**：Graph を内部に保持する StructuralNode（PipelineNode、LoopNode）は Runner を保持し、子ノードの execute を管理する。Runner は外部 API ではない。責務は実行可能判定・inputs 解決・node.execute 呼び出し・latest_output 管理である。Core の Graph は抽象構造であり、StructuralNode や Runner と同一ではない。

## 7.1 Pull型スケジューラ

**Input port requirements**：Input port の required は **node.yaml** に定義される。`required` を省略した場合は **true** とする。Runner は実行可能性を、required な port がすべて解決可能かどうかのみで判定する。未解決の input は fatal にはならない。

**実行可能条件**：ノードは **required:true の input port がすべて解決できた場合**に実行可能である。この条件を形式定義では **RequiredInputsResolved(n, t)** と表す（§12.2.1.9 の max_idle_sec 形式定義参照）。§7.1 の本項と同義であり、実装時に独自に拡張解釈してはならない。

**「解決できた」とは**、バインディング先ノードの latest_output に当該 port の値が存在することを指す。バインディング先ノードの status は問わない。fatal / limit ノードの出力が latest_output に存在する場合、それを有効な入力として扱う。これは意図的な設計であり、停止した Node の出力を後続 Node が参照できることを保証する。

* required:false の port は解決できなくても実行可能
* 循環初期値や optional port との整合のため、「すべての inputs が存在する」ではなく「required:true の inputs がすべて解決できる」で判定する

---

## 7.1.1 実行順序の決定性（確定仕様）

Runner は **graph.nodes の記述順にノードを走査し、実行可能ノードを判定する**。

並列実行を行う場合も、論理順序は nodes 配列順に従う。

* 再現性確保
* デバッグ容易
* determinism を担保
* 並列最適化の余地は残す

---

## 7.2 Runner の責務

Runner は：

* inputs 解決
* 実行可能判定
* node.execute 呼び出し
* 最新出力保存（§3.3。保存は Runner の API で行い、resume からも同 API を介する。`save_output` は `{}` をスキップする）。

Runner は：

* **READY ノードを execute する**（§7.3）。実行可能判定に status を参照するが、Runner は status の意味を解釈しない（停止制御は StructuralNode（§6.7）の責務）
* **usage を触らない**（§3.7）
* **limit を評価しない**（limit は Node / StructuralNode の責務）
* limit 管理をしない
* retry 管理をしない
* revision を解釈しない（§5.1）
* 例外処理をしない
* **kill しない**
* **interrupt しない**

**Runner は status の意味（停止制御）を解釈しないが、実行可能判定のために値は参照する。** 制御は StructuralNode（§6.7）の責務である。Runner は status を**実行可能判定のためのフィルタ述語**としてのみ用いる。Runner **MUST NOT**：fatal を伝播する、実行ループを停止する、終了条件を評価する、pause の意味を解釈する。これらはすべて StructuralNode（§6.7）の責務である。

---

## 7.3 実行制御（基本動作）

**Runner は status が ready または done のノードのみ execute する。** executing / pause / limit / fatal のノードは実行しない。（async ノードが内部で await している間も status は executing のため、Runner は当該ノードを再度 execute しない。）**pause 状態のノードへの execute は、StructuralNode の `resume()` 経由でのみ行われる（§6.7）。** Runner は pause ノードを自発的にスケジュールしない。子ノードの status を読んで制御するのは StructuralNode の責務（§6.7。§2.3.3 参照）。

**Atomicity requirement for parallel execution**  
When parallel execution is enabled (§7.6), the Runner MUST ensure that the check of a Node's status and the invocation of execute() are performed atomically with respect to other Runner threads or coroutines operating on the same Node. Concretely:

* A Node whose status transitions to `executing` MUST NOT be scheduled for execution again until its status leaves `executing`.
* The mechanism for ensuring this atomicity (lock, async guard, etc.) is implementation-defined, but the invariant MUST be preserved.

Violation of this invariant results in undefined behavior of Node.state.

---

## 7.4 終了判定・graceful stop

**終了判定および pause / limit / fatal 時の振る舞い（graceful stop）は §6.7 に完全に属する。** Runner は実行可能判定と execute 呼び出しのみ行う。制御は StructuralNode（§6.7）の責務である。

**graceful stop 中の制約**：StructuralNode が graceful stop に入った後は、**実行可能判定を無効化し、executing 中のノードの完了待ちのみ行う。** 新規に execute を開始してはならない（polling でも await でも、「待つ」の実装は自由だが禁止事項は守ること）。

**注意**：v1.2 では kill を持たないため、**外部 I/O を含む Node は timeout を内部で実装しなければならない（MUST）**。そうでない場合、executing 中のノード完了待ちで永久待機となり、max_idle_sec 等が機能しない（§6.1, §6.7 graceful stop）。

---

## 7.5 実行不能状態（deadlock）

**deadlock を構造的には検出しない。** 静的診断ツールで扱う。**ただし進捗がない状態は limit（max_idle_sec 等）によって停止できる。**（§12.2.1.9）

* deadlock は **fatal にならない**（StructuralNode が runtime で fatal として扱うことはない）
* 検出は limit や静的診断ツールに委ねる

---

## 7.6 並列実行

* 実行可能ノードは並列実行可能
* LLM 待機中に他ノードを実行可能
* 初期実装は最小限（例：async）でよい

---

## 7.7 状態とループの整合性

* Node は再実行可能
* done は再実行可能な状態
* reset は不要
* execution_id は不要
* LoopNode は Node を再 execute するだけ

設計は整合している。

---

## 7.8 非同期との整合

* Node は executing 中に await 可能
* Runner は kill しない
* graceful stop が基本
* リソースリークを避ける

---

# 8. 循環グラフ

* 循環は許可
* **初期値のない循環**：Runner が inputs を解決しようとした際、循環依存により参照先の出力が存在しない場合、該当ノードは「実行不能」として扱う。**初期値のない循環は runtime では fatal にしない。** 該当ノードは実行不能となり、結果として進捗のない状態になる。この状態は `max_idle_sec` により停止可能である（§12.2.1.9）。**静的診断ツールで事前検出することを強く推奨する。**
* **初期値の与え方**：(1) Flow の inputs として外部から注入する (2) 循環上のいずれかのノードが params から初期値を生成し、inputs なしで実行できるよう設計する（`required: false` の port を持つ）。どちらも行われない循環は静的診断ツールが検出すべきエラーとする。
* 収束は final ノード（graph で指定）の done または limit による

**空 dict と循環の整合**

§3.3 のため：

* limit / pause / fatal 時も既存出力は消えない
* 循環参照は壊れない
* 前回出力は保持される

---

# 9. 例外処理モデル

## 9.1 Node が吸収

BaseNode.execute 内で例外を捕捉し、status を fatal にする。execute の返り値には status を含めない（§2.2, §2.3.3）。

* PipelineNode は read_status() で status を読み、fatal なら制御する
* Retry は通常 Node で実装する

### Node の fatal 情報アクセス

BaseNode は fatal 発生時、その原因例外を **state** として保持しなければならない（MUST）。state の定義および「診断目的の実行結果情報」の扱いは §3.9.1 に従う。

```python
def read_error(self) -> Exception | None:
    """
    status == fatal のとき、原因例外を返す。
    fatal でない場合は None を返す。
    """
```

**仕様**

* status が fatal のときのみ例外オブジェクトを返す
* fatal でない場合は None を返す
* read_error() は read-only である（Node 外部からの変更は禁止）
* StructuralNode は子 Node の read_error() を集約して上位に伝達してよい
* Runner は read_error() を参照しない（Runner is dumb を維持）

**PauseSignal / LimitSignal**  
PauseSignal・LimitSignal は fatal ではないため read_error() は None を返す。

**StructuralNode の read_error()**  
StructuralNode は配下の**全子孫** Node の fatal 原因を集約し、`read_error()` で**すべて**の例外を返さなければならない（MUST）。返却形式は配下の全子孫の fatal 例外のリスト（`list[Exception]`）とする。自身が fatal の場合は自身の原因も含める。

---

# 10. スコープルール

## 10.1 Node が参照できるもの（実装の視点）

Node の実装が参照できるのは **常に次のみ** である：

* 自身の `inputs`
* 自身の `params`

Node は Graph や他ノードの存在を知らない。外側の世界（同一 Graph 内の他ノード出力や親 StructuralNode の inputs）を直接参照することはない。

---

## 10.2 入力バインディングのソース（Graph 定義の視点）

Graph 定義（node_pipeline.yaml）で、あるノードの `inputs` に **バインドしてよい値の出所** は次のいずれかである。これらは Runner（StructuralNode 内部）が `${node_id.port}` などを解決するときに参照するソースであり、Node 実装が直接「見る」ものではない。

* **同一 Graph 内の他ノードの出力** — `${node_id.port}` で参照
* **親 StructuralNode の inputs** — ネストした Graph 内のノードに対して、親が受け取った inputs を渡す場合（例：`${inputs.port}` など）
* **親 StructuralNode の params** — `${params.<param_name>}` で参照（§4, §12.2.1.2）

Runner がこれらのソースから値を取り、該当ノードの `inputs` に詰めてから `execute(inputs, params)` を呼ぶ。したがって Node から見れば、常に「自身の inputs と params だけ」である。

---

# 11. 定義ファイル

## 11.1 node.yaml

Node 型定義。

```yaml
version: "1.2"

name: string
description: string

inputs:
  <port>: <schema>

outputs:
  <port>: <schema>

params:
  <param>: <schema>
```

**Schema 最小サブセット**：type, description, required, properties, items, additionalProperties, default を許可。

**実行時検証（v1.2）**：node 存在チェック、port 存在チェックを必須とする。shape 検証は将来拡張。

PipelineNode の定義ファイル（node_pipeline.yaml）は §12.2.1.2 に記述する。

---

## 11.3 Node Type Registry

YAML の `type` フィールドは **Node Type Registry** を通じて具体クラスに解決される。

**Built-in Type（予約済み）**

| type 文字列 | マッピング先クラス |
|-------------|-------------------|
| `"pipeline"` | PipelineNode |
| `"loop"` | LoopNode |

これらの type 文字列はユーザー定義 Node に使用してはならない（MUST NOT）。使用した場合の挙動は undefined である。

**User-defined Type**

ユーザー定義 Node は Registry に登録することで YAML の type フィールドから参照できる。登録方式（デコレータ・設定ファイル等）は実装定義とする。

**Version との関係**  
Registry は version（§11.4）と独立している。type 解決は version チェック（§11.4）の後に行う。

---

## 11.4 Version Compatibility

node.yaml および node_pipeline.yaml の `version` フィールドは必須である（MUST）。

**ロード時の検証**

エンジンは YAML ロード時に version フィールドを検証しなければならない（MUST）。

```
if yaml.version != engine.supported_version:
    raise VersionMismatchError(
        f"Unsupported version: {yaml.version}. "
        f"Engine supports: {engine.supported_version}"
    )
```

* version フィールドが存在しない場合は **VersionMismatchError** とする
* マイナーバージョンの後方互換性ポリシー（例：1.2 エンジンが 1.1 を読めるか）はエンジン実装者が定義する。ただしデフォルトは **厳格一致（exact match）** とする
* version の比較は文字列完全一致とする（`"1.2"` と `"1.20"` は別物）

---

# 12. 各種 Node

# Part III — Concrete Nodes

Node そのものとまわりの仕組みを述べたあとで、具象の Node 種別を定義する。12.1 では各種 Leaf Node（DataNode）、12.2 では各種 StructuralNode に分けて記述する。

**記述構造の統一**：各 Node は以下の章立てに揃える。DataNode と StructuralNode で番号の意味は同一とする。

| 節 | 内容 |
|----|------|
| 12.x.x.1 | 役割 |
| 12.x.x.2 | node.yaml または node_pipeline.yaml |
| 12.x.x.3 | run() の責務 または 実行モデル・終了判定 |
| 12.x.x.4 | status の扱い |
| 12.x.x.5 | limit の例 |
| 12.x.x.6 以降 | 特記事項（pause 再開、condition、final_graph など） |

---

## 12.1 各種 Leaf Node（DataNode）

DataNode は run() を実装する単体 Node。revision は BaseNode が content-hash で付与する（§5.7）。usage 更新・status 設定は §6.6 の責務に従う。以下は代表的な具象の概要である。詳細は各 Node の仕様に委ねる。

### 12.1.1 LLMNode

#### 12.1.1.1 役割

LLM を呼び出す Node。入出力は port で定義する。pause（human-in-the-loop）の参考実装は §12.2.1.5 に記載する。

#### 12.1.1.2 node.yaml

```yaml
version: "1.2"
name: llm_chat
description: LLM チャット Node

inputs:
  prompt: { type: string, description: ユーザー入力 }

outputs:
  response: { type: string, description: LLM 応答 }

params:
  model: { type: string, default: "gpt-4" }
  temperature: { type: number, default: 0.7 }
  limit:
    max_tokens: 1000
```

形式は §11.1 参照。

#### 12.1.1.3 run() の責務

run() は LLM API 呼び出し、usage 更新（prompt_tokens / completion_tokens）、応答の output port 格納、必要に応じて PauseSignal の raise を行う。BaseNode が revision 自動補完・limit pre/post・status 設定を行う（§6.3）。

```python
def run(self, inputs, params):
    response = llm.chat(
        model=params["model"],
        prompt=inputs["prompt"],
        temperature=params["temperature"]
    )
    self.usage.add("prompt_tokens", response.prompt_tokens)
    self.usage.add("completion_tokens", response.completion_tokens)
    return {"response": response.text}
```

#### 12.1.1.4 status の扱い

| 状態   | 意味               |
|--------|--------------------|
| done   | 正常終了           |
| pause  | human review 待機  |
| limit  | トークン制限超過   |
| fatal  | API 例外等         |

#### 12.1.1.5 limit の例

```yaml
params:
  limit:
    max_tokens: 2000
    max_calls: 5
```

#### 12.1.1.6 特記事項

* LLM セッション履歴は Node 内部 state に保持する。pause/resume の参考実装は §12.2.1.5 参照。
* revision は response port に BaseNode が自動付与。resume は §6.7 に従う。

---

### 12.1.2 GitScriptNode

#### 12.1.2.1 役割

Git 操作をスクリプトで行う Node。clone / fetch / commit 等を run() 内で実行する。

#### 12.1.2.2 node.yaml

```yaml
version: "1.2"
name: git_script
description: Git 操作スクリプト Node

inputs:
  repo_path: { type: string, description: リポジトリパス }
  action: { type: string, description: "clone" | "fetch" | "commit" | "push" }

outputs:
  result: { type: string, description: 実行結果（stdout 等） }

params:
  limit:
    max_calls: 10
```

形式は §11.1 参照。

#### 12.1.2.3 run() の責務

clone / fetch / commit / push 等を run() 内で実行する。BaseNode が revision 自動補完・limit・status を扱う（§6.3）。

```python
def run(self, inputs, params):
    repo, action = inputs["repo_path"], inputs["action"]
    if action == "clone":
        result = subprocess.run(["git", "clone", repo], capture_output=True, text=True)
    elif action == "fetch":
        result = subprocess.run(["git", "fetch"], capture_output=True, text=True, cwd=repo)
    # ...
    return {"result": result.stdout}
```

#### 12.1.2.4 status の扱い

例外時は fatal。limit 超過時は status = limit（§6.3）。

#### 12.1.2.5 limit の例

```yaml
params:
  limit:
    max_calls: 10
```

#### 12.1.2.6 特記事項

* 外部 I/O を含むため、timeout の実装は MUST である（§6.1）。timeout_sec は params.limit で指定する（§12.1.2.5 参照）。
* revision は BaseNode が付与。

---

### 12.1.3 PythonScriptNode

#### 12.1.3.1 役割

任意の Python スクリプトを実行する Node。inputs / params を渡し、outputs を返す。DataNode の責務（revision・usage・status）を満たす。

#### 12.1.3.2 node.yaml

```yaml
version: "1.2"
name: python_script
description: Python スクリプト実行 Node

inputs:
  data: { type: object, description: スクリプトに渡す入力 }

outputs:
  result: { type: object, description: スクリプトの戻り値 }

params:
  script_path: { type: string, description: 実行する .py のパス }
  limit:
    timeout_sec: 60
```

形式は §11.1 参照。

#### 12.1.3.3 run() の責務

指定 script_path の Python スクリプトを実行し、inputs を渡して result を返す。DataNode の責務（revision・usage・status）は §6.6 に従う。

```python
def run(self, inputs, params):
    result = execute_script(params["script_path"], inputs["data"])
    return {"result": result}
```

#### 12.1.3.4 status の扱い

例外時は fatal。timeout 等は limit で制御する場合 status = limit。

#### 12.1.3.5 limit の例

```yaml
params:
  limit:
    timeout_sec: 60
```

#### 12.1.3.6 特記事項

* 外部 I/O を含むため、timeout の実装は MUST である（§6.1）。timeout_sec は params.limit で指定する（§12.1.3.5 参照）。
* revision は BaseNode が付与。

---

## 12.2 各種 StructuralNode

StructuralNode は Core Model における Graph の意味論 ⟦G⟧ を Execution Layer で具体実装するための Node である。Graph は抽象構造であり、Execution Layer では StructuralNode がその具体実現を担う。

### 12.2.1 PipelineNode

#### 12.2.1.1 役割

PipelineNode は **Graph を 1-shot 実行する StructuralNode** である。内部に graph を持ち、StructuralNode を継承する（§1.3, §6）。**Runner と制御層を兼ねる**：Runner を保有し、status 集約・終了判定・limit 解釈・resume 管理を行う。読み手は PipelineNode を「Runner + 制御層」として把握するとよい。

PipelineNode は Runner を内部に保持するが、**Runner は抽象インターフェースを通じて利用される**。

**Runner Interface Contract**：PipelineNode が依存するのは次の抽象インターフェースのみである。

* `resolve_inputs(node_id) -> dict`
* `is_executable(node_id) -> bool`
* `execute_node(node_id) -> dict`
* `save_output(node_id, output)`
* `get_latest_output(node_id) -> dict | None`

Runner の実装はこのインターフェースを満たさなければならない。内部スケジューリングアルゴリズムは Execution Layer では規定しない。実装は差し替え可能である（分散 Runner・Streaming Runner・外部 Executor 等を想定）。

#### 12.2.1.2 node_pipeline.yaml

PipelineNode の内部構造は **node_pipeline.yaml** により定義される。`graph` セクションの `graph.nodes` を記述順に走査する（§7.1.1）。**final ノード**を graph 内に必ず 1 つ指定しなければならない。`graph.final` に node_id を記述する。final は `graph.nodes` に存在する node_id であること。複数指定は禁止。

**定義ファイルの形式（node_pipeline.yaml）**

```yaml
version: "1.2"
name: hello_pipeline
inputs: ...
outputs: ...
params:
  limit:
    max_total_node_calls: 200
graph:
  nodes:
    - id: A
      type: node_type
      inputs: ...
      params: ...
    - id: B
      type: node_type
      inputs: ...
      params: ...
  final: B
```

**Param の明示的マッピング**：PipelineNode は自身の params を子ノードに**明示的にマッピング**する。暗黙継承は禁止（§4.3）。子ノードで親の param を使う場合は、graph の node 定義で `params` に記述する。例：親の `params.threshold` を子に渡す場合

```yaml
graph:
  nodes:
    - id: child
      type: some_node
      params:
        threshold: ${params.threshold}
```

PipelineNode は graph 定義に従い、子ノード実行時にこの param を解決する。

**実行例**

* **直列**：`A → B → C`（C を final に指定）。Runner は A → B → C の順に実行可能になり、**final ノード C が done** になった時点で当該 PipelineNode の実行が終了する。出力は C の出力をそのまま返す（§6.7）。
* **並列**：`A → B` と `A → C`（C を final に指定）。A の後 B と C が同時に実行可能になる。**final ノード C が done** になり、かつ C が executing でなくなった時点で終了する。出力は C の出力をそのまま返す（§6.7）。

#### 12.2.1.3 終了条件

**終了判定は §6.7 に完全に従う。** PipelineNode は Graph を 1-shot 実行するため、**final ノードが done** になった時点で当該 PipelineNode の実行が終了する。出力・revision は §6.7 に従う。

**final が `{}` を返す場合**：final ノードが `{}` を返したとき、PipelineNode は `{}` を返す。以前の出力へのフォールバックは行わない（strict）。

#### 12.2.1.4 execute の挙動

`PipelineNode.execute(inputs, params) -> dict`。**PipelineNode.execute 開始時、status は executing になる（BaseNode.execute による）。** 内部に Runner を持つ。Runner は execute 初回に生成し、同一 PipelineNode インスタンスの再実行時（LoopNode からの再 execute 等）は同一 Runner を再利用する。Runner の再利用に際して、latest_output（Context）は LoopNode の iteration 間で引き継がれる（§12.2.2.3 の iteration 間の Node 状態参照）。PipelineNode が独立した 1-shot として実行される場合（LoopNode 外）は、Context は execute 開始時に初期化される。実行フロー：graph 初期化 → Runner 生成（または再利用）→ Pull 型ループ → **final ノードの done 判定（§6.7）** → read_status() で status を読み制御判定 → 出力返却。limit チェック・graceful stop・戻り値はすべて §6.7 に従う。

#### 12.2.1.5 pause 再開モデル（確定）

**resume は §6.7 に完全に従う。** API・動作・戻り値・制約はすべて §6.7 の「resume の共通仕様」に記載する。PipelineNode 固有の補足：resume 後は termination 判定を再評価する（§6.7）。複数 pause ノードが存在する場合の resume 順序および実行開始位置（execution_cursor）は §17 に従う。

**LLMNode の run 設計（仕様上の参考実装）**

LLM セッション継続型 pause の標準的な実装パターン：

```python
class LLMNode(BaseNode):
    def __init__(self):
        self._session = []            # LLM セッション履歴（内部状態）
        self._waiting_for_human = False

    def run(self, inputs: dict, params: dict) -> dict:
        if not self._waiting_for_human:
            # 通常実行フェーズ
            response = llm.chat(self._session, inputs["prompt"])
            self._session.append(response)

            if needs_human_review(response):
                self._waiting_for_human = True
                raise PauseSignal(
                    reason="human review required",
                    resume_inputs_schema={"human_input": "string"}
                )
            return {"result": response}

        else:
            # resume フェーズ（同じセッションで続行）
            self._waiting_for_human = False
            response = llm.chat(self._session, inputs["human_input"])
            self._session.append(response)
            return {"result": response}
```

| 観点 | 内容 |
|------|------|
| LLM セッション | `self._session` に保持。resume 後も同じ履歴で継続 |
| resume_inputs | `human_input` として run に渡る |
| 状態管理 | `_waiting_for_human` フラグで分岐。Node 内部に完結 |
| revision | resume 後に run が成功すれば BaseNode が自動付与 |

**将来の WebUI 統合**：このモデルは REST API と相性が良い。例：`POST /pipeline/{id}/resume` に `{"resume_inputs": {"human_input": "承認します"}}` を送り、`pipeline.resume(resume_inputs)` をそのままエンドポイントにマッピングできる。PipelineNode と LoopNode で同一 API（内部構造を漏らさない）。

**ネストした StructuralNode での注意**：Graph 内に LoopNode 等の StructuralNode が含まれており、それが pause になっている場合、外側 PipelineNode の `resume()` は内側 StructuralNode の `resume()` を呼ぶことで再開する（§6.7 ネスト構造での resume）。呼び出し元は外側 PipelineNode の `resume()` のみを呼べばよく、内側の構造を知る必要はない。

---

#### 12.2.1.6 status の扱い

**§6.7 の「子の status に対する挙動」に従う。** 当該 PipelineNode では、final ノードが done になったときに実行が終了する。下表は子ノードの status に対する挙動の補足である。status の定義は §2.3 参照。

| kind      | 挙動（子ノードの status に対する扱い） |
| --------- | ----------------------------------------------- |
| ready     | 継続（入力待ち・再スケジュール可能）             |
| executing | 継続（実行中・完了待ち）                         |
| done      | 継続（子ノードが done → 終了判定へ。§6.7 で final ノードが done となれば PipelineNode 自身が done となり当該実行が終了） |
| fatal     | 即停止                                           |
| limit     | 停止                                             |
| pause     | 停止（外部判断待ち）                             |

Runner の status 利用は §7.3 に従う。

---

#### 12.2.1.7 実行不能状態（deadlock）

**§6.7 に従う。** §7.5 のとおり runtime では検出せず、deadlock を fatal として扱わない。

---

#### 12.2.1.8 状態保持

**§6.7 に従う。** PipelineNode も内部状態を持ってよい。

例：

* 総呼び出し回数
* iteration カウンタ

これらは `params.limit` に従って制御する。

#### 12.2.1.9 PipelineNode の limit の例（deadlock 実行時保護）

**§6.7 に従う。** PipelineNode の `params.limit` の例として、進捗のタイムアウトを指定できる。

```yaml
params:
  limit:
    max_total_node_calls: 200
    max_wall_time_sec: 300    # Flow 全体のウォールタイム上限
    max_idle_sec: 30          # 実行可能ノードが 0 の状態が続いたときの待機上限（推奨）
```

**`max_idle_sec`（推奨）**：**実行可能ノードが 0 かつ executing ノードが存在しない**状態が `max_idle_sec` 秒続いた場合、PipelineNode は status = limit にして graceful stop する。idle 判定は「実行可能ノードが 0」だけでは不十分で、executing 中のノードが残っていないことを条件とする。§7.5 のとおり deadlock は構造的には検出しないが、進捗がない状態は本 limit で停止できる。保存ルールは §3.3 参照。`max_idle_sec` を指定しない場合、停止保証はない。運用者の責任において limit を設定すること。

**時計の基準**：max_idle_sec の経過時間は **monotonic clock（単調増加時計）** で計測しなければならない（MUST）。壁時計（wall-clock）は NTP やシステム時刻変更の影響を受けるため、idle 判定には使用しない。分散実行・非同期実装でも同一の意味論を保つため、実装は monotonic 基準で idle 継続時間を計測すること。

**形式定義**：

```text
ExecutableNodes(t) := { n ∈ Nodes | status(n) ∈ {ready, done} ∧ RequiredInputsResolved(n, t) }
ExecutingNodes(t) := { n ∈ Nodes | status(n) == executing }
Idle(t) := |ExecutableNodes(t)| = 0 ∧ |ExecutingNodes(t)| = 0
If Idle(t) holds continuously for max_idle_sec, then status = limit.
```

RequiredInputsResolved(n, t) は §7.1 の「required:true の input port がすべて解決できた」に対応する。status は §2.3 の Node 状態を参照する。実装時に独自解釈しないこと。

---

### 12.2.2 LoopNode

#### 12.2.2.1 役割

`type: loop`。**StructuralNode の一種**（§1.3）。**Graph を条件付きで反復実行する StructuralNode** である。内部に Runner を持ち、外部からは 1-shot Node として振る舞う。

**Loop の本質（設計原則）**

1. **Loop の終了判定は condition のみで決定される**
2. **LoopNode は revision を参照しない**
3. **revision は I/O 契約（再実行最適化の補助情報）であり、Loop 制御とは無関係である**
4. **LoopNode は子 Node の内部状態（no-op 判定含む）を解釈しない**

**LoopNode 仕様の要約**：条件は declarative（§12.2.2.5）。終了は condition のみ。出力・revision は §6.7 に従う。Loop の condition では revision を参照しない。iteration ごとに Node を再生成しない（同一インスタンスを再利用。§12.2.2.3）。

#### 12.2.2.2 node_pipeline.yaml

graph 記述は PipelineNode と同じ。node_pipeline.yaml の形式・graph セクションは §12.2.1.2 参照。**final ノードを必ず 1 つ指定する**（`graph.final`）。`type: loop` で LoopNode として解釈する。

#### 12.2.2.3 実行モデル

`while True:` で subgraph を 1-shot 実行し、**final ノード**が done になったら condition を判定。true なら break、false なら次 iteration。limit 到達時は status が limit になる。

**LoopNode の subgraph 内での PauseSignal**：**LoopNode は subgraph の pause をそのまま伝播する。** subgraph 内で PauseSignal が発生した場合、LoopNode 自身の status は pause となる。resume は §6.7 に従う（pause ノードをすべて同時に再開。PipelineNode と同一 API。§10 スコープルール）。

**iteration 間の Node 状態**

LoopNode は各 iteration で**同じ Node インスタンスを再利用する**（新規生成しない）。Node の status は done → executing → done と遷移するだけで、reset は不要（§2.3.2 と同じ）。

| 項目 | iteration 間の扱い |
|------|---------------------|
| status | done のまま次 iteration で execute される（reset 不要）|
| 内部状態（revision カウンタなど）| Node が引き継ぐ（リセットしない）|
| latest_output | Context（StructuralNode が管理）に保持したまま引き継ぐ（§3.9.3）|

**Node の内部 state と latest_output の整合性は Node 実装者の責務である。Execution Layer は整合を保証しない。**

**LoopNode は Node の内部状態や no-op 判定を解釈しない。** Node が no-op を実装していても、LoopNode の iteration は進む。Loop の終了は **condition** に依存する。意図的なリセットが必要な場合は Node 実装側で run の冒頭に書く。LoopNode はリセットを強制しない（§12.2.2.10 limit の例）。

**Node 実装者のための iteration 境界ガイドライン**

iteration をまたいで内部 state を引き継ぐ Node は、run の冒頭で「前回の latest_output と自身の内部 state が整合しているか」を自己検証することを推奨する。

典型的な実装パターンは以下の 3 つである。

**パターン A：完全ステートレス（最もシンプル）**

```python
def run(self, inputs, params):
    # 内部 state を持たない。毎回 inputs のみで動作する。
    # iteration 間の整合問題は発生しない。
    result = process(inputs["data"])
    return {"result": result}
```

**パターン B：前回 revision を監視して no-op 判定**

前回の run 戻り値（_meta なし）をそのまま保持する。revision は BaseNode が毎回補完するため、実装者は _meta を直接操作しない（§5.2）。

```python
def run(self, inputs, params):
    current_rev = inputs["data"]["_meta"]["revision"]
    if current_rev == self._last_revision:
        # 入力が変化していない → 前回の run 結果をそのまま返す。
        # BaseNode が再度 revision 補完するが、内容が同一なら revision 値も同一になる。
        return self._last_output
    self._last_revision = current_rev
    result = process(inputs["data"])
    self._last_output = {"result": result}
    return self._last_output  # BaseNode が revision を付与する
```

**パターン C：iteration カウンタや累積 state を持つ**

```python
def run(self, inputs, params):
    # state を引き継ぐが、latest_output との整合は自己管理する。
    # LoopNode はこの state を感知しない。
    self._history.append(inputs["data"])
    result = aggregate(self._history)
    return {"result": result}
```

いずれのパターンでも、**LoopNode は Node の内部 state を感知しない**。iteration の継続・終了は condition のみで決まる。Node が no-op を返しても iteration は進む（§12.2.2.3）。

#### 12.2.2.4 execute の挙動

**§6.7 に従う。** 反復は .3 のとおり while + condition 判定。limit チェックのタイミングは §6.7。

#### 12.2.2.5 condition（標準）

**condition の評価対象は final ノードの「保存された最新出力」とする。** condition は **必ず Runner（StructuralNode）が保持している latest_output（Context 上の保存済み出力）** を参照して評価する。**final_node.execute() の戻り値で評価してはならない。** 出力が更新されない iteration が存在し得ることに注意する。

**condition 評価の前提（形式仕様）**：condition の評価は **final_node.status == done のときのみ** 行う。`final_node.status != done` のとき（例：limit post で `{}` を返した直後、subgraph 未完了、pause / fatal / limit 伝播時）は condition を評価せず、LoopNode は停止状態を伝播する。これにより、final ノードが `{}` を返した場合に path 解決不能で誤って fatal に陥ることを防ぐ。**同一 iteration で limit チェックと condition の両方に該当する場合は、limit を優先し、status = limit で終了する**（§6.7 の優先順位）。

**Loop condition 評価の定義域**：反復 n の終了時点における final_node の保存済み最新出力を O_n とする。condition は O_n 上で評価される。**condition が O_n 上で評価されるのは、subgraph 実行終了時に final_node.status == done である場合に限る。**

```text
if final_node.status != done:
    skip condition evaluation; propagate status and exit loop
else:
    # 参照元は Runner が保持する latest_output（保存済み）。execute の戻り値ではない。
    evaluate condition on latest_output[final_node_id]
```

**「正常完了」の定義**：condition を評価する「final ノードが done になった」とは、厳密には次のすべてを満たすことを指す。(1) subgraph 内に pause / fatal / limit のノードが存在しない。(2) final ノードの status が done である。(3) executing のノードが存在しない（§6.7 終了条件）。

停止状態（pause / limit / fatal）の場合：subgraph の実行は完了しない。**condition は評価されない**。LoopNode はその status をそのまま伝播する（§6.7）。

**final_node.status と LoopNode.status の対応**：

| final_node.status | LoopNode.status |
| ----------------- | --------------- |
| done              | condition 評価   |
| pause             | pause           |
| limit             | limit           |
| fatal             | fatal           |

```yaml
condition:
  path: "$.result"    # final ノード出力に対する JSONPath
  # 以下のいずれか1つを指定する（v1.2 でサポートする演算子）
  equals: <value>       # 等値比較
  not_equals: <value>  # 非等値比較
  less_than: <number>   # 数値比較（未満）
  greater_than: <number> # 数値比較（より大）
  # その他の演算子は将来拡張
```

例：`path: "$.score"` と `less_than: 0.5` のように、final ノードが `{"score": 0.3}` を返せば condition が true となり Loop を終了する。**revision の値はこの判定に使用しない。**

**condition 評価エラー時の fatal 条件（閉じた定義）**：次のいずれかに該当する場合は **fatal** とする。実装ブレを防ぐため、これ以外の解釈を拡張してはならない。

- **(a) path が存在しない**：指定した JSONPath（例：`$.score`）が対象出力に存在しない。
- **(b) 値が null**：path は解決されたが、その値が null であり演算子が null を許容しない場合。
- **(c) 値の型が演算子の要求と一致しない**：例 — `less_than` / `greater_than` は数値型を要求するため、値が文字列（例：`"0.5"`）や、演算子側の `value` が文字列で比較不能な場合は fatal。**型変換は行わない。**

condition 評価に失敗した場合、LoopNode は status = fatal とする。**エラーメッセージは必ず以下を含むこと**：JSONPath、演算子、実際の値、実際の型。例：

```
condition error:
path=$.score
operator=less_than
value="abc"
type=string
```

**Condition evaluation is not input binding.** It is a semantic termination check and MUST be strict. §3.1 の「未定義参照は例外を投げず当該ノードを実行不能とする」は **input バインディング** の話であり、condition には適用しない。condition の path 解決失敗・型不一致は fatal とする。実装者が「未定義参照は fatal じゃない」と解釈して condition エラーを無視してはならない。condition が解決できない状態は Loop 設計上のバグであり、実行継続することに意味がないため fatal とする。subgraph の途中ノードの出力を condition で参照したい場合は、final ノードがその値を pass-through して出力するよう設計すること。

**LoopNode 完全具体例（§12.2.2.5 の補足）**

**例：スコアが 0.5 未満になるまで改善する**

* subgraph：`improve`（LLM で改善案生成）→ `evaluate`（Python でスコア算出）。**evaluate を final ノードに指定**する。
* condition：`path: "$.score"`, `less_than: 0.5`。final ノード（evaluate）の出力の score が 0.5 未満なら break。

**実行トレース**

* **Iteration 1**：improve 実行 → evaluate 実行 → 出力例 `{"score": 0.72}` → condition: 0.72 < 0.5 → False → 継続。
* **Iteration 2**：improve 実行 → evaluate 実行 → 出力例 `{"score": 0.48}` → condition: 0.48 < 0.5 → True → break。LoopNode の最終出力は final ノード（evaluate）の出力をそのまま返す（§6.7）。

**pause を含む場合**：ある iteration で improve が PauseSignal を raise した場合、LoopNode.status = pause、graceful stop。resume() 呼び出しで同じ Node インスタンスで再開し、condition 判定へ戻る（§6.7）。

**limit の場合**：`max_iterations: 10` 等を指定しているとき、10 回実行しても condition が満たされない場合は status = limit、`{}` を返す。既存の最後の有効出力は保存済み（§3.3）。永久ループ防止のため max_iterations の指定を強く推奨（§12.2.2.10）。

#### 12.2.2.6 pause 再開モデル

**§6.7 の resume 共通仕様に従う。** PipelineNode と同一 API・同一動作。resume 後は通常の iteration フローに戻る。

#### 12.2.2.7 status の扱い

**§6.7 の「子の status に対する挙動」に従う。** 子ノードの status を集約し、pause / limit / fatal の場合は伝播して停止する。

#### 12.2.2.8 実行不能状態（deadlock）

**§6.7 に従う。** §7.5 のとおり runtime では検出せず、deadlock を fatal として扱わない。

#### 12.2.2.9 状態保持

**§6.7 に従う。** LoopNode も内部状態を持ってよい。

#### 12.2.2.10 LoopNode の limit の例

**§6.7 に従う。** Loop に本質的な上限として `max_iterations` を指定する（永久ループ防止のため強く推奨）。PipelineNode の max_idle_sec 等と併用してよい。詳細は §6.7。

```yaml
params:
  limit:
    max_iterations: 100
```

#### 12.2.2.11 final_graph（LoopNode のみ）

loop を抜けた直後に 1 回だけ実行可能な graph を定義できる。**v1.2 では未定義とする。** 将来拡張で仕様化する。

---

## 12.3 その他（将来拡張）

今回の仕様では 12.1 各種 Leaf Node（LLMNode、GitScriptNode、PythonScriptNode 等）、12.2 各種 StructuralNode（PipelineNode、LoopNode）を定義する。12.3 その他は将来拡張とする。

### 12.3.1 resume_inputs の node_id スコープ（v1.3 予約）

v1.2 では resume_inputs は単一 dict を全 pause Node に渡す（§6.7）。キー衝突は未定義動作である。

v1.3 では以下の形式を検討する：

```text
resume_inputs = {
    "node_a": {"human_input": "承認します"},
    "node_b": {"score_override": 0.3}
}
```

各 Node は自身の node_id に対応するサブ dict のみを受け取る。これにより複数 pause Node が同時発生する場合のキー衝突を構造的に解消できる。

ただし「Node は black-box であり内部構造を API に漏らさない」原則（§6.7）とのトレードオフがあるため、v1.3 で設計を確定する。

---

## 12.4 大規模データポリシー（Large Output Policy）

NodeFlow は JSON I/O モデルである。revision は content-hash のため、出力の正規化・ハッシュ計算が可能な範囲で運用する。

### 12.4.1 巨大バイナリの扱い

以下は output port に**直接含めない**ことを推奨する：

* 画像バイト列
* 動画バイト列
* 大規模バイナリ（1MB 以上を推奨上限とする）

巨大データを直接含めると、Canonical JSON 化・ハッシュ計算のコストが増大する。**大規模データを直接含めた場合、Canonical JSON 化および SHA-256 計算コストは Node の責任である。エンジンは最適化を保証しない。**

### 12.4.2 推奨方式

巨大データは **参照識別子を返す** 方式とする。

```json
{
  "file_path": "/tmp/image.png"
}
```

または

```json
{
  "blob_id": "s3://bucket/key"
}
```

revision はその識別子を含む JSON から content-hash として計算される。

### 12.4.3 hash_skip（例外的オプション）

特殊なケースで巨大出力を直接扱う必要がある場合、**hash_skip** を指定できる。

```json
{
  "_meta": { "hash_skip": true },
  "data": huge_binary
}
```

BaseNode は `hash_skip` が true の場合、content-hash を計算せず **revision を UUID 等の一意識別子で生成**する。

```text
if _meta.hash_skip:
    revision = UUID4()  # または同等の一意値
else:
    revision = SHA256(CanonicalJSON(port_value_without_meta))
```

**hash_skip は例外的用途であり、原則使用しない。** 通常は §12.4.2 の参照方式を用いる。**hash_skip を使用した port は deterministic ではない可能性がある**（UUID 等は実行ごとに異なるため）。

---

# 13. 実装構造

### クラス階層（§1.3 との対応）

```
Node (abstract)
 ├── DataNode (abstract)  … BaseNode を継承する単体 Node
 │       ├── LLMNode
 │       ├── ScriptNode
 │       └── ...
 │
 └── StructuralNode (abstract)
         ├── PipelineNode
         └── LoopNode
```

従来表記との対応：BaseNode は DataNode・StructuralNode 共通の基底クラス。DataNode はその上に run() 実装を追加した抽象クラスである。PipelineNodeBase / LLMNodeBase 等は上記の具象にマッピングする。

### ノード実体構成

```
nodes/
  node_name/
    node.yaml
    node.py
```

* node.yaml = 宣言
* node.py = 実装

---

# 14. Execution Philosophy（v1.30 追加）

NodeFlow Execution Layer は：

> 評価系（evaluator）ではなく、
> 再評価制御系（controlled re-evaluation system）である。

これにより：

* human-in-the-loop
* partial recomputation
* dynamic parameter adjustment

が可能になる。

---

# 15. Re-execution and Invalidation Model（v1.30 追加）

## 15.1 Motivation

Execution Layer は単なる 1-shot 実行機構ではない。

NodeFlow は：

> **制御付き再評価（controlled re-evaluation）モデル**

である。

本節では、再実行（re-execution）および invalidate の正式定義を与える。

---

## 15.2 Resume と Re-execution の統一

### 定義

```
resume := 再評価開始位置を変更せずに再実行
re-execution := invalidate + resume
```

両者の違いは：

| 操作           | invalidate | start_index |
| ------------ | ---------- | ----------- |
| resume       | なし         | 現在の停止位置     |
| re-execution | あり         | 指定位置        |

---

## 15.3 Invalidation の定義（確定）

PipelineNode は内部に：

```
node_states: dict[node_id → status]
execution_cursor: node_id
```

を持つ。

### invalidate(node_k) の意味

```
for all nodes n reachable from node_k (including itself):
    node_states[n] = ready
execution_cursor = node_k
```

* 子孫ノードの DONE / PAUSE / LIMIT / FATAL を破棄
* Node インスタンスは再生成しない
* Node 内部 state は保持される（仕様上リセットしない）

**重要**

invalidate は：

* state 巻き戻しではない
* snapshot 復元ではない
* 過去の出力履歴も保持しない

単なる：

> 部分再評価のための状態リセット

である。

---

## 15.4 再実行の正式 API（v1.30 追加）

```python
PipelineNode.re_execute(start_node_id: str) -> dict
```

内部動作：

```
invalidate(start_node_id)
resume({})
```

* resume_inputs は空 dict
* 必要な変更は params または Context 更新による

---

## 15.5 再実行と revision の関係

revision は I/O 契約であり、

* invalidate は revision を操作しない
* revision の比較は Node 内部責務
* Execution Layer は revision による再実行抑制を行わない

---

# 16. Unified Stop Model（v1.30 追加）

## 16.1 停止状態の統一

Node の停止状態は：

```
pause
limit
fatal
```

の 3 種。

これらはすべて：

> 実行の中断状態

である。

---

## 16.2 pause と limit の関係

limit は：

```
pause の特殊形
trigger = limit detection
```

である。

違いは：

| 状態    | trigger  | 再開条件                |
| ----- | -------- | ------------------- |
| pause | Node 内部  | resume 呼び出し         |
| limit | limit 判定 | limit 条件解除 + resume |

---

## 16.3 limit clear

新 API：

```python
PipelineNode.clear_limit()
```

動作：

* limit 状態を ready に戻す
* node_calls / idle timer 等は reset しない（TBD）
* その後 resume 可能

※ 詳細は TBD とする（branch C で詰める）

---

# 17. Resume Semantics Clarification（v1.30 追加）

### 複数 pause ノードが存在する場合

StructuralNode は：

```
graph.nodes 記述順
```

に従って pause ノードを resume する。

これは §7.1.1 の determinism と整合する。

---

### 実行開始位置

resume 後の実行は：

```
execution_cursor
```

から開始する。

execution_cursor は：

* pause 発生位置
* invalidate により設定された位置

---

# 18. Execution Cursor（v1.30 追加）

PipelineNode は内部に：

```
execution_cursor: node_id
```

を持つ。

意味：

> 次に実行可能判定を開始するノード

ルール：

1. execute 開始時は graph.nodes[0]
2. pause 発生時は当該ノード
3. re_execute(start_node_id) 時は start_node_id
4. resume 時は execution_cursor から継続

これにより：

* resume
* 再実行
* 部分再評価

が統一される。

---

# 19. 不変条件（Invariants）

# Part IV — Invariants

以下の不変条件を満たすことで、曖昧さの排除・実装ブレ防止・将来拡張可能性を保つ。

1. **Node の内部状態は Node のみが変更可能**（§2.3.1 制約。state / usage / Context の分離原則は §3.9.4）
2. **StructuralNode は子を black-box として扱う**（子の status を直接変更しない）
3. **usage は累積**（reset しない。§3.7, §3.9.2）
4. **revision は BaseNode が content-hash により自動生成する。StructuralNode は revision を変更しない。**（§5.7, §6.6, §6.7）
5. **execute は ready または done のときに通常経路で呼べる。pause のときは resume 経由でのみ呼べる**（§2.2, §7.3）
6. **Node インスタンスは同一 Execution Scope で再利用する**（execute のたびに再生成しない）。LoopNode は iteration 間で子 Node の同一インスタンスを再利用する。PipelineNode についても、同一 Execution Scope 内では同一インスタンスを使用する。
7. **revision は output 内容のみに依存する**（content-hash。§5.1, §5.7）
8. **Node 実装は revision を直接生成してはならない**（BaseNode が付与。§5.2, §5.7）
9. **revision 計算時に _meta は除外する**（§5.9.4）
10. **hash_skip が指定された場合、revision は content-hash である保証を失う**（§12.4.3）
11. **hash_skip は例外的用途であり、通常使用してはならない**（§12.4.3）
12. **StructuralNode は子ノードの出力内容を変更してはならない**
13. **StructuralNode は revision を再生成してはならない**（final ノードの出力をそのまま返す。§6.7）
14. **LoopNode は同一 Execution Scope 内の iteration 間で、子 Node の同一インスタンスを再利用しなければならない**（iteration ごとに子を再生成してはならない。§12.2.2.3）
15. **StructuralNode の status 集約は §6.7 に従う**
16. **並列実行において、status の読み取りと execute() の呼び出しはアトミックに行われなければならない**（§7.3 Atomicity requirement）
17. **node_calls は BaseNode.execute() の呼び出し回数として定義される**（§3.8）
18. **revision 計算において非 JSON 型は TypeError（fatal）とする。暗黙型変換は禁止する**（§5.9）
19. **BaseNode は fatal 発生時に原因例外を内部保持し、read_error() で公開しなければならない**（§9）
20. **invalidate は Node インスタンスを再生成してはならない**（§15.3）
21. **re_execute は invalidate + resume で定義される**（§15.4）
22. **limit は pause の特殊形である**（§16.2）
23. **StructuralNode は execution_cursor を保持する**（§18）
24. **resume と re-execution は execution_cursor によって統一される**（§15.2, §18）

---

# NodeFlow v1.2（二層構造版）の意味

この再構成により：

* NodeFlow は計算モデル（Part I）を持つ
* 実装仕様と理論が分離された
* 将来 v1.3 で Execution Layer を差し替え可能
* 分散 Runner 実装・Streaming 実装も設計しやすい

---

# NodeFlow v1.2 の特徴

* 完全 I/O ベース
* Context 廃止
* version 廃止 → port 単位 revision
* status — 内部状態（予約フィールドではない）。read_status() で読む。制御は StructuralNode（§2.3, §6.7）
* **実行順序は nodes 配列順で決定**（§7.1.1）
* **revision は BaseNode が content-hash で自動生成**（§5.7）
* Pull 型実行
* revision は content-hash（同一出力→同一 revision）
* 明示終了
* 循環許可
* 最新出力のみ保持
* Param 明示
* limit は params 内
* 例外時は status = fatal（返り値ではない）
* Runner は dumb（status の意味は解釈しない。実行可能判定に参照する。§7.3）
* **状態は Node 内部**（read_status() で読む）
* **graceful stop 基本**（kill / interrupt なし）
* **Loop も Node**（LoopNode）

---

# 用語ポリシー（改訂）

1. **Flow は Core の抽象構造であり、Execution Layer のクラスではない。**
2. **Graph の意味論 ⟦G⟧ は Execution Layer では StructuralNode.execute により実現される。**
3. **PipelineNode は Graph を 1-shot 実行する StructuralNode の一種である。**
4. **LoopNode は Graph を反復実行する StructuralNode の一種である。**
5. **Runner は StructuralNode の内部補助機構であり、Graph の実行主体ではない。**
6. **「Flow 実行」という語は使用せず、「StructuralNode の execute 呼び出し」と記述する。**
