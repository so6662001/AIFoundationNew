# DAG JSON 规范 v1.0（React 画布输出契约 · 前后端统一）

> 机器可读版本：[`schemas/dag.schema.json`](../schemas/dag.schema.json)  
> 完整示例：[`examples/reconciliation-demo.dag.json`](../examples/reconciliation-demo.dag.json)  
> 约定：本文中 **MUST / SHOULD / MAY** 语义遵循 RFC 2119。

DAG JSON 是三方共同遵守的唯一契约：

1. **画布**：`toDag(nodes, edges)` 输出；`fromDag(dag)` 还原（含布局 `ui` 字段）。
2. **中台**：`PUT /flows/{id}/draft` 接收后做 Schema 校验 + 语义校验；发布时编译为 `ExecutionPlan`。
3. **引擎**：`DagWorkflow` 只消费编译后的 `ExecutionPlan`（DAG 的子集 + 绑定信息），不直接消费 UI 字段。

---

## 1. 顶层结构

```jsonc
{
  "schemaVersion": "1.0",                 // MUST，契约版本，后端据此选择校验器
  "flow": {
    "key": "recon.purchase_monthly",      // MUST，全局唯一、稳定的流程标识（开放 API 用）
    "name": "采购月度对账",
    "description": "拉取采购单与供应商对账单，识别差异，大额差异人工确认",
    "version": 3,                         // 由后端分配，草稿为 null
    "tags": ["对账", "采购"]
  },
  "variables": { ... },                   // 流程输入变量声明（JSON Schema 子集）
  "settings": { ... },                    // 流程级策略：超时、默认重试、上下文限制、任务队列
  "nodes": [ ... ],                       // 节点数组
  "edges": [ ... ],                       // 有向边数组
  "ui": { "viewport": { "x": 0, "y": 0, "zoom": 1 } }   // 纯画布信息，引擎忽略
}
```

### 1.1 `variables`（流程输入声明）

```jsonc
"variables": {
  "type": "object",
  "required": ["period", "supplier_id"],
  "properties": {
    "period":      { "type": "string", "title": "对账期间", "pattern": "^\\d{4}-\\d{2}$", "default": "2026-08" },
    "supplier_id": { "type": "string", "title": "供应商编码" },
    "threshold":   { "type": "number", "title": "大额差异阈值(元)", "default": 10000 }
  }
}
```

- 运行时 `POST /runs` 的 `input` MUST 通过该 Schema 校验；校验后的值以 `{{input.xxx}}` 访问。
- 只允许 JSON Schema 子集：`type / title / description / default / enum / pattern / minimum / maximum / items / properties / required`。

### 1.2 `settings`

```jsonc
"settings": {
  "taskQueue": "nodes-recon",            // 默认 Activity 任务队列；节点可覆盖
  "runTimeout": "PT2H",                  // Workflow 执行超时，ISO-8601 Duration
  "defaultRetryPolicy": { "maximumAttempts": 3, "initialInterval": "PT2S", "backoffCoefficient": 2.0, "maximumInterval": "PT1M" },
  "defaultTimeouts": { "startToClose": "PT5M", "scheduleToClose": "PT30M" },
  "context": { "softLimitBytes": 262144, "hardLimitBytes": 1048576 },
  "onNodeError": "pause"                 // 节点重试耗尽后的默认行为：pause | fail
}
```

---

## 2. 节点（`nodes[]`）

### 2.1 通用字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `id` | string | MUST | 节点唯一 ID，`^[a-zA-Z][a-zA-Z0-9_]{0,63}$`；同时是 `{{nodes.<id>.output}}` 的引用键。**画布生成后不可变** |
| `kind` | enum | MUST | `start` / `end` / `task` / `branch` / `approval` / `subflow` / `assign` / `foreach` / `parallel` / `join` |
| `name` | string | MUST | 显示名 |
| `description` | string | MAY | |
| `position` | `{x,y}` | MUST | 画布坐标（引擎忽略） |
| `outputKey` | string | SHOULD | 输出写入 `context.<outputKey>`；缺省等于 `id` |
| `breakpoint` | boolean | MAY | 到达前暂停，等待 `resume` Signal（默认 `false`） |
| `ui` | object | MAY | 颜色、折叠、备注等，引擎忽略 |

### 2.2 `task`（原子能力节点，映射 Temporal Activity）

```jsonc
{
  "id": "fetch_docs",
  "kind": "task",
  "name": "拉取单据",
  "position": { "x": 260, "y": 120 },
  "nodeType": "recon.fetch_documents@1",     // MUST：注册中心中的 type@majorVersion
  "inputs": {                                 // 按 NodeSpec.inputSchema 映射，值支持模板
    "period":      "{{input.period}}",
    "supplier_id": "{{input.supplier_id}}",
    "sources":     ["erp_po", "supplier_statement"]
  },
  "config": {                                 // 按 NodeSpec.configSchema，节点静态配置，不支持模板
    "page_size": 500
  },
  "outputKey": "docs",
  "taskQueue": "nodes-recon",                 // MAY，覆盖 settings.taskQueue
  "retryPolicy": {                            // MAY，覆盖 NodeSpec 默认 / settings 默认
    "maximumAttempts": 5,
    "initialInterval": "PT2S",
    "backoffCoefficient": 2.0,
    "maximumInterval": "PT2M",
    "nonRetryableErrorTypes": ["ValidationError"]
  },
  "timeouts": { "startToClose": "PT10M", "heartbeat": "PT30S" },
  "onError": {                                // MAY，重试耗尽后的行为
    "action": "pause",                        // pause | fail | skip | route
    "skipOutput": null,                       // action=skip 时写入 outputKey 的值
    "routeEdgeId": null                       // action=route 时走的出边（错误分支）
  },
  "breakpoint": false
}
```

- `inputs` 的每个叶子值 MAY 为模板字符串（见 §4），渲染发生在 Workflow 内（纯函数）。
- `config` 是设计期常量，编译时校验类型，运行时原样传给 Activity。
- 编译期校验：`nodeType` 存在且未废弃；`inputs` 渲染后的静态部分符合 `inputSchema`（模板占位处按类型放行）；`retryPolicy` 不得超过 NodeSpec 声明的 `maxAllowedAttempts`（可选）。

### 2.3 `branch`（分支判断，Workflow 内纯函数求值）

```jsonc
{
  "id": "is_big_diff",
  "kind": "branch",
  "name": "是否大额差异",
  "position": { "x": 720, "y": 120 },
  "cases": [
    {
      "id": "big",                                  // 对应出边的 sourceHandle
      "label": "大额差异",
      "when": {                                     // 结构化规则（推荐，业务人员可编辑）
        "all": [
          { "left": "{{context.diff.total_abs_amount}}", "op": "gt", "right": "{{input.threshold}}" },
          { "left": "{{context.diff.count}}", "op": "gt", "right": 0 }
        ]
      }
    },
    {
      "id": "small",
      "label": "小额差异",
      "when": { "any": [
        { "left": "{{context.diff.count}}", "op": "gt", "right": 0 }
      ] }
    }
  ],
  "default": "none",                                 // MUST：无 case 命中时走的 handle
  "evaluation": "first_match"                        // first_match（默认）| all_match（多路并发）
}
```

**规则语法（RuleExpr）**

```
RuleExpr  := { "all": [RuleExpr...] } | { "any": [RuleExpr...] } | { "not": RuleExpr } | Cmp
Cmp       := { "left": Value, "op": Op, "right": Value }
Op        := eq | ne | gt | gte | lt | lte | in | not_in | contains | starts_with | ends_with
           | is_null | not_null | is_true | is_false | matches   // matches: 受限正则（RE2 语法）
Value     := JSON 字面量 | 模板字符串（单占位符时保留原类型，不转字符串）
```

- 求值器是 **纯函数**（无时间、随机、IO），前端（TS）与后端（Python）各有一份实现，并共享同一份 **规则测试向量**（`packages/dag-core/tests/rule_vectors.json`）保证语义一致。
- 高级用户 MAY 使用 `"expr": "context.diff.total_abs_amount > input.threshold && ..."`（受限表达式，白名单：算术、比较、逻辑、`len()`、`abs()`、`round()`），编译期做语法与引用检查。**不允许**函数调用之外的任何标识符。

### 2.4 `approval`（人工审批，Temporal Signal）

```jsonc
{
  "id": "approve_diff",
  "kind": "approval",
  "name": "运营确认大额差异",
  "position": { "x": 980, "y": 40 },
  "assignees": {                                            // 审批人来自组织架构（决策 4），多来源取并集
    "roles": ["recon_ops"],                                 // 角色（同步自 IdP / 平台定义）
    "departments": [{ "id": "D-FIN-AP", "includeChildren": false }],   // 部门（成员均可审批）
    "users": ["u_10023"],                                   // 指定人
    "dynamic": ["manager_of_initiator", "{{input.owner_user_id}}"],    // 动态：发起人上级 / 上下文中的用户 ID
    "strategy": "any"                                       // any：任一人通过即通过 | all：全部通过 | quorum(n)
  },
  "title": "供应商 {{input.supplier_id}} {{input.period}} 对账差异确认",
  "summary": "差异 {{context.diff.count}} 笔，金额 {{context.diff.total_abs_amount}} 元；明细见附件",
  "attachments": ["{{context.diff.detail_ref}}"],          // artifact 引用，前端可预览
  "formSchema": {                                           // 审批人需填写的字段
    "type": "object",
    "properties": { "comment": { "type": "string", "title": "备注" } }
  },
  "timeout": "P3D",                                          // 审批超时
  "onTimeout": "route",                                      // route（走 timeout 出边）| fail | approve | reject
  "reminder": { "every": "PT12H", "channel": "wecom" },      // 可选提醒
  "outputKey": "approval"
}
```

- 出边 `sourceHandle` MUST 为 `approved` / `rejected`，`onTimeout=route` 时还需 `timeout`。
- `assignees` 在**运行时由 platform-api 解析为用户集合**（Workflow 只持有表达式并等待 Signal；解析属于 IO，在 API 侧完成），审批动作校验操作人属于该集合；`strategy=all/quorum` 时 Workflow 累计多个 `approve` Signal 后再出边。
- 输出：`{ "decision": "approved|rejected|timeout", "operator": "...", "operators": [...], "comment": "...", "form": {...}, "decided_at": "..." }`。

### 2.5 `subflow`（子流程，Child Workflow）

```jsonc
{
  "id": "notify_supplier",
  "kind": "subflow",
  "name": "供应商通知子流程",
  "position": { "x": 1240, "y": 40 },
  "flowKey": "common.notify_supplier",
  "flowVersion": "published",            // "published"（发布时锁定为具体版本号）| 具体整数版本
  "inputs": { "supplier_id": "{{input.supplier_id}}", "report_ref": "{{context.report.ref}}" },
  "parentClosePolicy": "terminate",      // terminate | abandon | request_cancel
  "outputKey": "notify"
}
```

- 发布父流程时，`flowVersion: "published"` 被解析并**固化**为整数版本写入 ExecutionPlan，保证确定性。

### 2.6 `assign`（上下文赋值，Workflow 内纯函数）

```jsonc
{
  "id": "set_flags",
  "kind": "assign",
  "name": "设置标记",
  "position": { "x": 500, "y": 300 },
  "assignments": [
    { "target": "context.review_required", "value": true },
    { "target": "context.summary_text", "value": "{{input.supplier_id}} 差异 {{context.diff.count}} 笔" }
  ]
}
```

### 2.7 `foreach`（动态循环，阶段 2）

对上下文中的**数组**逐项执行循环体。循环体是一个 `task`（单节点）或一个 `subflow`（子流程，Child Workflow）。

```jsonc
{
  "id": "per_supplier",
  "kind": "foreach",
  "name": "按供应商逐个对账",
  "position": { "x": 500, "y": 200 },
  "items": "{{context.suppliers.ids}}",          // MUST：渲染后必须是数组；建议只放 ID / 小对象
  "itemAlias": "supplier",                        // 循环体内以 {{item}} 或 {{supplier}} 引用当前元素；{{loop.index}} 为下标
  "concurrency": 5,                               // 并发上限（默认 1 = 串行）
  "maxItems": 1000,                               // 超过则编译/运行期拒绝（坑 3 防线）
  "batchSize": 200,                               // 每处理 batchSize 个 item 后 continue-as-new（History 防线）
  "body": {
    "type": "subflow",                            // task | subflow
    "flowKey": "recon.single_supplier",
    "flowVersion": "published",
    "inputs": { "supplier_id": "{{item}}", "period": "{{input.period}}", "threshold": "{{input.threshold}}" }
    // type=task 时：{ "type": "task", "nodeType": "recon.match_and_diff@1", "inputs": {...}, "config": {...}, "retryPolicy": {...}, "timeouts": {...} }
  },
  "onItemError": "continue",                      // continue（记录失败，继续其他 item）| fail_fast（取消其余，节点失败进入 onError）
  "collect": {
    "mode": "list",                               // list：每个 item 的输出组成数组 | count：只统计 | none：不写回
    "fields": ["diff_count", "total_abs_amount", "report_ref"],   // list 模式下只收集这些字段（防上下文膨胀）
    "outputKey": "supplier_results"
  },
  "onError": { "action": "pause" }
}
```

- 输出结构：`{ total, succeeded, failed, items: [ { index, item, status, output(仅 collect.fields), error } ] }`。`collect.mode=list` 时若累计大小超过 64 KB，Workflow 侧只保留 `count` 并把完整结果交给 Local Activity 落 artifact，写入 `items_ref`。
- 循环体内模板作用域新增：`{{item}}` / `{{<itemAlias>}}`、`{{loop.index}}`、`{{loop.total}}`；仍可访问 `input` / `context`（只读快照，循环体不得写父上下文，避免并发写冲突）。
- 出边：`out`（全部 item 处理完毕，含 `continue` 模式下的部分失败）；`onError.action=route` 时 `error`。
- 画布上表现为一个可展开的容器节点，内部显示循环体（单任务卡片或子流程引用）。
- 校验：`items` 只能引用 `input` / `context` / 前驱节点输出；`body.type=subflow` 时子流程必须存在已发布版本；`concurrency ≤ 50`。

### 2.8 `parallel` / `join`（静态并行分叉与汇合）

- `parallel` 节点 MAY 有多条出边，全部同时启动；`join` 节点等待所有入边完成（`strategy: all | any`）。
- 实际上引擎按拓扑天然支持"一个节点多条出边即并行"，`parallel/join` 主要用于画布语义清晰与 `join.strategy=any` 场景。

### 2.9 `start` / `end`

- 恰好一个 `start`（无入边），至少一个 `end`。
- `end` MAY 声明 `outputs`（模板），作为 Workflow 返回值与开放 API 回调载荷：

```jsonc
{ "id": "end_ok", "kind": "end", "name": "完成", "position": {...},
  "status": "success",                        // success | failed（业务失败终止）
  "outputs": { "report_ref": "{{context.report.ref}}", "diff_count": "{{context.diff.count}}" } }
```

---

## 3. 边（`edges[]`）

```jsonc
{
  "id": "e_branch_big",
  "source": "is_big_diff",
  "sourceHandle": "big",           // branch 的 case id / approval 的 approved|rejected|timeout / 普通节点为 "out" 或省略
  "target": "approve_diff",
  "targetHandle": "in",            // 普通节点固定 "in"
  "label": "大额差异",
  "ui": { "type": "smoothstep" }
}
```

约束：

- `source` / `target` MUST 指向存在的节点；`start` 无入边，`end` 无出边。
- `branch` 的每个 `case.id` 与 `default` MUST 各有且仅有一条出边（`evaluation=first_match`）。
- `approval` MUST 有 `approved` 与 `rejected` 出边。
- 图 MUST 无环（动态循环由 `foreach` 节点承担，DAG 本身保持无环；`while` 型循环不在阶段 1/2 范围）。
- `task` 节点 `onError.action=route` 时，`routeEdgeId` 指向的出边 `sourceHandle` MUST 为 `error`。

---

## 4. 模板语法（变量透传）

| 形式 | 含义 | 示例 |
|---|---|---|
| `{{input.<path>}}` | 流程输入 | `{{input.period}}` |
| `{{context.<path>}}` | 上下文（各节点按 `outputKey` 写入） | `{{context.diff.total_abs_amount}}` |
| `{{nodes.<nodeId>.output.<path>}}` | 按节点 ID 直接引用输出（等价于 outputKey） | `{{nodes.fetch_docs.output.batch_id}}` |
| `{{run.id}}` / `{{run.flowKey}}` / `{{run.version}}` / `{{run.startedAt}}` | 运行元信息（`startedAt` 来自 `Workflow.currentTimeMillis()`，确定性） | |
| `{{item}}` / `{{<itemAlias>}}` / `{{loop.index}}` / `{{loop.total}}` | 仅 `foreach` 循环体内有效 | `{{supplier}}` |
| `{{secrets.<name>}}` | **不允许**在 DAG 中出现；密钥只在 Activity 侧按 `config.credentialRef` 解析 | |

规则：

1. `<path>` 为点分路径 + 数组下标：`a.b[0].c`。
2. **单占位符且无其他字符**时保留原 JSON 类型（数字 / 布尔 / 对象 / 数组）；否则做字符串拼接。
3. 引用不存在的路径 → 编译期告警（无法静态确定时）/ 运行期按 `settings.templateMissing`：`error`（默认）| `null` | `empty`。
4. 渲染函数 `render(template, scope)` 为纯函数，在 Workflow 内执行；不提供过滤器 / 函数调用（避免引入非确定性与复杂度）。需要格式化的场景放到节点内部或 `assign` 节点。
5. 引用 artifact：值形如 `"ref://artifact/<uuid>"` 的字符串按原样透传；Activity 侧用 `ctx.artifacts.get(ref)` 取回。

---

## 5. 校验规则清单（前端即时 + 后端强制）

| 级别 | 规则 |
|---|---|
| error | Schema 不通过；节点 ID 重复 / 非法；边引用不存在节点；存在环；不止一个 `start`；无 `end` |
| error | 存在不可达节点（从 `start` 出发）；存在无法到达任何 `end` 的节点 |
| error | `branch` 缺 `default` 或某 `case`/`default` 没有唯一出边；规则引用了非法运算符或非 `input/context/nodes/run` 作用域 |
| error | `approval` 缺 `approved`/`rejected` 出边；`assignees` 为空或引用不存在的角色 / 部门 |
| error | `foreach.items` 不是数组表达式；`body.type=subflow` 的子流程无已发布版本；`concurrency` > 50；循环体模板写父 `context` |
| error | `task.nodeType` 不存在 / 已废弃 / 主版本不匹配；`inputs` 缺少 `inputSchema.required` 字段 |
| error | 模板引用了在拓扑上**不可能先于当前节点完成**的节点输出 |
| warning | 未连接的节点；`task` 无 `outputKey`；重试次数 > 10；审批无超时 |
| warning | `end.outputs` 引用了可能为 artifact 的大字段 |

后端返回格式：

```json
{ "valid": false,
  "issues": [
    { "level": "error", "code": "BRANCH_MISSING_DEFAULT_EDGE", "nodeId": "is_big_diff", "message": "分支节点缺少 default 出边" },
    { "level": "warning", "code": "TEMPLATE_UNRESOLVED", "nodeId": "approve_diff", "path": "summary", "message": "无法静态确认 context.diff.count 存在" }
  ] }
```

---

## 6. 版本兼容策略

- `schemaVersion` 采用 `major.minor`；新增可选字段升 minor，破坏性变更升 major。
- 后端保留每个 major 的校验器与迁移器（`migrate_1_to_2(dag)`），打开旧草稿时自动迁移并提示。
- `ExecutionPlan` 随 run 快照存储，DAG 契约升级不影响已运行实例。
