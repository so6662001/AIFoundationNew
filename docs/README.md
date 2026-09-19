# AI 智能体底座（Agent Foundation）设计方案文档集

> 状态：**v0.2 — 决策点已确认（Java / 三种 ERP 接入 / 企业微信可配置 / SSO 组织架构 / MinIO / for-each 提前）**。高保真原型见 [`prototype/overview.html`](../prototype/overview.html)。

| 编号 | 文档 | 内容 |
|---|---|---|
| 00 | [总体方案](./00-总体方案.md) | 已确认决策、目标、设计原则、总体架构、技术选型（Java）、核心概念、仓库结构、关键时序、阶段范围、坑规避对照 |
| 01 | [DAG JSON 规范](./01-DAG-JSON规范.md) | 画布输出契约：节点类型（task / branch / approval / subflow / assign / **foreach** / parallel / join）、审批人模型、边、模板语法、规则语法、校验清单 |
| 02 | [原子节点注册 SDK 规范](./02-原子节点注册SDK规范.md) | Java `@AgentNode` + `NodeContext`、Spring Boot starter、硬性规则、**跨语言契约**、Python LLM 节点、现有 Java 服务接入方式、**ERP 三种接入 `DocumentSource` SPI**、测试工具、Checklist |
| 03 | [DAG 解析器与 Temporal Workflow 设计](./03-DAG解析器与Temporal-Workflow设计.md) | 编译 → ExecutionPlan；Java `DagWorkflowImpl`；执行 / 重试 / 断点 / 审批 Signal / **for-each** / 日志回传 / 上下文限制；Java 确定性保障（ArchUnit + Replay）；子流程 |
| 04 | [前端画布方案](./04-前端画布方案.md) | React + xyflow 编辑器、自定义节点（含 ForEach 容器）、属性面板（规则构建器 / 变量选择器 / 审批人选择器）、运行监控面板、版本页、企业配置页 |
| 05 | [中台后端与 API 设计](./05-中台后端与API设计.md) | Spring Boot 模块、数据模型 DDL（含组织架构 / 通知 / profiles）、REST/SSE API、版本管理语义、事件推送、**身份与组织架构**、**通知中心**、开放 API |
| 06 | [运维部署与监控告警](./06-运维部署与监控告警.md) | docker-compose 开发集群（Temporal / PG / Redis / **MinIO** / **Keycloak**）、指标、告警规则 → 可配置通道、Grafana 看板、Runbook、生产迁移路径 |
| 07 | [AI 对账 Demo 与阶段计划](./07-AI对账Demo与阶段计划.md) | 对账原子能力、ERP 三种接入、Demo 流程（单供应商 + for-each）、测试矩阵 T1–T21、工作包、验收标准、风险、决策结论 |

机器可读产物：

- [`schemas/dag.schema.json`](../schemas/dag.schema.json) — DAG JSON Schema（draft 2020-12，含 `foreach`、审批人模型）
- [`schemas/node-spec.schema.json`](../schemas/node-spec.schema.json) — NodeSpec Schema
- [`examples/reconciliation-demo.dag.json`](../examples/reconciliation-demo.dag.json) — 单供应商对账 Demo DAG
- [`examples/reconciliation-foreach.dag.json`](../examples/reconciliation-foreach.dag.json) — 全部供应商 for-each Demo DAG
- [`examples/node-spec.fetch_documents.json`](../examples/node-spec.fetch_documents.json) — 节点定义示例

高保真原型（静态 HTML，无需构建，浏览器直接打开）：

| 页面 | 文件 | 内容 |
|---|---|---|
| **原型总览** | [`prototype/overview.html`](../prototype/overview.html) | 所有页面实时缩略 + 说明 + 深链（评审入口） |
| 流程列表 | [`prototype/index.html`](../prototype/index.html) | 流程卡片、版本状态、最近运行 |
| 运行列表 | [`prototype/runs.html`](../prototype/runs.html) | 筛选、节点进度、子运行、一键重试 / 审批 |
| 流程度量 | [`prototype/metrics.html`](../prototype/metrics.html) | 耗时 / 失败率 / 审批时长 / 分支分布 / 热力图 |
| 节点注册中心 | [`prototype/registry.html`](../prototype/registry.html) | 节点类型、Schema、Worker、新增节点指引 |
| 画布编辑器 | [`prototype/editor.html`](../prototype/editor.html) | 节点面板 / 可拖拽画布 / 属性面板（分支规则、审批人、for-each、重试）/ 校验 / DAG JSON |
| 运行监控 | [`prototype/run.html`](../prototype/run.html) | 节点状态着色、实时日志流、IO 查看、断点 / 重试 / 跳过、审批弹窗、for-each item 下钻、时间线 |
| 待审批 | [`prototype/approvals.html`](../prototype/approvals.html) | 待办列表、审批详情、表单、企业微信卡片预览 |
| 版本管理 | [`prototype/versions.html`](../prototype/versions.html) | 版本列表、Diff、发布 / 回滚 |
| 企业配置 | [`prototype/settings.html`](../prototype/settings.html) | ERP 数据源 profile（三种接入）、通知通道、身份源、LLM |

## 一页纸总览

```
业务人员 ──画布(React+xyflow)──► DAG JSON ──校验/编译──► ExecutionPlan ──WorkflowClient.start──► Temporal
                                                                                              │
                  前端监控面板 ◄── SSE ◄── platform-api(Java) projector ◄── Redis Stream ◄── 事件 ◄── DagWorkflowImpl(Java 解释器, 只编排)
                                                                                              │ Activity（按 task queue）
                                                        Java 节点（对账 / AI 采购 / AI 销售 内嵌 Worker） · Python LLM 节点
                                                                     │ DocumentSource SPI（直连库 / 接口 / 文件） · MinIO · LLM profile
```

三条铁律：

1. Workflow 只编排（分支 / 循环 / 等待 / 调 Activity），所有 IO 在 Activity。
2. 流程走向由 DAG 规则决定，LLM 只产出结构化字段。
3. 上下文只存小数据与 `ref://artifact/<id>` 引用（MinIO）。
