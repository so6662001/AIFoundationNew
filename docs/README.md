# AI 智能体底座（Agent Foundation）设计方案文档集

> 状态：**v0.1 待评审**。评审通过后按 [07 文档 §7 待确认决策点](./07-AI对账Demo与阶段计划.md) 的结论进入高保真原型 HTML 与开发。

| 编号 | 文档 | 内容 |
|---|---|---|
| 00 | [总体方案](./00-总体方案.md) | 目标、设计原则、总体架构、技术选型、核心概念、仓库结构、关键时序、阶段范围、坑规避对照、待确认决策点 |
| 01 | [DAG JSON 规范](./01-DAG-JSON规范.md) | 画布输出契约：顶层结构、节点类型（task / branch / approval / subflow / assign / parallel / join）、边、模板语法、规则语法、校验清单 |
| 02 | [原子节点注册 SDK 规范](./02-原子节点注册SDK规范.md) | `@node` 装饰器、`NodeContext`、硬性规则（幂等 / 输出限制 / 错误分类 / LLM 结构化）、NodeSpec、注册流程、测试工具、接入 Checklist |
| 03 | [DAG 解析器与 Temporal Workflow 设计](./03-DAG解析器与Temporal-Workflow设计.md) | 编译 → ExecutionPlan；`DagWorkflow` 解释器；执行 / 重试 / 断点 / 审批 Signal / 日志回传 / 上下文限制；确定性保障；子流程 |
| 04 | [前端画布方案](./04-前端画布方案.md) | React + xyflow 编辑器、自定义节点、属性面板（规则构建器 / 变量选择器）、运行监控面板、版本页、业务人员路径 |
| 05 | [中台后端与 API 设计](./05-中台后端与API设计.md) | 数据模型 DDL、REST/SSE API、版本管理语义、事件推送链路、开放 API、权限 |
| 06 | [运维部署与监控告警](./06-运维部署与监控告警.md) | docker-compose 开发集群、Temporal / PostgreSQL 配置、指标、告警规则、Grafana 看板、Runbook、生产迁移路径 |
| 07 | [AI 对账 Demo 与阶段计划](./07-AI对账Demo与阶段计划.md) | 对账原子能力、Demo 流程、测试矩阵、阶段 1/2 工作包、验收标准、风险、待确认决策点 |

机器可读产物：

- [`schemas/dag.schema.json`](../schemas/dag.schema.json) — DAG JSON Schema（draft 2020-12）
- [`schemas/node-spec.schema.json`](../schemas/node-spec.schema.json) — NodeSpec Schema
- [`examples/reconciliation-demo.dag.json`](../examples/reconciliation-demo.dag.json) — 对账 Demo DAG（已通过 Schema 校验）
- [`examples/node-spec.fetch_documents.json`](../examples/node-spec.fetch_documents.json) — 节点定义示例（已通过 Schema 校验）

## 一页纸总览

```
业务人员 ──画布(React+xyflow)──► DAG JSON ──校验/编译──► ExecutionPlan ──start_workflow──► Temporal
                                                                                            │
                 前端监控面板 ◄── SSE ◄── platform-api projector ◄── Redis Stream ◄── 事件 ◄── DagWorkflow(解释器, 只编排)
                                                                                            │ execute_activity
                                                                                     业务节点 Activities(所有 IO / LLM / ERP)
```

三条铁律：

1. Workflow 只编排（分支 / 等待 / 调 Activity），所有 IO 在 Activity。
2. 流程走向由 DAG 规则决定，LLM 只产出结构化字段。
3. 上下文只存小数据与 `ref://artifact/<id>` 引用。
