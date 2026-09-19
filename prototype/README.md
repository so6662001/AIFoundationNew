# 高保真原型（静态 HTML）

评审用原型，无需构建，浏览器直接打开 `index.html`。所有交互为前端模拟（无后端），用于确认信息架构、页面布局、节点与面板的字段设计。

| 页面 | 文件 | 演示要点 |
|---|---|---|
| 流程列表 | `index.html` | 统计卡、流程表（版本 / 草稿 / 成功率 / 最近运行）、平台状态 |
| 画布编辑器 | `editor.html` | 左侧节点面板（按注册中心分类，含语言标记 / Worker 离线态）；画布可拖拽节点、从连接桩拖出连线（校验成环 / 分支单出边）、`Delete` 删除、`Ctrl+滚轮` 缩放；右侧属性面板按节点类型切换：任务（输入映射 + 变量选择器 / 配置 / 重试超时三层默认 / 失败处理四策略 / 断点）、分支（规则构建器 + 历史上下文试算）、审批（组织架构选人 / 策略 / 表单 / 超时提醒）、for-each（items / 并发 / 分批 / 循环体 / 收集字段）；底部校验问题抽屉；DAG JSON / 发布 / 运行弹窗。顶部可切换「单供应商 Demo」与「全部供应商 · for-each」 |
| 运行监控 | `run.html` | 只读画布按状态着色、已执行边高亮；右侧运行概览 + 节点详情（概览 / 输入 / 输出 / 日志 / 错误 + 操作按钮）；底部实时日志流（节点 / 级别过滤）、时间线甘特、事件流；审批弹窗（通过后模拟 Signal 流转到报告与结束）；失败暂停处置弹窗；for-each item 列表下钻（`#foreach&items`） |
| 待审批 | `approvals.html` | 待办列表、审批详情、差异表、表单、企业微信卡片与通知记录、催办 / 转办 / 通过 / 驳回 |
| 版本管理 | `versions.html` | 版本时间线、当前 / 草稿 / 归档、回滚弹窗、结构化 Diff（业务可读摘要 + JSON 并排） |
| 企业配置 | `settings.html` | ERP 数据源 profile（直连库 / 接口 / 导出文件 + 字段映射表）、通知通道（企业微信默认 + 路由规则）、身份与组织架构（OIDC / 同步 / 角色映射）、LLM profile、对象存储（MinIO）、密钥引用 |

深链参数（便于评审定位）：

- `editor.html#select=<nodeId>&tab=<Tab名>`，如 `#select=approve_diff&tab=审批人`；`#flow=foreach` 切换到 for-each Demo
- `run.html#approve` 直接打开审批弹窗；`run.html#foreach&items` 打开 for-each item 列表
- `settings.html#s=notify|identity|llm|storage|cred`

原型数据与 `examples/*.dag.json` 同构（`assets/data.js`），画布渲染逻辑为轻量模拟（`assets/canvas.js`），正式实现使用 `@xyflow/react`。
