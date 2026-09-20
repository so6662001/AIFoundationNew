/* 原型演示数据（与 examples/*.dag.json 同构） */
window.AF = window.AF || {};

AF.nodeTypes = [
  // 流程控制（内建 kind）
  { kind: "start",    name: "开始",   category: "流程控制", abbr: "S" },
  { kind: "end",      name: "结束",   category: "流程控制", abbr: "E" },
  { kind: "branch",   name: "分支判断", category: "流程控制", abbr: "IF" },
  { kind: "approval", name: "人工审批", category: "流程控制", abbr: "AP" },
  { kind: "foreach",  name: "循环 for-each", category: "流程控制", abbr: "FE" },
  { kind: "assign",   name: "赋值", category: "流程控制", abbr: "=" },
  { kind: "subflow",  name: "子流程", category: "流程控制", abbr: "SF" },
  // 注册中心节点
  { kind: "task", type: "recon.fetch_documents@1", version: "1.2.0", name: "拉取单据", category: "对账", abbr: "DB", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { period: "string", supplier_id: "string", sources: "string[]" }, config: { page_size: 500, source_profile: "erp_dev" },
    outputs: ["batches", "counts", "total_amount"], retry: { maximumAttempts: 5, initialInterval: "PT2S", backoffCoefficient: 2, maximumInterval: "PT2M" }, timeouts: { startToClose: "PT10M", heartbeat: "PT30S" } },
  { kind: "task", type: "recon.match_and_diff@1", version: "1.0.3", name: "对账差异识别", category: "对账", abbr: "Δ", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { erp_batch_ref: "ref", statement_batch_ref: "ref", amount_tolerance: "number" }, config: { match_keys: ["po_no", "invoice_no"], date_tolerance_days: 3 },
    outputs: ["matched_count", "count", "total_abs_amount", "by_type", "detail_ref"], retry: { maximumAttempts: 3, initialInterval: "PT2S", backoffCoefficient: 2, maximumInterval: "PT1M" }, timeouts: { startToClose: "PT15M", heartbeat: "PT30S" } },
  { kind: "task", type: "recon.classify_diff@1", version: "1.1.0", name: "差异分类", category: "对账", abbr: "CL", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { diff_detail_ref: "ref", mode: "rules|llm|hybrid" }, config: { llm_profile: "default", max_items_for_llm: 200 },
    outputs: ["categories", "llm_used", "confidence_avg", "classified_ref"], retry: { maximumAttempts: 3, initialInterval: "PT5S", backoffCoefficient: 2, maximumInterval: "PT1M" }, timeouts: { startToClose: "PT20M", heartbeat: "PT1M" } },
  { kind: "task", type: "recon.generate_report@1", version: "1.0.0", name: "生成对账报告", category: "对账", abbr: "RP", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { period: "string", supplier_id: "string", diff_summary: "object", classified: "object", approval: "object", diff_detail_ref: "ref" }, config: { format: "xlsx" },
    outputs: ["ref", "summary_text"], retry: { maximumAttempts: 3, initialInterval: "PT2S", backoffCoefficient: 2, maximumInterval: "PT1M" }, timeouts: { startToClose: "PT5M" } },
  { kind: "task", type: "recon.list_suppliers@1", version: "1.0.0", name: "列出本期供应商", category: "对账", abbr: "LS", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { period: "string" }, config: { source_profile: "erp_dev" }, outputs: ["ids", "count"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT5M" } },
  { kind: "task", type: "recon.generate_summary_report@1", version: "1.0.0", name: "生成汇总报告", category: "对账", abbr: "ΣR", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { period: "string", results: "object" }, config: { format: "xlsx" }, outputs: ["ref"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT5M" } },
  { kind: "task", type: "purchase.fetch_po_status@1", version: "2.3.1", name: "查询采购单状态", category: "AI 采购", abbr: "PO", lang: "java", queue: "nodes-purchase", workers: 3,
    inputs: { po_no: "string" }, config: {}, outputs: ["status", "received_qty", "invoice_qty"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT2M" } },
  { kind: "task", type: "purchase.create_debit_note@1", version: "1.0.0", name: "创建借项通知单", category: "AI 采购", abbr: "DN", lang: "java", queue: "nodes-purchase", workers: 3, sideEffect: true,
    inputs: { supplier_id: "string", amount: "string", reason: "string" }, config: {}, outputs: ["debit_note_no"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT2M" } },
  { kind: "task", type: "sales.fetch_receipts@1", version: "1.4.0", name: "拉取回款记录", category: "AI 销售", abbr: "RC", lang: "java", queue: "nodes-sales", workers: 0,
    inputs: { period: "string", customer_id: "string" }, config: {}, outputs: ["batch_ref", "count"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT5M" } },
  { kind: "task", type: "common.http_request@1", version: "1.0.0", name: "HTTP 请求", category: "通用", abbr: "HT", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { method: "string", url: "string", body: "object" }, config: { credential_ref: "" }, outputs: ["status", "body_ref"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT1M" } },
  { kind: "task", type: "common.sql_query@1", version: "1.0.0", name: "只读 SQL 查询", category: "通用", abbr: "SQ", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { sql: "string", params: "object" }, config: { source_profile: "erp_dev", max_rows: 5000 }, outputs: ["rows_ref", "row_count"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT5M" } },
  { kind: "task", type: "common.file_import@1", version: "1.0.0", name: "文件导入", category: "通用", abbr: "FI", lang: "java", queue: "nodes-recon", workers: 2,
    inputs: { file_ref: "ref" }, config: { source_profile: "supplier_files" }, outputs: ["batch_ref", "count"], retry: { maximumAttempts: 2 }, timeouts: { startToClose: "PT5M" } },
  { kind: "task", type: "common.notify@1", version: "1.0.0", name: "发送通知", category: "通用", abbr: "NT", lang: "java", queue: "nodes-recon", workers: 2, sideEffect: true,
    inputs: { channel: "string", to: "string[]", template: "string", params: "object" }, config: {}, outputs: ["message_id"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT1M" } },
  { kind: "task", type: "llm.classify_text@1", version: "1.0.0", name: "文本分类 (LLM)", category: "LLM", abbr: "AI", lang: "python", queue: "nodes-llm", workers: 1,
    inputs: { text_ref: "ref", categories: "string[]" }, config: { llm_profile: "default", max_tokens: 2000 }, outputs: ["category", "confidence", "rationale_ref"], retry: { maximumAttempts: 3, initialInterval: "PT5S" }, timeouts: { startToClose: "PT5M", heartbeat: "PT1M" } },
  { kind: "task", type: "llm.extract@1", version: "1.2.0", name: "结构化抽取 (LLM)", category: "LLM", abbr: "AI", lang: "python", queue: "nodes-llm", workers: 1,
    inputs: { text_ref: "ref", schema: "object" }, config: { llm_profile: "default" }, outputs: ["extracted", "confidence"], retry: { maximumAttempts: 3 }, timeouts: { startToClose: "PT5M" } },
];

AF.typeOf = function (type) { return AF.nodeTypes.find(function (t) { return t.type === type; }); };

/* ---------- Demo DAG：采购月度对账（单供应商） ---------- */
AF.dagDemo = {
  schemaVersion: "1.0",
  flow: { key: "recon.purchase_monthly", name: "采购月度对账", version: 3, description: "拉取采购单与供应商对账单 -> 匹配并识别差异 -> 差异分类 -> 大额差异人工确认 -> 生成对账报告", tags: ["对账", "采购"] },
  variables: { type: "object", required: ["period", "supplier_id"], properties: {
    period: { type: "string", title: "对账期间", pattern: "^\\d{4}-\\d{2}$", default: "2026-08" },
    supplier_id: { type: "string", title: "供应商编码", default: "SUP-0001" },
    threshold: { type: "number", title: "大额差异阈值(元)", default: 10000 } } },
  settings: { taskQueue: "nodes-recon", runTimeout: "P7D", onNodeError: "pause", defaultRetryPolicy: { maximumAttempts: 3, initialInterval: "PT2S", backoffCoefficient: 2.0, maximumInterval: "PT1M" }, context: { softLimitBytes: 262144, hardLimitBytes: 1048576 } },
  nodes: [
    { id: "start", kind: "start", name: "开始", position: { x: 40, y: 250 } },
    { id: "fetch_docs", kind: "task", name: "拉取单据", position: { x: 170, y: 216 }, nodeType: "recon.fetch_documents@1",
      inputs: { period: "{{input.period}}", supplier_id: "{{input.supplier_id}}", sources: ["erp_po", "supplier_statement"] }, config: { page_size: 500, source_profile: "erp_dev" }, outputKey: "docs",
      retryPolicy: { maximumAttempts: 5, initialInterval: "PT2S", backoffCoefficient: 2.0, maximumInterval: "PT2M", nonRetryableErrorTypes: ["ValidationError"] }, timeouts: { startToClose: "PT10M", heartbeat: "PT30S" }, onError: { action: "pause" } },
    { id: "match_diff", kind: "task", name: "对账差异识别", position: { x: 410, y: 216 }, nodeType: "recon.match_and_diff@1",
      inputs: { erp_batch_ref: "{{context.docs.batches.erp_po}}", statement_batch_ref: "{{context.docs.batches.supplier_statement}}", amount_tolerance: 0.01 }, config: { match_keys: ["po_no", "invoice_no"], date_tolerance_days: 3 }, outputKey: "diff",
      timeouts: { startToClose: "PT15M", heartbeat: "PT30S" }, onError: { action: "pause" } },
    { id: "classify_diff", kind: "task", name: "差异分类（规则/LLM）", position: { x: 650, y: 216 }, nodeType: "recon.classify_diff@1",
      inputs: { diff_detail_ref: "{{context.diff.detail_ref}}", mode: "rules" }, config: { llm_profile: "default", max_items_for_llm: 200 }, outputKey: "classified",
      retryPolicy: { maximumAttempts: 3, initialInterval: "PT5S", backoffCoefficient: 2.0, maximumInterval: "PT1M", nonRetryableErrorTypes: ["ValidationError", "BudgetExceeded"] }, timeouts: { startToClose: "PT20M", heartbeat: "PT1M" },
      onError: { action: "skip", skipOutput: { categories: {}, skipped: true } }, breakpoint: true },
    { id: "is_big_diff", kind: "branch", name: "是否大额差异", position: { x: 890, y: 210 },
      cases: [
        { id: "big", label: "大额差异", when: { all: [ { left: "{{context.diff.count}}", op: "gt", right: 0 }, { left: "{{context.diff.total_abs_amount}}", op: "gt", right: "{{input.threshold}}" } ] } },
        { id: "small", label: "小额差异", when: { left: "{{context.diff.count}}", op: "gt", right: 0 } } ],
      default: "none", evaluation: "first_match" },
    { id: "approve_diff", kind: "approval", name: "运营确认大额差异", position: { x: 1160, y: 30 },
      assignees: { roles: ["recon_ops"], departments: [{ id: "D-FIN-AP", name: "财务共享中心 / 应付组", includeChildren: false }], users: [], dynamic: ["manager_of_initiator"], strategy: "any" },
      title: "供应商 {{input.supplier_id}} {{input.period}} 对账差异确认",
      summary: "差异 {{context.diff.count}} 笔，差异金额合计 {{context.diff.total_abs_amount}} 元（阈值 {{input.threshold}}）。分类：{{context.classified.categories}}",
      attachments: ["{{context.diff.detail_ref}}"],
      formSchema: { type: "object", properties: { comment: { type: "string", title: "处理意见" }, adjustment: { type: "string", title: "调整方式", enum: ["以ERP为准", "以供应商为准", "下期处理"] } } },
      timeout: "P3D", onTimeout: "route", reminder: { every: "PT12H", channel: "wecom-default" }, outputKey: "approval" },
    { id: "set_auto_pass", kind: "assign", name: "标记自动通过", position: { x: 1160, y: 262 }, assignments: [ { target: "context.approval", value: { decision: "auto_approved", operator: "system" } } ] },
    { id: "set_no_diff", kind: "assign", name: "标记无差异", position: { x: 1160, y: 400 }, assignments: [ { target: "context.approval", value: { decision: "no_diff", operator: "system" } } ] },
    { id: "gen_report", kind: "task", name: "生成对账报告", position: { x: 1450, y: 216 }, nodeType: "recon.generate_report@1",
      inputs: { period: "{{input.period}}", supplier_id: "{{input.supplier_id}}", diff_summary: "{{context.diff}}", classified: "{{context.classified}}", approval: "{{context.approval}}", diff_detail_ref: "{{context.diff.detail_ref}}" }, config: { format: "xlsx" }, outputKey: "report", timeouts: { startToClose: "PT5M" } },
    { id: "end_rejected", kind: "end", name: "驳回结束", position: { x: 1480, y: 40 }, status: "failed", outputs: { reason: "运营驳回", approval: "{{context.approval}}" } },
    { id: "end_ok", kind: "end", name: "完成", position: { x: 1720, y: 250 }, status: "success", outputs: { report_ref: "{{context.report.ref}}", diff_count: "{{context.diff.count}}", decision: "{{context.approval.decision}}" } }
  ],
  edges: [
    { id: "e1", source: "start", sourceHandle: "out", target: "fetch_docs" },
    { id: "e2", source: "fetch_docs", sourceHandle: "out", target: "match_diff" },
    { id: "e3", source: "match_diff", sourceHandle: "out", target: "classify_diff" },
    { id: "e4", source: "classify_diff", sourceHandle: "out", target: "is_big_diff" },
    { id: "e5_big", source: "is_big_diff", sourceHandle: "big", target: "approve_diff", label: "大额差异" },
    { id: "e5_small", source: "is_big_diff", sourceHandle: "small", target: "set_auto_pass", label: "小额差异" },
    { id: "e5_none", source: "is_big_diff", sourceHandle: "none", target: "set_no_diff", label: "无差异" },
    { id: "e6_approved", source: "approve_diff", sourceHandle: "approved", target: "gen_report", label: "通过" },
    { id: "e6_rejected", source: "approve_diff", sourceHandle: "rejected", target: "end_rejected", label: "驳回" },
    { id: "e6_timeout", source: "approve_diff", sourceHandle: "timeout", target: "gen_report", label: "超时" },
    { id: "e7", source: "set_auto_pass", sourceHandle: "out", target: "gen_report" },
    { id: "e8", source: "set_no_diff", sourceHandle: "out", target: "gen_report" },
    { id: "e9", source: "gen_report", sourceHandle: "out", target: "end_ok" }
  ]
};

/* ---------- Demo DAG：全部供应商（for-each） ---------- */
AF.dagForeach = {
  schemaVersion: "1.0",
  flow: { key: "recon.purchase_monthly_all_suppliers", name: "采购月度对账 · 全部供应商", version: 1, description: "列出供应商 -> 按供应商并发执行单供应商对账子流程 -> 汇总报告", tags: ["对账", "foreach"] },
  variables: { type: "object", required: ["period"], properties: { period: { type: "string", title: "对账期间", default: "2026-08" }, threshold: { type: "number", title: "大额差异阈值(元)", default: 10000 } } },
  settings: { taskQueue: "nodes-recon", runTimeout: "P14D", onNodeError: "pause" },
  nodes: [
    { id: "start", kind: "start", name: "开始", position: { x: 60, y: 250 } },
    { id: "list_suppliers", kind: "task", name: "列出本期供应商", position: { x: 220, y: 216 }, nodeType: "recon.list_suppliers@1", inputs: { period: "{{input.period}}" }, config: { source_profile: "erp_dev" }, outputKey: "suppliers", timeouts: { startToClose: "PT5M" } },
    { id: "per_supplier", kind: "foreach", name: "按供应商逐个对账", position: { x: 480, y: 196 }, items: "{{context.suppliers.ids}}", itemAlias: "supplier", concurrency: 5, maxItems: 1000, batchSize: 200,
      body: { type: "subflow", flowKey: "recon.purchase_monthly", flowVersion: "published", inputs: { supplier_id: "{{supplier}}", period: "{{input.period}}", threshold: "{{input.threshold}}" } },
      onItemError: "continue", collect: { mode: "list", fields: ["diff_count", "decision", "report_ref"], outputKey: "supplier_results" }, onError: { action: "pause" } },
    { id: "summary_report", kind: "task", name: "生成汇总报告", position: { x: 840, y: 216 }, nodeType: "recon.generate_summary_report@1", inputs: { period: "{{input.period}}", results: "{{context.supplier_results}}" }, config: { format: "xlsx" }, outputKey: "summary", timeouts: { startToClose: "PT5M" } },
    { id: "end_ok", kind: "end", name: "完成", position: { x: 1100, y: 250 }, status: "success", outputs: { summary_ref: "{{context.summary.ref}}", suppliers_total: "{{context.supplier_results.total}}", suppliers_failed: "{{context.supplier_results.failed}}" } }
  ],
  edges: [
    { id: "e1", source: "start", sourceHandle: "out", target: "list_suppliers" },
    { id: "e2", source: "list_suppliers", sourceHandle: "out", target: "per_supplier" },
    { id: "e3", source: "per_supplier", sourceHandle: "out", target: "summary_report" },
    { id: "e4", source: "summary_report", sourceHandle: "out", target: "end_ok" }
  ]
};

/* ---------- 流程列表 ---------- */
AF.flows = [
  { key: "recon.purchase_monthly", name: "采购月度对账", current: 3, draft: 4, tags: ["对账", "采购"], owner: "alice", runs24h: 38, successRate: 0.95, running: 3, pendingApprovals: 2, recent: ["ok", "ok", "wait", "ok", "fail", "ok", "ok", "run"], updated: "今天 13:42" },
  { key: "recon.purchase_monthly_all_suppliers", name: "采购月度对账 · 全部供应商", current: 1, draft: null, tags: ["对账", "foreach"], owner: "alice", runs24h: 1, successRate: 1, running: 1, pendingApprovals: 4, recent: ["run"], updated: "昨天 18:05" },
  { key: "sales.receipt_recon", name: "销售回款对账", current: 2, draft: 3, tags: ["对账", "销售"], owner: "chen", runs24h: 12, successRate: 0.83, running: 0, pendingApprovals: 0, recent: ["ok", "fail", "ok", "ok", "fail", "ok"], updated: "09-17" },
  { key: "purchase.invoice_match", name: "采购发票三单匹配", current: 7, draft: null, tags: ["AI 采购"], owner: "wang", runs24h: 156, successRate: 0.99, running: 5, pendingApprovals: 1, recent: ["ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok"], updated: "09-15" },
  { key: "common.notify_supplier", name: "供应商通知子流程", current: 9, draft: null, tags: ["子流程", "通用"], owner: "wang", runs24h: 44, successRate: 1, running: 0, pendingApprovals: 0, recent: ["ok", "ok", "ok", "ok"], updated: "09-10" },
  { key: "sales.credit_review", name: "客户信用复核（草稿）", current: null, draft: 1, tags: ["AI 销售"], owner: "chen", runs24h: 0, successRate: null, running: 0, pendingApprovals: 0, recent: [], updated: "今天 10:20" }
];

/* ---------- 运行实例：单供应商，等待审批 ---------- */
AF.runDemo = {
  id: "run_01J8QK7V3X9M2N4P6R8T0W2Y4A",
  flowKey: "recon.purchase_monthly", flowName: "采购月度对账", version: 3,
  workflowId: "run:recon.purchase_monthly:01J8QK7V", status: "waiting_approval",
  triggeredBy: "alice（手动）", startedAt: "2026-09-19 14:02:11", input: { period: "2026-08", supplier_id: "SUP-0002", threshold: 10000 },
  contextBytes: 6124,
  nodes: {
    start:         { status: "succeeded", attempts: 1, start: 0, end: 0.2 },
    fetch_docs:    { status: "succeeded", attempts: 1, start: 0.2, end: 41.8, durationMs: 41600,
                     input: { period: "2026-08", supplier_id: "SUP-0002", sources: ["erp_po", "supplier_statement"] },
                     output: { batches: { erp_po: "ref://artifact/7d2c…e1", supplier_statement: "ref://artifact/9a41…c7" }, counts: { erp_po: 1203, supplier_statement: 1187 }, total_amount: { erp_po: "8,432,190.55", supplier_statement: "8,399,870.20" } } },
    match_diff:    { status: "succeeded", attempts: 3, start: 41.8, end: 68.1, durationMs: 26300, retried: true,
                     input: { erp_batch_ref: "ref://artifact/7d2c…e1", statement_batch_ref: "ref://artifact/9a41…c7", amount_tolerance: 0.01 },
                     output: { matched_count: 1180, count: 23, total_abs_amount: "32,320.35", by_type: { missing_in_erp: 4, missing_in_statement: 7, amount_mismatch: 10, qty_mismatch: 2 }, detail_ref: "ref://artifact/2fbe…9d" } },
    classify_diff: { status: "succeeded", attempts: 1, start: 68.1, end: 74.9, durationMs: 6800, resumedFromBreakpoint: true,
                     input: { diff_detail_ref: "ref://artifact/2fbe…9d", mode: "rules" },
                     output: { categories: { price_diff: 8, qty_diff: 2, missing: 11, duplicate: 1, other: 1 }, llm_used: false, confidence_avg: 1.0, classified_ref: "ref://artifact/51aa…03" } },
    is_big_diff:   { status: "succeeded", attempts: 1, start: 74.9, end: 74.95, branch: "big" },
    approve_diff:  { status: "waiting_approval", attempts: 1, start: 75.0, waitingSince: "14:03:26", assignees: ["财务共享中心/应付组 (6人)", "角色 recon_ops (3人)", "发起人上级：李经理"], dueAt: "2026-09-22 14:03" },
    set_auto_pass: { status: "pending" }, set_no_diff: { status: "pending" }, gen_report: { status: "pending" }, end_rejected: { status: "pending" }, end_ok: { status: "pending" }
  },
  edgesDone: ["e1", "e2", "e3", "e4", "e5_big"],
  logs: [
    ["14:02:11.020", "run", "info", "run.started flow=recon.purchase_monthly v3 input={period:2026-08, supplier_id:SUP-0002}"],
    ["14:02:11.204", "fetch_docs", "info", "开始拉取 period=2026-08 supplier=SUP-0002 profile=erp_dev(jdbc)"],
    ["14:02:15.871", "fetch_docs", "debug", "heartbeat erp_po: 500 rows"],
    ["14:02:20.334", "fetch_docs", "debug", "heartbeat erp_po: 1000 rows"],
    ["14:02:22.902", "fetch_docs", "info", "数据源完成 source=erp_po count=1203 amount=8432190.55"],
    ["14:02:23.110", "fetch_docs", "info", "切换数据源 supplier_statement profile=supplier_files(file) file=对账单_SUP-0002_202608.xlsx"],
    ["14:02:51.577", "fetch_docs", "info", "数据源完成 source=supplier_statement count=1187 amount=8399870.20"],
    ["14:02:52.801", "fetch_docs", "info", "输出 2 个 artifact 引用（明细 1.9MB 已落 MinIO）"],
    ["14:02:53.015", "match_diff", "info", "attempt=1 加载 batches"],
    ["14:02:55.402", "match_diff", "error", "ExternalServiceError: MinIO 连接超时 (retryable) → 2s 后重试"],
    ["14:02:57.611", "match_diff", "info", "attempt=2 加载 batches"],
    ["14:02:59.940", "match_diff", "error", "ExternalServiceError: MinIO 连接超时 (retryable) → 4s 后重试"],
    ["14:03:04.108", "match_diff", "info", "attempt=3 加载 batches"],
    ["14:03:17.336", "match_diff", "info", "匹配完成 matched=1180 unmatched=23 by_type={missing_in_erp:4, missing_in_statement:7, amount_mismatch:10, qty_mismatch:2}"],
    ["14:03:19.290", "match_diff", "info", "差异明细已落 artifact ref://artifact/2fbe…9d (23 rows)"],
    ["14:03:19.400", "classify_diff", "warn", "node.paused reason=breakpoint 等待运营 resume"],
    ["14:03:24.115", "classify_diff", "info", "node.resumed operator=alice"],
    ["14:03:24.300", "classify_diff", "info", "mode=rules 逐条归因 23 条"],
    ["14:03:26.011", "classify_diff", "info", "分类完成 price_diff=8 qty_diff=2 missing=11 duplicate=1 other=1"],
    ["14:03:26.090", "is_big_diff", "info", "node.branch matched=big (total_abs_amount 32320.35 > threshold 10000)"],
    ["14:03:26.210", "approve_diff", "info", "approval.requested assignees=部门 D-FIN-AP(6) + 角色 recon_ops(3) + 发起人上级(1) strategy=any timeout=P3D"],
    ["14:03:26.845", "approve_diff", "info", "通知已发送 channel=wecom-default recipients=9 message_id=wx_7f2e…"]
  ]
};

/* ---------- 运行实例：for-each，进行中 ---------- */
AF.runForeach = {
  id: "run_01J8QM2B7C5D9F1H3J5K7M9N1P",
  flowKey: "recon.purchase_monthly_all_suppliers", flowName: "采购月度对账 · 全部供应商", version: 1,
  workflowId: "run:recon.purchase_monthly_all_suppliers:01J8QM2B", status: "running",
  triggeredBy: "schedule:monthly-recon（定时）", startedAt: "2026-09-19 13:00:00", input: { period: "2026-08", threshold: 10000 }, contextBytes: 3410,
  nodes: {
    start: { status: "succeeded", attempts: 1, start: 0, end: 0.2 },
    list_suppliers: { status: "succeeded", attempts: 1, start: 0.2, end: 3.1, durationMs: 2900, output: { ids: ["SUP-0001", "SUP-0002", "…(20)"], count: 20 } },
    per_supplier: { status: "running", attempts: 1, start: 3.1, total: 20, succeeded: 11, failed: 1, running: 5, pending: 3 },
    summary_report: { status: "pending" }, end_ok: { status: "pending" }
  },
  edgesDone: ["e1", "e2"],
  items: [
    ["SUP-0001", "succeeded", "2m 41s", "diff_count=0 decision=no_diff"], ["SUP-0002", "waiting_approval", "1h 02m", "diff_count=23 大额 · 等待审批"],
    ["SUP-0003", "succeeded", "3m 05s", "diff_count=2 decision=auto_approved"], ["SUP-0004", "succeeded", "2m 12s", "diff_count=0"],
    ["SUP-0005", "failed", "0m 48s", "ValidationError: 对账单文件缺少列 invoice_no"], ["SUP-0006", "succeeded", "4m 30s", "diff_count=5 decision=auto_approved"],
    ["SUP-0007", "succeeded", "2m 03s", "diff_count=0"], ["SUP-0008", "waiting_approval", "38m", "diff_count=11 大额 · 等待审批"],
    ["SUP-0009", "succeeded", "1m 58s", "diff_count=0"], ["SUP-0010", "succeeded", "2m 44s", "diff_count=1 decision=auto_approved"],
    ["SUP-0011", "succeeded", "3m 11s", "diff_count=0"], ["SUP-0012", "running", "1m 20s", "match_and_diff attempt=1"],
    ["SUP-0013", "succeeded", "2m 09s", "diff_count=3 decision=auto_approved"], ["SUP-0014", "succeeded", "2m 51s", "diff_count=0"],
    ["SUP-0015", "running", "0m 35s", "fetch_documents erp_po 500 rows"], ["SUP-0016", "running", "0m 12s", "fetch_documents"],
    ["SUP-0017", "pending", "—", ""], ["SUP-0018", "pending", "—", ""], ["SUP-0019", "pending", "—", ""], ["SUP-0020", "pending", "—", ""]
  ],
  logs: [
    ["13:00:00.102", "run", "info", "run.started flow=recon.purchase_monthly_all_suppliers v1 trigger=schedule"],
    ["13:00:03.220", "list_suppliers", "info", "本期有交易供应商 20 家（只返回编码列表）"],
    ["13:00:03.400", "per_supplier", "info", "foreach items=20 concurrency=5 batchSize=200 body=subflow recon.purchase_monthly@v3"],
    ["13:00:03.512", "per_supplier", "info", "item[0] SUP-0001 child=run:…:per_supplier:0 started"],
    ["13:00:03.515", "per_supplier", "info", "item[1] SUP-0002 child started"],
    ["13:00:51.004", "per_supplier", "error", "item[4] SUP-0005 failed: ValidationError 对账单文件缺少列 invoice_no (non-retryable) → onItemError=continue"],
    ["13:03:26.210", "per_supplier", "warn", "item[1] SUP-0002 waiting_approval（子流程审批中，父流程继续调度其余 item）"],
    ["14:01:10.330", "per_supplier", "info", "进度 11/20 succeeded, 1 failed, 5 running"]
  ]
};

/* ---------- 待审批 ---------- */
AF.approvals = [
  { id: "ap_1", run: AF.runDemo.id, flow: "采购月度对账", node: "运营确认大额差异", title: "供应商 SUP-0002 2026-08 对账差异确认", summary: "差异 23 笔，差异金额合计 32,320.35 元（阈值 10,000）。分类：单价差 8 · 数量差 2 · 漏单 11 · 重复 1 · 其他 1",
    requestedAt: "今天 14:03", dueAt: "09-22 14:03", waited: "1h 02m", strategy: "any", assignees: ["财务共享中心 / 应付组 (6)", "recon_ops (3)", "李经理（发起人上级）"], attachments: ["差异明细.xlsx (23 行)"], severity: "high",
    form: { comment: "", adjustment: "" } },
  { id: "ap_2", run: "run_01J8QM2B…:per_supplier:7", flow: "采购月度对账 · 全部供应商 › item[7]", node: "运营确认大额差异", title: "供应商 SUP-0008 2026-08 对账差异确认", summary: "差异 11 笔，差异金额合计 15,880.00 元（阈值 10,000）。分类：单价差 9 · 漏单 2",
    requestedAt: "今天 13:27", dueAt: "09-22 13:27", waited: "38m", strategy: "any", assignees: ["财务共享中心 / 应付组 (6)", "recon_ops (3)"], attachments: ["差异明细.xlsx (11 行)"], severity: "medium", form: {} },
  { id: "ap_3", run: "run_01J8QH…", flow: "采购发票三单匹配", node: "采购经理确认", title: "PO-2026-08-0912 发票金额超出采购单 3.2%", summary: "发票 128,400.00 元 vs 采购单 124,420.00 元；供应商说明：运费调整", requestedAt: "昨天 17:50", dueAt: "09-21 17:50", waited: "20h", strategy: "all", assignees: ["王采购经理", "李经理"], attachments: ["发票影像.pdf"], severity: "low", form: {} }
];

/* ---------- 版本 ---------- */
AF.versions = [
  { v: 4, status: "draft", by: "alice", at: "今天 13:42", note: "（草稿）供应商 SUP-A 全部人工确认；阈值改为 50,000", runs: 0 },
  { v: 3, status: "published", current: true, by: "alice", at: "09-15 10:20", note: "增加差异分类节点，大额阈值改由输入变量控制", runs: 61 },
  { v: 2, status: "published", by: "alice", at: "09-08 16:05", note: "增加人工审批节点（Signal）与超时策略 P3D", runs: 118 },
  { v: 1, status: "archived", by: "wang", at: "08-30 11:11", note: "首版：拉取单据 → 差异识别 → 报告", runs: 24 }
];
