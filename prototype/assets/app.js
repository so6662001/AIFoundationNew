/* 应用壳：侧边栏 / 顶栏 / 通用工具 */
window.AF = window.AF || {};

AF.icons = {
  flows: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="1.5" y="2" width="5" height="4" rx="1"/><rect x="9.5" y="10" width="5" height="4" rx="1"/><path d="M6.5 4h2a2 2 0 0 1 2 2v3a2 2 0 0 0 2 2h-3"/></svg>',
  runs: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="6"/><path d="M6.5 5.5v5l4-2.5z" fill="currentColor" stroke="none"/></svg>',
  approvals: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 8.5l3 3 7-7"/></svg>',
  registry: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="5" height="5" rx="1"/><rect x="9" y="2" width="5" height="5" rx="1"/><rect x="2" y="9" width="5" height="5" rx="1"/><rect x="9" y="9" width="5" height="5" rx="1"/></svg>',
  metrics: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 13h12M4 11V7M8 11V4M12 11V8"/></svg>',
  settings: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="8" cy="8" r="2.2"/><path d="M8 1.8v1.7M8 12.5v1.7M1.8 8h1.7M12.5 8h1.7M3.6 3.6l1.2 1.2M11.2 11.2l1.2 1.2M3.6 12.4l1.2-1.2M11.2 4.8l1.2-1.2"/></svg>',
  versions: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="4" cy="4" r="1.6"/><circle cx="4" cy="12" r="1.6"/><circle cx="12" cy="6" r="1.6"/><path d="M4 5.6v4.8M12 7.6c0 2.4-3 2.4-6 3"/></svg>'
};

AF.renderShell = function (opts) {
  var active = opts.active, crumbs = opts.crumbs || [];
  var nav = [
    { g: "编排" },
    { id: "flows", href: "index.html", label: "流程", ic: "flows" },
    { id: "editor", href: "editor.html", label: "画布编辑器", ic: "flows" },
    { id: "versions", href: "versions.html", label: "版本管理", ic: "versions" },
    { g: "运行" },
    { id: "runs", href: "run.html", label: "运行监控", ic: "runs", badge: "4" },
    { id: "approvals", href: "approvals.html", label: "待审批", ic: "approvals", badge: "3" },
    { id: "metrics", href: "#", label: "流程度量", ic: "metrics" },
    { g: "平台" },
    { id: "registry", href: "#", label: "节点注册中心", ic: "registry" },
    { id: "settings", href: "settings.html", label: "企业配置", ic: "settings" }
  ];
  var html = '<aside class="sidebar"><div class="brand"><div class="logo">AF</div><div><div class="name">Agent Foundation</div><div class="sub">AI 智能体底座 · Temporal</div></div></div><nav class="nav">';
  nav.forEach(function (n) {
    if (n.g) { html += '<div class="group">' + n.g + '</div>'; return; }
    html += '<a href="' + n.href + '" class="' + (n.id === active ? 'active' : '') + '"><span class="ic">' + AF.icons[n.ic] + '</span>' + n.label + (n.badge ? '<span class="badge">' + n.badge + '</span>' : '') + '</a>';
  });
  html += '</nav><div class="foot">Temporal <b>agent-platform</b> · 7233<br/>Workers 在线 <b>4</b> · 队列 <b>5</b></div></aside>';
  html += '<header class="topbar"><div class="crumbs">';
  crumbs.forEach(function (c, i) { html += (i ? '<span>/</span>' : '') + (i === crumbs.length - 1 ? '<b>' + c + '</b>' : '<span>' + c + '</span>'); });
  html += '</div><div class="spacer"></div><span class="env">开发环境 · docker-compose</span><div class="user"><div class="avatar">A</div><div class="who">alice<small>财务共享中心 · editor / recon_ops</small></div></div></header>';
  var shell = document.getElementById('shell');
  shell.insertAdjacentHTML('afterbegin', html);
};

AF.toast = function (msg) {
  var t = document.querySelector('.toast');
  if (!t) { t = document.createElement('div'); t.className = 'toast'; document.body.appendChild(t); }
  t.textContent = msg; t.classList.add('show');
  clearTimeout(AF._tt); AF._tt = setTimeout(function () { t.classList.remove('show'); }, 2200);
};

AF.modal = function (id, show) {
  var m = document.getElementById(id); if (!m) return;
  m.classList.toggle('show', show !== false);
};

AF.tabs = function (root) {
  root = root || document;
  root.querySelectorAll('.tabs').forEach(function (tabs) {
    var btns = tabs.querySelectorAll('button');
    btns.forEach(function (b) {
      b.addEventListener('click', function () {
        btns.forEach(function (x) { x.classList.remove('active'); });
        b.classList.add('active');
        var target = tabs.dataset.target ? document.getElementById(tabs.dataset.target) : tabs.parentElement;
        target.querySelectorAll(':scope > .tabpane, :scope .tabpanes > .tabpane').forEach(function (p) { p.classList.toggle('active', p.dataset.tab === b.dataset.tab); });
      });
    });
  });
};

AF.esc = function (s) { return String(s).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); };

AF.json = function (obj) {
  var s = JSON.stringify(obj, null, 2);
  s = AF.esc(s)
    .replace(/(&quot;[^&]*?&quot;)(\s*:)/g, '<span class="k">$1</span>$2')
    .replace(/:\s*(&quot;.*?&quot;)/g, function (m, g) { return ': <span class="s">' + g + '</span>'; })
    .replace(/:\s*(-?\d+(\.\d+)?|true|false|null)\b/g, ': <span class="n">$1</span>');
  return s;
};

AF.statusLabel = { pending: "待执行", running: "执行中", succeeded: "成功", failed: "失败", paused: "已暂停", waiting_approval: "等待审批", skipped: "已跳过", cancelled: "已取消" };
AF.stag = function (st) { return '<span class="st-' + st + '"><span class="stag">' + (AF.statusLabel[st] || st) + '</span></span>'; };

document.addEventListener('click', function (e) {
  if (e.target.classList.contains('modal-mask')) e.target.classList.remove('show');
  if (e.target.classList.contains('close')) { var m = e.target.closest('.modal-mask'); if (m) m.classList.remove('show'); }
  if (e.target.classList.contains('switch')) e.target.classList.toggle('on');
});
