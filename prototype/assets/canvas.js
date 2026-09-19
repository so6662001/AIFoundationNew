/* 轻量画布渲染器（原型用，模拟 xyflow 行为：拖拽、连线、选中、缩放、状态着色） */
window.AF = window.AF || {};

AF.Canvas = function (wrap, dag, opts) {
  this.wrap = wrap; this.dag = dag; this.opts = opts || {};
  this.mode = this.opts.mode || 'edit';          // edit | run
  this.run = this.opts.run || null;
  this.scale = this.opts.scale || 0.9; this.tx = this.opts.tx || 20; this.ty = this.opts.ty || 10;
  this.selected = null;
  this.el = document.createElement('div'); this.el.className = 'canvas';
  this.svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); this.svg.classList.add('edges');
  this.svg.setAttribute('width', 4000); this.svg.setAttribute('height', 3000);
  this.el.appendChild(this.svg);
  wrap.classList.add('canvas-wrap'); wrap.appendChild(this.el);
  this._bind();
  this.render();
  if (this.opts.fit !== false) this.fit();
};

AF.Canvas.prototype._kindMeta = function (n) {
  var t = n.nodeType ? AF.typeOf(n.nodeType) : null;
  var abbr = { start: '▶', end: '■', branch: 'IF', approval: 'AP', assign: '=', subflow: 'SF', foreach: 'FE', parallel: '∥', join: '⊕' }[n.kind] || (t ? t.abbr : 'T');
  return { abbr: abbr, type: t };
};

AF.Canvas.prototype._outHandles = function (n) {
  if (n.kind === 'branch') return n.cases.map(function (c) { return { id: c.id, label: c.label || c.id }; }).concat([{ id: n.default, label: '默认 · ' + n.default }]);
  if (n.kind === 'approval') { var hs = [{ id: 'approved', label: '通过' }, { id: 'rejected', label: '驳回' }]; if (n.timeout && (n.onTimeout || 'route') === 'route') hs.push({ id: 'timeout', label: '超时' }); return hs; }
  if (n.kind === 'end') return [];
  var out = [{ id: 'out', label: '' }];
  if (n.onError && n.onError.action === 'route') out.push({ id: 'error', label: '错误' });
  return out;
};

AF.Canvas.prototype.render = function () {
  var self = this;
  this.el.querySelectorAll('.node').forEach(function (x) { x.remove(); });
  this.dag.nodes.forEach(function (n) { self.el.appendChild(self._nodeEl(n)); });
  this._applyTransform();
  this._drawEdges();
};

AF.Canvas.prototype._nodeEl = function (n) {
  var self = this, meta = this._kindMeta(n), t = meta.type;
  var d = document.createElement('div');
  d.className = 'node k-' + n.kind + (n.kind === 'end' && n.status === 'failed' ? ' failed' : '');
  d.dataset.id = n.id; d.style.left = n.position.x + 'px'; d.style.top = n.position.y + 'px';
  var rs = this.run && this.run.nodes[n.id];
  if (this.mode === 'run') { d.classList.add('run', 'st-' + (rs ? rs.status : 'pending')); }
  if (this.selected === n.id) d.classList.add('selected');

  var html = '';
  if (n.kind !== 'start') html += '<span class="handle in" data-handle="in"></span>';
  if (n.kind === 'start' || n.kind === 'end') {
    html += '<div class="nh"><span class="ico">' + meta.abbr + '</span><span class="ttl">' + AF.esc(n.name) + '</span></div>';
  } else {
    html += '<div class="nh"><span class="ico">' + meta.abbr + '</span><div><div class="ttl">' + AF.esc(n.name) + '</div><div class="typ">' + AF.esc(n.nodeType || n.kind) + '</div></div></div>';
    html += '<div class="nb">';
    if (n.kind === 'task') {
      var ins = Object.keys(n.inputs || {}).slice(0, 2).map(function (k) { var v = n.inputs[k]; return k + ' ← ' + (typeof v === 'string' ? v : JSON.stringify(v)); });
      ins.forEach(function (s) { html += '<span class="mono" style="font-size:10.5px;color:var(--text-3);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px">' + AF.esc(s) + '</span>'; });
      html += '<span>输出 → <span class="out">context.' + AF.esc(n.outputKey || n.id) + '</span></span>';
    } else if (n.kind === 'branch') {
      html += '<span class="rule">' + AF.esc(this._ruleText(n.cases[0].when)) + '</span><span class="muted" style="font-size:10.5px">' + n.cases.length + ' 个分支 + 默认 · ' + (n.evaluation === 'all_match' ? '全部匹配' : '首个匹配') + '</span>';
    } else if (n.kind === 'approval') {
      var who = [];
      (n.assignees.roles || []).forEach(function (r) { who.push('角色 ' + r); });
      (n.assignees.departments || []).forEach(function (r) { who.push('部门 ' + (r.name || r.id)); });
      (n.assignees.dynamic || []).forEach(function (r) { who.push(r === 'manager_of_initiator' ? '发起人上级' : r); });
      html += '<span>' + AF.esc(who.join(' · ')) + '</span><span class="muted" style="font-size:10.5px">策略 ' + (n.assignees.strategy || 'any') + ' · 超时 ' + (n.timeout || '—') + ' · ' + (n.reminder ? '提醒 ' + n.reminder.every : '') + '</span>';
    } else if (n.kind === 'assign') {
      (n.assignments || []).forEach(function (a) { html += '<span class="mono" style="font-size:10.5px">' + AF.esc(a.target) + ' = ' + AF.esc(typeof a.value === 'string' ? a.value : JSON.stringify(a.value)) + '</span>'; });
    } else if (n.kind === 'subflow') {
      html += '<span>子流程 <b>' + AF.esc(n.flowKey) + '</b> @ ' + AF.esc(String(n.flowVersion)) + '</span>';
    } else if (n.kind === 'foreach') {
      html += '<span class="mono" style="font-size:10.5px">items = ' + AF.esc(n.items) + '</span><span class="muted" style="font-size:10.5px">并发 ' + n.concurrency + ' · 逐项失败 ' + (n.onItemError === 'fail_fast' ? '快速失败' : '继续') + ' · 收集 ' + (n.collect ? n.collect.mode : 'none') + ' → context.' + (n.collect ? n.collect.outputKey : '') + '</span>';
    }
    if (this.mode === 'run' && rs) {
      html += '<div class="meta">';
      if (rs.attempts) html += '<span>尝试 <b>' + rs.attempts + '</b></span>';
      if (rs.durationMs) html += '<span>耗时 <b>' + (rs.durationMs / 1000).toFixed(1) + 's</b></span>';
      if (rs.status === 'waiting_approval') html += '<span>等待 <b>' + (rs.waitingSince || '') + ' 起</b></span>';
      if (rs.branch) html += '<span>命中 <b>' + rs.branch + '</b></span>';
      if (rs.status === 'running' && n.kind !== 'foreach') html += '<span><b>执行中…</b></span>';
      html += '</div>';
    }
    html += '</div>';
    if (n.kind === 'foreach') {
      var b = n.body;
      html += '<div class="body-slot"><div class="bt">循环体 · ' + (b.type === 'subflow' ? '子流程 (Child Workflow / item)' : '单任务 (Activity / item)') + '</div><div class="bn">' + AF.esc(b.type === 'subflow' ? b.flowKey : b.nodeType) + '</div><div class="bk">' + AF.esc(b.type === 'subflow' ? 'version=' + b.flowVersion : '') + ' · {{' + (n.itemAlias || 'item') + '}} → ' + AF.esc(Object.keys(b.inputs || {}).map(function (k) { return k; }).join(', ')) + '</div>';
      if (this.mode === 'run' && rs && rs.total) {
        var p = Math.round(((rs.succeeded + rs.failed) / rs.total) * 100);
        html += '<div class="row" style="margin-top:8px;gap:10px"><div class="ring" style="--p:' + p + '%"><i>' + (rs.succeeded + rs.failed) + '/' + rs.total + '</i></div><div class="col" style="gap:2px;font-size:11px"><span><span class="dot" style="background:var(--success)"></span> 成功 ' + rs.succeeded + ' &nbsp;<span class="dot" style="background:var(--danger)"></span> 失败 ' + rs.failed + '</span><span><span class="dot" style="background:var(--primary)"></span> 执行中 ' + rs.running + ' &nbsp;<span class="dot" style="background:#b6bdcc"></span> 待执行 ' + rs.pending + '</span></div><span class="spacer"></span><a class="small" href="#" data-items="' + n.id + '">查看 item ›</a></div>';
      }
      html += '</div>';
    }
  }
  // badges
  var badges = [];
  if (n.breakpoint) badges.push('<span class="bp">断点</span>');
  if (n.retryPolicy && n.retryPolicy.maximumAttempts) badges.push('<span class="rt">重试 ×' + n.retryPolicy.maximumAttempts + '</span>');
  if (t && t.workers === 0) badges.push('<span class="off">Worker 离线</span>');
  if (t && t.lang === 'python') badges.push('<span>py</span>');
  if (rs && rs.retried) badges.push('<span class="rt">已重试 ' + (rs.attempts - 1) + ' 次</span>');
  if (badges.length) html += '<div class="badges">' + badges.join('') + '</div>';
  // out handles
  var outs = this._outHandles(n);
  if (outs.length === 1) html += '<span class="handle out" data-handle="' + outs[0].id + '"></span>';
  else if (outs.length > 1) {
    html += '<div class="case-handles">';
    outs.forEach(function (h) { html += '<div class="ch"><span class="lbl">' + AF.esc(h.label) + '</span><span class="handle out" data-handle="' + h.id + '"></span></div>'; });
    html += '</div>';
  }
  if (this.mode === 'run') html += '<span class="stbar"></span>';
  d.innerHTML = html;
  return d;
};

AF.Canvas.prototype._ruleText = function (w) {
  var self = this;
  if (!w) return '';
  if (w.all) return w.all.map(function (x) { return self._ruleText(x); }).join(' && ');
  if (w.any) return w.any.map(function (x) { return self._ruleText(x); }).join(' || ');
  if (w.not) return '!(' + this._ruleText(w.not) + ')';
  if (w.expr) return w.expr;
  var op = { eq: '==', ne: '!=', gt: '>', gte: '>=', lt: '<', lte: '<=' }[w.op] || w.op;
  var l = String(w.left).replace(/\{\{|\}\}/g, ''), r = typeof w.right === 'string' ? w.right.replace(/\{\{|\}\}/g, '') : JSON.stringify(w.right);
  return l + ' ' + op + ' ' + r;
};

AF.Canvas.prototype._handlePos = function (nodeId, handle, isIn) {
  var nd = this.el.querySelector('.node[data-id="' + nodeId + '"]'); if (!nd) return null;
  var h = nd.querySelector('.handle' + (isIn ? '.in' : '[data-handle="' + handle + '"]')) || nd.querySelector(isIn ? '.handle.in' : '.handle.out');
  if (!h) { return { x: nd.offsetLeft + (isIn ? 0 : nd.offsetWidth), y: nd.offsetTop + nd.offsetHeight / 2 }; }
  var hr = h.getBoundingClientRect(), nr = this.el.getBoundingClientRect();
  return { x: (hr.left + hr.width / 2 - nr.left) / this.scale, y: (hr.top + hr.height / 2 - nr.top) / this.scale };
};

AF.Canvas.prototype._path = function (a, b) {
  var dx = Math.max(40, Math.abs(b.x - a.x) / 2);
  return 'M' + a.x + ',' + a.y + ' C' + (a.x + dx) + ',' + a.y + ' ' + (b.x - dx) + ',' + b.y + ' ' + b.x + ',' + b.y;
};

AF.Canvas.prototype._drawEdges = function () {
  var self = this; this.svg.innerHTML = '';
  var defs = '<defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#98a2b8"/></marker><marker id="arr-done" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#22a06b"/></marker><marker id="arr-active" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#2f54eb"/></marker></defs>';
  this.svg.insertAdjacentHTML('beforeend', defs);
  this.dag.edges.forEach(function (e) {
    var a = self._handlePos(e.source, e.sourceHandle || 'out', false), b = self._handlePos(e.target, 'in', true);
    if (!a || !b) return;
    var cls = 'edge';
    if (self.mode === 'run' && self.run) {
      if ((self.run.edgesDone || []).indexOf(e.id) >= 0) cls += ' done';
      else { var s = self.run.nodes[e.source]; if (!s || s.status === 'pending' || s.status === 'waiting_approval' || s.status === 'running') cls += ' dim'; }
    }
    if (self.selected && (e.source === self.selected || e.target === self.selected) && self.mode === 'edit') cls += ' active';
    var p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    p.setAttribute('d', self._path(a, b)); p.setAttribute('class', cls); p.dataset.id = e.id;
    p.setAttribute('marker-end', 'url(#' + (cls.indexOf('done') >= 0 ? 'arr-done' : cls.indexOf('active') >= 0 ? 'arr-active' : 'arr') + ')');
    self.svg.appendChild(p);
    if (e.label) {
      var mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2 - 4;
      var t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      t.setAttribute('x', mx); t.setAttribute('y', my); t.setAttribute('text-anchor', 'middle'); t.setAttribute('class', 'edge-label'); t.textContent = e.label;
      self.svg.appendChild(t);
    }
  });
  if (this._temp) { var tp = document.createElementNS('http://www.w3.org/2000/svg', 'path'); tp.setAttribute('d', this._path(this._temp.a, this._temp.b)); tp.setAttribute('class', 'edge temp'); this.svg.appendChild(tp); }
};

AF.Canvas.prototype._applyTransform = function () {
  this.el.style.transform = 'translate(' + this.tx + 'px,' + this.ty + 'px) scale(' + this.scale + ')';
};

AF.Canvas.prototype.zoom = function (f) { this.scale = Math.min(1.6, Math.max(0.35, this.scale * f)); this._applyTransform(); this._drawEdges(); };
AF.Canvas.prototype.fit = function () {
  var nodes = this.el.querySelectorAll('.node'); if (!nodes.length) return;
  var minX = 1e9, minY = 1e9, maxX = -1e9, maxY = -1e9;
  nodes.forEach(function (n) { var x = n.offsetLeft, y = n.offsetTop; minX = Math.min(minX, x); minY = Math.min(minY, y); maxX = Math.max(maxX, x + n.offsetWidth); maxY = Math.max(maxY, y + n.offsetHeight); });
  var pad = 40, availW = this.wrap.clientWidth - pad * 2, availH = this.wrap.clientHeight - (this.opts.bottomInset || 0) - pad * 2 - (this.opts.topInset || 0);
  var s = Math.min(1.15, availW / (maxX - minX + 40), availH / (maxY - minY + 40));
  var minFit = this.opts.minFit || 0.62;
  if (s < minFit) {                                   // 太宽时不缩得过小，左对齐让用户平移
    this.scale = minFit;
    this.tx = pad - minX * this.scale;
    this.ty = pad + (this.opts.topInset || 0) + Math.max(0, (availH - (maxY - minY) * this.scale) / 2) - minY * this.scale;
  } else {
    this.scale = s;
    this.tx = pad + (availW - (maxX - minX) * this.scale) / 2 - minX * this.scale;
    this.ty = pad + (this.opts.topInset || 0) + (availH - (maxY - minY) * this.scale) / 2 - minY * this.scale;
  }
  this._applyTransform(); this._drawEdges();
};

AF.Canvas.prototype.select = function (id) {
  this.selected = id;
  this.el.querySelectorAll('.node').forEach(function (n) { n.classList.toggle('selected', n.dataset.id === id); });
  this._drawEdges();
  if (this.opts.onSelect) this.opts.onSelect(id ? this.dag.nodes.find(function (n) { return n.id === id; }) : null);
};

AF.Canvas.prototype.addNode = function (node) { this.dag.nodes.push(node); this.render(); this.select(node.id); if (this.opts.onChange) this.opts.onChange('add', node); };
AF.Canvas.prototype.removeSelected = function () {
  var id = this.selected; if (!id) return; var self = this;
  this.dag.nodes = this.dag.nodes.filter(function (n) { return n.id !== id; });
  this.dag.edges = this.dag.edges.filter(function (e) { return e.source !== id && e.target !== id; });
  this.selected = null; this.render(); if (this.opts.onSelect) this.opts.onSelect(null); if (this.opts.onChange) this.opts.onChange('remove', id);
};
AF.Canvas.prototype.update = function () { this.render(); };

AF.Canvas.prototype._bind = function () {
  var self = this, wrap = this.wrap, drag = null, pan = null;
  wrap.addEventListener('mousedown', function (e) {
    var handle = e.target.closest('.handle.out'), node = e.target.closest('.node');
    if (e.target.closest('a')) return;
    if (self.mode === 'edit' && handle) {
      var nid = node.dataset.id, a = self._handlePos(nid, handle.dataset.handle, false);
      self._temp = { from: nid, handle: handle.dataset.handle, a: a, b: a }; e.preventDefault(); return;
    }
    if (node) {
      self.select(node.dataset.id);
      if (self.mode === 'edit') { var n = self.dag.nodes.find(function (x) { return x.id === node.dataset.id; }); drag = { n: n, sx: e.clientX, sy: e.clientY, ox: n.position.x, oy: n.position.y, el: node }; }
      e.preventDefault(); return;
    }
    pan = { sx: e.clientX, sy: e.clientY, ox: self.tx, oy: self.ty }; self.select(null);
  });
  window.addEventListener('mousemove', function (e) {
    if (drag) { drag.n.position.x = Math.round(drag.ox + (e.clientX - drag.sx) / self.scale); drag.n.position.y = Math.round(drag.oy + (e.clientY - drag.sy) / self.scale); drag.el.style.left = drag.n.position.x + 'px'; drag.el.style.top = drag.n.position.y + 'px'; self._drawEdges(); }
    else if (pan) { self.tx = pan.ox + (e.clientX - pan.sx); self.ty = pan.oy + (e.clientY - pan.sy); self._applyTransform(); }
    else if (self._temp) { var r = self.el.getBoundingClientRect(); self._temp.b = { x: (e.clientX - r.left) / self.scale, y: (e.clientY - r.top) / self.scale }; self._drawEdges(); }
  });
  window.addEventListener('mouseup', function (e) {
    if (drag) { if (self.opts.onChange) self.opts.onChange('move', drag.n); drag = null; }
    pan = null;
    if (self._temp) {
      var node = e.target.closest && e.target.closest('.node');
      if (node && node.dataset.id !== self._temp.from) {
        var tid = node.dataset.id, exists = self.dag.edges.some(function (x) { return x.source === self._temp.from && x.sourceHandle === self._temp.handle; });
        var tgt = self.dag.nodes.find(function (x) { return x.id === tid; });
        if (tgt.kind === 'start') AF.toast('开始节点不能有入边');
        else if (exists && self._temp.handle !== 'out') AF.toast('该分支已有出边（每个 case 只允许一条）');
        else if (self._wouldCycle(self._temp.from, tid)) AF.toast('不允许成环：DAG 必须无环（循环请用 for-each 节点）');
        else { var edge = { id: 'e_' + Date.now().toString(36), source: self._temp.from, sourceHandle: self._temp.handle, target: tid }; self.dag.edges.push(edge); if (self.opts.onChange) self.opts.onChange('connect', edge); AF.toast('已连线 ' + self._temp.from + ' → ' + tid); }
      }
      self._temp = null; self._drawEdges();
    }
  });
  wrap.addEventListener('wheel', function (e) { if (e.ctrlKey || e.metaKey) { e.preventDefault(); self.zoom(e.deltaY < 0 ? 1.1 : 0.9); } }, { passive: false });
  document.addEventListener('keydown', function (e) { if (self.mode === 'edit' && (e.key === 'Delete' || e.key === 'Backspace') && self.selected && !e.target.closest('input,textarea,select')) { self.removeSelected(); } });
  // drop from palette
  wrap.addEventListener('dragover', function (e) { e.preventDefault(); });
  wrap.addEventListener('drop', function (e) {
    e.preventDefault(); var data = e.dataTransfer.getData('text/plain'); if (!data || !self.opts.onDrop) return;
    var r = self.el.getBoundingClientRect(); self.opts.onDrop(JSON.parse(data), Math.round((e.clientX - r.left) / self.scale) - 90, Math.round((e.clientY - r.top) / self.scale) - 30);
  });
};

AF.Canvas.prototype._wouldCycle = function (from, to) {
  var adj = {}; this.dag.edges.forEach(function (e) { (adj[e.source] = adj[e.source] || []).push(e.target); });
  var seen = {}, stack = [to];
  while (stack.length) { var x = stack.pop(); if (x === from) return true; if (seen[x]) continue; seen[x] = 1; (adj[x] || []).forEach(function (y) { stack.push(y); }); }
  return false;
};
