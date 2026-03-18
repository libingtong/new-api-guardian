const DASHBOARD_STATE = {
  overview: null,
  modelUsage: [],
  recent: [],
  rank: { items: [], dimension: 'token' },
};

const COLUMN_STORAGE_PREFIX = 'guardian.analytics.columns.';

const TABLE_COLUMNS = {
  modelUsage: [
    { key: 'model_name', label: '模型', render: (r) => text(r.model_name) },
    { key: 'requests', label: '请求数', render: (r) => num(r.requests) },
    { key: 'total_tokens', label: '总 Tokens', render: (r) => num(r.total_tokens) },
    { key: 'total_quota', label: '总 Quota', render: (r) => num(r.total_quota) },
    { key: 'error_requests', label: '错误数', render: (r) => num(r.error_requests) },
    { key: 'unique_ip_count', label: '访问 IP 数', render: (r) => num(r.unique_ip_count) },
  ],
  rank: [
    { key: 'name', label: '名称', render: (r) => text(r.name || r.username) },
    { key: 'requests', label: '请求数', render: (r) => num(r.requests) },
    { key: 'total_tokens', label: '总 Tokens', render: (r) => num(r.total_tokens) },
    { key: 'total_quota', label: '总 Quota', render: (r) => num(r.total_quota) },
    { key: 'error_requests', label: '错误数', render: (r) => num(r.error_requests) },
    { key: 'unique_ip_count', label: 'IP 数量', render: (r) => num(r.unique_ip_count) },
    { key: 'ip_list', label: '访问 IP（Top5）', render: (r) => text(r.ip_list) },
    { key: 'detail', label: '详情', render: (r) => createDetailButton(r, DASHBOARD_STATE.rank.dimension) },
  ],
  recent: [
    { key: 'id', label: 'ID', render: (r) => r.id },
    { key: 'created_at', label: '时间', render: (r) => formatTs(r.created_at) },
    { key: 'username', label: '用户', render: (r) => text(r.username) },
    { key: 'token', label: '令牌', render: (r) => `${r.token_name || '-'}（${r.token_id ?? '-'}）` },
    { key: 'model_name', label: '模型', render: (r) => text(r.model_name) },
    { key: 'ip', label: 'IP', render: (r) => text(r.ip) },
    { key: 'type', label: '类型', render: (r) => r.type },
    { key: 'quota', label: 'Quota', render: (r) => num(r.quota) },
    { key: 'prompt_tokens', label: 'Prompt', render: (r) => num(r.prompt_tokens) },
    { key: 'completion_tokens', label: 'Completion', render: (r) => num(r.completion_tokens) },
    { key: 'use_time', label: '耗时(ms)', render: (r) => num(r.use_time) },
  ],
};

function toLocalInputValue(ts) {
  const d = new Date(ts * 1000);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function parseLocalInputToTs(v) {
  if (!v) return null;
  return Math.floor(new Date(v).getTime() / 1000);
}

function num(v) {
  return Number(v || 0).toLocaleString('zh-CN');
}

function text(v) {
  return v ? String(v) : '-';
}

function formatTs(ts) {
  if (!ts) return '-';
  return new Date(ts * 1000).toLocaleString('zh-CN');
}

function qs(params) {
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') sp.append(k, v);
  });
  return sp.toString();
}

async function getJson(url, params = {}) {
  const q = qs(params);
  const full = q ? `${url}?${q}` : url;
  const r = await fetch(full);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

function getFilters() {
  return {
    start_ts: parseLocalInputToTs(document.getElementById('startTime').value),
    end_ts: parseLocalInputToTs(document.getElementById('endTime').value),
    token_id: document.getElementById('tokenSelect').value,
    user_id: document.getElementById('userSelect').value,
    model_name: document.getElementById('modelSelect').value,
    group: document.getElementById('groupSelect').value,
  };
}

function storageKey(tableKey) {
  return `${COLUMN_STORAGE_PREFIX}${tableKey}`;
}

function getVisibleColumns(tableKey) {
  const all = TABLE_COLUMNS[tableKey].map((item) => item.key);
  try {
    const raw = localStorage.getItem(storageKey(tableKey));
    if (!raw) return all;
    const parsed = JSON.parse(raw);
    const filtered = parsed.filter((item) => all.includes(item));
    return filtered.length ? filtered : all;
  } catch {
    return all;
  }
}

function saveVisibleColumns(tableKey, visibleColumns) {
  localStorage.setItem(storageKey(tableKey), JSON.stringify(visibleColumns));
}

function createDetailButton(row, dimension) {
  let label = '';
  if (dimension === 'user' && row.user_id !== undefined && row.user_id !== null) {
    label = row.name || row.username || `用户 ${row.user_id}`;
    return `
      <button
        type="button"
        class="link-btn detail-btn"
        data-dimension="user"
        data-user-id="${row.user_id}"
        data-label="${encodeURIComponent(label)}"
      >详情</button>
    `;
  }
  if (dimension === 'token' && row.token_id !== undefined && row.token_id !== null) {
    label = `${row.name || row.token_name || '-'}（${row.token_id}）`;
    return `
      <button
        type="button"
        class="link-btn detail-btn"
        data-dimension="token"
        data-token-id="${row.token_id}"
        data-label="${encodeURIComponent(label)}"
      >详情</button>
    `;
  }
  if (dimension === 'model' && row.name) {
    label = row.name;
    return `
      <button
        type="button"
        class="link-btn detail-btn"
        data-dimension="model"
        data-model-name="${encodeURIComponent(row.name)}"
        data-label="${encodeURIComponent(label)}"
      >详情</button>
    `;
  }
  return '-';
}

function renderColumnPicker(mountId, tableKey, title) {
  const mount = document.getElementById(mountId);
  if (!mount) return;
  const visible = new Set(getVisibleColumns(tableKey));
  mount.innerHTML = `
    <div class="column-picker" data-column-picker="${tableKey}">
      <button class="ghost-btn mini-btn column-picker-trigger" type="button" aria-expanded="false">列设置</button>
      <div class="column-picker-panel hidden">
        <h4>${title}</h4>
        <div class="column-picker-grid">
          ${TABLE_COLUMNS[tableKey]
            .map(
              (column) => `
                <label>
                  <input type="checkbox" data-table-key="${tableKey}" value="${column.key}" ${visible.has(column.key) ? 'checked' : ''} />
                  <span>${column.label}</span>
                </label>
              `
            )
            .join('')}
        </div>
      </div>
    </div>
  `;
}

function renderOverview(data) {
  const cards = [
    ['请求数', num(data.requests)],
    ['总 Quota', num(data.total_quota)],
    ['总 Tokens', num(data.total_tokens)],
    ['Prompt Tokens', num(data.total_prompt_tokens)],
    ['Completion Tokens', num(data.total_completion_tokens)],
    ['成功率', `${data.success_rate}%`],
    ['错误率', `${data.error_rate}%`],
    ['平均耗时(ms)', Number(data.avg_use_time_ms || 0).toFixed(2)],
    ['访问 IP 数', num(data.unique_ip_count)],
    ['访问 IP（Top5）', text(data.ip_list)],
  ];

  const wrap = document.getElementById('overviewCards');
  wrap.innerHTML = cards.map(([k, v]) => `
    <div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>
  `).join('');
}

function renderConfigurableTable(tableId, tableKey, rows, emptyText) {
  const table = document.getElementById(tableId);
  const thead = table.querySelector('thead');
  const tbody = table.querySelector('tbody');
  const visibleKeys = getVisibleColumns(tableKey);
  const columns = TABLE_COLUMNS[tableKey].filter((column) => visibleKeys.includes(column.key));
  thead.innerHTML = `<tr>${columns.map((column) => `<th>${column.label}</th>`).join('')}</tr>`;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="${Math.max(columns.length, 1)}">${emptyText}</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map((row) => `
    <tr>
      ${columns.map((column) => `<td>${column.render(row)}</td>`).join('')}
    </tr>
  `).join('');
}

function renderDashboard() {
  if (DASHBOARD_STATE.overview) renderOverview(DASHBOARD_STATE.overview);
  renderConfigurableTable('modelUsageTable', 'modelUsage', DASHBOARD_STATE.modelUsage, '暂无模型分布数据');
  renderConfigurableTable('rankTable', 'rank', DASHBOARD_STATE.rank.items || [], '暂无排行榜数据');
  renderConfigurableTable('recentTable', 'recent', DASHBOARD_STATE.recent, '暂无最近请求数据');
}

function openModal() {
  document.getElementById('ipDetailModal').classList.remove('hidden');
}

function closeModal() {
  document.getElementById('ipDetailModal').classList.add('hidden');
}

function renderDetailSummary(summary) {
  const cards = [
    ['IP 数量', num(summary.unique_ip_count)],
    ['请求数', num(summary.requests)],
    ['总 Tokens', num(summary.total_tokens)],
    ['总 Quota', num(summary.total_quota)],
    ['错误数', num(summary.error_requests)],
  ];
  const wrap = document.getElementById('ipDetailSummary');
  wrap.innerHTML = cards.map(([k, v]) => `
    <div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>
  `).join('');
}

function renderDetailTable(items) {
  const tbody = document.querySelector('#ipDetailTable tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="7">暂无有 IP 记录的访问明细</td></tr>';
    return;
  }
  tbody.innerHTML = items.map((item) => `
    <tr>
      <td>${text(item.ip)}</td>
      <td>${num(item.requests)}</td>
      <td>${num(item.total_tokens)}</td>
      <td>${num(item.total_quota)}</td>
      <td>${num(item.error_requests)}</td>
      <td>${formatTs(item.first_seen_at)}</td>
      <td>${formatTs(item.last_seen_at)}</td>
    </tr>
  `).join('');
}

async function openIpDetail(detail) {
  const filters = getFilters();
  const params = {
    dimension: detail.dimension,
    start_ts: filters.start_ts,
    end_ts: filters.end_ts,
    group: filters.group,
    filter_token_id: filters.token_id,
    filter_user_id: filters.user_id,
    filter_model_name: filters.model_name,
    limit: 200,
  };
  if (detail.dimension === 'user') {
    params.user_id = detail.userId;
  } else if (detail.dimension === 'token') {
    params.token_id = detail.tokenId;
  } else {
    params.model_name = detail.modelName;
  }

  document.getElementById('ipDetailTitle').textContent = `${detail.label} 的 IP 详情`;
  document.getElementById('ipDetailSubtitle').textContent = '加载中...';
  document.querySelector('#ipDetailTable tbody').innerHTML = '';
  document.getElementById('ipDetailSummary').innerHTML = '';
  openModal();

  const data = await getJson('/api/ip-usage-details', params);
  const summary = data.summary || {};
  document.getElementById('ipDetailSubtitle').textContent =
    `当前筛选条件下有记录的访问 IP 共 ${num(summary.unique_ip_count)} 个`;
  renderDetailSummary(summary);
  renderDetailTable(data.items || []);
}

async function loadFilters() {
  const data = await getJson('/api/filters');

  const tokenSelect = document.getElementById('tokenSelect');
  data.tokens.forEach((t) => {
    const opt = document.createElement('option');
    opt.value = t.token_id;
    opt.textContent = `${t.token_name || '(unknown)'}（${t.token_id}）`;
    tokenSelect.appendChild(opt);
  });

  const userSelect = document.getElementById('userSelect');
  data.users.forEach((u) => {
    const opt = document.createElement('option');
    opt.value = u.user_id;
    opt.textContent = `${u.user_id} - ${u.username || '(unknown)'}`;
    userSelect.appendChild(opt);
  });

  const modelSelect = document.getElementById('modelSelect');
  data.models.forEach((m) => {
    const opt = document.createElement('option');
    opt.value = m;
    opt.textContent = m;
    modelSelect.appendChild(opt);
  });

  const groupSelect = document.getElementById('groupSelect');
  data.groups.forEach((g) => {
    const opt = document.createElement('option');
    opt.value = g;
    opt.textContent = g;
    groupSelect.appendChild(opt);
  });
}

async function run() {
  const filters = getFilters();
  const rankDimension = document.getElementById('rankDimension').value;
  const rankMetric = document.getElementById('rankMetric').value;

  const [overview, modelUsage, recent, rank] = await Promise.all([
    getJson('/api/overview', filters),
    getJson('/api/token-model-usage', { ...filters, limit: 50 }),
    getJson('/api/recent-logs', { ...filters, limit: 50 }),
    getJson('/api/rankings', {
      ...filters,
      dimension: rankDimension,
      metric: rankMetric,
      limit: 50,
    }),
  ]);

  DASHBOARD_STATE.overview = overview;
  DASHBOARD_STATE.modelUsage = modelUsage.items || [];
  DASHBOARD_STATE.recent = recent.items || [];
  DASHBOARD_STATE.rank = rank;
  renderDashboard();
}

function initDefaultTime() {
  const now = new Date();
  const end = Math.floor(now.getTime() / 1000);
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0);
  const start = Math.floor(monthStart.getTime() / 1000);
  document.getElementById('startTime').value = toLocalInputValue(start);
  document.getElementById('endTime').value = toLocalInputValue(end);
}

function bindModal() {
  document.getElementById('ipDetailCloseBtn').addEventListener('click', closeModal);
  document.querySelector('#ipDetailModal .modal-backdrop').addEventListener('click', closeModal);
}

function bindRankDetails() {
  document.querySelector('#rankTable tbody').addEventListener('click', async (event) => {
    const btn = event.target.closest('.detail-btn');
    if (!btn) return;
    try {
      await openIpDetail({
        dimension: btn.dataset.dimension,
        userId: btn.dataset.userId ? Number(btn.dataset.userId) : null,
        tokenId: btn.dataset.tokenId ? Number(btn.dataset.tokenId) : null,
        modelName: btn.dataset.modelName ? decodeURIComponent(btn.dataset.modelName) : '',
        label: btn.dataset.label ? decodeURIComponent(btn.dataset.label) : '当前对象',
      });
    } catch (e) {
      console.error(e);
      alert('加载 IP 详情失败，请检查后端日志');
      closeModal();
    }
  });
}

function bindColumnPickers() {
  renderColumnPicker('modelUsageColumnPicker', 'modelUsage', '模型分布列');
  renderColumnPicker('rankColumnPicker', 'rank', '排行榜列');
  renderColumnPicker('recentColumnPicker', 'recent', '最近请求列');

  document.addEventListener('click', (event) => {
    const trigger = event.target.closest('.column-picker-trigger');
    const clickedInsidePanel = event.target.closest('.column-picker-panel');
    if (!trigger && clickedInsidePanel) return;
    document.querySelectorAll('.column-picker').forEach((picker) => {
      const panel = picker.querySelector('.column-picker-panel');
      const button = picker.querySelector('.column-picker-trigger');
      const shouldOpen = trigger && picker === trigger.closest('.column-picker') ? panel.classList.contains('hidden') : false;
      panel.classList.toggle('hidden', !shouldOpen);
      button.setAttribute('aria-expanded', shouldOpen ? 'true' : 'false');
    });
  });

  document.addEventListener('change', (event) => {
    const checkbox = event.target.closest('input[type="checkbox"][data-table-key]');
    if (!checkbox) return;
    const tableKey = checkbox.dataset.tableKey;
    const selected = Array.from(
      document.querySelectorAll(`input[type="checkbox"][data-table-key="${tableKey}"]:checked`)
    ).map((node) => node.value);
    if (!selected.length) {
      checkbox.checked = true;
      return;
    }
    saveVisibleColumns(tableKey, selected);
    renderDashboard();
  });
}

async function main() {
  initDefaultTime();
  bindModal();
  bindRankDetails();
  bindColumnPickers();
  await loadFilters();
  document.getElementById('runBtn').addEventListener('click', run);
  await run();
}

main().catch((e) => {
  console.error(e);
  alert('加载失败，请检查数据库连接和后端日志');
});
