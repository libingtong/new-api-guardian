const LEADERBOARD_STATE = {
  items: [],
};

const LEADERBOARD_COLUMN_STORAGE_KEY = 'guardian.leaderboard.columns.v2';
const LEADERBOARD_DEFAULT_COLUMNS = [
  'rank',
  'token',
  'total_tokens',
  'requests',
  'top_model',
  'unique_ip_count',
  'ip_list',
  'detail',
  'workload_index',
];

const LEADERBOARD_COLUMNS = [
  { key: 'rank', label: '排名', render: (r) => r.rank },
  { key: 'token', label: '令牌', render: (r) => `${r.token_id} - ${r.token_name || '-'}` },
  { key: 'total_tokens', label: '总 Tokens', render: (r) => num(r.total_tokens) },
  { key: 'requests', label: '请求数', render: (r) => num(r.requests) },
  { key: 'total_quota', label: '总 Quota', render: (r) => num(r.total_quota) },
  { key: 'active_days', label: '活跃天数', render: (r) => num(r.active_days) },
  { key: 'model_count', label: '模型数', render: (r) => num(r.model_count) },
  { key: 'top_model', label: '主力模型', render: (r) => r.top_model || '-' },
  { key: 'models_used_top5', label: '模型列表（Top5）', render: (r) => (r.models_used_top5 || []).join(', ') || '-' },
  { key: 'unique_ip_count', label: 'IP 数量', render: (r) => num(r.unique_ip_count) },
  { key: 'ip_list', label: '访问 IP（Top5）', render: (r) => text(r.ip_list) },
  {
    key: 'detail',
    label: '详情',
    render: (r) => `
      <button
        type="button"
        class="link-btn detail-btn"
        data-token-id="${r.token_id}"
        data-label="${encodeURIComponent(`${r.token_id} - ${r.token_name || '-'}`)}"
      >详情</button>
    `,
  },
  { key: 'workload_index', label: '工作量指数', render: (r) => `<span class="score-pill">${r.workload_index}</span>` },
];

function toLocalInputValue(ts) {
  const d = new Date(ts * 1000);
  const pad = (n) => String(n).padStart(2, '0');
  const y = d.getFullYear();
  const m = pad(d.getMonth() + 1);
  const day = pad(d.getDate());
  const hh = pad(d.getHours());
  const mm = pad(d.getMinutes());
  return `${y}-${m}-${day}T${hh}:${mm}`;
}

function parseLocalInputToTs(v) {
  if (!v) return null;
  const [datePart, timePart] = v.split('T');
  if (!datePart || !timePart) return null;
  const [y, m, d] = datePart.split('-').map(Number);
  const [hh, mm] = timePart.split(':').map(Number);
  const dt = new Date(y, (m || 1) - 1, d || 1, hh || 0, mm || 0, 0);
  return Math.floor(dt.getTime() / 1000);
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

function getVisibleColumns() {
  const all = LEADERBOARD_COLUMNS.map((item) => item.key);
  try {
    const raw = localStorage.getItem(LEADERBOARD_COLUMN_STORAGE_KEY);
    if (!raw) return LEADERBOARD_DEFAULT_COLUMNS.filter((item) => all.includes(item));
    const parsed = JSON.parse(raw);
    const filtered = parsed.filter((item) => all.includes(item));
    return filtered.length ? filtered : LEADERBOARD_DEFAULT_COLUMNS.filter((item) => all.includes(item));
  } catch {
    return LEADERBOARD_DEFAULT_COLUMNS.filter((item) => all.includes(item));
  }
}

function saveVisibleColumns(keys) {
  localStorage.setItem(LEADERBOARD_COLUMN_STORAGE_KEY, JSON.stringify(keys));
}

function initDefaultTime() {
  const now = new Date();
  const end = Math.floor(now.getTime() / 1000);
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0);
  const start = Math.floor(monthStart.getTime() / 1000);
  document.getElementById('startTime').value = toLocalInputValue(start);
  document.getElementById('endTime').value = toLocalInputValue(end);
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

async function openIpDetail(tokenId, label) {
  const params = {
    dimension: 'token',
    token_id: tokenId,
    start_ts: parseLocalInputToTs(document.getElementById('startTime').value),
    end_ts: parseLocalInputToTs(document.getElementById('endTime').value),
    group: document.getElementById('groupSelect').value,
    limit: 200,
  };

  document.getElementById('ipDetailTitle').textContent = `${label} 的 IP 详情`;
  document.getElementById('ipDetailSubtitle').textContent = '加载中...';
  document.getElementById('ipDetailSummary').innerHTML = '';
  document.querySelector('#ipDetailTable tbody').innerHTML = '';
  openModal();

  const data = await getJson('/api/ip-usage-details', params);
  const summary = data.summary || {};
  document.getElementById('ipDetailSubtitle').textContent =
    `当前筛选条件下有记录的访问 IP 共 ${num(summary.unique_ip_count)} 个`;
  renderDetailSummary(summary);
  renderDetailTable(data.items || []);
}

function renderColumnPicker() {
  const mount = document.getElementById('leaderboardColumnPicker');
  const visible = new Set(getVisibleColumns());
  mount.innerHTML = `
    <div class="column-picker" data-column-picker="leaderboard">
      <button class="ghost-btn mini-btn column-picker-trigger" type="button" aria-expanded="false">列设置</button>
      <div class="column-picker-panel hidden">
        <h4>排行榜列</h4>
        <div class="column-picker-grid">
          ${LEADERBOARD_COLUMNS.map((column) => `
            <label>
              <input type="checkbox" value="${column.key}" ${visible.has(column.key) ? 'checked' : ''} />
              <span>${column.label}</span>
            </label>
          `).join('')}
        </div>
      </div>
    </div>
  `;
}

function renderTable() {
  const table = document.getElementById('leaderboardTable');
  const thead = table.querySelector('thead');
  const tbody = table.querySelector('tbody');
  const visibleKeys = getVisibleColumns();
  const columns = LEADERBOARD_COLUMNS.filter((column) => visibleKeys.includes(column.key));
  thead.innerHTML = `<tr>${columns.map((column) => `<th>${column.label}</th>`).join('')}</tr>`;
  if (!LEADERBOARD_STATE.items.length) {
    tbody.innerHTML = `<tr><td colspan="${Math.max(columns.length, 1)}">暂无排行榜数据</td></tr>`;
    return;
  }
  tbody.innerHTML = LEADERBOARD_STATE.items.map((row) => `
    <tr>
      ${columns.map((column) => `<td>${column.render(row)}</td>`).join('')}
    </tr>
  `).join('');
}

async function loadGroups() {
  const data = await getJson('/api/filters');
  const groupSelect = document.getElementById('groupSelect');
  data.groups.forEach((g) => {
    const opt = document.createElement('option');
    opt.value = g;
    opt.textContent = g;
    groupSelect.appendChild(opt);
  });
  if (data.groups.includes('coding')) {
    groupSelect.value = 'coding';
  }
}

async function run() {
  const params = {
    start_ts: parseLocalInputToTs(document.getElementById('startTime').value),
    end_ts: parseLocalInputToTs(document.getElementById('endTime').value),
    group: document.getElementById('groupSelect').value,
    limit: document.getElementById('limitInput').value || 100,
  };
  const data = await getJson('/api/leaderboard', params);
  LEADERBOARD_STATE.items = data.items || [];
  renderTable();
}

function bindModal() {
  document.getElementById('ipDetailCloseBtn').addEventListener('click', closeModal);
  document.querySelector('#ipDetailModal .modal-backdrop').addEventListener('click', closeModal);
}

function bindDetails() {
  document.querySelector('#leaderboardTable tbody').addEventListener('click', async (event) => {
    const btn = event.target.closest('.detail-btn');
    if (!btn) return;
    try {
      await openIpDetail(
        Number(btn.dataset.tokenId),
        btn.dataset.label ? decodeURIComponent(btn.dataset.label) : '当前令牌',
      );
    } catch (e) {
      console.error(e);
      alert('加载 IP 详情失败，请检查服务日志');
      closeModal();
    }
  });
}

function bindColumnPicker() {
  renderColumnPicker();
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
    const checkbox = event.target.closest('#leaderboardColumnPicker input[type="checkbox"]');
    if (!checkbox) return;
    const selected = Array.from(document.querySelectorAll('#leaderboardColumnPicker input[type="checkbox"]:checked'))
      .map((node) => node.value);
    if (!selected.length) {
      checkbox.checked = true;
      return;
    }
    saveVisibleColumns(selected);
    renderTable();
  });
}

async function main() {
  initDefaultTime();
  bindModal();
  bindDetails();
  bindColumnPicker();
  await loadGroups();
  document.getElementById('runBtn').addEventListener('click', run);
  await run();
}

main().catch((e) => {
  console.error(e);
  alert('加载排行榜失败，请检查服务日志');
});
