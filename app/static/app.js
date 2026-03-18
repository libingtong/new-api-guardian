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
    label = `${row.token_id} - ${row.name || row.token_name || '-'}`;
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

function renderTable(tbodyId, rows, cols) {
  const tbody = document.querySelector(`#${tbodyId} tbody`);
  tbody.innerHTML = rows.map((row) => `
    <tr>
      ${cols.map((c) => `<td>${c(row)}</td>`).join('')}
    </tr>
  `).join('');
}

function renderRankTable(rows, dimension) {
  const tbody = document.querySelector('#rankTable tbody');
  tbody.innerHTML = rows.map((row) => `
    <tr>
      <td>${text(row.name || row.username)}</td>
      <td>${num(row.requests)}</td>
      <td>${num(row.total_tokens)}</td>
      <td>${num(row.total_quota)}</td>
      <td>${num(row.error_requests)}</td>
      <td>${num(row.unique_ip_count)}</td>
      <td>${text(row.ip_list)}</td>
      <td>${createDetailButton(row, dimension)}</td>
    </tr>
  `).join('');
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
    opt.textContent = `${t.token_id} - ${t.token_name || '(unknown)'}`;
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

  const [overview, modelUsage, recent] = await Promise.all([
    getJson('/api/overview', filters),
    getJson('/api/token-model-usage', { ...filters, limit: 50 }),
    getJson('/api/recent-logs', { ...filters, limit: 50 }),
  ]);

  const rankDimension = document.getElementById('rankDimension').value;
  const rankMetric = document.getElementById('rankMetric').value;
  const rank = await getJson('/api/rankings', {
    ...filters,
    dimension: rankDimension,
    metric: rankMetric,
    limit: 50,
  });

  renderOverview(overview);

  renderTable('modelUsageTable', modelUsage.items, [
    (r) => text(r.model_name),
    (r) => num(r.requests),
    (r) => num(r.total_tokens),
    (r) => num(r.total_quota),
    (r) => num(r.error_requests),
    (r) => num(r.unique_ip_count),
  ]);

  renderRankTable(rank.items || [], rank.dimension);

  renderTable('recentTable', recent.items, [
    (r) => r.id,
    (r) => formatTs(r.created_at),
    (r) => text(r.username),
    (r) => `${r.token_id ?? '-'} ${r.token_name || ''}`,
    (r) => text(r.model_name),
    (r) => text(r.ip),
    (r) => r.type,
    (r) => num(r.quota),
    (r) => num(r.prompt_tokens),
    (r) => num(r.completion_tokens),
    (r) => num(r.use_time),
  ]);
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

async function main() {
  initDefaultTime();
  bindModal();
  bindRankDetails();
  await loadFilters();
  document.getElementById('runBtn').addEventListener('click', run);
  await run();
}

main().catch((e) => {
  console.error(e);
  alert('加载失败，请检查数据库连接和后端日志');
});
