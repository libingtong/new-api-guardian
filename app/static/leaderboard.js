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

function renderTable(items) {
  const tbody = document.querySelector('#leaderboardTable tbody');
  tbody.innerHTML = items.map((r) => {
    const models = (r.models_used_top5 || []).join(', ');
    const label = `${r.token_id} - ${r.token_name || '-'}`;
    return `
      <tr>
        <td>${r.rank}</td>
        <td>${label}</td>
        <td>${num(r.total_tokens)}</td>
        <td>${num(r.requests)}</td>
        <td>${num(r.total_quota)}</td>
        <td>${num(r.active_days)}</td>
        <td>${num(r.model_count)}</td>
        <td>${r.top_model || '-'}</td>
        <td>${models || '-'}</td>
        <td>${num(r.unique_ip_count)}</td>
        <td>${text(r.ip_list)}</td>
        <td>
          <button
            type="button"
            class="link-btn detail-btn"
            data-token-id="${r.token_id}"
            data-label="${encodeURIComponent(label)}"
          >详情</button>
        </td>
        <td><span class="score-pill">${r.workload_index}</span></td>
      </tr>
    `;
  }).join('');
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
  renderTable(data.items || []);
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

async function main() {
  initDefaultTime();
  bindModal();
  bindDetails();
  await loadGroups();
  document.getElementById('runBtn').addEventListener('click', run);
  await run();
}

main().catch((e) => {
  console.error(e);
  alert('加载排行榜失败，请检查服务日志');
});
