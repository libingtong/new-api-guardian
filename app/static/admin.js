function qs(params) {
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') sp.append(k, v);
  });
  return sp.toString();
}

async function getJson(url, params = {}) {
  const full = qs(params) ? `${url}?${qs(params)}` : url;
  const response = await fetch(full);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function sendJson(url, method, payload) {
  const response = await fetch(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

function num(value) {
  return Number(value || 0).toLocaleString('zh-CN');
}

function text(value) {
  return value === undefined || value === null || value === '' ? '-' : String(value);
}

function formatTs(ts) {
  if (!ts) return '-';
  return new Date(ts * 1000).toLocaleString('zh-CN');
}

function badge(label, tone = 'neutral') {
  return `<span class="status-pill ${tone}">${label}</span>`;
}

function parseListInput(value, numeric = false) {
  const raw = String(value || '')
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter(Boolean);
  if (!numeric) return raw;
  return raw.map((item) => Number(item)).filter((item) => Number.isFinite(item));
}

function listToText(values) {
  return Array.isArray(values) && values.length ? values.join(', ') : '';
}

const state = {
  rules: [],
  refreshWarningShown: false,
};

const ACTION_LABELS = {
  disable_channel: '禁用渠道',
  restore_channel: '恢复渠道',
  disable_model: '禁用模型',
  restore_model: '恢复模型',
  disable_unstable_channel: '禁用不稳定渠道（人工恢复）',
};

function actionLabel(actionType) {
  return ACTION_LABELS[actionType] || text(actionType);
}

function isStabilityRule(rule) {
  return rule?.action_type === 'disable_unstable_channel';
}

function scopeSummary(rule) {
  const chunks = [];
  if (rule.match_channel_ids?.length) chunks.push(`渠道: ${rule.match_channel_ids.join(',')}`);
  if (rule.match_groups?.length) chunks.push(`分组: ${rule.match_groups.join(',')}`);
  if (rule.match_models?.length) chunks.push(`模型: ${rule.match_models.join(',')}`);
  return chunks.length ? chunks.join(' | ') : '全部渠道范围';
}

function summarizeRule(rule) {
  if (isStabilityRule(rule)) return scopeSummary(rule);
  const chunks = [];
  if (rule.match_channel_ids?.length) chunks.push(`渠道: ${rule.match_channel_ids.join(',')}`);
  if (rule.match_groups?.length) chunks.push(`分组: ${rule.match_groups.join(',')}`);
  if (rule.match_models?.length) chunks.push(`模型: ${rule.match_models.join(',')}`);
  if (rule.match_error_text?.length) chunks.push(`关键字: ${rule.match_error_text.join(' / ')}`);
  if (rule.match_error_codes?.length) chunks.push(`错误码: ${rule.match_error_codes.join(',')}`);
  if (rule.match_status_codes?.length) chunks.push(`状态码: ${rule.match_status_codes.join(',')}`);
  if (rule.match_request_paths?.length) chunks.push(`路径: ${rule.match_request_paths.join(',')}`);
  return chunks.length ? chunks.join(' | ') : '无额外匹配条件';
}

function syncRuleFormByAction(actionType) {
  const isStability = actionType === 'disable_unstable_channel';
  document.querySelectorAll('[data-event-field="true"]').forEach((node) => {
    node.classList.toggle('hidden', isStability);
  });
  document.querySelectorAll('[data-advanced-field="true"]').forEach((node) => {
    node.classList.toggle('hidden', isStability);
  });
  const advancedBlock = document.getElementById('advancedRuleFields');
  const advancedToggle = document.getElementById('toggleAdvancedRuleFieldsBtn');
  if (isStability) {
    advancedBlock.classList.add('hidden');
    advancedToggle.classList.add('hidden');
    advancedToggle.textContent = '显示高级条件';
  } else {
    advancedToggle.classList.remove('hidden');
  }
  document.getElementById('ruleModalDesc').textContent = isStability
    ? '按历史自动禁用次数定义稳定性规则，用于识别反复抖动且需要人工恢复的渠道'
    : '按渠道、模型、状态码、错误码、返回内容组合定义错误事件规则';
  document.getElementById('ruleWindowSecondsLabel').childNodes[0].textContent = isStability ? '统计窗口（秒）' : '窗口秒数';
  document.getElementById('ruleThresholdCountLabel').childNodes[0].textContent = isStability ? '自动禁用次数阈值' : '阈值次数';
  document.getElementById('ruleFormHint').textContent = isStability
    ? '稳定性规则按历史“自动禁用渠道”次数判断，不使用错误码、状态码、请求路径等实时错误条件。'
    : '保存前提醒：如果没有限定渠道、分组、模型，规则覆盖面会非常大。系统已禁止“全空条件”规则。';
}

function openRuleModal(mode = 'create') {
  document.getElementById('ruleModal').classList.remove('hidden');
  document.getElementById('ruleModalTitle').textContent = mode === 'edit' ? '编辑规则' : '新增规则';
  syncRuleFormByAction(document.getElementById('ruleActionType').value);
}

function closeRuleModal() {
  document.getElementById('ruleModal').classList.add('hidden');
}

function renderSummary(summary) {
  const cards = [
    ['启用规则', num(summary.enabled_rules)],
    ['恢复队列', num(summary.recovery_pending)],
    ['渠道恢复中', num(summary.channel_recovery_pending)],
    ['模型恢复中', num(summary.model_recovery_pending)],
    ['累计禁用渠道', num(summary.disabled_total)],
    ['累计不稳定禁用', num(summary.unstable_disabled_total)],
    ['累计恢复渠道', num(summary.restored_total)],
    ['累计禁用模型', num(summary.disabled_model_total)],
    ['累计恢复模型', num(summary.restored_model_total)],
    ['累计命中', num(summary.hit_total)],
  ];
  document.getElementById('summaryCards').innerHTML = cards
    .map(([k, v]) => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`)
    .join('');
  renderRefreshWarning(summary.new_api_refresh || {});
}

function renderRefreshWarning(refreshStatus) {
  const panel = document.getElementById('refreshWarningPanel');
  const textNode = document.getElementById('refreshWarningText');
  const warning = refreshStatus.warning || '';
  if (!warning) {
    panel.classList.add('hidden');
    textNode.textContent = '';
    return;
  }
  panel.classList.remove('hidden');
  textNode.textContent = warning;
  if (!state.refreshWarningShown && !window.sessionStorage.getItem('newApiRefreshWarningShown')) {
    state.refreshWarningShown = true;
    window.sessionStorage.setItem('newApiRefreshWarningShown', '1');
    alert(warning);
  }
}

function renderRules(items) {
  state.rules = items;
  renderRuleTable('#errorRulesTable', items.filter((rule) => !isStabilityRule(rule)), false);
  renderRuleTable('#stabilityRulesTable', items.filter((rule) => isStabilityRule(rule)), true);
}

function renderRuleTable(selector, items, stabilityMode) {
  const tbody = document.querySelector(`${selector} tbody`);
  if (!items.length) {
    tbody.innerHTML = `<tr><td colspan="7">${stabilityMode ? '暂无稳定性规则' : '暂无错误事件规则'}</td></tr>`;
    return;
  }
  tbody.innerHTML = items
    .map((rule) => `
      <tr>
        <td>${rule.id}</td>
        <td>
          <strong>${text(rule.name)}</strong>
          <div class="subtle-text">优先级 ${num(rule.priority)}</div>
        </td>
        <td>${rule.enabled ? badge('启用', 'success') : badge('停用', 'warning')}</td>
        <td>${badge(actionLabel(rule.action_type), stabilityMode ? 'danger' : rule.action_type === 'disable_model' ? 'warning' : 'danger')}</td>
        <td>${rule.threshold_count} 次 / ${rule.window_seconds} 秒</td>
        <td>${summarizeRule(rule)}</td>
        <td class="inline-actions">
          <button class="link-btn" type="button" data-action="edit" data-id="${rule.id}">编辑</button>
          <button class="ghost-btn mini-btn" type="button" data-action="toggle" data-id="${rule.id}">
            ${rule.enabled ? '停用' : '启用'}
          </button>
          <button class="ghost-btn mini-btn danger-btn" type="button" data-action="delete" data-id="${rule.id}">
            删除
          </button>
        </td>
      </tr>
    `)
    .join('');
}

function renderDisabled(items) {
  const tbody = document.querySelector('#disabledTable tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="7">暂无自动禁用渠道</td></tr>';
    return;
  }
  tbody.innerHTML = items
    .map((item) => `
      <tr>
        <td>${item.id}</td>
        <td>${text(item.name)}</td>
        <td>${badge(`状态 ${item.status}`, 'warning')}</td>
        <td>${text(item.probe_model || item.test_model)}</td>
        <td>${text(item.disabled_reason)}</td>
        <td>${formatTs(item.disabled_at)}</td>
        <td>${formatTs(item.last_probe_at)}</td>
      </tr>
    `)
    .join('');
}

function renderRecovery(items) {
  const tbody = document.querySelector('#recoveryTable tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="6">暂无恢复探测中的渠道</td></tr>';
    return;
  }
  tbody.innerHTML = items
    .map((item) => {
      const tone = item.last_probe_result === 'success' ? 'success' : item.last_probe_result === 'failure' ? 'danger' : 'neutral';
      return `
        <tr>
          <td>${item.id}</td>
          <td>${text(item.name)}</td>
          <td>${item.consecutive_success_count} / 3</td>
          <td>${badge(text(item.last_probe_result || '未探测'), tone)}</td>
          <td>${text(item.last_error)}</td>
          <td>${formatTs(item.last_probe_at)}</td>
        </tr>
      `;
    })
    .join('');
}

function renderUnstable(items) {
  const tbody = document.querySelector('#unstableTable tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="7">暂无需人工恢复的不稳定渠道</td></tr>';
    return;
  }
  tbody.innerHTML = items
    .map((item) => `
      <tr>
        <td>${item.id}</td>
        <td>${text(item.name)}</td>
        <td>${badge('人工恢复', 'danger')}</td>
        <td>${num(item.disable_count_within_window)}</td>
        <td>${text(item.disabled_reason)}</td>
        <td>${formatTs(item.disabled_at)}</td>
        <td><button class="ghost-btn mini-btn" type="button" data-action="manual-restore" data-id="${item.id}">手动恢复</button></td>
      </tr>
    `)
    .join('');
}

function renderModelRecovery(items) {
  const tbody = document.querySelector('#modelRecoveryTable tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="7">暂无恢复探测中的模型</td></tr>';
    return;
  }
  tbody.innerHTML = items
    .map((item) => {
      const tone = item.last_probe_result === 'success' ? 'success' : item.last_probe_result === 'failure' ? 'danger' : 'neutral';
      return `
        <tr>
          <td>${item.channel_id}</td>
          <td>${text(item.channel_name)}</td>
          <td>${text(item.model_name)}</td>
          <td>${item.consecutive_success_count} / 3</td>
          <td>${badge(text(item.last_probe_result || '未探测'), tone)}</td>
          <td>${text(item.last_error)}</td>
          <td>${formatTs(item.last_probe_at)}</td>
        </tr>
      `;
    })
    .join('');
}

function actionBadge(actionType) {
  if (actionType === 'restore_channel') return badge(actionLabel(actionType), 'success');
  if (actionType === 'disable_channel') return badge(actionLabel(actionType), 'danger');
  if (actionType === 'disable_unstable_channel') return badge('禁用不稳定渠道', 'danger');
  if (actionType === 'restore_model') return badge(actionLabel(actionType), 'success');
  if (actionType === 'disable_model') return badge(actionLabel(actionType), 'warning');
  return badge(actionLabel(actionType), 'neutral');
}

function renderActions(items) {
  const tbody = document.querySelector('#actionsTable tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="5">暂无动作记录</td></tr>';
    return;
  }
  tbody.innerHTML = items
    .map((item) => `
      <tr>
        <td>${formatTs(item.created_at)}</td>
        <td>${item.channel_id} - ${text(item.channel_name)}</td>
        <td>${actionBadge(item.action_type)}</td>
        <td>${text(item.before_status)} → ${text(item.after_status)}</td>
        <td>${text(item.reason)}</td>
      </tr>
    `)
    .join('');
}

function renderHits(items) {
  const tbody = document.querySelector('#hitsTable tbody');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="5">暂无命中记录</td></tr>';
    return;
  }
  tbody.innerHTML = items
    .map((item) => {
      const snapshot = item.snapshot_json || {};
      const content = snapshot.content || snapshot.error_code || '-';
      return `
        <tr>
          <td>${formatTs(item.matched_at)}</td>
          <td>${text(item.rule_name)}</td>
          <td>${item.channel_id} - ${text(item.channel_name)}</td>
          <td>${item.log_id}</td>
          <td>${text(content).slice(0, 120)}</td>
        </tr>
      `;
    })
    .join('');
}

function fillRuleForm(rule) {
  document.getElementById('ruleId').value = rule?.id || '';
  document.getElementById('ruleName').value = rule?.name || '';
  document.getElementById('ruleEnabled').value = rule?.enabled ? '1' : '0';
  document.getElementById('rulePriority').value = rule?.priority ?? 100;
  document.getElementById('ruleWindowSeconds').value = rule?.window_seconds ?? 300;
  document.getElementById('ruleThresholdCount').value = rule?.threshold_count ?? 3;
  document.getElementById('ruleActionType').value = rule?.action_type || 'disable_channel';
  document.getElementById('matchChannelIds').value = listToText(rule?.match_channel_ids || []);
  document.getElementById('matchGroups').value = listToText(rule?.match_groups || []);
  document.getElementById('matchModels').value = listToText(rule?.match_models || []);
  document.getElementById('matchErrorText').value = listToText(rule?.match_error_text || []);
  document.getElementById('matchErrorCodes').value = listToText(rule?.match_error_codes || []);
  document.getElementById('matchStatusCodes').value = listToText(rule?.match_status_codes || []);
  document.getElementById('matchRequestPaths').value = listToText(rule?.match_request_paths || []);
  syncRuleFormByAction(document.getElementById('ruleActionType').value);
  const shouldShowAdvanced = Boolean(
    rule?.match_channel_ids?.length ||
    rule?.match_models?.length ||
    rule?.match_groups?.length ||
    rule?.match_request_paths?.length
  );
  setAdvancedFieldsVisible(shouldShowAdvanced && !isStabilityRule(rule));
}

function readRuleForm() {
  return {
    name: document.getElementById('ruleName').value.trim(),
    enabled: document.getElementById('ruleEnabled').value === '1',
    priority: Number(document.getElementById('rulePriority').value || 0),
    window_seconds: Number(document.getElementById('ruleWindowSeconds').value || 300),
    threshold_count: Number(document.getElementById('ruleThresholdCount').value || 3),
    action_type: document.getElementById('ruleActionType').value.trim() || 'disable_channel',
    match_channel_ids: parseListInput(document.getElementById('matchChannelIds').value, true),
    match_groups: parseListInput(document.getElementById('matchGroups').value),
    match_models: parseListInput(document.getElementById('matchModels').value),
    match_error_text: parseListInput(document.getElementById('matchErrorText').value),
    match_error_codes: parseListInput(document.getElementById('matchErrorCodes').value),
    match_status_codes: parseListInput(document.getElementById('matchStatusCodes').value, true),
    match_request_paths: parseListInput(document.getElementById('matchRequestPaths').value),
  };
}

function hasAnyMatchCondition(payload) {
  if (payload.action_type === 'disable_unstable_channel') {
    return Boolean(
      payload.match_channel_ids.length ||
      payload.match_groups.length ||
      payload.match_models.length
    );
  }
  return Boolean(
    payload.match_channel_ids.length ||
    payload.match_groups.length ||
    payload.match_models.length ||
    payload.match_error_text.length ||
    payload.match_error_codes.length ||
    payload.match_status_codes.length ||
    payload.match_request_paths.length
  );
}

function isBroadRiskRule(payload) {
  return !payload.match_channel_ids.length && !payload.match_groups.length && !payload.match_models.length;
}

function setAdvancedFieldsVisible(visible) {
  const advancedBlock = document.getElementById('advancedRuleFields');
  const advancedToggle = document.getElementById('toggleAdvancedRuleFieldsBtn');
  advancedBlock.classList.toggle('hidden', !visible);
  advancedToggle.textContent = visible ? '隐藏高级条件' : '显示高级条件';
}

async function loadDashboard() {
  const [summary, rules, disabled, unstable, recovery, events] = await Promise.all([
    getJson('/api/admin-summary'),
    getJson('/api/rules'),
    getJson('/api/channels/auto-disabled'),
    getJson('/api/channels/unstable-disabled'),
    getJson('/api/channels/recovery-state'),
    getJson('/api/events', { limit: 50 }),
  ]);
  renderSummary(summary);
  renderRules(rules.items || []);
  renderDisabled(disabled.items || []);
  renderUnstable(unstable.items || []);
  renderRecovery(recovery.channel_items || []);
  renderModelRecovery(recovery.model_items || []);
  renderActions(events.actions || []);
  renderHits(events.hits || []);
}

function bindRuleTable() {
  document.querySelectorAll('.rules-table tbody').forEach((tbody) => {
    tbody.addEventListener('click', async (event) => {
      const btn = event.target.closest('button');
      if (!btn) return;
      const ruleId = Number(btn.dataset.id);
      const rule = state.rules.find((item) => item.id === ruleId);
      if (!rule) return;
      if (btn.dataset.action === 'edit') {
        fillRuleForm(rule);
        openRuleModal('edit');
        return;
      }
      if (btn.dataset.action === 'delete') {
        const confirmed = window.confirm(`确认删除规则「${rule.name}」吗？`);
        if (!confirmed) return;
        await fetch(`/api/rules/${ruleId}`, { method: 'DELETE' });
        await loadDashboard();
        return;
      }
      if (btn.dataset.action === 'toggle') {
        const payload = { ...rule, enabled: !rule.enabled };
        await sendJson(`/api/rules/${ruleId}`, 'PUT', payload);
        await loadDashboard();
      }
    });
  });
}

function bindUnstableTable() {
  document.querySelector('#unstableTable tbody').addEventListener('click', async (event) => {
    const btn = event.target.closest('button');
    if (!btn || btn.dataset.action !== 'manual-restore') return;
    const channelId = Number(btn.dataset.id);
    const confirmed = window.confirm(`确认手动恢复渠道 ${channelId} 吗？`);
    if (!confirmed) return;
    await fetch(`/api/channels/${channelId}/manual-restore`, { method: 'POST' });
    await loadDashboard();
  });
}

function bindRuleForm() {
  document.getElementById('toggleAdvancedRuleFieldsBtn').addEventListener('click', () => {
    const advancedBlock = document.getElementById('advancedRuleFields');
    setAdvancedFieldsVisible(advancedBlock.classList.contains('hidden'));
  });
  document.getElementById('ruleActionType').addEventListener('change', (event) => {
    syncRuleFormByAction(event.target.value);
  });
  document.getElementById('ruleForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const payload = readRuleForm();
    if (!payload.name) {
      alert('请填写规则名称');
      return;
    }
    if (!hasAnyMatchCondition(payload)) {
      alert(
        payload.action_type === 'disable_unstable_channel'
          ? '稳定性规则至少需要限定渠道、分组、模型中的一项。'
          : '至少需要填写一项匹配条件，系统不允许保存全空条件规则。'
      );
      return;
    }
    if (isBroadRiskRule(payload)) {
      const confirmed = window.confirm('当前规则没有限定渠道、分组、模型，覆盖范围很大，可能误伤大量渠道。确认继续保存吗？');
      if (!confirmed) return;
    }
    const ruleId = document.getElementById('ruleId').value;
    if (ruleId) {
      await sendJson(`/api/rules/${ruleId}`, 'PUT', payload);
    } else {
      await sendJson('/api/rules', 'POST', payload);
    }
    fillRuleForm(null);
    closeRuleModal();
    await loadDashboard();
  });

  document.getElementById('resetRuleBtn').addEventListener('click', () => {
    fillRuleForm(null);
  });

  document.getElementById('refreshBtn').addEventListener('click', async () => {
    await loadDashboard();
  });
}

function bindRuleModal() {
  document.getElementById('openCreateErrorRuleBtn').addEventListener('click', () => {
    fillRuleForm(null);
    document.getElementById('ruleActionType').value = 'disable_channel';
    syncRuleFormByAction('disable_channel');
    setAdvancedFieldsVisible(false);
    openRuleModal('create');
  });
  document.getElementById('openCreateStabilityRuleBtn').addEventListener('click', () => {
    fillRuleForm(null);
    document.getElementById('ruleActionType').value = 'disable_unstable_channel';
    syncRuleFormByAction('disable_unstable_channel');
    setAdvancedFieldsVisible(false);
    openRuleModal('create');
  });
  document.getElementById('closeRuleModalBtn').addEventListener('click', closeRuleModal);
  document.querySelector('#ruleModal .modal-backdrop').addEventListener('click', closeRuleModal);
}

function bindLogout() {
  const btn = document.getElementById('logoutBtn');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    await fetch('/api/admin-auth/logout', { method: 'POST' });
    window.location.reload();
  });
}

async function main() {
  bindRuleTable();
  bindUnstableTable();
  bindRuleForm();
  bindRuleModal();
  bindLogout();
  fillRuleForm(null);
  await loadDashboard();
}

main().catch((error) => {
  console.error(error);
  alert('控制台加载失败，请检查后端日志和数据库连接');
});
