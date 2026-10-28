// Application State & Polling
let reports = [];
let pollInterval = null;

const STATUS_CONFIG = {
  pending: {
    bg: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
    pulse: true,
    label: 'Pending'
  },
  queued: {
    bg: 'bg-blue-500/10 text-blue-400 border-blue-500/20',
    pulse: true,
    label: 'Queued'
  },
  processing: {
    bg: 'bg-indigo-500/10 text-indigo-400 border-indigo-500/20',
    pulse: true,
    label: 'Processing'
  },
  retrying: {
    bg: 'bg-orange-500/10 text-orange-400 border-orange-500/20',
    pulse: true,
    label: 'Retrying'
  },
  completed: {
    bg: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    pulse: false,
    label: 'Completed'
  },
  failed: {
    bg: 'bg-rose-500/10 text-rose-400 border-rose-500/20',
    pulse: false,
    label: 'Failed'
  }
};

document.addEventListener('DOMContentLoaded', () => {
  setupEventListeners();
  checkSystemStatus();
  fetchReports();
  startPolling();
});

function setupEventListeners() {
  document.getElementById('report-form').addEventListener('submit', handleFormSubmit);
  document.getElementById('gen-key-btn').addEventListener('click', generateRandomIdempotencyKey);
  document.getElementById('refresh-btn').addEventListener('click', fetchReports);
  document.getElementById('reconcile-btn').addEventListener('click', handleReconcile);
}

function generateRandomIdempotencyKey() {
  const key = 'idem-' + Math.random().toString(36).substring(2, 10) + '-' + Date.now();
  document.getElementById('idempotency-key').value = key;
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(() => {
    fetchReports();
    checkSystemStatus();
  }, 2000);
}

async function checkSystemStatus() {
  try {
    const res = await fetch('/ready');
    const data = await res.json();
    const dot = document.getElementById('health-dot');
    const text = document.getElementById('health-text');

    if (data.status === 'ready') {
      dot.className = 'w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse';
      text.innerText = 'RabbitMQ & DB Ready';
      text.className = 'text-emerald-400 font-medium';
    } else {
      dot.className = 'w-2.5 h-2.5 rounded-full bg-rose-500';
      text.innerText = `Degraded (DB: ${data.database}, RMQ: ${data.broker})`;
      text.className = 'text-rose-400 font-medium';
    }
  } catch (err) {
    const dot = document.getElementById('health-dot');
    const text = document.getElementById('health-text');
    dot.className = 'w-2.5 h-2.5 rounded-full bg-rose-500';
    text.innerText = 'API Disconnected';
    text.className = 'text-rose-400 font-medium';
  }
}

async function fetchReports() {
  try {
    const res = await fetch('/reports?limit=50');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    reports = await res.json();
    updateMetrics();
    renderReportsTable();
  } catch (err) {
    console.error('Error fetching reports:', err);
  }
}

function updateMetrics() {
  const total = reports.length;
  const active = reports.filter(r => ['pending', 'queued', 'processing'].includes(r.status)).length;
  const completed = reports.filter(r => r.status === 'completed').length;
  const failed = reports.filter(r => ['failed', 'retrying'].includes(r.status)).length;

  document.getElementById('stat-total').innerText = total;
  document.getElementById('stat-active').innerText = active;
  document.getElementById('stat-completed').innerText = completed;
  document.getElementById('stat-failed').innerText = failed;
}

function renderReportsTable() {
  const tbody = document.getElementById('reports-table-body');
  if (!reports || reports.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" class="text-center py-8 text-slate-500">
          No reports found. Submit a new report request above.
        </td>
      </tr>
    `;
    return;
  }

  tbody.innerHTML = reports.map(report => {
    const statusCfg = STATUS_CONFIG[report.status] || {
      bg: 'bg-slate-800 text-slate-300',
      pulse: false,
      label: report.status
    };

    const pulseClass = statusCfg.pulse ? 'animate-pulse' : '';
    const dateRange = `${report.parameters?.start_date || 'N/A'} ~ ${report.parameters?.end_date || 'N/A'}`;
    const rowCount = report.row_count !== null && report.row_count !== undefined ? report.row_count.toLocaleString() : '-';
    const worker = report.worker_id || '-';

    // Formatting timestamps
    const created = new Date(report.created_at).toLocaleTimeString();
    const updated = new Date(report.updated_at).toLocaleTimeString();

    let actionBtn = '';
    if (report.status === 'completed') {
      actionBtn = `
        <a href="/reports/${report.report_id}/download" 
           class="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-emerald-600/20 hover:bg-emerald-600/30 text-emerald-300 border border-emerald-500/30 font-medium text-[11px] transition">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
          </svg>
          CSV
        </a>
      `;
    } else if (report.status === 'failed') {
      actionBtn = `
        <button onclick="alert('Error description: ${escapeHtml(report.error || 'Unknown error')}')" 
                class="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-rose-600/20 hover:bg-rose-600/30 text-rose-300 border border-rose-500/30 font-medium text-[11px] transition">
          Error Details
        </button>
      `;
    } else {
      actionBtn = `
        <span class="inline-flex items-center gap-1 text-slate-500 text-[11px]">
          <svg class="w-3 h-3 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          In Progress
        </span>
      `;
    }

    return `
      <tr class="hover:bg-slate-800/40 transition">
        <td class="py-3 px-4 font-mono text-[11px] text-slate-300">
          <div class="flex items-center gap-1.5">
            <span>${report.report_id.substring(0, 8)}...</span>
            <button onclick="navigator.clipboard.writeText('${report.report_id}')" title="Copy Full ID" class="text-slate-500 hover:text-slate-300">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"></path>
              </svg>
            </button>
          </div>
        </td>
        <td class="py-3 px-4">
          <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold border ${statusCfg.bg} ${pulseClass}">
            <span class="w-1.5 h-1.5 rounded-full bg-current"></span>
            ${statusCfg.label}
          </span>
        </td>
        <td class="py-3 px-4 text-slate-300">${dateRange}</td>
        <td class="py-3 px-4 text-center text-slate-300">${report.attempts}</td>
        <td class="py-3 px-4 text-right text-slate-300">${rowCount}</td>
        <td class="py-3 px-4 font-mono text-[10px] text-slate-400">${worker}</td>
        <td class="py-3 px-4 text-slate-400 text-[11px]">
          <div>Created: ${created}</div>
          <div class="text-[10px] text-slate-500">Updated: ${updated}</div>
        </td>
        <td class="py-3 px-4 text-center">${actionBtn}</td>
      </tr>
    `;
  }).join('');
}

async function handleFormSubmit(e) {
  e.preventDefault();
  const alertBox = document.getElementById('form-alert');
  const submitBtn = document.getElementById('submit-btn');

  const reportType = document.getElementById('report-type').value;
  const startDate = document.getElementById('start-date').value;
  const endDate = document.getElementById('end-date').value;
  const idempotencyKey = document.getElementById('idempotency-key').value.trim();
  const simulateFailure = document.getElementById('simulate-failure').value;

  alertBox.className = 'mt-4 hidden p-3 rounded-lg text-xs';
  submitBtn.disabled = true;
  submitBtn.innerText = 'Submitting...';

  const headers = { 'Content-Type': 'application/json' };
  if (idempotencyKey) {
    headers['Idempotency-Key'] = idempotencyKey;
  }

  const payload = {
    report_type: reportType,
    start_date: startDate,
    end_date: endDate,
  };
  if (simulateFailure) {
    payload.simulate_failure = simulateFailure;
  }

  try {
    const res = await fetch('/reports', {
      method: 'POST',
      headers: headers,
      body: JSON.stringify(payload)
    });

    const data = await res.json();

    if (res.status === 202 || res.status === 200) {
      alertBox.className = 'mt-4 p-3 rounded-lg text-xs bg-emerald-950/60 border border-emerald-500/30 text-emerald-300';
      alertBox.innerHTML = `<strong>Accepted:</strong> ${data.message} (Report ID: <code class="font-mono">${data.report_id}</code>)`;
      fetchReports();
    } else if (res.status === 409) {
      alertBox.className = 'mt-4 p-3 rounded-lg text-xs bg-amber-950/60 border border-amber-500/30 text-amber-300';
      alertBox.innerHTML = `<strong>409 Conflict:</strong> ${data.detail || 'Idempotency conflict'}`;
    } else {
      alertBox.className = 'mt-4 p-3 rounded-lg text-xs bg-rose-950/60 border border-rose-500/30 text-rose-300';
      alertBox.innerHTML = `<strong>Error ${res.status}:</strong> ${data.detail || JSON.stringify(data)}`;
    }
  } catch (err) {
    alertBox.className = 'mt-4 p-3 rounded-lg text-xs bg-rose-950/60 border border-rose-500/30 text-rose-300';
    alertBox.innerHTML = `<strong>Network Error:</strong> ${err.message}`;
  } finally {
    submitBtn.disabled = false;
    submitBtn.innerText = 'Submit Asynchronous Request';
  }
}

async function handleReconcile() {
  const btn = document.getElementById('reconcile-btn');
  btn.disabled = true;
  btn.innerText = 'Reconciling...';

  try {
    const res = await fetch('/reports/reconcile', { method: 'POST' });
    const data = await res.json();
    alert(`Reconciled ${data.reconciled_count} pending report(s).`);
    fetchReports();
  } catch (err) {
    alert(`Reconciliation failed: ${err.message}`);
  } finally {
    btn.disabled = false;
    btn.innerHTML = `
      <svg class="w-3.5 h-3.5 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
      </svg>
      Reconcile Pending
    `;
  }
}

function escapeHtml(text) {
  const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
  return String(text).replace(/[&<>"']/g, m => map[m]);
}
