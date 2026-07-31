// ui.js -- Dashboard UI helpers
// KPI loading, IoT simulation, blockchain table, alerts table, timeline

/* ============================================================
   KPI LOADING  --  /api/kpis
============================================================ */
async function loadKPIs() {
  try {
    var res = await fetch('/api/kpis', { credentials: 'include' });
    if (!res.ok) return;
    var d = await res.json();
    var map = {
      'total-batches':     d.total_batches,
      'total-sensors':     d.total_sensors,
      'active-shipments':  d.active_shipments,
      'blockchain-tx':     d.blockchain_transactions,
      'alerts-today':      d.alerts_today,
      'healthy-shipments': d.healthy_shipments
    };
    Object.keys(map).forEach(function(id) {
      var el = document.getElementById(id);
      if (el) el.textContent = map[id];
    });
  } catch (err) {
    console.warn('[ui.js] KPI fetch failed:', err);
  }
}

/* ============================================================
   BLOCKCHAIN TABLE  --  reuses existing /api/data endpoint
   Shows last 20 sensor records with hash and Fabric TX
============================================================ */
async function loadBlockchainTable() {
  var tbody = document.getElementById('blockchain-table-body');
  if (!tbody) return;
  try {
    var res = await fetch('/api/data', { credentials: 'include' });
    if (!res.ok) { tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;">Failed to load</td></tr>'; return; }
    var json = await res.json();
    var rows = (json.data || []).slice(0, 20);
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--muted);">No records yet.</td></tr>'; return; }
    tbody.innerHTML = rows.map(function(r) {
      var hash = r.block_hash || '';
      var shortHash = hash ? (hash.slice(0,8) + '...' + hash.slice(-6)) : '<em style="color:var(--muted)">pending</em>';
      var fabricTx = r.fabric_tx_id || '';
      var shortTx = fabricTx ? (fabricTx.slice(0,10) + '...') : '<em style="color:var(--muted)">offline</em>';
      var stageColor = {'field':'#10b981','warehouse':'#3b82f6','transport':'#f59e0b','retailer':'#8b5cf6','consumer':'#ef4444'}[r.current_stage] || '#64748b';
      return '<tr>' +
        '<td>' + r.id + '</td>' +
        '<td>' + (r.timestamp || '') + '</td>' +
        '<td><strong>' + escapeHtml(r.batch_id) + '</strong></td>' +
        '<td>' + escapeHtml(r.sensor_id || 'SENSOR') + '</td>' +
        '<td>' + (r.temperature !== null ? r.temperature.toFixed(1) : '--') + '</td>' +
        '<td><span style="background:' + stageColor + '22; color:' + stageColor + '; padding:2px 8px; border-radius:12px; font-size:11px; font-weight:700;">' + (r.current_stage || '').toUpperCase() + '</span></td>' +
        '<td style="font-family:monospace; font-size:11px;" title="' + hash + '">' + shortHash + '</td>' +
        '<td style="font-family:monospace; font-size:11px;">' + shortTx + '</td>' +
        '</tr>';
    }).join('');
  } catch (err) {
    console.warn('[ui.js] Blockchain table load failed:', err);
    if (tbody) tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--muted);">Error loading data</td></tr>';
  }
}

/* ============================================================
   ALERTS TABLE  --  reuses /api/batches, filters warning/critical
============================================================ */
async function loadAlertsTable() {
  var tbody = document.getElementById('alerts-table-body');
  if (!tbody) return;
  try {
    var res = await fetch('/api/batches', { credentials: 'include' });
    if (!res.ok) { tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;">Failed to load</td></tr>'; return; }
    var json = await res.json();
    var batches = (json.batches || []).filter(function(b) { return b.risk_level !== 'stable'; });
    if (!batches.length) {
      tbody.innerHTML = '<tr><td colspan="8" style="text-align:center; color:var(--success); font-weight:600;">No active violations — all batches compliant ✓</td></tr>';
      return;
    }
    tbody.innerHTML = batches.map(function(b) {
      var riskColor = b.risk_level === 'critical' ? '#ef4444' : '#f59e0b';
      var riskLabel = b.risk_level === 'critical' ? 'CRITICAL HOLD' : 'WARNING';
      return '<tr>' +
        '<td><strong>' + escapeHtml(b.batch_id) + '</strong></td>' +
        '<td>' + escapeHtml(b.product_name || b.product || '') + '</td>' +
        '<td>' + escapeHtml(b.sensor_id || '') + '</td>' +
        '<td>' + (b.temperature !== null ? b.temperature.toFixed(1) : '--') + '</td>' +
        '<td>' + (b.humidity !== null ? b.humidity.toFixed(1) : '--') + '</td>' +
        '<td>' + escapeHtml(b.current_stage_label || b.current_stage || '') + '</td>' +
        '<td><span style="background:' + riskColor + '22; color:' + riskColor + '; padding:2px 8px; border-radius:12px; font-size:11px; font-weight:700;">' + riskLabel + '</span></td>' +
        '<td style="font-size:11px;">' + escapeHtml(b.edge_decision || '') + '</td>' +
        '</tr>';
    }).join('');
  } catch (err) {
    console.warn('[ui.js] Alerts table load failed:', err);
  }
}

/* ============================================================
   TIMELINE HIGHLIGHTER
   Highlights the stage step that most batches are currently at
============================================================ */
async function updateTimeline() {
  try {
    var res = await fetch('/api/batches', { credentials: 'include' });
    if (!res.ok) return;
    var json = await res.json();
    var batches = json.batches || [];
    if (!batches.length) return;
    // Count stage frequency
    var counts = {};
    batches.forEach(function(b) { var s = b.current_stage || 'transport'; counts[s] = (counts[s] || 0) + 1; });
    var topStage = Object.keys(counts).reduce(function(a, b) { return counts[a] > counts[b] ? a : b; });
    // Highlight steps
    document.querySelectorAll('.timeline-step').forEach(function(el) {
      var s = el.getAttribute('data-stage');
      el.style.background = (s === topStage) ? 'var(--accent)' : '';
      el.style.color = (s === topStage) ? '#fff' : '';
      el.style.borderColor = (s === topStage) ? 'var(--accent)' : 'var(--line)';
    });
    var info = document.getElementById('timeline-batch-info');
    if (info) info.textContent = 'Most active stage: ' + topStage.toUpperCase() + ' (' + counts[topStage] + ' batches). Last updated: ' + new Date().toLocaleTimeString();
  } catch (err) {
    console.warn('[ui.js] Timeline update failed:', err);
  }
}

/* ============================================================
   IOT SIMULATION MODE
   Runs when no real ESP32 device is connected.
============================================================ */
var IOT_STAGES = ['field', 'warehouse', 'transport', 'retailer'];

function simulatedReading() {
  return {
    batch_id:      'SIM-' + (Math.floor(Math.random() * 9000) + 1000),
    temperature:   (20 + Math.random() * 10).toFixed(1),
    humidity:      (40 + Math.random() * 20).toFixed(1),
    current_stage: IOT_STAGES[Math.floor(Math.random() * IOT_STAGES.length)]
  };
}

function updateIoTPanel(r) {
  var el = document.getElementById('iot-simulation');
  if (!el) return;
  var tc = parseFloat(r.temperature) < 28 ? 'sim-ok' : 'sim-warn';
  var hc = parseFloat(r.humidity) < 55    ? 'sim-ok' : 'sim-warn';
  el.innerHTML =
    '<div class="sim-row"><span class="sim-label">Batch ID</span>'    +
      '<span class="sim-value">'             + r.batch_id                    + '</span></div>' +
    '<div class="sim-row"><span class="sim-label">Temperature</span>' +
      '<span class="sim-value ' + tc + '">' + r.temperature + ' &deg;C'     + '</span></div>' +
    '<div class="sim-row"><span class="sim-label">Humidity</span>'    +
      '<span class="sim-value ' + hc + '">' + r.humidity    + ' %'          + '</span></div>' +
    '<div class="sim-row"><span class="sim-label">Stage</span>'       +
      '<span class="sim-value sim-stage">'  + r.current_stage.toUpperCase() + '</span></div>' +
    '<p class="sim-time">Last update: ' + new Date().toLocaleTimeString() + '</p>';
}

var _simTimer = null;

function startIoTSimulation() {
  var badge = document.getElementById('iot-mode-badge');
  if (badge) badge.style.display = 'inline-flex';
  updateIoTPanel(simulatedReading());
  _simTimer = setInterval(function() { updateIoTPanel(simulatedReading()); }, 4000);
}

/* ============================================================
   ADD SIM STYLES INLINE (avoids editing CSS files)
============================================================ */
function injectSimStyles() {
  var s = document.createElement('style');
  s.textContent =
    '.sim-row { display:flex; justify-content:space-between; align-items:center; padding:4px 0; border-bottom:1px solid var(--line); }' +
    '.sim-label { font-size:12px; color:var(--muted); font-weight:600; }' +
    '.sim-value { font-size:14px; font-weight:700; }' +
    '.sim-ok    { color:#10b981; }' +
    '.sim-warn  { color:#ef4444; }' +
    '.sim-stage { color:var(--accent); }' +
    '.sim-time  { font-size:11px; color:var(--muted); margin-top:10px; text-align:right; }' +
    '.timeline-step { text-align:center; padding:14px 20px; border-radius:12px; border:2px solid var(--line); cursor:default; transition:all 0.3s; min-width:100px; }' +
    '.tl-icon  { font-size:28px; margin-bottom:4px; }' +
    '.tl-label { font-weight:700; font-size:13px; }' +
    '.tl-sub   { font-size:11px; color:var(--muted); margin-top:2px; }' +
    '.tl-arrow { font-size:22px; color:var(--muted); padding:0 8px; }';
  document.head.appendChild(s);
}

/* ============================================================
   INIT
============================================================ */
document.addEventListener('DOMContentLoaded', function() {
  injectSimStyles();
  loadKPIs();
  loadBlockchainTable();
  loadAlertsTable();
  updateTimeline();

  var USE_SIMULATION = true;  // set false when ESP32 is connected
  if (USE_SIMULATION) startIoTSimulation();

  // Refresh blockchain + alerts every 30s to stay up to date
  setInterval(function() {
    loadKPIs();
    loadBlockchainTable();
    loadAlertsTable();
    updateTimeline();
  }, 30000);
});
