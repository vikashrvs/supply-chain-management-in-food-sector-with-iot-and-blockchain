/* ═══════════════════════════════════════════════════════════════════════
   FoodChain Dashboard v3.0 — Live Data Manager
   Connects to: /api/kpis, /data, /batches, /alerts
   ═══════════════════════════════════════════════════════════════════════ */

'use strict';

/* ── State ──────────────────────────────────────────────────────────── */
let leafletMap   = null;
let mapPolyline  = null;
let mapMarkers   = [];
let mapTileLayer = null;
let sparkCharts  = {};
let refreshTimer = null;

/* ── Bootstrap ──────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  if (!checkAuth()) return;  // redirect to login if no token

  initClock();
  initUserProfile();
  initMap();
  initSparklines();
  fetchAll();

  // Refresh every 5 seconds
  refreshTimer = setInterval(fetchAll, 5000);
});

/* ══════════════════════════════════════════════════════════════════════
   CLOCK & DATE
═══════════════════════════════════════════════════════════════════════ */
function initClock() {
  function tick() {
    const now = new Date();
    const dateEl = document.getElementById('current-date');
    const timeEl = document.getElementById('current-time');
    if (dateEl) dateEl.textContent = now.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
    if (timeEl) timeEl.textContent = now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }
  tick();
  setInterval(tick, 1000);  // live clock every second
}

/* ══════════════════════════════════════════════════════════════════════
   USER PROFILE
═══════════════════════════════════════════════════════════════════════ */
function initUserProfile() {
  const username = getUsername();
  const avatarEl = document.getElementById('user-avatar');
  if (avatarEl) {
    avatarEl.textContent = username.charAt(0).toUpperCase();
    avatarEl.title = `Logged in as ${username}`;
  }
}

/* ══════════════════════════════════════════════════════════════════════
   LEAFLET MAP — Dynamic, real GPS from sensor rows
═══════════════════════════════════════════════════════════════════════ */
function initMap() {
  const el = document.getElementById('supply-chain-map');
  if (!el || typeof L === 'undefined') return;

  leafletMap = L.map('supply-chain-map', {
    zoomControl: true,
    attributionControl: false
  }).setView([18.52, 73.85], 8);

  // Dark tile layer
  mapTileLayer = L.tileLayer(
    'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    { maxZoom: 19 }
  ).addTo(leafletMap);

  mapPolyline = L.polyline([], {
    color: '#3b82f6',
    weight: 3,
    dashArray: '6, 8',
    opacity: 0.9
  }).addTo(leafletMap);
}

function updateMap(rows) {
  if (!leafletMap || !mapPolyline) return;

  // Remove old markers
  mapMarkers.forEach(m => leafletMap.removeLayer(m));
  mapMarkers = [];

  const latLngs = [];
  const stagesSeen = new Set();

  rows.forEach((row, idx) => {
    const lat = row.latitude ?? row.location?.lat;
    const lng = row.longitude ?? row.location?.lng;
    if (!lat || !lng) return;

    latLngs.push([lat, lng]);
    stagesSeen.add(row.current_stage);

    const isLatest = idx === 0;
    const pinColor = isLatest ? '#3b82f6' : '#22c55e';
    const emoji = stageEmoji(row.current_stage);

    const icon = L.divIcon({
      html: `<div style="
        width:30px;height:30px;border-radius:50%;
        background:${pinColor};color:white;
        display:grid;place-items:center;font-size:14px;
        box-shadow:0 4px 14px ${pinColor}99;
        border:2px solid white;
      ">${emoji}</div>`,
      className: '',
      iconSize: [30, 30],
      iconAnchor: [15, 15]
    });

    const marker = L.marker([lat, lng], { icon })
      .bindPopup(`
        <strong>${(row.current_stage || '').toUpperCase()}</strong><br>
        Batch: ${row.batch_id || '—'}<br>
        Temp: ${row.temperature !== null ? row.temperature.toFixed(1) + '°C' : '—'}<br>
        ${row.timestamp || ''}
      `)
      .addTo(leafletMap);

    mapMarkers.push(marker);
  });

  mapPolyline.setLatLngs(latLngs);
  if (latLngs.length > 1) {
    leafletMap.fitBounds(latLngs, { padding: [30, 30], maxZoom: 13 });
  }

  // Update stage progress bar
  const stages = ['field', 'warehouse', 'transport', 'retailer', 'consumer'];
  stages.forEach(stage => {
    const el = document.getElementById(`stage-${stage}`);
    if (!el) return;
    el.className = 'stage-dot';
    if (stagesSeen.has(stage)) el.classList.add('done');
  });
}

function stageEmoji(stage) {
  const map = { field: '🌾', warehouse: '🏭', transport: '🚚', retailer: '🏢', consumer: '🛒' };
  return map[stage] || '📦';
}

/* ══════════════════════════════════════════════════════════════════════
   SPARKLINE CHARTS — Chart.js mini line charts
═══════════════════════════════════════════════════════════════════════ */
function initSparklines() {
  if (typeof Chart === 'undefined') return;

  const configs = [
    { id: 'temp-spark',  color: '#3b82f6' },
    { id: 'humid-spark', color: '#a855f7' },
    { id: 'co2-spark',   color: '#06b6d4' },
    { id: 'light-spark', color: '#f59e0b' }
  ];

  configs.forEach(cfg => {
    const ctx = document.getElementById(cfg.id);
    if (!ctx) return;
    sparkCharts[cfg.id] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: Array(8).fill(''),
        datasets: [{
          data: Array(8).fill(null),
          borderColor: cfg.color,
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.4,
          fill: false
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 400 },
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales:  { x: { display: false }, y: { display: false } }
      }
    });
    ctx.style.filter = `drop-shadow(0 0 5px ${cfg.color}60)`;
  });
}

function updateSparkline(id, newValue) {
  const chart = sparkCharts[id];
  if (!chart || newValue === null || newValue === undefined) return;
  const data = chart.data.datasets[0].data;
  data.push(parseFloat(newValue));
  if (data.length > 12) data.shift();
  chart.data.labels = data.map(() => '');
  chart.update('none');
}

/* ══════════════════════════════════════════════════════════════════════
   MAIN DATA FETCH — All endpoints
═══════════════════════════════════════════════════════════════════════ */
async function fetchAll() {
  try {
    await Promise.all([fetchKPIs(), fetchSensorData(), fetchAlerts()]);
  } catch (err) {
    console.warn('[Dashboard] Fetch error:', err);
  }
}

/* ── 1. KPIs ────────────────────────────────────────────────────────── */
async function fetchKPIs() {
  try {
    const res = await fetchWithAuth('/api/kpis');
    if (!res.ok) return;
    const kpis = await res.json();

    setText('kpi-batches',       kpis.total_batches ?? '—');
    setText('kpi-shipments',     kpis.active_shipments ?? '—');
    setText('kpi-blockchain-tx', kpis.blockchain_transactions ?? '—');
    setText('kpi-sensors',       kpis.total_sensors ?? '—');
    setText('kpi-alerts',        kpis.alerts_today ?? '—');

    // System health = healthy_shipments / total_batches
    if (kpis.total_batches > 0 && kpis.healthy_shipments !== undefined) {
      const pct = Math.round((kpis.healthy_shipments / kpis.total_batches) * 100);
      setText('kpi-health', `${Math.min(pct, 100)}%`);
    }

    // Sidebar alert badge
    setText('sidebar-alert-count', kpis.alerts_today ?? 0);

    // Blockchain mode indicator
    updateFabricStatusUI(kpis.fabric_available ?? false);
  } catch (err) {
    console.warn('[KPIs] Error:', err);
  }
}

/* ── 2. Sensor & Blockchain Data ─────────────────────────────────────── */
async function fetchSensorData() {
  try {
    const res = await fetchWithAuth('/data');
    if (!res.ok) return;
    const json = await res.json();

    // /data now returns dict objects (fixed earlier)
    const rows   = json.data   || [];
    const latest = json.latest || (rows.length > 0 ? rows[0] : null);

    if (latest) {
      // KPI temperature & humidity
      const temp  = latest.temperature !== null ? parseFloat(latest.temperature) : null;
      const humid = latest.humidity    !== null ? parseFloat(latest.humidity)    : null;

      if (temp  !== null) setText('kpi-temp',    `${temp.toFixed(1)}°C`);
      if (humid !== null) setText('kpi-humid',   `${humid.toFixed(1)}%`);

      // IoT Panel
      if (temp  !== null) setText('sensor-temp',  `${temp.toFixed(1)} °C`);
      if (humid !== null) setText('sensor-humid',  `${humid.toFixed(1)} %`);

      // Sparklines — push latest reading
      updateSparkline('temp-spark',  temp);
      updateSparkline('humid-spark', humid);

      // Last updated time
      setText('sensor-last-update', latest.timestamp || new Date().toLocaleTimeString());

      // Temperature range hint by stage
      setTempRangeHint(latest.current_stage);
    }

    // Update map with GPS waypoints
    updateMap(rows);

    // Blockchain ledger table
    renderBlockchainTable(rows.slice(0, 8));

  } catch (err) {
    console.warn('[SensorData] Error:', err);
  }
}

/* ── 3. Alerts ──────────────────────────────────────────────────────── */
async function fetchAlerts() {
  try {
    const res = await fetchWithAuth('/batches');
    if (!res.ok) return;
    const json = await res.json();
    const batches = (json.batches || []).filter(b => b.risk_level !== 'stable');
    renderAlerts(batches);
  } catch (err) {
    console.warn('[Alerts] Error:', err);
  }
}

/* ══════════════════════════════════════════════════════════════════════
   RENDER FUNCTIONS
═══════════════════════════════════════════════════════════════════════ */

/* Blockchain Ledger Table */
function renderBlockchainTable(rows) {
  const tbody = document.getElementById('blockchain-table-body');
  if (!tbody) return;

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;padding:24px;color:var(--text3);">No records yet</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map(r => {
    const fabricTx  = r.fabric_tx_id;
    const blockHash = r.block_hash;

    // Short display of the TX / hash
    let txDisplay = '—';
    let statusHtml = '';

    if (fabricTx) {
      txDisplay  = fabricTx.slice(0, 14) + '…';
      statusHtml = `<span class="status-pill fabric"><i class="fa-solid fa-link"></i> Hyperledger</span>`;
    } else if (blockHash) {
      txDisplay  = blockHash.slice(0, 8) + '…' + blockHash.slice(-4);
      statusHtml = `<span class="status-pill hash"><i class="fa-solid fa-shield"></i> SHA-256</span>`;
    } else {
      txDisplay  = 'Pending';
      statusHtml = `<span class="status-pill pending"><i class="fa-solid fa-clock"></i> Pending</span>`;
    }

    const stageKey   = r.current_stage || 'transport';
    const stageLabel = stageKey.charAt(0).toUpperCase() + stageKey.slice(1);
    const tempStr    = r.temperature !== null ? `${parseFloat(r.temperature).toFixed(1)}°C` : '—';
    const fullTitle  = fabricTx || blockHash || 'No hash';

    return `<tr>
      <td><span class="tx-id" title="${fullTitle}">${txDisplay}</span></td>
      <td>${stageEmoji(stageKey)} ${r.batch_id || '—'}</td>
      <td><span class="stage-pill ${stageKey}">${stageLabel}</span></td>
      <td>${tempStr}</td>
      <td>${r.timestamp || '—'}</td>
      <td>${statusHtml}</td>
    </tr>`;
  }).join('');
}

/* Alerts Panel */
function renderAlerts(batches) {
  const list = document.getElementById('alerts-list');
  if (!list) return;

  if (!batches || batches.length === 0) {
    list.innerHTML = `
      <div class="empty-state">
        <i class="fa-solid fa-circle-check"></i>
        <span>All systems nominal — no active alerts</span>
      </div>`;
    return;
  }

  list.innerHTML = batches.slice(0, 8).map(b => {
    const isCrit = b.risk_level === 'critical';
    const cls    = isCrit ? 'critical' : 'warning';
    const icon   = isCrit ? 'fa-triangle-exclamation' : 'fa-circle-exclamation';
    const color  = isCrit ? 'var(--red)' : 'var(--amber)';
    const bg     = isCrit ? 'var(--red-dim)' : 'var(--amber-dim)';
    const label  = isCrit ? 'CRITICAL' : 'WARNING';
    const temp   = b.temperature !== null ? parseFloat(b.temperature).toFixed(1) : '—';
    const humid  = b.humidity    !== null ? parseFloat(b.humidity).toFixed(1)    : '—';

    return `<div class="alert-item ${cls}">
      <div class="alert-icon-wrap" style="background:${bg};color:${color}">
        <i class="fa-solid ${icon}"></i>
      </div>
      <div class="alert-body">
        <div class="alert-title">${b.batch_id || 'Batch'}: ${label}</div>
        <div class="alert-sub">Temp ${temp}°C · Humidity ${humid}%</div>
        <div class="alert-sub" style="color:${color};font-size:10px">${b.edge_decision || b.risk_note || 'Review required'}</div>
        <div class="alert-time">${b.timestamp || ''}</div>
      </div>
    </div>`;
  }).join('');
}

/* Fabric status indicator */
function updateFabricStatusUI(available) {
  const el   = document.getElementById('fabric-status-text');
  const kpEl = document.getElementById('kpi-blockchain-mode');

  if (available) {
    if (el)   { el.textContent = 'Hyperledger Active'; el.style.color = 'var(--green)'; }
    if (kpEl) kpEl.innerHTML = '<i class="fa-solid fa-link"></i> Fabric Ledger Active';
  } else {
    if (el)   { el.textContent = 'Hash-Chain Mode'; el.style.color = 'var(--amber)'; }
    if (kpEl) kpEl.innerHTML = '<i class="fa-solid fa-shield"></i> SHA-256 Fallback';
  }
}

/* Stage temp range hint */
function setTempRangeHint(stage) {
  const ranges = {
    field:     'Optimal: 18 – 27 °C',
    warehouse: 'Optimal: 4 – 10 °C',
    transport: 'Optimal: 5 – 12 °C',
    retailer:  'Optimal: 6 – 14 °C',
    consumer:  'Optimal: 8 – 16 °C',
  };
  const el = document.getElementById('sensor-temp-range');
  if (el && stage && ranges[stage]) el.textContent = ranges[stage];
}

/* ── Utility ──────────────────────────────────────────────────────── */
function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}
