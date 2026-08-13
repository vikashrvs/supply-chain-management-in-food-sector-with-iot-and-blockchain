let leafletMap = null;
let routeLine = null;
let completedLine = null;
let vehicleMarker = null;
let originMarker = null;
let destinationMarker = null;
let sparklineCharts = {};
let replayRoute = [];
let selectedReplayBatch = 'FC-001';

document.addEventListener('DOMContentLoaded', () => {
  initUserProfile();
  initThemeToggle();
  initDate();
  initSparklines();
  initReplayControls();
  initLeafletMap();
  initSidebarNav();
  fetchDashboardData();
  tickClock();
  setInterval(tickClock, 1000);
  setInterval(fetchDashboardData, 5000);
  window.addEventListener('resize', repositionStageStrip);
});

function initDate() {
  const dateEl = document.getElementById('current-date');
  if (dateEl) {
    const options = { day: 'numeric', month: 'long', year: 'numeric' };
    dateEl.textContent = new Date().toLocaleDateString('en-GB', options);
  }
}

function tickClock() {
  const timeEl = document.getElementById('current-time');
  if (timeEl) timeEl.textContent = new Date().toLocaleTimeString('en-GB', { hour12: false });
}

function initUserProfile() {
  const username = getUsername();
  const role = getRole();
  const nameEl = document.getElementById('user-name');
  const roleEl = document.getElementById('user-role');
  const avatarEl = document.getElementById('user-avatar');
  if (nameEl) nameEl.textContent = username;
  if (roleEl) roleEl.textContent = role.toUpperCase();
  if (avatarEl) avatarEl.textContent = username.charAt(0).toUpperCase();
}

function initThemeToggle() {
  const savedTheme = localStorage.getItem('ag_theme') || 'dark';
  document.documentElement.setAttribute('data-theme', savedTheme);

  const toggleBtn = document.getElementById('theme-toggle-btn');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', () => {
      const current = document.documentElement.getAttribute('data-theme');
      const next = current === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('ag_theme', next);
      updateMapTileTheme(next);
    });
  }
}

async function initLeafletMap() {
  const mapContainer = document.getElementById('supply-chain-map');
  if (!mapContainer || typeof L === 'undefined') return;

  leafletMap = L.map('supply-chain-map', {
    zoomControl: false,
    attributionControl: false
  }).setView([12.65, 77.1], 8);

  updateMapTileTheme(document.documentElement.getAttribute('data-theme') || 'light');

  await loadReplayRoute(selectedReplayBatch);

  if (!replayRoute.length) {
    replayRoute = [
      [12.971599, 77.594566],
      [12.723900, 77.280900],
      [12.521800, 76.895100],
      [12.295810, 76.639381]
    ];
  }

  drawReplayRoute();
}

async function loadReplayRoute(batchId) {
  try {
    const res = await fetchWithAuth(`/api/replay/dataset?batch_id=${encodeURIComponent(batchId)}`);
    if (res.ok) {
      const dataset = await res.json();
      replayRoute = (dataset.records || []).map(r => [r.latitude, r.longitude]);
      setText('demo-origin', dataset.origin);
      setText('demo-destination', dataset.destination);
      setText('demo-batch', dataset.batch_id);
      setText('demo-device', dataset.device_id);
      setText('demo-route-summary', `Origin: ${dataset.origin} | Destination: ${dataset.destination}`);
      updateBatchOptions(dataset.available_batches || [], dataset.batch_id);
    }
  } catch (err) {
    console.warn('Replay dataset route unavailable:', err);
  }
}

function updateBatchOptions(batches, activeBatch) {
  const select = document.getElementById('replay-batch-select');
  if (!select || !batches.length) return;
  select.innerHTML = batches.map(batch => {
    const selected = batch.batch_id === activeBatch ? 'selected' : '';
    return `<option value="${batch.batch_id}" ${selected}>${batch.batch_id} | ${batch.product}</option>`;
  }).join('');
  select.value = activeBatch;
}

function drawReplayRoute() {
  if (!leafletMap || !replayRoute.length) return;

  if (routeLine) leafletMap.removeLayer(routeLine);
  if (completedLine) leafletMap.removeLayer(completedLine);
  if (originMarker) leafletMap.removeLayer(originMarker);
  if (destinationMarker) leafletMap.removeLayer(destinationMarker);
  if (vehicleMarker) leafletMap.removeLayer(vehicleMarker);

  routeLine = L.polyline(replayRoute, {
    color: '#64748b',
    weight: 4,
    dashArray: '8, 8',
    opacity: 0.75
  }).addTo(leafletMap);

  completedLine = L.polyline([replayRoute[0]], {
    color: '#22c55e',
    weight: 5,
    opacity: 0.9
  }).addTo(leafletMap);

  const originName = document.getElementById('demo-origin')?.textContent || 'Demo Origin';
  const destinationName = document.getElementById('demo-destination')?.textContent || 'Demo Destination';

  originMarker = L.marker(replayRoute[0], { icon: mapIcon('OR', '#22c55e') })
    .bindPopup(`<strong>${originName}</strong><br>Demo origin`)
    .addTo(leafletMap);

  destinationMarker = L.marker(replayRoute[replayRoute.length - 1], { icon: mapIcon('DS', '#3b82f6') })
    .bindPopup(`<strong>${destinationName}</strong><br>Demo destination`)
    .addTo(leafletMap);

  vehicleMarker = L.marker(replayRoute[0], { icon: truckIcon() })
    .bindPopup(`<strong>${selectedReplayBatch}</strong><br>Demo Telemetry / Replay Mode`)
    .addTo(leafletMap);

  leafletMap.fitBounds(replayRoute, { padding: [34, 34] });
}

function mapIcon(label, color) {
  return L.divIcon({
    className: '',
    iconSize: [34, 34],
    iconAnchor: [17, 17],
    html: `<div style="width:34px;height:34px;border-radius:50%;background:${color};color:#fff;display:grid;place-items:center;font-size:11px;font-weight:800;border:2px solid #fff;box-shadow:0 6px 16px ${color}66;">${label}</div>`
  });
}

function truckIcon(isMoving = true) {
  const pulseClass = isMoving ? 'vehicle-marker-glowing' : '';
  return L.divIcon({
    className: '',
    iconSize: [38, 38],
    iconAnchor: [19, 19],
    html: `<div class="${pulseClass}" style="width:38px;height:38px;border-radius:50%;background:#f59e0b;color:#111827;display:grid;place-items:center;font-size:18px;border:2px solid #fff;box-shadow:0 8px 18px rgba(245,158,11,.5);">🚚</div>`
  });
}

function updateVehicleOnMap(latest, progressPct) {
  if (!leafletMap || !latest || latest.latitude == null || latest.longitude == null) return;
  const coords = [latest.latitude, latest.longitude];
  const isMoving = latest.transportation_status !== 'DELIVERED';
  if (vehicleMarker) {
    vehicleMarker.setLatLng(coords);
    vehicleMarker.setIcon(truckIcon(isMoving));
  }

  const completedCount = Math.max(1, Math.min(replayRoute.length, Math.round((progressPct / 100) * replayRoute.length)));
  const completedRoute = replayRoute.slice(0, completedCount);
  completedRoute.push(coords);
  if (completedLine) completedLine.setLatLngs(completedRoute);
}

function updateMapTileTheme(theme) {
  if (!leafletMap) return;
  const tileUrl = theme === 'dark'
    ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
    : 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png';

  if (window.mapTileLayer) leafletMap.removeLayer(window.mapTileLayer);
  window.mapTileLayer = L.tileLayer(tileUrl, { maxZoom: 19 }).addTo(leafletMap);
}

function initReplayControls() {
  const speed = document.getElementById('replay-speed');
  const speedLabel = document.getElementById('replay-speed-label');
  const batchSelect = document.getElementById('replay-batch-select');

  if (speed && speedLabel) {
    speed.addEventListener('input', () => {
      const val = parseFloat(speed.value);
      if (val >= 60) {
        speedLabel.textContent = `${(val / 60).toFixed(1)}m`;
      } else {
        speedLabel.textContent = `${val}s`;
      }
    });
  }

  if (batchSelect) {
    selectedReplayBatch = batchSelect.value || selectedReplayBatch;
    batchSelect.addEventListener('change', async () => {
      selectedReplayBatch = batchSelect.value;
      const intervalSec = speed?.value || 2;
      // Selecting batch starts playing back that batch's prerecorded readings from DB
      await postReplay(`/api/replay/start?batch_id=${encodeURIComponent(selectedReplayBatch)}&interval_seconds=${intervalSec}&reset=true`);
      await loadReplayRoute(selectedReplayBatch);
      drawReplayRoute();
      await fetchDashboardData();
    });
  }

  bindReplayButton('replay-start', () => postReplay(`/api/replay/start?batch_id=${encodeURIComponent(selectedReplayBatch)}&interval_seconds=${speed?.value || 2}&reset=false`));
  bindReplayButton('replay-pause', () => postReplay('/api/replay/pause'));
  bindReplayButton('replay-step', () => postReplay('/api/replay/step'));
  bindReplayButton('replay-reset', () => postReplay(`/api/replay/reset?batch_id=${encodeURIComponent(selectedReplayBatch)}`));
}

function bindReplayButton(id, handler) {
  const button = document.getElementById(id);
  if (button) button.addEventListener('click', handler);
}

// Sidebar in-page navigation: intercept # anchors and scroll/show sections in the dashboard
function initSidebarNav() {
  const navLinks = document.querySelectorAll('.sidebar-nav a.nav-item');
  navLinks.forEach(a => {
    const href = a.getAttribute('href');
    if (!href) return;

    // Links that point to dashboard sections (hashes) should open in-page
    if (href.startsWith('#')) {
      a.addEventListener('click', (e) => {
        e.preventDefault();
        const id = href.slice(1);
        const target = document.getElementById(id);
        if (target) {
          // Smooth scroll to section and highlight active nav
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
          document.querySelectorAll('.sidebar-nav .nav-item').forEach(n => n.classList.remove('active'));
          a.classList.add('active');
        } else {
          // Fallback: navigate to same-page hash
          window.location.hash = href;
        }
      });
    }

    // Keep full-page links (ending with .html) as normal navigation
    if (href.endsWith('.html')) {
      // No interception — allow default navigation for full pages like track.html/qr.html/home.html
    }
  });
}

async function postReplay(url) {
  const res = await fetchWithAuth(url, { method: 'POST' });
  if (!res.ok) console.warn('Replay control failed:', await res.text());
  await fetchDashboardData();
}

function initSparklines() {
  if (typeof Chart === 'undefined') return;

  const sparkConfigs = [
    { id: 'temp-sparkline', data: [6.4, 6.6, 6.8, 7.0, 7.1, 6.9, 6.7], color: '#3b82f6' },
    { id: 'humidity-sparkline', data: [67, 68, 70, 72, 71, 69, 68], color: '#a855f7' },
    { id: 'co2-sparkline', data: [124, 128, 132, 130, 136, 133, 129], color: '#06b6d4' },
    { id: 'light-sparkline', data: [310, 325, 318, 320, 322, 319, 320], color: '#f59e0b' }
  ];

  sparkConfigs.forEach(cfg => {
    const ctx = document.getElementById(cfg.id);
    if (!ctx) return;
    sparklineCharts[cfg.id] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: ['', '', '', '', '', '', ''],
        datasets: [{ data: cfg.data, borderColor: cfg.color, borderWidth: 3, pointRadius: 0, tension: 0.4, fill: false }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } }
      }
    });
    ctx.style.filter = `drop-shadow(0 0 6px ${cfg.color}80)`;
  });
}

async function fetchDashboardData() {
  try {
    await Promise.all([fetchKpis(), fetchTransportation(), fetchAlerts(), checkFabricServiceStatus()]);
  } catch (err) {
    console.warn('Dashboard fetch error:', err);
  }
}

// Derive active header button glow state from the MOST RECENT transaction's fabric_tx_id
function updateFabricIndicator(history) {
  const btnFabric = document.getElementById('btn-ledger-fabric');
  const btnSha256 = document.getElementById('btn-ledger-sha256');
  if (!btnFabric || !btnSha256) return;

  const latestRecord = Array.isArray(history) && history.length > 0 ? history[history.length - 1] : null;
  const isFabricActive = Boolean(latestRecord && latestRecord.fabric_tx_id);

  if (isFabricActive) {
    btnFabric.classList.add('active-glowing-fabric');
    btnSha256.classList.remove('active-glowing-sha256');
  } else {
    btnSha256.classList.add('active-glowing-sha256');
    btnFabric.classList.remove('active-glowing-fabric');
  }
}

// Standalone Fabric service connectivity check (updates service status dot/label only)
async function checkFabricServiceStatus() {
  try {
    const res = await fetchWithAuth('/api/fabric-status');
    if (!res.ok) return;
    const data = await res.json();
    const isOnline = !!data.fabric_available;

    const serviceDot = document.getElementById('fabric-service-dot');
    const serviceLabel = document.getElementById('fabric-mode-label');

    if (serviceDot) {
      serviceDot.className = `pulse-dot ${isOnline ? 'dot-green' : 'dot-red'}`;
      serviceDot.title = isOnline ? 'Fabric Service: Online (localhost:7051)' : 'Fabric Service: Offline (SHA-256 fallback)';
    }
    if (serviceLabel) {
      serviceLabel.textContent = isOnline ? 'Fabric Service Online' : 'Fabric Service Offline';
    }
  } catch (err) {
    console.warn('Fabric service check error:', err);
  }
}

async function fetchKpis() {
  const resKpi = await fetchWithAuth('/api/kpis');
  if (!resKpi.ok) return;
  const kpis = await resKpi.json();
  document.getElementById('kpi-total-products').textContent = kpis.total_batches || 0;
  document.getElementById('kpi-shipments').textContent = kpis.active_shipments || 0;
  document.getElementById('kpi-blockchain-tx').textContent = kpis.blockchain_transactions || 0;
  setText('kpi-sensor-readings', kpis.total_sensors || 0);
  setText('kpi-health', `${kpis.total_sensors ? Math.round((kpis.healthy_shipments / Math.max(kpis.total_batches, 1)) * 100) : 100}%`);
}

async function fetchTransportation() {
  const res = await fetchWithAuth(`/api/replay/transportation?batch_id=${encodeURIComponent(selectedReplayBatch)}`);
  if (!res.ok) return;
  const state = await res.json();
  const latest = state.latest;
  const progress = state.progress_pct || 0;
  const history = state.history || [];

  setText('demo-origin', state.origin);
  setText('demo-destination', state.destination);
  setText('demo-batch', state.batch_id);
  setText('demo-device', state.device_id);
  setText('demo-route-summary', `Origin: ${state.origin} | Destination: ${state.destination}`);
  setText('demo-progress', `${progress}%`);
  setText('demo-count', `${state.received_count || 0} / ${state.total_records || 100}`);
  setText('demo-status-pill', state.status || 'READY');

  if (latest) {
    setText('demo-coords', `${latest.latitude.toFixed(6)}, ${latest.longitude.toFixed(6)}`);
    setText('demo-temp', `${latest.temperature.toFixed(1)} C`);
    setText('demo-humidity', `${latest.humidity.toFixed(1)} %`);
    setText('demo-gas', `${(latest.gas_value || 0).toFixed(0)} ppm`);
    setText('demo-last-update', latest.timestamp || '--');
    setText('sensor-val-temp', `${latest.temperature.toFixed(1)} C`);
    setText('sensor-val-hum', `${latest.humidity.toFixed(1)} %`);
    setHtml('sensor-val-gas', `${(latest.gas_value || 0).toFixed(0)} <span style="font-size: 11px;">ppm</span>`);
    setText('kpi-latest-temp', `${latest.temperature.toFixed(1)}C`);
    setText('kpi-latest-humidity', `${latest.humidity.toFixed(1)}%`);
    updateVehicleOnMap(latest, progress);
    updateStageStrip(latest.current_stage, progress);
  } else {
    setText('demo-coords', '--');
    setText('demo-temp', '-- C');
    setText('demo-humidity', '-- %');
    setText('demo-gas', '-- ppm');
    setText('demo-last-update', '--');
    updateStageStrip(null, 0);
  }

  updateBlockchainTable(history);
  updateFabricIndicator(history);
}

function updateStageStrip(currentStage, progressPct) {
  const strip = document.getElementById('stage-strip');
  const track = strip?.querySelector('.stage-track-line');
  const glow = strip?.querySelector('.stage-track-glow');
  const flowDot = document.getElementById('stage-flow-dot');
  const stageNodes = document.querySelectorAll('.stage-node-wrapper');

  if (!stageNodes.length) return;

  /*
   * IMPORTANT:
   * The timeline is controlled by current_stage from the database.
   * We do NOT use progress_pct to move a ball.
   *
   * This prevents the visual timeline from getting out of sync
   * with the actual prerecorded sensor stage.
   */

  const stageOrder = [
    'field',
    'processing',
    'transport',
    'warehouse',
    'retailer'
  ];

  const stageAliases = {
    farm: 'field',
    production: 'field',

    packaging: 'processing',
    'processing/packaging': 'processing',

    transportation: 'transport',
    transit: 'transport',

    retail: 'retailer',
    store: 'retailer',

    consumer: 'retailer'
  };

  let stage = String(currentStage || '')
    .trim()
    .toLowerCase();

  stage = stageAliases[stage] || stage;

  let currentIndex = stageOrder.indexOf(stage);

  /*
   * Only use progress as a fallback if current_stage
   * is missing or unknown.
   */
  if (currentIndex === -1) {
    const progress = Math.min(
      100,
      Math.max(0, Number(progressPct) || 0)
    );

    if (progress >= 87.5) {
      currentIndex = 4;
    } else if (progress >= 62.5) {
      currentIndex = 3;
    } else if (progress >= 37.5) {
      currentIndex = 2;
    } else if (progress >= 12.5) {
      currentIndex = 1;
    } else {
      currentIndex = 0;
    }
  }

  /*
   * Update stage states.
   */
  stageNodes.forEach((node, index) => {
    node.classList.remove(
      'completed',
      'active',
      'route-origin-moving'
    );

    if (index < currentIndex) {
      node.classList.add('completed');
    } else if (index === currentIndex) {
      node.classList.add('active');
    }
  });

  /*
   * Position the timeline itself.
   * The green line ends at the ACTIVE STAGE,
   * not at replay percentage.
   */
  requestAnimationFrame(() => {
    if (!strip || !track) return;

    const stripRect = strip.getBoundingClientRect();

    const firstRect =
      stageNodes[0].getBoundingClientRect();

    const activeRect =
      stageNodes[currentIndex].getBoundingClientRect();

    const lastRect =
      stageNodes[stageNodes.length - 1].getBoundingClientRect();

    const startX =
      firstRect.left +
      firstRect.width / 2 -
      stripRect.left;

    const activeX =
      activeRect.left +
      activeRect.width / 2 -
      stripRect.left;

    const endX =
      lastRect.left +
      lastRect.width / 2 -
      stripRect.left;

    /*
     * Base timeline.
     */
    track.style.left = `${startX}px`;

    track.style.right =
      `${Math.max(
        0,
        stripRect.width - endX
      )}px`;

    /*
     * Completed/active portion.
     */
    if (glow) {
      glow.style.width =
        `${Math.max(
          0,
          activeX - startX
        )}px`;
    }

    /*
     * Completely disable the old moving ball.
     * This also protects against an older dashboard.html
     * that still contains #stage-flow-dot.
     */
    if (flowDot) {
      flowDot.style.display = 'none';
      flowDot.style.left = '0px';
    }
  });
}

function repositionStageStrip() {
  const activeNode =
    document.querySelector(
      '.stage-node-wrapper.active'
    );

  const latestStage =
    activeNode?.dataset.stage || null;

  const progressEl =
    document.getElementById('demo-progress');

  const progress =
    progressEl
      ? parseFloat(progressEl.textContent) || 0
      : 0;

  updateStageStrip(
    latestStage,
    progress
  );
}

function updateBlockchainTable(history) {
  const tbody = document.getElementById('blockchain-table-body');
  if (!tbody) return;
  const rows = history.slice(-5).reverse();
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; padding:16px; color:var(--text-muted);">Replay has not started yet. Select a batch to view ledger transactions.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r => {
    const fabricTx = r.fabric_tx_id;
    const hash = r.block_hash || '';
    const shortHash = hash ? `${hash.slice(0, 8)}...${hash.slice(-4)}` : 'pending';

    const statusHtml = fabricTx
      ? `<div class="status-cell-flex">
           <span class="status-text text-green"><i class="fa-solid fa-circle-check"></i> Verified</span>
           <span class="glass-pill pill-purple" title="Fabric TX: ${fabricTx}"><i class="fa-solid fa-cube"></i> Hyperledger Fabric</span>
         </div>`
      : `<div class="status-cell-flex">
           <span class="status-text text-blue"><i class="fa-solid fa-shield-halved"></i> Confirmed</span>
           <span class="glass-pill pill-blue" title="SHA-256 Hash Chain"><i class="fa-solid fa-shield-halved"></i> SHA-256</span>
         </div>`;

    return `<tr>
      <td><a href="#" class="tx-hash" title="${fabricTx || hash}">${fabricTx ? `${fabricTx.slice(0, 10)}...` : shortHash}</a></td>
      <td>${r.batch_id || selectedReplayBatch}</td>
      <td><span class="stage-tag">${(r.current_stage || r.alert_status || r.transportation_status || 'IN TRANSIT').toUpperCase()}</span></td>
      <td>${r.temperature != null ? `${r.temperature.toFixed(1)}°C` : '--'}</td>
      <td>${r.timestamp || '--'}</td>
      <td>${statusHtml}</td>
    </tr>`;
  }).join('');
}

async function fetchAlerts() {
  const alertsList = document.getElementById('alerts-list');
  if (!alertsList) return;

  let allAlerts = [];

  // 1. Fetch Agent Alerts (includes Hyperledger downtime & rule alerts with sources)
  try {
    const resAgent = await fetchWithAuth('/api/agent-alerts');
    if (resAgent.ok) {
      const dataAgent = await resAgent.json();
      if (dataAgent.alerts) {
        allAlerts.push(...dataAgent.alerts);
      }
    }
  } catch (err) {
    console.warn('Agent alerts fetch error:', err);
  }

  // 2. Fetch Replay Transportation History Alerts
  try {
    const resReplay = await fetchWithAuth(`/api/replay/transportation?batch_id=${encodeURIComponent(selectedReplayBatch)}`);
    if (resReplay.ok) {
      const state = await resReplay.json();
      const replayAlerts = (state.history || []).filter(row => row.risk_level !== 'stable' || row.alert_status !== 'NORMAL' || row.alert_flag);
      replayAlerts.forEach(row => {
        const source = row.alert_source || row.source || 'sensor';
        allAlerts.push({
          severity: row.risk_level === 'critical' ? 'critical' : 'warning',
          rule: row.alert_status || 'sensor-alert',
          source: source,
          alert_source: source,
          batch_id: row.batch_id || selectedReplayBatch,
          product: row.product || 'Batch Stock',
          timestamp: row.timestamp || '--',
          message: `Temp: ${row.temperature != null ? row.temperature.toFixed(1) : '--'}°C | Humid: ${row.humidity != null ? row.humidity.toFixed(1) : '--'}% | Gas: ${(row.gas_value || 0).toFixed(0)} ppm`,
          recommendation: row.edge_decision || 'Review sensor readings',
          icon: row.alert_status === 'ENVIRONMENT_ALERT' ? 'fa-cloud-rain' : 'fa-triangle-exclamation'
        });
      });
    }
  } catch (err) {
    console.warn('Replay alerts fetch error:', err);
  }

  // Deduplicate by batch, message & timestamp
  const seen = new Set();
  const uniqueAlerts = allAlerts.filter(a => {
    const key = `${a.batch_id}-${a.message}-${a.timestamp}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).slice(0, 7);

  // Update counters on UI
  const totalCount = uniqueAlerts.length;
  setText('kpi-alerts', totalCount);
  setText('sidebar-alert-count', totalCount);

  if (!uniqueAlerts.length) {
    alertsList.innerHTML = '<div class="empty-alert"><i class="fa-solid fa-check"></i><span>All systems nominal - no active alerts</span></div>';
    return;
  }

  alertsList.innerHTML = uniqueAlerts.map(item => {
    const isCrit = item.severity === 'critical';
    const iconBg = isCrit ? 'rgba(239, 68, 68, 0.18)' : 'rgba(245, 158, 11, 0.18)';
    const iconColor = isCrit ? '#ef4444' : '#f59e0b';
    const sourceName = item.alert_source || item.source || 'sensor';
    const sourceClass = `source-${sourceName.toLowerCase().replace(/[^a-z0-9]/g, '')}`;

    return `<div class="alert-item">
      <div class="alert-icon" style="background: ${iconBg}; color: ${iconColor};">
        <i class="fa-solid ${item.icon || 'fa-triangle-exclamation'}"></i>
      </div>
      <div class="alert-content" style="flex:1;">
        <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
          <strong>Batch ${item.batch_id}: ${item.rule || item.severity.toUpperCase()}</strong>
          <span class="alert-source-pill ${sourceClass}"><i class="fa-solid fa-robot"></i> Source: ${sourceName}</span>
        </div>
        <p style="margin-top:2px;">${item.message}</p>
        <p style="font-size:10px; margin-top:3px; color:${iconColor};">${item.recommendation}</p>
      </div>
      <span class="alert-time">${item.timestamp || '--'}</span>
    </div>`;
  }).join('');
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function setHtml(id, value) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = value;
}
