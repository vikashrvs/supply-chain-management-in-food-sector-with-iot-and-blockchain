/* ── Dashboard Logic & Live Data Manager ────────────────────────────────── */

let leafletMap = null;
let sparklineCharts = {};

document.addEventListener('DOMContentLoaded', () => {
  initUserProfile();
  initThemeToggle();
  initLeafletMap();
  initSparklines();
  fetchDashboardData();
  initDate();
  
  // Auto refresh every 5s
  setInterval(fetchDashboardData, 5000);
});

function initDate() {
  const dateEl = document.getElementById('current-date');
  if (dateEl) {
    const options = { day: 'numeric', month: 'long', year: 'numeric' };
    dateEl.textContent = new Date().toLocaleDateString('en-GB', options);
  }
}

function initUserProfile() {
  const username = getUsername();
  const role = getRole();
  
  const userEl = document.getElementById('user-name');
  const roleEl = document.getElementById('user-role');
  const avatarEl = document.getElementById('user-avatar');
  
  if (userEl) userEl.textContent = username;
  if (roleEl) roleEl.textContent = role.toUpperCase();
  if (avatarEl) avatarEl.textContent = username.charAt(0).toUpperCase();
}

function initThemeToggle() {
  const savedTheme = localStorage.getItem('ag_theme') || 'light';
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

/* ── Leaflet Interactive Route Map ─────────────────────────────────────── */
function initLeafletMap() {
  const mapContainer = document.getElementById('supply-chain-map');
  if (!mapContainer || typeof L === 'undefined') return;

  // Center on transit route (e.g. Maharashtra / Karnataka region sample waypoints)
  leafletMap = L.map('supply-chain-map', {
    zoomControl: false,
    attributionControl: false
  }).setView([18.5204, 73.8567], 8);

  const theme = document.documentElement.getAttribute('data-theme') || 'light';
  updateMapTileTheme(theme);

  // Sample Waypoints: Farm -> Processing -> Transport -> Distribution -> Delivery
  const waypoints = [
    { coords: [18.5204, 73.8567], title: 'Farm (Harvested)', status: 'completed', icon: '🌾' },
    { coords: [18.7387, 73.6765], title: 'Processing (Completed)', status: 'completed', icon: '🏭' },
    { coords: [18.9894, 73.1175], title: 'Transport (In Transit)', status: 'active', icon: '🚚' },
    { coords: [19.0760, 72.8777], title: 'Distribution Center', status: 'pending', icon: '🏢' },
    { coords: [19.2183, 72.9781], title: 'Retail / Delivery', status: 'pending', icon: '🛒' }
  ];

  const latLngs = waypoints.map(w => w.coords);

  // Draw connecting line
  L.polyline(latLngs, {
    color: '#3b82f6',
    weight: 3,
    dashArray: '6, 8',
    opacity: 0.8
  }).addTo(leafletMap);

  // Add custom markers
  waypoints.forEach((wp, index) => {
    const isCompleted = wp.status === 'completed';
    const isActive = wp.status === 'active';
    
    const pinColor = isCompleted ? '#22c55e' : (isActive ? '#3b82f6' : '#94a3b8');
    
    const customHtml = `
      <div style="
        width: 28px; height: 28px; border-radius: 50%;
        background: ${pinColor}; color: white;
        display: grid; place-items: center; font-size: 14px;
        box-shadow: 0 4px 12px ${pinColor}88; border: 2px solid white;
      ">
        ${wp.icon}
      </div>
    `;

    const customIcon = L.divIcon({
      html: customHtml,
      className: '',
      iconSize: [28, 28],
      iconAnchor: [14, 14]
    });

    L.marker(wp.coords, { icon: customIcon })
      .bindPopup(`<strong>${wp.title}</strong>`)
      .addTo(leafletMap);
  });

  leafletMap.fitBounds(latLngs, { padding: [30, 30] });
}

function updateMapTileTheme(theme) {
  if (!leafletMap) return;
  const tileUrl = theme === 'dark'
    ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
    : 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png';
    
  if (window.mapTileLayer) leafletMap.removeLayer(window.mapTileLayer);
  window.mapTileLayer = L.tileLayer(tileUrl, { maxZoom: 19 }).addTo(leafletMap);
}

/* ── Chart.js Sparklines for Sensors ───────────────────────────────────── */
function initSparklines() {
  if (typeof Chart === 'undefined') return;

  const sparkConfigs = [
    { id: 'temp-sparkline', data: [4.1, 4.3, 4.2, 4.5, 4.7, 4.4, 4.3], color: '#3b82f6' },
    { id: 'humidity-sparkline', data: [62, 65, 68, 67, 65, 64, 65], color: '#a855f7' },
    { id: 'co2-sparkline', data: [420, 415, 412, 408, 412, 410, 412], color: '#06b6d4' },
    { id: 'light-sparkline', data: [310, 325, 318, 320, 322, 319, 320], color: '#f59e0b' }
  ];

  sparkConfigs.forEach(cfg => {
    const ctx = document.getElementById(cfg.id);
    if (!ctx) return;

    sparklineCharts[cfg.id] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: ['', '', '', '', '', '', ''],
        datasets: [{
          data: cfg.data,
          borderColor: cfg.color,
          borderWidth: 3,
          pointRadius: 0,
          tension: 0.4,
          fill: false
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } }
      }
    });
    // Add neon glow
    ctx.style.filter = `drop-shadow(0 0 6px ${cfg.color}80)`;
  });
}

/* ── Live Data API Polling ──────────────────────────────────────────────── */
async function fetchDashboardData() {
  try {
    // 1. Fetch KPIs
    const resKpi = await fetchWithAuth('/api/kpis');
    if (resKpi.ok) {
      const kpis = await resKpi.json();
      if (kpis.total_batches !== undefined) {
        document.getElementById('kpi-total-products').textContent = kpis.total_batches || 128;
      }
      if (kpis.active_shipments !== undefined) {
        document.getElementById('kpi-shipments').textContent = kpis.active_shipments || 35;
      }
      if (kpis.blockchain_transactions !== undefined) {
        document.getElementById('kpi-blockchain-tx').textContent = `${Math.min(Math.round((kpis.blockchain_transactions / (kpis.total_sensors || 1)) * 100), 100)}%`;
      }
    }

    // 2. Fetch Blockchain Data (Recent Activity) & Sensors
    const resData = await fetchWithAuth('/data');
    if (resData.ok) {
      const json = await resData.json();
      const rows = (json.data || []).slice(0, 5);
      
      if (rows.length > 0) {
        // Update Live IoT Sensors from the latest reading
        const latest = rows[0];
        const tempEl = document.getElementById('sensor-val-temp');
        const humEl = document.getElementById('sensor-val-hum');
        if (tempEl && latest.temperature !== null) tempEl.textContent = `${latest.temperature.toFixed(1)} °C`;
        if (humEl && latest.humidity !== null) humEl.textContent = `${latest.humidity.toFixed(1)} %`;
      }

      const tbody = document.getElementById('blockchain-table-body');
      if (tbody && rows.length > 0) {
        tbody.innerHTML = rows.map(r => {
          const hash = r.block_hash || '';
          const fabricTx = r.fabric_tx_id;
          
          const shortHash = hash ? (hash.slice(0,8) + '...' + hash.slice(-4)) : 'pending';
          
          let statusHtml = '';
          if (fabricTx) {
             statusHtml = `<span class="glass-pill pill-green" title="Hyperledger TX: ${fabricTx}"><i class="fa-solid fa-link"></i> Fabric Ledger</span>`;
          } else if (hash) {
             statusHtml = `<span class="glass-pill pill-blue" title="Local SHA-256 Hash Fallback"><i class="fa-solid fa-shield"></i> Local Hash</span>`;
          } else {
             statusHtml = `<span class="glass-pill pill-amber"><i class="fa-solid fa-clock"></i> Pending</span>`;
          }
          
          return `<tr>
              <td><a href="#" class="tx-hash" title="${fabricTx || hash}">${fabricTx ? fabricTx.slice(0,10)+'...' : shortHash}</a></td>
              <td>📦 ${r.batch_id || 'Unknown'}</td>
              <td>${(r.current_stage || 'Farm').toUpperCase()}</td>
              <td>${r.timestamp || '--'}</td>
              <td>${statusHtml}</td>
          </tr>`;
        }).join('');
      }
    }

    // 3. Fetch Alerts Feed
    const resBatches = await fetchWithAuth('/batches');
    if (resBatches.ok) {
      const jsonBatches = await resBatches.json();
      const batches = (jsonBatches.batches || []).filter(b => b.risk_level !== 'stable');
      const alertsList = document.getElementById('alerts-list');
      if (alertsList) {
        if (!batches.length) {
          alertsList.innerHTML = `<div style="padding:16px; text-align:center; color:var(--text-muted);">No active alerts ✓</div>`;
        } else {
          alertsList.innerHTML = batches.map(b => {
            const isCrit = b.risk_level === 'critical';
            const iconBg = isCrit ? 'var(--amber-soft)' : 'var(--blue-soft)';
            const iconColor = isCrit ? 'var(--amber)' : 'var(--blue)';
            const icon = isCrit ? 'fa-triangle-exclamation' : 'fa-circle-exclamation';
            const riskLabel = isCrit ? 'CRITICAL HOLD' : 'WARNING';
            return `<div class="alert-item">
              <div class="alert-icon" style="background: ${iconBg}; color: ${iconColor};">
                  <i class="fa-solid ${icon}"></i>
              </div>
              <div class="alert-content">
                  <strong>Batch ${b.batch_id || ''}: ${riskLabel}</strong>
                  <p>Temp: ${b.temperature !== null ? b.temperature.toFixed(1) : '--'}°C | Humidity: ${b.humidity !== null ? b.humidity.toFixed(1) : '--'}%</p>
                  <p style="font-size: 11px; margin-top: 4px; color: ${iconColor};">${b.edge_decision || 'Needs Review'}</p>
              </div>
            </div>`;
          }).join('');
        }
      }
    }
  } catch (err) {
    console.warn('Dashboard fetch error:', err);
  }
}
