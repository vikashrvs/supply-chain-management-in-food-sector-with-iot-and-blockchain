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
  function updateDate() {
    const dateEl = document.getElementById('current-date');
    if (dateEl) {
      const now = new Date();
      const options = { day: 'numeric', month: 'long', year: 'numeric' };
      dateEl.textContent = now.toLocaleDateString('en-GB', options);
    }
  }
  updateDate();
  // Refresh date every minute in case user leaves tab open overnight
  setInterval(updateDate, 60000);
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

  // We will initialize with an empty map line and points, and update them when we fetch data.
  window.mapPolyline = L.polyline([], {
    color: '#3b82f6',
    weight: 3,
    dashArray: '6, 8',
    opacity: 0.8
  }).addTo(leafletMap);

  window.mapMarkers = [];
}

function updateMapWaypoints(rows) {
  if (!leafletMap || !window.mapPolyline) return;

  // Clear old markers
  window.mapMarkers.forEach(m => leafletMap.removeLayer(m));
  window.mapMarkers = [];

  const latLngs = [];
  rows.forEach((row, index) => {
    if (row.latitude && row.longitude) {
      const coords = [row.latitude, row.longitude];
      latLngs.push(coords);

      const isActive = index === 0; // Most recent is active
      const pinColor = isActive ? '#3b82f6' : '#22c55e';
      let iconChar = '📦';
      if (row.current_stage === 'field') iconChar = '🌾';
      else if (row.current_stage === 'warehouse') iconChar = '🏭';
      else if (row.current_stage === 'transport') iconChar = '🚚';
      else if (row.current_stage === 'retailer') iconChar = '🏢';
      else if (row.current_stage === 'consumer') iconChar = '🛒';
      
      const customHtml = `
        <div style="
          width: 28px; height: 28px; border-radius: 50%;
          background: ${pinColor}; color: white;
          display: grid; place-items: center; font-size: 14px;
          box-shadow: 0 4px 12px ${pinColor}88; border: 2px solid white;
        ">
          ${iconChar}
        </div>
      `;

      const customIcon = L.divIcon({
        html: customHtml,
        className: '',
        iconSize: [28, 28],
        iconAnchor: [14, 14]
      });

      const marker = L.marker(coords, { icon: customIcon })
        .bindPopup(`<strong>${(row.current_stage || 'Unknown').toUpperCase()}</strong><br>Time: ${row.timestamp}`)
        .addTo(leafletMap);
      
      window.mapMarkers.push(marker);
    }
  });

  window.mapPolyline.setLatLngs(latLngs);
  if (latLngs.length > 0) {
    leafletMap.fitBounds(latLngs, { padding: [30, 30], maxZoom: 14 });
  }
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
    { id: 'temp-sparkline', data: [], color: '#3b82f6' },
    { id: 'humidity-sparkline', data: [], color: '#a855f7' },
    { id: 'co2-sparkline', data: [], color: '#06b6d4' },
    { id: 'light-sparkline', data: [], color: '#f59e0b' }
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
        document.getElementById('kpi-total-products').textContent = kpis.total_batches;
      }
      if (kpis.active_shipments !== undefined) {
        document.getElementById('kpi-shipments').textContent = kpis.active_shipments;
      }
      if (kpis.blockchain_transactions !== undefined) {
        document.getElementById('kpi-blockchain-tx').textContent = kpis.blockchain_transactions;
        const verifiedPct = Math.min(Math.round((kpis.blockchain_transactions / (kpis.total_sensors || 1)) * 100), 100);
        document.getElementById('kpi-verified').textContent = `${verifiedPct}%`;
      }
      if (kpis.total_sensors !== undefined) {
        // Mock connected devices based on total sensors recorded
        document.getElementById('kpi-sensors').textContent = kpis.total_sensors;
      }
      if (kpis.alerts_today !== undefined) {
        document.getElementById('kpi-alerts').textContent = kpis.alerts_today;
      }
      if (kpis.healthy_shipments !== undefined && kpis.total_batches > 0) {
        const healthPct = Math.min(Math.round((kpis.healthy_shipments / kpis.total_batches) * 100), 100);
        document.getElementById('kpi-health').textContent = `${healthPct}%`;
      }
    }

    // 2. Fetch Blockchain Data (Recent Activity) & Sensors
    const resData = await fetchWithAuth('/data');
    if (resData.ok) {
      const json = await resData.json();
      const rows = json.data || [];
      const latest = json.latest || (rows.length > 0 ? rows[0] : null);
      
      if (latest) {
        // Update Live IoT Sensors from the latest reading (now proper dict objects)
        const tempEl = document.getElementById('sensor-val-temp');
        const humEl = document.getElementById('sensor-val-hum');
        if (tempEl && latest.temperature !== null && latest.temperature !== undefined)
          tempEl.textContent = `${parseFloat(latest.temperature).toFixed(1)} °C`;
        if (humEl && latest.humidity !== null && latest.humidity !== undefined)
          humEl.textContent = `${parseFloat(latest.humidity).toFixed(1)} %`;
        
        // Update sparkline data based on recent history (rows are now dicts)
        const temps = rows.map(r => r.temperature).filter(v => v !== null).reverse().slice(-7);
        const hums  = rows.map(r => r.humidity).filter(v => v !== null).reverse().slice(-7);
        
        if (sparklineCharts['temp-sparkline'] && temps.length > 0) {
          sparklineCharts['temp-sparkline'].data.datasets[0].data = temps;
          sparklineCharts['temp-sparkline'].data.labels = temps.map(() => '');
          sparklineCharts['temp-sparkline'].update();
        }
        if (sparklineCharts['humidity-sparkline'] && hums.length > 0) {
          sparklineCharts['humidity-sparkline'].data.datasets[0].data = hums;
          sparklineCharts['humidity-sparkline'].data.labels = hums.map(() => '');
          sparklineCharts['humidity-sparkline'].update();
        }

        // Update Map Waypoints with real GPS from sensor data
        if (typeof updateMapWaypoints === 'function') {
          updateMapWaypoints(rows);
        }
      }

      const tbody = document.getElementById('blockchain-table-body');
      if (tbody && rows.length > 0) {
        tbody.innerHTML = rows.slice(0, 5).map(r => {
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
              <td><a href="#" class="tx-hash" title="${fabricTx || hash}">${fabricTx ? fabricTx.slice(0,12)+'...' : shortHash}</a></td>
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
