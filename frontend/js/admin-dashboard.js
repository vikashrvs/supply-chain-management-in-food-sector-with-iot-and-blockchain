// FoodChain Admin Dashboard Controller
const mockData = {
    kpis: {
        batches: 6,
        shipments: 6,
        blockchainTx: 7944,
        sensorReadings: 7944,
        health: 100,
        devices: 1,
        temp: '27.7°C',
        humidity: '73.6%',
        alerts: 0
    },
    transactions: [
        { tx: '0xA7A1', batch: 'FC-201', product: 'Fresh Apples', stage: 'Transport', temp: '4.2°C', time: '2026-09-19 18:40', status: 'Verified', ledger: 'fabric' },
        { tx: '0xB8C2', batch: 'FC-203', product: 'Organic Tomatoes', stage: 'Processing', temp: '5.1°C', time: '2026-09-19 18:20', status: 'Verified', ledger: 'fabric' },
        { tx: '0xC9D3', batch: 'FC-208', product: 'Dairy Milk', stage: 'Warehouse', temp: '3.8°C', time: '2026-09-19 17:55', status: 'Verified', ledger: 'sha256' },
        { tx: '0xD4E2', batch: 'FC-214', product: 'Poultry Cut', stage: 'Retail', temp: '2.4°C', time: '2026-09-19 17:30', status: 'Flagged', ledger: 'fabric' }
    ],
    alerts: [
        { kind: 'info', title: 'ESP32 Live Telemetry Active', meta: 'Sensor Node #1 · Streaming' },
        { kind: 'info', title: 'Hyperledger Fabric Peer Connected', meta: 'Channel: mychannel · 0ms latency' }
    ]
};

let adminCharts = {};
let adminTransfers = [];
let allAdminTransfers = [];
let ledgerViewMode = 'single';
let sensorRecords = [];
let adminRefreshInFlight = false;

function updateCurrentDate() {
    const el = document.getElementById('current-date');
    if (!el) return;
    el.textContent = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function populateKpis(data) {
    const values = {
        'kpi-total-batches': Number(data.batches || 0),
        'kpi-active-shipments': Number(data.shipments || 0),
        'kpi-blockchain-tx': Number(data.blockchainTx || 0).toLocaleString(),
        'kpi-sensor-readings': Number(data.sensorReadings || 0).toLocaleString(),
        'kpi-health': (Number(data.health || 0)) + '%',
        'kpi-devices': Number(data.devices || 1),
        'kpi-temp': data.temp || '27.7°C',
        'kpi-humidity': data.humidity || '73.6%',
        'kpi-alerts': Number(data.alerts || 0)
    };

    Object.entries(values).forEach(([id, value]) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    });

    const badge = document.getElementById('sidebar-alert-count');
    if (badge) badge.textContent = values['kpi-alerts'];
}

function safeEscape(str) {
    if (str === null || str === undefined) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function updateFabricIndicator(isFabricOnline) {
    const btnFabric = document.getElementById('btn-ledger-fabric');
    const btnSha256 = document.getElementById('btn-ledger-sha256');
    if (!btnFabric || !btnSha256) return;

    if (isFabricOnline) {
        btnFabric.classList.add('active-glowing-fabric');
        btnSha256.classList.remove('active-glowing-sha256');
    } else {
        btnSha256.classList.add('active-glowing-sha256');
        btnFabric.classList.remove('active-glowing-fabric');
    }
}

function populateTransactions(rows) {
    const body = document.getElementById('ledger-table-body');
    if (!body) return;
    if (!rows || rows.length === 0) {
        body.innerHTML = '<tr><td colspan="9" style="text-align:center; padding:16px; color:var(--muted);">Replay has not started yet. Select a batch to view ledger transactions.</td></tr>';
        return;
    }

    function ledgerValue(value) {
        return value === null || value === undefined || value === '' ? 'Not recorded' : safeEscape(value);
    }

    function shortHash(value) {
        const raw = String(value || '');
        if (!raw) return 'Not recorded';
        return raw.length > 20 ? `${safeEscape(raw.slice(0, 10))}...${safeEscape(raw.slice(-8))}` : safeEscape(raw);
    }

    function populateLedgerDetails(rows) {
        const body = document.getElementById('ledger-detail-body');
        if (!body) return;
        adminTransfers = Array.isArray(rows) ? rows : [];
        if (!adminTransfers.length) {
            body.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:16px;color:var(--muted);">No ledger transactions are available.</td></tr>';
            return;
        }
        let previousBatch = null;
        body.innerHTML = adminTransfers.map((tx) => {
            const verified = (tx.verification_status || '').toLowerCase() === 'verified';
            const batchId = tx.batch_id || 'Unknown batch';
            const groupRow = ledgerViewMode === 'multiple' && batchId !== previousBatch
                ? `<tr class="ledger-group-row"><td colspan="10"><i class="fa-solid fa-boxes-stacked"></i> Product / batch: <strong>${ledgerValue(batchId)}</strong></td></tr>`
                : '';
            previousBatch = batchId;
            const fabricTx = tx.fabric_tx_id || tx.blockchain_tx_id;
            const mode = fabricTx ? 'Hyperledger Fabric' : 'SHA-256';
            return `${groupRow}<tr>
                <td><code>${ledgerValue(tx.transaction_no || tx.id)}</code></td>
                <td><code class="ledger-full-hash" title="${ledgerValue(tx.block_hash)}">${shortHash(tx.block_hash)}</code></td>
                <td><code>${ledgerValue(tx.fabric_tx_id || tx.blockchain_tx_id)}</code></td>
                <td>${ledgerValue(tx.block_number)}</td>
                <td><strong>${ledgerValue(tx.batch_id)}</strong></td>
                <td>${ledgerValue(tx.created_at)}</td>
                <td><code title="${ledgerValue(tx.block_hash)}">${shortHash(tx.block_hash)}</code></td>
                <td><code title="${ledgerValue(tx.previous_hash)}">${ledgerValue(tx.previous_hash)}</code></td>
                <td><span class="${verified ? 'status-ok' : 'status-warn'}">${ledgerValue(tx.verification_status)}</span></td>
                <td>${ledgerValue(tx.ledger_status)}</td>
            </tr>`;
        }).join('');
    }

    function filterLedgerDetails(query) {
        const term = String(query || '').trim().toLowerCase();
        const filtered = allAdminTransfers.filter((tx) => [
            tx.transaction_id, tx.id, tx.fabric_tx_id, tx.blockchain_tx_id, tx.batch_id
        ].some(value => String(value || '').toLowerCase().includes(term)));
        populateLedgerDetails(filtered);
    }

    function openLedgerChooser() {
        openModal('ledger-choice');
        const form = document.getElementById('ledger-single-form');
        const message = document.getElementById('ledger-choice-message');
        if (form) form.hidden = true;
        if (message) message.textContent = '';
        populateActiveShipmentOptions();
    }

    function populateActiveShipmentOptions() {
        const select = document.getElementById('ledger-product-id');
        if (!select) return;
        const current = select.value;
        const shipments = [...new Map(allAdminTransfers
            .filter(tx => tx.batch_id)
            .map(tx => [String(tx.batch_id), tx])).values()];
        select.innerHTML = '<option value="">Select an active shipment</option>' + shipments.map(tx =>
            `<option value="${safeEscape(tx.batch_id)}">${safeEscape(tx.batch_id)}${tx.product_name ? ` — ${safeEscape(tx.product_name)}` : ''}</option>`
        ).join('');
        if (shipments.some(tx => String(tx.batch_id) === current)) select.value = current;
    }

    async function loadExpandedLedger(batchId) {
        const query = batchId ? `?batch_id=${encodeURIComponent(batchId)}&limit=200` : '?limit=200';
        try {
            ledgerViewMode = batchId ? 'single' : 'multiple';
            const result = await FoodChainAPI.fetchJson(`/api/admin/transfers${query}`);
            const rows = Array.isArray(result?.transfers) ? result.transfers : [];
            allAdminTransfers = rows.map((tx) => ({
                ...tx,
                transaction_id: tx.transaction_id || String(tx.id || ''),
                fabric_tx_id: tx.fabric_tx_id || tx.blockchain_tx_id,
                verification_status: tx.verification_status || (tx.blockchain_tx_id ? 'Verified' : 'Pending'),
                ledger_status: tx.ledger_status || (tx.blockchain_tx_id ? 'Hyperledger Fabric' : 'Not recorded')
            }));
            populateActiveShipmentOptions();
            populateLedgerDetails(allAdminTransfers);
            openModal('ledger');
            const search = document.getElementById('ledger-detail-search');
            if (search) search.value = '';
        } catch (error) {
            const message = document.getElementById('ledger-choice-message');
            if (message) message.textContent = 'Ledger transactions could not be loaded.';
        }
    }
    body.innerHTML = rows.map((tx) => {
        const isFabric = Boolean(tx.fabric_tx_id || tx.blockchain_tx_id);
        const modeLabel = isFabric ? 'Hyperledger Fabric' : 'SHA-256';
        const isFlagged = tx.status === 'Flagged';
        const statusHtml = isFlagged
            ? `<div class="status-cell-flex">
                 <span class="status-text" style="color:var(--amber);"><i class="fa-solid fa-triangle-exclamation"></i> Flagged</span>
                 <span class="glass-pill pill-purple" title="Hyperledger Fabric"><i class="fa-solid fa-cube"></i> Hyperledger Fabric</span>
               </div>`
            : isFabric
            ? `<div class="status-cell-flex">
                 <span class="status-text text-green"><i class="fa-solid fa-circle-check"></i> Verified</span>
                 <span class="glass-pill pill-purple" title="Hyperledger Fabric"><i class="fa-solid fa-cube"></i> Hyperledger Fabric</span>
               </div>`
            : `<div class="status-cell-flex">
                 <span class="status-text text-blue"><i class="fa-solid fa-shield-halved"></i> Confirmed</span>
                 <span class="glass-pill pill-blue" title="SHA-256 Hash Chain"><i class="fa-solid fa-shield-halved"></i> SHA-256</span>
               </div>`;

        const batchDisplay = tx.product
            ? `<span>${safeEscape(tx.batch)}</span> <small style="color:var(--muted); font-size:10px; display:block;">${safeEscape(tx.product)}</small>`
            : `<span>${safeEscape(tx.batch)}</span>`;
        const tempDisplay = tx.temp || (tx.temperature != null ? `${tx.temperature}°C` : '--');

        const blockHash = tx.block_hash || '';
        const fabricTx = tx.fabric_tx_id || tx.blockchain_tx_id || '';
        return `<tr>
            <td>
                <strong class="ledger-batch">${safeEscape(tx.batch || 'Unknown batch')}</strong>
                <span class="ledger-event">${safeEscape(tx.stage || 'Transfer')} · #${safeEscape(tx.transaction_no || tx.id || '—')}</span>
            </td>
            <td>
                <span class="mode-pill ${isFabric ? 'mode-fabric' : 'mode-sha'}">${modeLabel}</span>
                <code class="ledger-proof" title="${safeEscape(fabricTx || blockHash)}">${shortHash(fabricTx || blockHash)}</code>
            </td>
            <td>
                <strong class="ledger-sensor">${safeEscape(tx.device_id || '—')}</strong>
                <span class="ledger-event">${safeEscape(tx.latest_iot?.current_stage || tx.stage || '—')}</span>
            </td>
            <td><span class="ledger-temperature">${safeEscape(tempDisplay)}</span></td>
            <td><time datetime="${safeEscape(tx.time || tx.created_at || '')}">${safeEscape(tx.time || tx.created_at || 'Recent')}</time></td>
            <td>${statusHtml}</td>
        </tr>`;
    }).join('');
}

function ledgerValue(value) {
   return value === null || value === undefined || value === '' ? 'Not recorded' : safeEscape(value);
}

function populateLedgerDetails(rows) {
   const body = document.getElementById('ledger-detail-body');
   if (!body) return;
   adminTransfers = Array.isArray(rows) ? rows : [];
   if (!adminTransfers.length) {
       body.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:16px;color:var(--muted);">No ledger transactions are available.</td></tr>';
       return;
   }
   let previousBatch = null;
   body.innerHTML = adminTransfers.map((tx) => {
       const verified = (tx.verification_status || '').toLowerCase() === 'verified';
       const batchId = tx.batch_id || 'Unknown batch';
       const groupRow = ledgerViewMode === 'multiple' && batchId !== previousBatch
           ? `<tr class="ledger-group-row"><td colspan="9">Product / batch: <strong>${ledgerValue(batchId)}</strong></td></tr>` : '';
       previousBatch = batchId;
       return `${groupRow}<tr><td><code>${ledgerValue(tx.transaction_id || tx.id)}</code></td><td><code>${ledgerValue(tx.fabric_tx_id || tx.blockchain_tx_id)}</code></td><td>${ledgerValue(tx.block_number)}</td><td><strong>${ledgerValue(tx.batch_id)}</strong></td><td>${ledgerValue(tx.created_at)}</td><td><code>${ledgerValue(tx.sha256)}</code></td><td><code>${ledgerValue(tx.previous_hash)}</code></td><td><span class="${verified ? 'status-ok' : 'status-warn'}">${ledgerValue(tx.verification_status)}</span></td><td>${ledgerValue(tx.ledger_status)}</td></tr>`;
   }).join('');
}

function populateActiveShipmentOptions() {
   const select = document.getElementById('ledger-product-id');
   if (!select) return;
   const shipments = [...new Map(allAdminTransfers.filter(tx => tx.batch_id).map(tx => [String(tx.batch_id), tx])).values()];
   select.innerHTML = '<option value="">Select an active shipment</option>' + shipments.map(tx =>
       `<option value="${safeEscape(tx.batch_id)}">${safeEscape(tx.batch_id)}${tx.product_name ? ` — ${safeEscape(tx.product_name)}` : ''}</option>`
   ).join('');
}

async function openLedgerChooser() {
   openModal('ledger-choice');
   const form = document.getElementById('ledger-single-form');
   if (form) form.hidden = true;
   if (!allAdminTransfers.length) {
       try {
           const result = await FoodChainAPI.fetchJson('/api/admin/transfers?limit=200');
           allAdminTransfers = (result?.transfers || []).map((tx) => ({
               ...tx,
               transaction_id: tx.transaction_id || String(tx.id || ''),
               fabric_tx_id: tx.fabric_tx_id || tx.blockchain_tx_id,
               block_hash: tx.block_hash || (sensorRecords.find(record => String(record.batch_id || '') === String(tx.batch_id || '')) || {}).block_hash || null,
               verification_status: tx.verification_status || (tx.blockchain_tx_id ? 'Verified' : 'Pending'),
               ledger_status: tx.ledger_status || (tx.blockchain_tx_id ? 'Hyperledger Fabric' : 'SHA-256')
           }));
       } catch (error) {
           console.error('Unable to load live ledger records:', error);
       }
   }
   populateActiveShipmentOptions();
}

async function loadExpandedLedger(batchId) {
   ledgerViewMode = batchId ? 'single' : 'multiple';
   try {
       const query = batchId ? `?batch_id=${encodeURIComponent(batchId)}&limit=200` : '?limit=200';
       const result = await FoodChainAPI.fetchJson(`/api/admin/transfers${query}`);
       allAdminTransfers = (result?.transfers || []).map(tx => ({
           ...tx,
           ...(sensorRecords.find(record => String(record.batch_id || '') === String(tx.batch_id || '')) || {}),
           transaction_id: tx.transaction_id || String(tx.id || ''),
           fabric_tx_id: tx.fabric_tx_id || tx.blockchain_tx_id,
           verification_status: tx.verification_status || (tx.blockchain_tx_id ? 'Verified' : 'Pending'),
           ledger_status: tx.ledger_status || (tx.blockchain_tx_id ? 'Hyperledger Fabric' : 'Not recorded')
       }));
       populateLedgerDetails(allAdminTransfers);
       openModal('ledger');
   } catch (error) {
       console.error('Ledger load failed:', error);
   }
}

function populateAlerts(list) {
    const container = document.getElementById('alerts-list');
    if (!container) return;
    if (!Array.isArray(list) || list.length === 0) {
        container.innerHTML = `
            <div class="alerts-empty">
                <i class="fa-solid fa-circle-check" aria-hidden="true"></i>
                <strong>All systems clear</strong>
                <small>No active alerts from the latest sensor and service checks.</small>
            </div>
        `;
        return;
    }
    container.innerHTML = list.map((alert) => `
        <div class="alert-item ${safeEscape(alert.kind || 'info')}">
            <span>${safeEscape(alert.title)}</span>
            <small>${safeEscape(alert.meta)}</small>
        </div>
    `).join('');
}

function initMap() {
    const mapEl = document.getElementById('supply-chain-map');
    if (!mapEl || typeof L === 'undefined') return;
    const map = L.map('supply-chain-map', { zoomControl: false, scrollWheelZoom: false }).setView([20.5937, 78.9629], 5);
    L.tileLayer('https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors',
        maxZoom: 19
    }).addTo(map);

    const points = [
        { lat: 12.9716, lon: 77.5946, name: 'Producer & ESP32 Node - Bengaluru', color: '#2dd4bf' },
        { lat: 13.0827, lon: 80.2707, name: 'Processing Center - Chennai', color: '#4ec3ff' },
        { lat: 19.0760, lon: 72.8777, name: 'Logistics Hub - Mumbai', color: '#fbbf24' },
        { lat: 28.6139, lon: 77.2090, name: 'Cold Warehouse - Delhi', color: '#a78bfa' },
        { lat: 18.5204, lon: 73.8567, name: 'Retail Audit - Pune', color: '#f87171' }
    ];

    points.forEach((point) => {
        L.circleMarker([point.lat, point.lon], {
            radius: 8,
            color: point.color,
            fillColor: point.color,
            fillOpacity: 0.9,
            weight: 2
        }).bindPopup(point.name).addTo(map);
    });
}

function initSensorChart() {
    const canvas = document.getElementById('sensor-history-chart');
    if (!canvas || typeof Chart === 'undefined') return;
    const ctx = canvas.getContext('2d');
    adminCharts.sensorHistory = new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['13:20', '13:22', '13:24', '13:26', '13:28', '13:30', 'Now'],
            datasets: [{
                label: 'ESP32 Live Temp (°C)',
                data: [27.2, 27.4, 27.5, 27.6, 27.7, 27.7, 27.8],
                borderColor: '#2dd4bf',
                backgroundColor: 'rgba(45, 212, 191, 0.15)',
                fill: true,
                tension: 0.35,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { display: false },
                y: { display: false, min: 25, max: 30 }
            }
        }
    });
}

function initModals() {
    const navItems = document.querySelectorAll('.nav-item[data-modal]:not([data-detail])');
    const overlay = document.getElementById('modal-overlay');
    const sidebarLedger = document.querySelector('.sidebar .nav-item[data-detail="blockchain"]');
    if (sidebarLedger) {
        sidebarLedger.addEventListener('click', (event) => {
            event.preventDefault();
            document.querySelectorAll('.sidebar .nav-item').forEach((nav) => nav.classList.remove('active'));
            sidebarLedger.classList.add('active');
            openLedgerChooser();
        });
    }
    navItems.forEach((item) => {
        item.addEventListener('click', function (e) {
            e.preventDefault();
            const modalId = this.getAttribute('data-modal');
            if (modalId && modalId !== 'dashboard') {
                openModal(modalId);
            } else {
                closeAllModals();
            }
            navItems.forEach((el) => el.classList.remove('active'));
            this.classList.add('active');
        });
    });

    document.querySelectorAll('.modal-close').forEach((btn) => btn.addEventListener('click', closeAllModals));
    const viewAll = document.getElementById('ledger-view-all');
    if (viewAll) viewAll.addEventListener('click', openLedgerChooser);
    const singleChoice = document.getElementById('ledger-single-choice');
    if (singleChoice) singleChoice.addEventListener('click', () => {
        const form = document.getElementById('ledger-single-form');
        if (form) form.hidden = false;
        const select = document.getElementById('ledger-product-id');
        if (select) select.focus();
    });
    const multipleChoice = document.getElementById('ledger-multiple-choice');
    if (multipleChoice) multipleChoice.addEventListener('click', () => loadExpandedLedger(''));
    const singleSubmit = document.getElementById('ledger-single-submit');
    if (singleSubmit) singleSubmit.addEventListener('click', () => {
        const input = document.getElementById('ledger-product-id');
        const value = input ? input.value.trim() : '';
        if (value) loadExpandedLedger(value);
        else {
            const message = document.getElementById('ledger-choice-message');
            if (message) message.textContent = 'Enter a product or batch ID first.';
        }
    });
    if (overlay) overlay.addEventListener('click', closeAllModals);
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeAllModals(); });
}

function renderQrCode() {
    const canvas = document.getElementById('qr-canvas');
    if (!canvas || typeof QRCode === 'undefined') return;
    const batchId = 'BATCH-001';
    const verifyUrl = `${FoodChainAPI.getApiBaseUrl()}/verify/${batchId}`;
    QRCode.toCanvas(canvas, verifyUrl, {
        width: 180,
        margin: 1,
        color: { dark: '#0b1220', light: '#ffffff' }
    }, function (error) {
        if (error) console.error('QR Render Error:', error);
    });
}

function renderAnalyticsChart() {
    const canvas = document.getElementById('admin-analytics-chart');
    if (!canvas || typeof Chart === 'undefined') return;
    if (adminCharts.analyticsChart) return; // already initialized

    const ctx = canvas.getContext('2d');
    adminCharts.analyticsChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Batch 001', 'Batch 002', 'Batch 003', 'Batch 004', 'Batch 005', 'Batch 006'],
            datasets: [
                {
                    label: 'Cold-Chain Compliance (%)',
                    data: [100, 98, 99, 97, 100, 100],
                    backgroundColor: 'rgba(45, 212, 191, 0.7)',
                    borderRadius: 6
                },
                {
                    label: 'Blockchain Commit Time (ms)',
                    data: [42, 38, 45, 50, 39, 41],
                    backgroundColor: 'rgba(78, 195, 255, 0.7)',
                    borderRadius: 6
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: { color: '#9bb2c8', font: { family: 'Inter', size: 11 } }
                }
            },
            scales: {
                x: { ticks: { color: '#9bb2c8', font: { size: 10 } }, grid: { display: false } },
                y: { ticks: { color: '#9bb2c8', font: { size: 10 } }, grid: { color: 'rgba(255,255,255,0.06)' } }
            }
        }
    });
}

function openModal(modalId) {
    closeAllModals();
    const modal = document.getElementById(`modal-${modalId}`);
    const overlay = document.getElementById('modal-overlay');
    if (modal) modal.classList.add('active');
    if (overlay) overlay.classList.add('active');

    if (modalId === 'qr') {
        setTimeout(renderQrCode, 50);
    } else if (modalId === 'analytics') {
        setTimeout(renderAnalyticsChart, 50);
    }
}

function closeAllModals() {
    document.querySelectorAll('.modal').forEach((modal) => modal.classList.remove('active'));
    const overlay = document.getElementById('modal-overlay');
    if (overlay) overlay.classList.remove('active');
}

async function loadAdminDashboardData() {
    if (adminRefreshInFlight) return;
    adminRefreshInFlight = true;
    try {
        const [adminStats, kpiRes, fabricRes, dataRes, alertRes] = await Promise.all([
            FoodChainAPI.fetchJson('/api/admin/stats'),
            fetch(FoodChainAPI.resolveApiUrl('/api/kpis')).then(r => r.ok ? r.json() : null).catch(() => null),
            fetch(FoodChainAPI.resolveApiUrl('/api/fabric-status')).then(r => r.ok ? r.json() : null).catch(() => null),
            fetch(FoodChainAPI.resolveApiUrl('/data')).then(r => r.ok ? r.json() : null).catch(() => null),
            fetch(FoodChainAPI.resolveApiUrl('/api/agent-alerts')).then(r => r.ok ? r.json() : null).catch(() => null)
        ]);

        const liveAlerts = Array.isArray(alertRes?.alerts) ? alertRes.alerts : [];
        populateAlerts(liveAlerts.slice(0, 6).map((alert) => ({
            kind: alert.severity === 'critical' ? 'critical' : alert.severity === 'warning' ? 'warning' : 'info',
            title: alert.message || alert.title || 'System alert',
            meta: [
                alert.batch_id && `Batch ${alert.batch_id}`,
                alert.source || alert.alert_source,
                alert.timestamp
            ].filter(Boolean).join(' · ') || 'Live monitoring'
        })));

        const latestReading = (dataRes && Array.isArray(dataRes.data) && dataRes.data.length > 0)
            ? dataRes.data[dataRes.data.length - 1]
            : null;
        sensorRecords = Array.isArray(dataRes?.data) ? dataRes.data : [];

        const kpis = {
            batches: adminStats?.batches?.total ?? kpiRes?.total_batches ?? 0,
            shipments: kpiRes?.active_shipments ?? 0,
            blockchainTx: adminStats?.batches?.transfers ?? kpiRes?.blockchain_transactions ?? 0,
            sensorReadings: adminStats?.iot?.sensor_readings ?? kpiRes?.total_sensors ?? 0,
            health: fabricRes?.fabric_available ? 100 : 95,
            devices: new Set(sensorRecords.map(record => record.device_id || record.sensor_id).filter(Boolean)).size,
            temp: latestReading?.temperature != null ? `${latestReading.temperature.toFixed(1)}°C` : '--',
            humidity: latestReading?.humidity != null ? `${latestReading.humidity.toFixed(1)}%` : '--',
            alerts: liveAlerts.length
        };

        populateKpis(kpis);

        const isFabricOnline = Boolean(fabricRes?.fabric_available);
        updateFabricIndicator(isFabricOnline);

        if (latestReading && adminCharts.sensorHistory) {
            const chart = adminCharts.sensorHistory;
            const dataArr = chart.data.datasets[0].data;
            dataArr[dataArr.length - 1] = latestReading.temperature;
            chart.update();
        }

        FoodChainAPI.setApiStatus('Live Backend & ESP32 Connected', 'success');

        // Optional authenticated calls if token is valid
        try {
            const transfers = await FoodChainAPI.fetchJson('/api/admin/transfers?limit=6');
            if (transfers && Array.isArray(transfers.transfers) && transfers.transfers.length > 0) {
                allAdminTransfers = transfers.transfers.map((tx) => ({
                    ...tx,
                    transaction_id: tx.transaction_id || String(tx.id || ''),
                    transaction_no: tx.id,
                    fabric_tx_id: tx.fabric_tx_id || tx.blockchain_tx_id,
                    verification_status: tx.verification_status || (tx.blockchain_tx_id ? 'Verified' : 'Pending'),
                    ledger_status: tx.ledger_status || (tx.blockchain_tx_id ? 'Hyperledger Fabric' : 'Not recorded')
                }));
                populateLedgerDetails(allAdminTransfers);
                const mapped = transfers.transfers.map(tx => {
                    const sensor = sensorRecords.find(record => String(record.batch_id || '') === String(tx.batch_id || '')) || {};
                    return {
                    tx: tx.blockchain_tx_id ? `${tx.blockchain_tx_id.slice(0, 8)}...` : ('0x' + (tx.id || 'N/A')),
                    batch: tx.batch_id || 'FC-201',
                    product: tx.product_name || '',
                    stage: tx.event_type || 'Transport',
                    temp: tx.temperature != null ? `${tx.temperature}°C` : '--',
                    time: tx.created_at || 'Recent',
                    status: tx.status || 'Verified',
                    ledger: (tx.fabric_tx_id || tx.blockchain_tx_id) ? 'fabric' : 'sha256',
                    fabric_tx_id: tx.fabric_tx_id || tx.blockchain_tx_id || null,
                    block_hash: tx.block_hash || sensor.block_hash || null,
                    field_hash: tx.field_hash || sensor.field_hash || null
                };
                });
                populateTransactions(mapped);
            } else {
                allAdminTransfers = [];
                populateLedgerDetails([]);
                populateTransactions([]);
            }
        } catch (authErr) {
            allAdminTransfers = [];
            populateLedgerDetails([]);
            populateTransactions([]);
        }

    } catch (err) {
        console.warn('Dashboard live update issue:', err);
        populateKpis({});
        populateTransactions([]);
        populateLedgerDetails([]);
        allAdminTransfers = [];
        populateAlerts([]);
        const ledgerSearch = document.getElementById('ledger-detail-search');
        if (ledgerSearch) ledgerSearch.addEventListener('input', (event) => filterLedgerDetails(event.target.value));
        FoodChainAPI.setApiStatus('Admin data unavailable', 'warning');
    } finally {
        adminRefreshInFlight = false;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Role protection
    FoodChainAPI.initRoleGuard('admin');

    updateCurrentDate();
    initMap();
    initSensorChart();
    initModals();
    populateKpis({});
    populateTransactions([]);
    populateAlerts([]);
    const printButton = document.getElementById('admin-print-report');
    if (printButton) printButton.addEventListener('click', () => window.print());

    // Initial fetch
    loadAdminDashboardData();

    // Live polling every 5s for live ESP32 telemetry updates
    setInterval(loadAdminDashboardData, 5000);
});

window.addEventListener('resize', function () {
    if (adminCharts.sensorHistory) adminCharts.sensorHistory.resize();
    if (adminCharts.analyticsChart) adminCharts.analyticsChart.resize();
});

// ── Create User Modal Logic ──────────────────────────────────────────────────
(function () {
    function showCreateUserModal() {
        const modal = document.getElementById('modal-create-user');
        if (!modal) return;
        // Reset form and feedback
        const form = document.getElementById('create-user-form');
        if (form) form.reset();
        const fb = document.getElementById('create-user-feedback');
        if (fb) { fb.style.display = 'none'; fb.textContent = ''; }
        modal.style.display = 'flex';
        modal.style.alignItems = 'center';
        modal.style.justifyContent = 'center';
        setTimeout(() => { const inp = document.getElementById('cu-username'); if (inp) inp.focus(); }, 80);
    }

    function hideCreateUserModal() {
        const modal = document.getElementById('modal-create-user');
        if (modal) modal.style.display = 'none';
    }

    function setFeedback(msg, isSuccess) {
        const fb = document.getElementById('create-user-feedback');
        if (!fb) return;
        fb.textContent = msg;
        fb.style.display = 'block';
        if (isSuccess) {
            fb.style.background = 'rgba(34,197,94,0.15)';
            fb.style.border = '1px solid rgba(34,197,94,0.4)';
            fb.style.color = '#86efac';
        } else {
            fb.style.background = 'rgba(239,68,68,0.15)';
            fb.style.border = '1px solid rgba(239,68,68,0.4)';
            fb.style.color = '#fca5a5';
        }
    }

    document.addEventListener('DOMContentLoaded', () => {
        // Sidebar button opens modal
        const sidebarBtn = document.getElementById('sidebar-create-user-btn');
        if (sidebarBtn) {
            sidebarBtn.addEventListener('click', (e) => {
                e.preventDefault();
                showCreateUserModal();
            });
        }

        // Close buttons
        document.getElementById('create-user-modal-close')?.addEventListener('click', hideCreateUserModal);
        document.getElementById('cu-cancel-btn')?.addEventListener('click', hideCreateUserModal);

        // Click outside modal content to close
        document.getElementById('modal-create-user')?.addEventListener('click', (e) => {
            if (e.target === document.getElementById('modal-create-user')) hideCreateUserModal();
        });

        // Form submit → POST /api/admin/users
        document.getElementById('create-user-form')?.addEventListener('submit', async (e) => {
            e.preventDefault();
            const username = (document.getElementById('cu-username')?.value || '').trim();
            const password = (document.getElementById('cu-password')?.value || '');
            const role = (document.getElementById('cu-role')?.value || '');

            if (!username || !password || !role) {
                setFeedback('Please fill in all fields.', false);
                return;
            }
            if (password.length < 6) {
                setFeedback('Password must be at least 6 characters.', false);
                return;
            }

            const btn = document.getElementById('cu-submit-btn');
            if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>&nbsp; Creating...'; }

            try {
                const result = await FoodChainAPI.postJson('/api/admin/users', { username, password, role });
                setFeedback(`✅ User "${result.username}" created successfully with role: ${result.role}`, true);
                document.getElementById('create-user-form')?.reset();
                // Auto-close after 2.5 seconds
                setTimeout(hideCreateUserModal, 2500);
            } catch (err) {
                setFeedback(`❌ ${err.message || 'Failed to create user. Check credentials.'}`, false);
            } finally {
                if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-user-plus"></i>&nbsp; Create User'; }
            }
        });
    });
})();
