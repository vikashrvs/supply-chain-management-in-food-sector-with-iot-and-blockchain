const mockData = {
    kpis: {
        incoming: 8,
        active: 12,
        delivered: 28,
        delayed: 3,
        tempAlerts: 2,
        iot: 17
    },
    shipments: [
        { batch: 'FC-1024', product: 'Bananas', from: 'Bengaluru', to: 'Delhi', status: 'In Transit', eta: 'Today 18:30' },
        { batch: 'FC-1113', product: 'Mangoes', from: 'Hubli', to: 'Mumbai', status: 'Processing', eta: 'Today 16:10' },
        { batch: 'FC-2015', product: 'Tomatoes', from: 'Coimbatore', to: 'Hyderabad', status: 'Delayed', eta: 'Tomorrow 09:45' },
        { batch: 'FC-2281', product: 'Grapes', from: 'Vijayawada', to: 'Pune', status: 'Delivered', eta: 'Completed' },
        { batch: 'FC-3102', product: 'Onions', from: 'Nagpur', to: 'Chandigarh', status: 'In Transit', eta: 'Tomorrow 13:20' }
    ],
    alerts: [
        { level: 'critical', title: 'Temperature rise detected', meta: 'Batch FC-2015 · 10 mins ago' },
        { level: 'warning', title: 'Route delay in progress', meta: 'Batch FC-3102 · 28 mins ago' },
        { level: 'info', title: 'GPS sync refreshed', meta: 'Device ESP32-08 · 2 mins ago' },
        { level: 'info', title: 'Delivery verified', meta: 'Batch FC-2281 · 1 hr ago' }
    ]
};

function populateKpis(kpis) {
    const ids = {
        'kpi-incoming': Number(kpis.incoming || 0),
        'kpi-active': Number(kpis.active || 0),
        'kpi-delivered': Number(kpis.delivered || 0),
        'kpi-delayed': Number(kpis.delayed || 0),
        'kpi-temp-alerts': Number(kpis.tempAlerts || 0),
        'kpi-iot': Number(kpis.iot || 0)
    };

    Object.entries(ids).forEach(([id, value]) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    });
}

function populateShipments(rows) {
    const tbody = document.getElementById('shipments-table-body');
    if (!tbody) return;

    tbody.innerHTML = rows.map((item) => {
        const className = item.status === 'Delayed' ? 'delayed' : item.status === 'Delivered' ? 'delivered' : 'in-transit';
        return `
            <tr>
                <td>${item.batch || item.batch_id || 'N/A'}</td>
                <td>${item.product || 'Unknown'}</td>
                <td>${item.from || item.location_name || 'Unknown'}</td>
                <td>${item.to || item.destination || 'Unknown'}</td>
                <td><span class="status-pill ${className}">${item.status}</span></td>
            </tr>
        `;
    }).join('');
}

function populateAlerts(list) {
    const container = document.getElementById('alerts-list');
    if (!container) return;
    container.innerHTML = list.map((alert) => `
        <div class="alert-item ${alert.level || 'info'}">
            <strong>${alert.title || alert.message || 'System alert'}</strong>
            <small>${alert.meta || alert.timestamp || 'Recent event'}</small>
        </div>
    `).join('');
}

function updateDate() {
    const el = document.getElementById('current-date');
    if (!el) return;
    el.textContent = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function initMap() {
    const mapEl = document.getElementById('distributor-map');
    if (!mapEl || typeof L === 'undefined') return;

    const map = L.map('distributor-map', { zoomControl: false, scrollWheelZoom: false }).setView([20.5937, 78.9629], 5);
    L.tileLayer('https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap contributors', maxZoom: 19 }).addTo(map);

    const points = [
        { lat: 12.9716, lon: 77.5946, label: 'Bengaluru' },
        { lat: 13.0827, lon: 80.2707, label: 'Chennai' },
        { lat: 18.5204, lon: 73.8567, label: 'Pune' },
        { lat: 19.0760, lon: 72.8777, label: 'Mumbai' },
        { lat: 28.6139, lon: 77.2090, label: 'Delhi' }
    ];

    points.forEach((point) => {
        L.circleMarker([point.lat, point.lon], {
            radius: 7,
            color: '#f59e0b',
            fillColor: '#f59e0b',
            fillOpacity: 0.95,
            weight: 2
        }).bindPopup(point.label).addTo(map);
    });

    L.polyline([
        [12.9716, 77.5946],
        [13.0827, 80.2707],
        [19.0760, 72.8777],
        [28.6139, 77.2090]
    ], { color: '#f59e0b', weight: 3, opacity: 0.8, dashArray: '8 10' }).addTo(map);
}

function initModals() {
    const navItems = document.querySelectorAll('.nav-item[data-modal]:not([data-detail])');
    const overlay = document.getElementById('modal-overlay');

    navItems.forEach((item) => {
        item.addEventListener('click', function (e) {
            e.preventDefault();
            const modalId = this.getAttribute('data-modal');
            if (modalId && modalId !== 'dashboard') openModal(modalId);
            else closeAllModals();
            navItems.forEach((el) => el.classList.remove('active'));
            this.classList.add('active');
        });
    });

    document.querySelectorAll('.modal-close').forEach((button) => button.addEventListener('click', closeAllModals));
    if (overlay) overlay.addEventListener('click', closeAllModals);
    document.addEventListener('keydown', (event) => { if (event.key === 'Escape') closeAllModals(); });
}

function openModal(modalId) {
    closeAllModals();
    const modal = document.getElementById(`modal-${modalId}`);
    const overlay = document.getElementById('modal-overlay');
    if (modal) modal.classList.add('active');
    if (overlay) overlay.classList.add('active');
}

function closeAllModals() {
    document.querySelectorAll('.modal').forEach((modal) => modal.classList.remove('active'));
    const overlay = document.getElementById('modal-overlay');
    if (overlay) overlay.classList.remove('active');
}

function updateDetailCard(batch) {
    const card = document.querySelector('.detail-card');
    if (!card) return;
    const batchId = batch?.batch_id || batch?.batchId || 'Unknown';
    const product = batch?.batch_info?.product_name || batch?.product_name || 'Unknown';
    const from = batch?.batch_info?.origin || 'Bengaluru';
    const to = batch?.batch_info?.destination || 'Delhi';
    const status = batch?.batch_info?.status || 'In Transit';
    card.innerHTML = `
        <div class="detail-meta"><span>Batch</span><strong>${batchId}</strong></div>
        <div class="detail-meta"><span>Product</span><strong>${product}</strong></div>
        <div class="detail-meta"><span>From</span><strong>${from}</strong></div>
        <div class="detail-meta"><span>To</span><strong>${to}</strong></div>
        <div class="detail-meta"><span>Status</span><strong class="status-pill ${String(status).toLowerCase().replace(/\s+/g, '-')}">${status}</strong></div>
    `;
}

async function searchBatchById(batchId) {
    const value = String(batchId || '').trim();
    if (!value) return;
    try {
        const data = await fetchJson('/api/distributor/batches/' + encodeURIComponent(value));
        updateDetailCard(data);
        setApiStatus('Batch lookup successful', 'success');
    } catch (error) {
        const card = document.querySelector('.detail-card');
        if (card) {
            card.innerHTML = `
                <div class="detail-meta"><span>Batch</span><strong>${value}</strong></div>
                <div class="detail-meta"><span>Product</span><strong>Demo fallback</strong></div>
                <div class="detail-meta"><span>From</span><strong>Unknown</strong></div>
                <div class="detail-meta"><span>To</span><strong>Unknown</strong></div>
                <div class="detail-meta"><span>Status</span><strong class="status-pill in-transit">Demo mode</strong></div>
            `;
        }
        setApiStatus('Demo mode: ' + (error && error.message ? error.message : 'Batch lookup unavailable'), 'warning');
    }
}

function bindBatchSearch() {
    const input = document.getElementById('batch-search-input');
    const button = document.querySelector('.search-box button');
    if (!input) return;
    const run = () => searchBatchById(input.value);
    if (button) button.addEventListener('click', run);
    input.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') run();
    });
}

async function loadDistributorData() {
    if (loadDistributorData.inFlight) return;
    loadDistributorData.inFlight = true;
    try {
        const [transfers, alerts] = await Promise.all([
            fetchJson('/api/distributor/transfers?limit=8'),
            fetchJson('/api/agent-alerts')
        ]);

        const shipmentRows = Array.isArray(transfers) ? transfers : [];
        const mappedRows = shipmentRows.slice(0, 5).map((item) => ({
            batch: item.batch_id || 'N/A',
            product: item.notes || 'Shipment',
            from: item.location_name || 'Bengaluru',
            to: item.location_name || 'Delhi',
            status: item.event_type === 'anomaly' ? 'Delayed' : item.event_type === 'transferred' ? 'In Transit' : 'Processing'
        }));

        const alertRows = Array.isArray(alerts) ? alerts : (alerts && Array.isArray(alerts.alerts) ? alerts.alerts : []);
        populateKpis({ incoming: mappedRows.length || mockData.kpis.incoming, active: mappedRows.length || mockData.kpis.active, delivered: 28, delayed: 3, tempAlerts: alertRows.filter((a) => a.severity === 'critical' || a.severity === 'warning').length || mockData.kpis.tempAlerts, iot: 17 });
        populateShipments(mappedRows.length ? mappedRows : mockData.shipments);
        populateAlerts(alertRows.length ? alertRows.slice(0, 4).map((alert) => ({
            level: alert.severity === 'critical' ? 'critical' : alert.severity === 'warning' ? 'warning' : 'info',
            title: alert.message || alert.title || 'System alert',
            meta: alert.batch_id ? `Batch ${alert.batch_id} · ${alert.timestamp || 'recent'}` : (alert.source || 'System')
        })) : mockData.alerts);
        setApiStatus('Live backend connected', 'success');
    } catch (error) {
        populateKpis(mockData.kpis);
        populateShipments(mockData.shipments);
        populateAlerts(mockData.alerts);
        setApiStatus('Demo mode: ' + (error && error.message ? error.message : 'Backend unavailable'), 'warning');
    } finally {
        loadDistributorData.inFlight = false;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    FoodChainAPI.initRoleGuard('distributor');
    populateKpis(mockData.kpis);
    populateShipments(mockData.shipments);
    populateAlerts(mockData.alerts);
    initMap();
    initModals();
    bindBatchSearch();
    updateDate();
    loadDistributorData();
    setInterval(loadDistributorData, 5000);
});
