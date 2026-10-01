const mockData = {
    batches: [
        { id: 'FC-1024', product: 'Apples', quantity: '500 kg', status: 'In Transit', updated: 'Sep 19' },
        { id: 'FC-1023', product: 'Tomatoes', quantity: '300 kg', status: 'Delivered', updated: 'Sep 18' },
        { id: 'FC-1022', product: 'Carrots', quantity: '200 kg', status: 'Processing', updated: 'Sep 18' },
        { id: 'FC-1021', product: 'Spinach', quantity: '150 kg', status: 'In Transit', updated: 'Sep 17' },
        { id: 'FC-1020', product: 'Mangoes', quantity: '400 kg', status: 'Flagged', updated: 'Sep 17' }
    ],
    alerts: [
        { type: 'critical', title: 'Temperature high', meta: 'Batch FC-1020 · 20 min ago' },
        { type: 'warning', title: 'Humidity low', meta: 'Batch FC-1019 · 1 hour ago' },
        { type: 'info', title: 'Gas level abnormal', meta: 'Batch FC-1018 · 2 hours ago' }
    ]
};

function normalizeStatus(value) {
    return String(value || '').trim();
}

function updateKpis(rows) {
    const num = rows.length || 1;
    const activeCount = rows.filter((row) => /processing|in transit|created/i.test(normalizeStatus(row.status))).length;
    const transitCount = rows.filter((row) => /in transit/i.test(normalizeStatus(row.status))).length;
    const deliveredCount = rows.filter((row) => /delivered/i.test(normalizeStatus(row.status))).length;
    const flaggedCount = rows.filter((row) => /flagged|anomaly|alert/i.test(normalizeStatus(row.status))).length;

    const ids = {
        'kpi-batches': rows.length,
        'kpi-active': activeCount,
        'kpi-transit': transitCount,
        'kpi-delivered': deliveredCount,
        'kpi-flagged': flaggedCount,
        'kpi-condition': Math.max(80, 100 - flaggedCount * 5) + '%'
    };

    Object.entries(ids).forEach(([id, value]) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    });
}

function renderBatches(rows) {
    const body = document.getElementById('batches-table-body');
    if (!body) return;
    body.innerHTML = rows.map((batch) => {
        const status = normalizeStatus(batch.status || 'Created');
        const safeStatus = status.toLowerCase().replace(/\s+/g, '-');
        return `<tr><td>${batch.batch_id || batch.id}</td><td>${batch.product_name || batch.product}</td><td>${batch.quantity || batch.qty || 'N/A'}</td><td><span class="status-pill ${safeStatus}">${status}</span></td><td>${batch.updated_at || batch.updated || 'recent'}</td></tr>`;
    }).join('');
}

function renderAlerts(list) {
    const container = document.getElementById('alerts-list');
    if (!container) return;
    container.innerHTML = list.map((alert) => `<div class="alert-item ${alert.type || 'info'}"><strong>${alert.title || alert.message || 'System alert'}</strong><small>${alert.meta || alert.timestamp || 'Recent event'}</small></div>`).join('');
}

function initMap() {
    const target = document.getElementById('producer-map');
    if (!target || typeof L === 'undefined') return;
    const map = L.map(target, { zoomControl: false, scrollWheelZoom: false }).setView([12.9716, 77.5946], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png', { attribution: '&copy; OpenStreetMap contributors', maxZoom: 19 }).addTo(map);
    L.marker([12.9716, 77.5946]).bindPopup('GreenFarm · Bengaluru, Karnataka').addTo(map).openPopup();
}

function initRegistration() {
    const form = document.getElementById('batch-form');
    const message = document.getElementById('form-message');
    if (!form || !message) return;

    form.addEventListener('submit', async (event) => {
        event.preventDefault();
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }

        const formData = new FormData(form);
        const fields = {
            product_name: formData.get('product_name') || form.querySelector('select').value,
            product_type: formData.get('product_type') || form.querySelector('select').value,
            quantity: formData.get('quantity') || form.querySelector('input[type="number"]').value,
            origin: 'Bengaluru',
            destination: form.querySelectorAll('select')[2]?.value || 'Delhi Distributor',
            description: 'Farm production batch registered from producer dashboard',
            harvest_date: form.querySelector('input[type="date"]').value
        };

        message.textContent = 'Submitting batch...';
        message.classList.remove('success');

        try {
            const response = await fetchJson('/api/producer/batches', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(fields)
            });
            message.textContent = 'Batch created successfully.';
            message.classList.add('success');
            form.reset();
            await loadProducerData();
            setApiStatus('Producer batch created successfully', 'success');
        } catch (error) {
            message.textContent = 'Demo mode: backend unavailable, mock batch queued locally.';
            const fallback = { batch_id: 'DEMO-' + Date.now(), product: fields.product_name || 'Demo Produce', quantity: fields.quantity || '1', status: 'Created', updated: 'now' };
            const current = [...mockData.batches, fallback];
            renderBatches(current);
            updateKpis(current);
            setApiStatus('Demo mode: ' + (error && error.message ? error.message : 'Backend unavailable'), 'warning');
        }
    });
}

function initModals() {
    const overlay = document.getElementById('modal-overlay');
    document.querySelectorAll('.nav-item[data-modal]:not([data-detail]), [data-modal="batches"]:not([data-detail])').forEach((item) => {
        item.addEventListener('click', (event) => {
            event.preventDefault();
            const id = item.getAttribute('data-modal');
            if (id === 'dashboard') closeAllModals();
            else openModal(id);
            document.querySelectorAll('.nav-item[data-modal]').forEach((nav) => nav.classList.remove('active'));
            if (item.classList.contains('nav-item')) item.classList.add('active');
        });
    });
    document.querySelectorAll('.modal-close').forEach((button) => button.addEventListener('click', closeAllModals));
    if (overlay) overlay.addEventListener('click', closeAllModals);
    document.addEventListener('keydown', (event) => { if (event.key === 'Escape') closeAllModals(); });
}

function openModal(id) {
    closeAllModals();
    const modal = document.getElementById(`modal-${id}`);
    const overlay = document.getElementById('modal-overlay');
    if (modal) modal.classList.add('active');
    if (overlay) overlay.classList.add('active');
}

function closeAllModals() {
    document.querySelectorAll('.modal').forEach((modal) => modal.classList.remove('active'));
    const overlay = document.getElementById('modal-overlay');
    if (overlay) overlay.classList.remove('active');
}

function updateDate() {
    const date = document.getElementById('current-date');
    if (date) date.textContent = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

async function loadProducerData() {
    try {
        const [batches, alerts] = await Promise.all([
            fetchJson('/api/producer/batches?limit=8'),
            fetchJson('/api/agent-alerts')
        ]);

        const rows = Array.isArray(batches) ? batches : [];
        const alertList = Array.isArray(alerts) ? alerts : (alerts && Array.isArray(alerts.alerts) ? alerts.alerts : []);
        renderBatches(rows.length ? rows : mockData.batches);
        updateKpis(rows.length ? rows : mockData.batches);
        renderAlerts(alertList.length ? alertList.map((alert) => ({
            type: alert.severity === 'critical' ? 'critical' : alert.severity === 'warning' ? 'warning' : 'info',
            title: alert.message || alert.title || 'System alert',
            meta: alert.batch_id ? `Batch ${alert.batch_id} · ${alert.timestamp || 'recent'}` : (alert.source || 'System')
        })) : mockData.alerts);
        setApiStatus('Live backend connected', 'success');
    } catch (error) {
        renderBatches(mockData.batches);
        updateKpis(mockData.batches);
        renderAlerts(mockData.alerts);
        setApiStatus('Demo mode: ' + (error && error.message ? error.message : 'Backend unavailable'), 'warning');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    FoodChainAPI.initRoleGuard('producer');
    updateDate();
    renderBatches(mockData.batches);
    updateKpis(mockData.batches);
    renderAlerts(mockData.alerts);
    initMap();
    initModals();
    initRegistration();
    loadProducerData();
});

async function updateProducerLiveSensor() {
    try {
        const res = await fetch(FoodChainAPI.resolveApiUrl('/data'));
        if (res.ok) {
            const data = await res.json();
            if (data && Array.isArray(data.data) && data.data.length > 0) {
                const latest = data.data[data.data.length - 1];
                const tempEl = document.getElementById('producer-temp');
                const humEl = document.getElementById('producer-humidity');
                const devEl = document.getElementById('producer-device');
                if (tempEl && latest.temperature != null) tempEl.textContent = latest.temperature.toFixed(1) + '°C';
                if (humEl && latest.humidity != null) humEl.textContent = latest.humidity.toFixed(1) + '%';
                if (devEl) devEl.textContent = 'ESP32 Live';
            }
        }
    } catch(e) {}
}
setInterval(updateProducerLiveSensor, 5000);
