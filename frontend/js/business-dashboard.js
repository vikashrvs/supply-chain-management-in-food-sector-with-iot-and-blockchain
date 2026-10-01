let charts = {};
let map;
let latestOverview = null;
let latestAlerts = [];
const CACHE_VERSION = 1;

function getCacheKey() {
    const username = FoodChainAPI.getUsername().replace(/[^a-z0-9_-]/gi, '_');
    return `fc_business_dashboard_snapshot_v${CACHE_VERSION}_${username}`;
}

function saveSnapshot(overview, alerts) {
    localStorage.setItem(getCacheKey(), JSON.stringify({
        saved_at: new Date().toISOString(),
        overview,
        alerts
    }));
}

function readSnapshot() {
    try {
        const snapshot = JSON.parse(localStorage.getItem(getCacheKey()) || 'null');
        if (!snapshot || !snapshot.overview || !snapshot.saved_at) return null;
        return snapshot;
    } catch {
        return null;
    }
}

function getOfflineFallback() {
    return {
        data_source: 'offline_baseline',
        kpis: {
            total_batches: 6,
            product_types: 6,
            active_shipments: 4,
            in_transit: 3,
            delivered: 2,
            delayed: 1,
            efficiency_percent: 92,
            active_iot_devices: 6
        },
        flow: { producer: 1, processing: null, in_transit: 3, distributor: null, delivered: 2 },
        product_distribution: [
            { label: 'Product FC-001', value: 1 },
            { label: 'Product FC-002', value: 1 },
            { label: 'Product FC-003', value: 1 },
            { label: 'Product FC-004', value: 1 },
            { label: 'Product FC-005', value: 1 },
            { label: 'Product FC-006', value: 1 }
        ],
        batches: [],
        locations: [
            { batch_id: 'FC-001', latitude: 12.971599, longitude: 77.594566, status: 'transport', location_name: 'Bengaluru · ESP32-01' },
            { batch_id: 'FC-002', latitude: 12.5216, longitude: 76.8958, status: 'consumer', location_name: 'Mysuru · ESP32-02' },
            { batch_id: 'FC-003', latitude: 13.008891, longitude: 77.581039, status: 'transport', location_name: 'Bengaluru North · ESP32-03' },
            { batch_id: 'BATCH_003', latitude: 13.058901, longitude: 77.702083, status: 'consumer', location_name: 'Whitefield · ESP32-04' },
            { batch_id: 'BATCH_002', latitude: 13.052442, longitude: 77.659144, status: 'retailer', location_name: 'Bengaluru East · ESP32-05' },
            { batch_id: 'BATCH_001', latitude: 13.007243, longitude: 77.611357, status: 'transport', location_name: 'Bengaluru · ESP32-06' }
        ],
        activities: [],
        sensor_readings: { temperature: 27.7, humidity: 71.5, sensor_id: 'ESP32-01' }
    };
}

function formatSnapshotTime(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? 'unknown time' : date.toLocaleString();
}

const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[char]));

function showValue(value, suffix = '') {
    return value === null || value === undefined ? 'No data' : `${value}${suffix}`;
}

function renderKpis(kpis) {
    document.querySelectorAll('[data-kpi]').forEach((element) => {
        const key = element.dataset.kpi;
        element.textContent = showValue(kpis[key], key === 'efficiency_percent' && kpis[key] !== null ? '%' : '');
    });
}

function renderFlow(flow, batches) {
    const stages = [
        ['producer', 'Producer'],
        ['processing', 'Processing'],
        ['in_transit', 'In Transit'],
        ['distributor', 'Distributor'],
        ['delivered', 'Delivered']
    ];
    const container = document.getElementById('flow-grid');
    container.innerHTML = stages.map(([key, label]) => `
        <button class="flow-stage" data-stage="${key}">
            <strong>${showValue(flow[key])}</strong><span>${label}</span>
        </button>
    `).join('');
    container.querySelectorAll('[data-stage]').forEach((button) => {
        button.addEventListener('click', () => {
            const stage = button.dataset.stage;
            const matching = batches.filter((batch) => {
                if (stage === 'producer') return batch.status === 'created';
                if (stage === 'in_transit') return batch.status === 'in_transit';
                if (stage === 'delivered') return batch.status === 'transferred';
                return false;
            });
            renderRecords('activities', matching.map((batch) => ({
                title: `${batch.batch_id} — ${batch.product_name}`,
                detail: `${batch.status} | ${batch.origin || 'Origin unavailable'} → ${batch.destination || 'Destination unavailable'}`,
                time: batch.updated_at || batch.created_at
            })), matching.length ? `Stage: ${stage.replace('_', ' ')}` : 'No persisted batches for this stage');
            document.getElementById('activities').scrollIntoView({ behavior: 'smooth', block: 'center' });
        });
    });
}

function renderRecords(id, records, emptyText = 'No persisted records available.') {
    const container = document.getElementById(id);
    if (!records.length) {
        container.innerHTML = `<p class="empty-state">${escapeHtml(emptyText)}</p>`;
        return;
    }
    container.innerHTML = records.map((record) => `
        <div class="record"><strong>${escapeHtml(record.title)}</strong>
        <small>${escapeHtml(record.detail || '')}${record.time ? ` · ${escapeHtml(record.time)}` : ''}</small></div>
    `).join('');
}

function renderCharts(overview) {
    Object.values(charts).forEach((chart) => chart.destroy());
    charts = {};
    const productCanvas = document.getElementById('product-chart');
    const productEmpty = document.getElementById('product-empty');
    if (overview.product_distribution.length && typeof Chart !== 'undefined') {
        charts.product = new Chart(productCanvas, {
            type: 'doughnut',
            data: {
                labels: overview.product_distribution.map((row) => row.label),
                datasets: [{ data: overview.product_distribution.map((row) => row.value),
                    backgroundColor: ['#d4af37', '#22c55e', '#3b82f6', '#f59e0b', '#a855f7'] }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
        productEmpty.hidden = true;
    }
    if (typeof Chart !== 'undefined') {
        charts.shipments = new Chart(document.getElementById('shipment-chart'), {
            type: 'bar',
            data: {
                labels: ['Active', 'In Transit', 'Delivered', 'Delayed'],
                datasets: [{ data: [overview.kpis.active_shipments, overview.kpis.in_transit,
                    overview.kpis.delivered, overview.kpis.delayed],
                    backgroundColor: ['#3b82f6', '#f59e0b', '#22c55e', '#ef4444'] }]
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
        });
    }
}

function renderMap(locations) {
    const mapElement = document.getElementById('business-map');
    const emptyElement = document.getElementById('map-empty');
    if (map) {
        map.remove();
        map = null;
    }
    if (!locations.length) {
        mapElement.innerHTML = '';
        emptyElement.hidden = false;
        return;
    }
    const latitudes = locations.map((location) => Number(location.latitude));
    const longitudes = locations.map((location) => Number(location.longitude));
    const minLat = Math.min(...latitudes);
    const maxLat = Math.max(...latitudes);
    const minLng = Math.min(...longitudes);
    const maxLng = Math.max(...longitudes);
    const latSpan = Math.max(maxLat - minLat, 0.02);
    const lngSpan = Math.max(maxLng - minLng, 0.02);
    mapElement.innerHTML = `
        <div class="shipment-map-grid" aria-label="Persisted shipment locations">
            <span class="map-label map-label-north">N</span><span class="map-label map-label-east">E</span>
            <span class="map-label map-label-south">S</span><span class="map-label map-label-west">W</span>
            <div class="map-route"></div>
            ${locations.map((location, index) => {
                const left = 12 + ((Number(location.longitude) - minLng) / lngSpan) * 76;
                const top = 78 - ((Number(location.latitude) - minLat) / latSpan) * 62;
                return `<button class="shipment-marker" style="left:${left}%;top:${top}%" title="${escapeHtml(location.batch_id)} · ${escapeHtml(location.status || 'Unknown')}">${index + 1}</button>`;
            }).join('')}
        </div>`;
    mapElement.querySelectorAll('.shipment-marker').forEach((marker, index) => {
        const location = locations[index];
        marker.addEventListener('click', () => {
            marker.textContent = marker.textContent === '×' ? String(index + 1) : '×';
            marker.setAttribute('aria-label', `${location.batch_id}: ${location.location_name || 'Location unavailable'}, ${location.status || 'Status unavailable'}`);
        });
    });
    emptyElement.hidden = true;
}

function renderOverview(overview, alerts) {
    latestOverview = overview;
    latestAlerts = alerts;
    renderKpis(overview.kpis);
    const sensorReadout = document.getElementById('sensor-readout');
    if (sensorReadout) {
        const sensor = overview.sensor_readings;
        sensorReadout.textContent = sensor
            ? `Temperature ${showValue(sensor.temperature, '°C')} · Humidity ${showValue(sensor.humidity, '%')}`
            : 'Temperature -- · Humidity --';
    }
    renderFlow(overview.flow, overview.batches);
    renderCharts(overview);
    renderMap(overview.locations);
    renderRecords('activities', overview.activities.map((activity) => ({
        title: `${activity.event_type} ${activity.batch_id ? `— ${activity.batch_id}` : ''}`,
        detail: `${activity.result || ''} ${activity.detail || ''}`,
        time: activity.timestamp
    })));
    renderRecords('alerts-list', alerts.map((alert) => ({
        title: alert.message || alert.title || alert.alert_source || 'Alert',
        detail: `${alert.severity || 'unclassified'} · active`,
        time: alert.timestamp
    })), 'No active persisted alerts.');
    document.getElementById('sidebar-alert-count').textContent = alerts.length;
}

function popoverStat(label, value) {
    return `<div class="popover-stat"><span>${escapeHtml(label)}</span><strong>${escapeHtml(showValue(value))}</strong></div>`;
}

function popoverList(records, emptyText) {
    if (!records.length) return `<div class="popover-empty">${escapeHtml(emptyText)}</div>`;
    return `<div class="popover-list">${records.slice(0, 5).map((record) => `
        <div class="popover-list-item"><strong>${escapeHtml(record.title)}</strong><small>${escapeHtml(record.detail || '')}</small></div>
    `).join('')}</div>`;
}

function openSidebarDetail(kind, trigger) {
    const popover = document.getElementById('sidebar-detail-popover');
    const title = document.getElementById('popover-title');
    const content = document.getElementById('popover-content');
    const overview = latestOverview;
    const kpis = overview?.kpis || {};
    const flow = overview?.flow || {};
    const batches = overview?.batches || [];
    let heading = trigger.querySelector('span')?.textContent || 'Details';
    let body = '<div class="popover-empty">Live data is still loading.</div>';

    if (overview) {
        if (kind === 'overview') {
            body = `<div class="popover-summary">${popoverStat('Batches', kpis.total_batches)}${popoverStat('Active shipments', kpis.active_shipments)}${popoverStat('Delivered', kpis.delivered)}${popoverStat('Efficiency', showValue(kpis.efficiency_percent, kpis.efficiency_percent === null ? '' : '%'))}</div>
                ${popoverList(batches.map((batch) => ({ title: `${batch.batch_id} — ${batch.product_name}`, detail: `${batch.status} · ${batch.updated_at || batch.created_at || 'Time unavailable'}` })), 'No persisted batches available.')}`;
        } else if (kind === 'flow') {
            body = `<div class="popover-summary">${popoverStat('Producer', flow.producer)}${popoverStat('Processing', flow.processing)}${popoverStat('In transit', flow.in_transit)}${popoverStat('Delivered', flow.delivered)}</div>
                <div class="popover-empty">Select a flow stage in the dashboard to inspect its persisted batches.</div>`;
        } else if (kind === 'map') {
            body = `<div class="popover-summary">${popoverStat('GPS records', overview.locations.length)}${popoverStat('Mapped batches', new Set(overview.locations.map((row) => row.batch_id)).size)}</div>
                ${popoverList(overview.locations.map((row) => ({ title: row.batch_id, detail: `${row.location_name || 'Location unavailable'} · ${row.status || 'Status unavailable'}` })), 'No persisted GPS locations available.')}`;
        } else if (kind === 'alerts') {
            body = `<div class="popover-summary">${popoverStat('Active alerts', latestAlerts.length)}</div>
                ${popoverList(latestAlerts.map((alert) => ({ title: alert.message || alert.title || alert.alert_source || 'Alert', detail: `${alert.severity || 'Unclassified'} · ${alert.timestamp || 'Time unavailable'}` })), 'No active persisted alerts.')}`;
        } else if (kind === 'analytics') {
            body = `<div class="popover-summary">${popoverStat('Product types', kpis.product_types)}${popoverStat('IoT devices', kpis.active_iot_devices)}${popoverStat('Delayed', kpis.delayed)}${popoverStat('In transit', kpis.in_transit)}</div>
                <div class="popover-empty">Charts below use only persisted aggregate values. Historical trends are shown only when dated performance records exist.</div>`;
        } else if (kind === 'report') {
            body = `<div class="popover-empty">Prints the current factual dashboard view. No AI summary or invented values are added. Use the browser print dialog to save it as PDF.</div>`;
        }
    }

    title.textContent = heading;
    content.innerHTML = body;
    popover.classList.add('open');
    popover.setAttribute('aria-hidden', 'false');
    document.getElementById('popover-close').focus();
}

function closeSidebarDetail() {
    const popover = document.getElementById('sidebar-detail-popover');
    popover.classList.remove('open');
    popover.setAttribute('aria-hidden', 'true');
}

function initSidebarDetails() {
    document.querySelectorAll('.sidebar .nav-item[data-detail]').forEach((item) => {
        item.addEventListener('click', (event) => {
            event.preventDefault();
            document.querySelectorAll('.sidebar .nav-item[data-detail]').forEach((nav) => nav.classList.remove('active'));
            item.classList.add('active');
            openSidebarDetail(item.dataset.detail, item);
            const target = document.querySelector(item.getAttribute('href'));
            if (target && item.dataset.detail !== 'report') target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
    });
    document.getElementById('popover-close').addEventListener('click', closeSidebarDetail);
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') closeSidebarDetail();
    });
    document.addEventListener('click', (event) => {
        const popover = document.getElementById('sidebar-detail-popover');
        if (popover.classList.contains('open') && !popover.contains(event.target) && !event.target.closest('.sidebar .nav-item[data-detail]')) {
            closeSidebarDetail();
        }
    });
}

function initDashboardControls() {
    const profile = document.querySelector('.user-chip');
    const menu = document.getElementById('profile-menu');
    const logout = document.getElementById('profile-logout');
    if (profile && menu) {
        profile.setAttribute('tabindex', '0');
        const toggle = () => {
            menu.classList.toggle('open');
            menu.setAttribute('aria-hidden', menu.classList.contains('open') ? 'false' : 'true');
        };
        profile.addEventListener('click', toggle);
        profile.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                toggle();
            }
        });
        document.addEventListener('click', (event) => {
            if (!profile.contains(event.target) && !menu.contains(event.target)) {
                menu.classList.remove('open');
                menu.setAttribute('aria-hidden', 'true');
            }
        });
    }
    if (logout) logout.addEventListener('click', () => FoodChainAPI.logout());

    const search = document.getElementById('dashboard-search-input');
    if (search) {
        search.addEventListener('input', () => {
            const query = search.value.trim().toLowerCase();
            document.querySelectorAll('.record, .flow-stage').forEach((item) => {
                item.hidden = Boolean(query && !item.textContent.toLowerCase().includes(query));
            });
        });
    }
}

async function loadBusinessDashboard() {
    if (loadBusinessDashboard.inFlight) return;
    loadBusinessDashboard.inFlight = true;
    const status = document.getElementById('data-status') || document.getElementById('sidebar-system-status');
    const error = document.getElementById('data-error');
    try {
        const [overview, alertPayload] = await Promise.all([
            FoodChainAPI.fetchJson('/api/business/overview'),
            FoodChainAPI.fetchJson('/api/agent-alerts')
        ]);
        const alerts = Array.isArray(alertPayload) ? alertPayload : (alertPayload.alerts || []);
        renderOverview(overview, alerts);
        saveSnapshot(overview, alerts);
        status.textContent = 'System live';
        error.hidden = true;
    } catch (err) {
        const snapshot = readSnapshot();
        const offlineData = snapshot || { overview: getOfflineFallback(), alerts: [] };
        renderOverview(offlineData.overview, offlineData.alerts);
        status.textContent = snapshot ? 'System offline · last saved data' : 'System offline · baseline view';
        error.hidden = false;
        error.textContent = snapshot
            ? 'Backend is offline. Showing the last successfully loaded database snapshot.'
            : 'Backend is offline. Showing an offline baseline until live database data is available.';
    } finally {
        loadBusinessDashboard.inFlight = false;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    FoodChainAPI.initRoleGuard('business');
    initSidebarDetails();
    initDashboardControls();
    document.getElementById('print-report').addEventListener('click', () => window.print());
    loadBusinessDashboard();
    setInterval(loadBusinessDashboard, 5000);
});
