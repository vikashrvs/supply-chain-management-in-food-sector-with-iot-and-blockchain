(function () {
    const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[char]));

    const copy = {
        admin: {
            map: ['Supply Chain Map', 'Connected organizations, routes, and tracked locations.'],
            sensors: ['IoT Sensor Monitor', 'Current device health and recent telemetry.'],
            blockchain: ['Blockchain Ledger', 'Verified transaction and custody records.'],
            products: ['Products & Batches', 'Search and inspect registered batches.'],
            alerts: ['Alerts & Notifications', 'Review operational alerts by severity.'],
            analytics: ['Analytics & Reports', 'System-wide performance and activity summary.'],
            reports: ['System Settings', 'Administrative controls and reporting preferences.']
        },
        producer: {
            products: ['Products & Batches', 'Create, search, and review your production batches.'],
            sensors: ['IoT Readings', 'Temperature, humidity, and device readings for your batches.'],
            analytics: ['Batch History', 'Review production history and batch outcomes.']
        },
        distributor: {
            products: ['Search / Scan Batch', 'Find a batch before recording a custody action.'],
            network: ['Record Transfer', 'Record a transfer and review destination details.'],
            alerts: ['Flag Anomaly', 'Review and report shipment or condition anomalies.'],
            analytics: ['Transfer History', 'Review completed and pending custody transfers.']
        }
    };

    function role() {
        const title = document.title.toLowerCase();
        return title.includes('admin') ? 'admin' : title.includes('producer') ? 'producer' : 'distributor';
    }

    function rows(kind) {
        const table = document.querySelector('table');
        if (table) {
            return [...table.querySelectorAll('tbody tr')].slice(0, 8).map((row) => ({
                title: row.cells[0]?.textContent.trim() || 'Record',
                detail: [...row.cells].slice(1).map((cell) => cell.textContent.trim()).filter(Boolean).join(' · ')
            }));
        }
        return [];
    }

    function openDetail(item) {
        const kind = item.dataset.detail;
        const currentRole = role();
        const definition = copy[currentRole][kind] || [item.querySelector('span')?.textContent || 'Details', 'Role-specific information from this dashboard.'];
        let records = rows(kind);
        const overlay = document.createElement('section');
        overlay.className = 'fc-detail-overlay';
        overlay.setAttribute('role', 'dialog');
        overlay.setAttribute('aria-modal', 'true');
        overlay.innerHTML = `<div class="fc-detail-window"><header class="fc-detail-header"><div><h2>${escapeHtml(definition[0])}</h2><p>${escapeHtml(definition[1])}</p></div><button class="fc-detail-close" aria-label="Close details">&times;</button></header><div class="fc-detail-body"><div class="fc-detail-grid"><div class="fc-detail-stat"><span>Visible records</span><strong>${records.length || '—'}</strong></div><div class="fc-detail-stat"><span>Role</span><strong>${escapeHtml(currentRole)}</strong></div><div class="fc-detail-stat"><span>State</span><strong>Live view</strong></div></div><div class="fc-detail-toolbar"><input type="search" placeholder="Search this detail window..." aria-label="Search details"><select aria-label="Filter detail records"><option value="">All statuses</option><option>Active</option><option>In Transit</option><option>Delivered</option><option>Critical</option></select></div><div class="fc-detail-list"></div></div></div>`;
        document.body.appendChild(overlay);
        const list = overlay.querySelector('.fc-detail-list');
        const render = (query = '', filter = '') => {
            const visible = records.filter((record) => `${record.title} ${record.detail}`.toLowerCase().includes(`${query} ${filter}`.trim().toLowerCase()));
            list.innerHTML = visible.length ? visible.map((record) => `<div class="fc-detail-row" tabindex="0"><strong>${escapeHtml(record.title)}</strong><small>${escapeHtml(record.detail)}</small></div>`).join('') : '<div class="fc-detail-empty">No matching persisted records in this view.</div>';
        };
        render();
        overlay.querySelector('input').addEventListener('input', (event) => render(event.target.value, overlay.querySelector('select').value));
        overlay.querySelector('select').addEventListener('change', (event) => render(overlay.querySelector('input').value, event.target.value));
        const close = () => { overlay.remove(); item.focus(); };
        overlay.querySelector('.fc-detail-close').addEventListener('click', close);
        overlay.addEventListener('click', (event) => { if (event.target === overlay) close(); });
        overlay.addEventListener('keydown', (event) => { if (event.key === 'Escape') close(); });
        requestAnimationFrame(() => { overlay.classList.add('open'); overlay.querySelector('.fc-detail-close').focus(); });
    }

    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('.sidebar .nav-item[data-detail]').forEach((item) => {
            item.addEventListener('click', (event) => {
                if (role() === 'admin' && item.dataset.detail === 'blockchain') return;
                event.preventDefault();
                document.querySelectorAll('.sidebar .nav-item').forEach((nav) => nav.classList.remove('active'));
                item.classList.add('active');
                openDetail(item);
            });
        });
    });
}());
