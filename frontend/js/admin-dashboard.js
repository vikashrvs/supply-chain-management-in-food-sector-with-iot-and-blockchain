// FoodChain Admin Dashboard Controller
const mockData = {};

let adminCharts = {};
let adminTransfers = [];
let allAdminTransfers = [];
let ledgerViewMode = 'single';
let sensorRecords = [];
let adminRefreshInFlight = false;

function updateCurrentDate() {
    const el = document.getElementById('current-date');
    if (!el) return;

    el.textContent = new Date().toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric'
    });
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
        body.innerHTML =
            '<tr><td colspan="6" style="text-align:center; padding:16px; color:var(--muted);">No live sensor readings are available yet.</td></tr>';
        return;
    }

    const value = (v) =>
        v === null || v === undefined || v === ''
            ? 'Not recorded'
            : safeEscape(v);

    const hash = (v) => {
        if (!v) return 'Not recorded';

        return `<code title="${safeEscape(v)}">${safeEscape(
            FoodChainAPI.formatHash(v)
        )}</code>`;
    };

    const numberValue = (v) =>
        v === null ||
        v === undefined ||
        v === '' ||
        Number.isNaN(Number(v))
            ? '--'
            : Number(v).toFixed(1);

    body.innerHTML = rows.map((row) => {

        const batchId =
            row.batch_id ||
            row.product_id ||
            row.batch ||
            'Not recorded';

        const device =
            row.device_id ||
            row.sensor_id ||
            'ESP32-01';

        const blockHash =
            row.block_hash ||
            row.sensor_block_hash;

        const temperature =
            `${numberValue(row.temperature)}°C / ${numberValue(row.humidity)}%`;

        const recorded =
            row.timestamp ||
            row.created_at ||
            row.recorded_at ||
            'Not recorded';

        const fabricTx =
            row.fabric_tx_id ||
            row.sensor_fabric_tx_id ||
            row.blockchain_tx_id;

        const status = fabricTx
            ? '<span class="glass-pill pill-purple"><i class="fa-solid fa-cube"></i> Hyperledger Fabric</span>'
            : '<span class="glass-pill pill-blue"><i class="fa-solid fa-shield-halved"></i> SHA-256</span>';

        return `
            <tr>
                <td><strong>${value(batchId)}</strong></td>
                <td>${hash(blockHash)}</td>
                <td>${value(device)}</td>
                <td>${temperature}</td>
                <td>${value(recorded)}</td>
                <td>${status}</td>
            </tr>
        `;
    }).join('');
}

function ledgerValue(value) {
    return value === null ||
        value === undefined ||
        value === ''
        ? 'Not recorded'
        : safeEscape(value);
}

function ledgerHash(value) {
    if (!value) return 'Not recorded';

    return `<code title="${safeEscape(value)}">${safeEscape(
        FoodChainAPI.formatHash(value)
    )}</code>`;
}

function ledgerTemp(value) {
    return value === null ||
        value === undefined ||
        value === ''
        ? 'Not recorded'
        : `${Number(value).toFixed(1)}°C`;
}

function ledgerHumidity(value) {
    return value === null ||
        value === undefined ||
        value === ''
        ? 'Not recorded'
        : `${Number(value).toFixed(1)}%`;
}

function ledgerStatusLabel(value) {
    return ledgerValue(
        String(value || '').replace(/_/g, ' ')
    );
}

function isActiveBatch(batch) {
    const status = String(batch.status || '').toLowerCase();

    return status === 'created' ||
        status === 'in_transit' ||
        status === 'transferred';
}

function normalizeLedgerTransfer(tx) {
    return {
        ...tx,
        transaction_id: tx.transaction_id || String(tx.id || ''),
        fabric_tx_id: tx.fabric_tx_id || tx.blockchain_tx_id,
        event_fabric_tx_id:
            tx.event_fabric_tx_id ||
            tx.blockchain_tx_id
    };
}

function ledgerFindBatch(batchId) {
    return ledgerBatches.find(
        (batch) =>
            String(batch.batch_id) === String(batchId)
    ) || null;
}

function ledgerBatchFacts(batch) {
    const iot = batch.latest_iot || {};

    return {
        temp: iot.temperature ?? batch.temperature,
        humidity: iot.humidity ?? batch.humidity,
        lastReading: iot.timestamp || null,
        device:
            iot.device_id ||
            iot.sensor_id ||
            batch.device_id ||
            null,
        events:
            ledgerRows.filter(
                (tx) =>
                    String(tx.batch_id) ===
                    String(batch.batch_id)
            ).length
    };
}

async function loadLedgerBatches() {
    const result =
        await FoodChainAPI.fetchJson(
            '/api/admin/batches?limit=200'
        );

    const all =
        Array.isArray(result?.batches)
            ? result.batches
            : [];

    const active =
        all.filter(isActiveBatch);

    ledgerBatches =
        active.length ? active : all;

    return ledgerBatches;
}

function populateActiveShipmentOptions() {
    const select =
        document.getElementById('ledger-product-id');

    if (!select) return;

    select.innerHTML =
        '<option value="">Select an active shipment</option>' +
        ledgerBatches.map((batch) =>
            `<option value="${safeEscape(batch.batch_id)}">${
                safeEscape(batch.batch_id)
            }${
                batch.product_name
                    ? ` — ${safeEscape(batch.product_name)}`
                    : ''
            }</option>`
        ).join('');
}

function ledgerEventRow(tx) {
    const sensorVerified =
        String(
            tx.sensor_verification_status || ''
        ).toLowerCase() === 'verified';

    return `
        <tr>
            <td>
                <code>
                    ${ledgerValue(tx.transaction_no || tx.id)}
                </code>
            </td>

            <td>
                ${ledgerHash(
                    tx.event_fabric_tx_id ||
                    tx.blockchain_tx_id
                )}
            </td>

            <td>
                <strong>
                    ${ledgerValue(tx.batch_id)}
                </strong>
            </td>

            <td>
                ${ledgerValue(tx.event_type)}
            </td>

            <td>
                ${ledgerValue(tx.created_at)}
            </td>

            <td>
                ${ledgerHash(
                    tx.sensor_block_hash ||
                    tx.block_hash
                )}
            </td>

            <td>
                ${ledgerHash(
                    tx.sensor_field_hash ||
                    tx.field_hash
                )}
            </td>

            <td>
                ${ledgerHash(
                    tx.sensor_fabric_tx_id ||
                    tx.fabric_tx_id
                )}
            </td>

            <td>
                <span class="${
                    sensorVerified
                        ? 'status-ok'
                        : 'status-warn'
                }">
                    ${ledgerValue(
                        tx.sensor_verification_status
                    )}
                </span>
            </td>

            <td>
                ${ledgerValue(
                    tx.event_verification_status
                )}
                ·
                ${ledgerValue(
                    tx.event_ledger_status
                )}
            </td>
        </tr>
    `;
}

function renderLedgerSummary() {
    const box =
        document.getElementById('ledger-summary');

    if (!box) return;

    if (ledgerSelection.mode === 'single') {

        const batch =
            ledgerFindBatch(
                ledgerSelection.batchId
            ) || {
                batch_id:
                    ledgerSelection.batchId
            };

        const f =
            ledgerBatchFacts(batch);

        const route =
            (batch.origin || batch.destination)
                ? `${batch.origin || '—'} → ${
                    batch.destination || '—'
                }`
                : null;

        const items = [
            ['Product', ledgerValue(batch.product_name)],
            ['Type', ledgerValue(batch.product_type)],
            ['Route', ledgerValue(route)],
            ['Quantity', ledgerValue(batch.quantity)],
            ['Harvest date', ledgerValue(batch.harvest_date)],
            ['Created', ledgerValue(batch.created_at)],
            ['Latest temperature', ledgerTemp(f.temp)],
            ['Latest humidity', ledgerHumidity(f.humidity)],
            ['Last reading', ledgerValue(f.lastReading)],
            ['Sensor device', ledgerValue(f.device)],
            ['Sensor proof', ledgerValue(batch.sensor_verification_status)],
            ['Batch ledger TX', ledgerHash(batch.blockchain_tx_id)],
            ['Ledger events', String(f.events)]
        ];

        box.innerHTML = `
            <div class="ledger-detail-card">

                <div class="ledger-detail-head">
                    <strong>
                        ${ledgerValue(batch.batch_id)}
                    </strong>

                    <span class="ledger-badge">
                        ${ledgerStatusLabel(batch.status)}
                    </span>
                </div>

                <div class="ledger-detail-grid">

                    ${items.map(
                        ([label, value]) =>
                            `<div>
                                <span>${label}</span>
                                <b>${value}</b>
                            </div>`
                    ).join('')}

                </div>

            </div>
        `;

        return;
    }

    if (!ledgerBatches.length) {
        box.innerHTML =
            '<div class="ledger-detail-card">No active shipments found.</div>';
        return;
    }

    box.innerHTML = `
        <div class="ledger-multi-grid">

            ${ledgerBatches.map((batch) => {

                const f =
                    ledgerBatchFacts(batch);

                return `
                    <div class="ledger-mini-card">

                        <div class="ledger-detail-head">
                            <strong>
                                ${ledgerValue(batch.batch_id)}
                            </strong>

                            <span class="ledger-badge">
                                ${ledgerStatusLabel(batch.status)}
                            </span>
                        </div>

                        <p>
                            ${ledgerValue(batch.product_name)}
                            ·
                            ${ledgerValue(batch.product_type)}
                        </p>

                        <p>
                            ${ledgerValue(batch.origin)}
                            →
                            ${ledgerValue(batch.destination)}
                            · Qty
                            ${ledgerValue(batch.quantity)}
                        </p>

                        <p>
                            ${ledgerTemp(f.temp)}
                            ·
                            ${ledgerHumidity(f.humidity)}
                            ·
                            ${ledgerValue(f.lastReading)}
                        </p>

                        <p>
                            ${f.events}
                            ledger event(s)
                            · Sensor proof:
                            ${ledgerValue(
                                batch.sensor_verification_status
                            )}
                        </p>

                    </div>
                `;
            }).join('')}

        </div>
    `;
}

function renderLedgerTable(query) {
    const body =
        document.getElementById(
            'ledger-detail-body'
        );

    if (!body) return;

    const q =
        String(query || '')
            .trim()
            .toLowerCase();

    const matches = (tx) =>
        !q ||
        [
            tx.transaction_id,
            tx.id,
            tx.event_fabric_tx_id,
            tx.blockchain_tx_id,
            tx.batch_id,
            tx.event_type,
            tx.sensor_block_hash,
            tx.sensor_field_hash,
            tx.sensor_fabric_tx_id
        ].some(
            (value) =>
                String(value ?? '')
                    .toLowerCase()
                    .includes(q)
        );

    const emptyRow = (text) =>
        `<tr class="ledger-empty-row">
            <td colspan="10">${text}</td>
        </tr>`;

    let html = '';

    if (ledgerSelection.mode === 'multiple') {

        ledgerBatches.forEach((batch) => {

            const shown =
                ledgerRows
                    .filter(
                        (tx) =>
                            String(tx.batch_id) ===
                            String(batch.batch_id)
                    )
                    .filter(matches);

            if (q && !shown.length) return;

            html += `
                <tr class="ledger-group-row">
                    <td colspan="10">
                        <i class="fa-solid fa-boxes-stacked"></i>
                        Product / batch:
                        <strong>
                            ${ledgerValue(batch.batch_id)}
                        </strong>
                        —
                        ${ledgerValue(batch.product_name)}
                        ·
                        ${ledgerStatusLabel(batch.status)}
                    </td>
                </tr>
            `;

            html += shown.length
                ? shown.map(ledgerEventRow).join('')
                : emptyRow(
                    'No ledger events recorded for this batch yet.'
                );
        });

    } else {

        const shown =
            ledgerRows.filter(matches);

        html =
            shown.map(ledgerEventRow).join('');
    }

    body.innerHTML =
        html ||
        emptyRow(
            'No supply-chain events match.'
        );
}

function renderLedgerSensorProofs() {
    const section =
        document.getElementById(
            'ledger-sensor-section'
        );

    const body =
        document.getElementById(
            'ledger-sensor-body'
        );

    if (!section || !body) return;

    if (ledgerSelection.mode !== 'single') {
        section.hidden = true;
        return;
    }

    section.hidden = false;

    body.innerHTML =
        ledgerSensorRows.length
            ? ledgerSensorRows.map((row) => {

                const verified =
                    Boolean(
                        row.block_hash &&
                        row.field_hash
                    );

                return `
                    <tr>

                        <td>
                            ${ledgerValue(row.timestamp)}
                        </td>

                        <td>
                            ${ledgerValue(
                                row.device_id ||
                                row.sensor_id
                            )}
                        </td>

                        <td>
                            ${ledgerTemp(
                                row.temperature
                            )}
                        </td>

                        <td>
                            ${ledgerHumidity(
                                row.humidity
                            )}
                        </td>

                        <td>
                            ${ledgerHash(
                                row.block_hash
                            )}
                        </td>

                        <td>
                            ${ledgerHash(
                                row.field_hash
                            )}
                        </td>

                        <td>
                            ${ledgerHash(
                                row.fabric_tx_id
                            )}
                        </td>

                        <td>
                            <span class="${
                                verified
                                    ? 'status-ok'
                                    : 'status-warn'
                            }">
                                ${
                                    verified
                                        ? 'Verified'
                                        : 'Not recorded'
                                }
                            </span>
                        </td>

                    </tr>
                `;

            }).join('')
            : `
                <tr class="ledger-empty-row">
                    <td colspan="8">
                        No sensor readings recorded
                        for this batch yet.
                    </td>
                </tr>
            `;
}

function renderLedgerModal() {

    const title =
        document.getElementById(
            'ledger-modal-title'
        );

    if (title) {
        title.textContent =
            ledgerSelection.mode === 'single'
                ? `Blockchain Ledger · ${ledgerSelection.batchId}`
                : `Blockchain Ledger · All active products (${ledgerBatches.length})`;
    }

    const search =
        document.getElementById(
            'ledger-detail-search'
        );

    renderLedgerSummary();

    renderLedgerTable(
        search ? search.value : ''
    );

    renderLedgerSensorProofs();
}

function filterLedgerDetails(query) {
    renderLedgerTable(query);
}

async function openLedgerChooser() {

    openModal('ledger-choice');

    const form =
        document.getElementById(
            'ledger-single-form'
        );

    if (form) form.hidden = true;

    const message =
        document.getElementById(
            'ledger-choice-message'
        );

    if (message) message.textContent = '';

    try {

        await loadLedgerBatches();

    } catch (error) {

        console.error(
            'Unable to load active shipments:',
            error
        );

        if (message) {
            message.textContent =
                'Active shipments could not be loaded.';
        }
    }

    populateActiveShipmentOptions();
}

async function loadExpandedLedger(
    batchId,
    options = {}
) {

    const silent =
        Boolean(options.silent);

    ledgerSelection = {
        mode: batchId
            ? 'single'
            : 'multiple',
        batchId: batchId || ''
    };

    ledgerViewMode =
        ledgerSelection.mode;

    try {

        const query =
            batchId
                ? `?batch_id=${encodeURIComponent(batchId)}&limit=200`
                : '?limit=200';

        const [
            ,
            transfersRes,
            sensorRes
        ] = await Promise.all([

            loadLedgerBatches()
                .catch(() => ledgerBatches),

            FoodChainAPI.fetchJson(
                `/api/admin/transfers${query}`
            ),

            batchId
                ? FoodChainAPI.fetchJson(
                    `/data?product_id=${encodeURIComponent(batchId)}`
                ).catch(() => null)
                : Promise.resolve(null)

        ]);

        ledgerRows =
            (transfersRes?.transfers || [])
                .map(normalizeLedgerTransfer);

        ledgerSensorRows =
            Array.isArray(sensorRes?.data)
                ? sensorRes.data.slice(0, 10)
                : [];

        if (!silent) {

            const search =
                document.getElementById(
                    'ledger-detail-search'
                );

            if (search) search.value = '';
        }

        renderLedgerModal();

        if (!silent) {
            openModal('ledger');
        }

    } catch (error) {

        console.error(
            'Unable to load ledger records:',
            error
        );

        if (!silent) {

            const message =
                document.getElementById(
                    'ledger-choice-message'
                );

            if (message) {
                message.textContent =
                    'Ledger transactions could not be loaded.';
            }
        }
    }
}

function populateAlerts(list) {

    const container =
        document.getElementById(
            'alerts-list'
        );

    if (!container) return;

    const previousScroll =
        container.scrollTop;

    if (!Array.isArray(list) || list.length === 0) {

        container.innerHTML = `
            <div class="alerts-empty">

                <i
                    class="fa-solid fa-circle-check"
                    aria-hidden="true">
                </i>

                <strong>
                    All systems clear
                </strong>

                <small>
                    No active alerts from the latest
                    sensor and service checks.
                </small>

            </div>
        `;

        return;
    }

    container.innerHTML =
        list.map((alert) => `
            <div class="alert-item ${
                safeEscape(alert.kind || 'info')
            }">

                <span>
                    ${safeEscape(
                        alert.title
                    )}
                </span>

                <small>
                    ${safeEscape(
                        alert.meta
                    )}
                </small>

            </div>
        `).join('');

    container.scrollTop =
        previousScroll;
}

function initMap() {

    const mapEl =
        document.getElementById('supply-chain-map');

    if (!mapEl) {
        return;
    }

    if (typeof initializeFoodChainMap !== 'function') {
        console.error(
            'FoodChain Map service is not loaded.'
        );
        return;
    }

    initializeFoodChainMap(
        'supply-chain-map',
        {
            latitude: 12.971599,
            longitude: 77.594566,
            zoom: 12
        }
    );
}

function initSensorChart() {

    const canvas =
        document.getElementById(
            'sensor-history-chart'
        );

    if (
        !canvas ||
        typeof Chart === 'undefined'
    ) return;

    const ctx =
        canvas.getContext('2d');

    adminCharts.sensorHistory =
        new Chart(ctx, {

            type: 'line',

            data: {

                labels: [
                    '13:20',
                    '13:22',
                    '13:24',
                    '13:26',
                    '13:28',
                    '13:30',
                    'Now'
                ],

                datasets: [{

                    label:
                        'ESP32 Live Temp (°C)',

                    data: [
                        27.2,
                        27.4,
                        27.5,
                        27.6,
                        27.7,
                        27.7,
                        27.8
                    ],

                    borderColor:
                        '#2dd4bf',

                    backgroundColor:
                        'rgba(45, 212, 191, 0.15)',

                    fill: true,

                    tension: 0.35,

                    borderWidth: 2
                }]
            },

            options: {

                responsive: true,

                maintainAspectRatio: false,

                plugins: {
                    legend: {
                        display: false
                    }
                },

                scales: {

                    x: {
                        display: false
                    },

                    y: {
                        display: false,
                        min: 25,
                        max: 30
                    }
                }
            }
        });
}// ============================================================
// REAL HYPERLEDGER FABRIC BLOCK EXPLORER
// ============================================================

function fabricChainEscape(value) {
    return safeEscape(
        value === null ||
        value === undefined ||
        value === ''
            ? 'Not recorded'
            : value
    );
}

function fabricChainHash(value) {
    if (!value) return 'Not recorded';

    return `<code title="${fabricChainEscape(value)}">${
        fabricChainEscape(FoodChainAPI.formatHash(value))
    }</code>`;
}

function renderFabricChain(blocks) {
    const list =
        document.getElementById(
            'fabric-chain-list'
        );

    if (!list) return;

    if (
        !Array.isArray(blocks) ||
        blocks.length === 0
    ) {
        list.innerHTML =
            '<div class="fabric-chain-empty">No committed Fabric blocks were returned.</div>';
        return;
    }

    list.innerHTML = blocks.map(
        (block, index) => {

            // REAL API RESPONSE:
            // blockNumber
            // previousHash
            // dataHash
            // transactionCount

            const blockNumber =
                block.blockNumber ??
                block.block_number;

            const previousHash =
                block.previousHash ??
                block.previous_block_hash;

            const dataHash =
                block.dataHash ??
                block.data_hash;

            const transactionCount =
                block.transactionCount ??
                block.transaction_count;

            return `
                <article class="fabric-chain-block ${
                    index === 0
                        ? 'latest'
                        : ''
                }">

                    <div class="fabric-chain-block-head">

                        <div>
                            <span class="fabric-chain-kicker">
                                BLOCK
                            </span>

                            <strong>
                                #${fabricChainEscape(
                                    blockNumber
                                )}
                            </strong>
                        </div>

                        <span class="fabric-chain-live">
                            REAL FABRIC
                        </span>

                    </div>

                    <div class="fabric-chain-link-label">
                        Previous block hash
                    </div>

                    <div class="fabric-chain-link-value">
                        ${fabricChainHash(
                            previousHash
                        )}
                    </div>

                    <div class="fabric-chain-grid">

                        <div>
                            <span>Block Number</span>
                            <b>
                                ${fabricChainEscape(
                                    blockNumber
                                )}
                            </b>
                        </div>

                        <div>
                            <span>Previous Hash</span>
                            <b>
                                ${fabricChainHash(
                                    previousHash
                                )}
                            </b>
                        </div>

                        <div>
                            <span>Data Hash</span>
                            <b>
                                ${fabricChainHash(
                                    dataHash
                                )}
                            </b>
                        </div>

                        <div>
                            <span>Transactions</span>
                            <b>
                                ${fabricChainEscape(
                                    transactionCount
                                )}
                            </b>
                        </div>

                        <div>
                            <span>Channel</span>
                            <b>
                                foodchainchannel
                            </b>
                        </div>

                        <div>
                            <span>Chaincode</span>
                            <b>
                                foodchain
                            </b>
                        </div>

                        <div>
                            <span>Source</span>
                            <b>
                                Fabric Peer / QSCC
                            </b>
                        </div>

                        <div>
                            <span>Merkle Root</span>
                            <b>
                                Not a Fabric BlockHeader field
                            </b>
                        </div>

                        <div>
                            <span>Proof-of-Work Nonce</span>
                            <b>
                                Not used by Fabric
                            </b>
                        </div>

                    </div>

                    <div class="fabric-chain-transactions">

                        <div class="fabric-chain-section-title">
                            Block contents
                        </div>

                        <div class="fabric-tx-empty">

                            ${
                                Number(
                                    transactionCount
                                ) === 1

                                ? '1 transaction envelope is recorded in this block.'

                                : `${fabricChainEscape(
                                    transactionCount
                                )} transaction envelope(s) are recorded in this block.`
                            }

                            <br>

                            <small>
                                Transaction IDs are not exposed
                                by the current block-summary endpoint.
                            </small>

                        </div>

                    </div>

                </article>
            `;
        }
    ).join(`
        <div class="fabric-chain-arrow">
            <i class="fa-solid fa-arrow-down"></i>
            previous hash links to the next older block
        </div>
    `);
}


async function openFabricChainExplorer() {

    const modal =
        document.getElementById(
            'modal-fabric-chain'
        );

    const status =
        document.getElementById(
            'fabric-chain-status'
        );

    const list =
        document.getElementById(
            'fabric-chain-list'
        );

    if (
        !modal ||
        !status ||
        !list
    ) {
        return;
    }

    openModal('fabric-chain');

    status.textContent =
        'Reading real blocks from Hyperledger Fabric…';

    status.className =
        'fabric-chain-status loading';

    list.innerHTML = '';

    try {

        const baseUrl =
            FoodChainAPI.getApiBaseUrl();

        const response =
            await fetch(
                `${baseUrl}/api/blockchain/blocks?limit=12`
            );

        const payload =
            await response.json();

        if (!response.ok) {

            throw new Error(
                payload.detail ||
                'Fabric block explorer unavailable'
            );
        }

        if (!payload.success) {

            throw new Error(
                payload.error ||
                'Fabric block explorer returned an error'
            );
        }

        const blocks =
            Array.isArray(payload.blocks)
                ? payload.blocks
                : [];

        renderFabricChain(blocks);

        const height =
            payload.height ?? '0';

        const currentBlock =
            payload.currentBlock ?? 'N/A';

        status.textContent =
            `Channel: ${
                payload.channel ||
                'foodchainchannel'
            } · ` +

            `Chaincode: ${
                payload.chaincode ||
                'foodchain'
            } · ` +

            `Height: ${height} · ` +

            `Current block: #${currentBlock} · ` +

            `Showing ${
                blocks.length
            } real committed block(s)`;

        status.className =
            'fabric-chain-status online';

    } catch (error) {

        console.error(
            'Fabric block explorer error:',
            error
        );

        status.textContent =
            `Fabric explorer unavailable: ${
                error.message
            }`;

        status.className =
            'fabric-chain-status offline';

        list.innerHTML = `
            <div class="fabric-chain-empty">

                <strong>
                    Real Fabric blocks could not be loaded.
                </strong>

                <br>

                <small>
                    Make sure the Hyperledger Fabric
                    network is running and try again.
                </small>

            </div>
        `;
    }
}


// ============================================================
// MODALS
// ============================================================

function initModals() {

    const navItems =
        document.querySelectorAll(
            '.nav-item[data-modal]:not([data-detail])'
        );

    const overlay =
        document.getElementById(
            'modal-overlay'
        );

    const sidebarLedger =
        document.querySelector(
            '.sidebar .nav-item[data-detail="blockchain"]'
        );

    if (sidebarLedger) {

        sidebarLedger.addEventListener(
            'click',
            (event) => {

                event.preventDefault();

                document
                    .querySelectorAll(
                        '.sidebar .nav-item'
                    )
                    .forEach(
                        (nav) =>
                            nav.classList.remove(
                                'active'
                            )
                    );

                sidebarLedger.classList.add(
                    'active'
                );

                openLedgerChooser();
            }
        );
    }

    navItems.forEach((item) => {

        item.addEventListener(
            'click',
            function (e) {

                e.preventDefault();

                const modalId =
                    this.getAttribute(
                        'data-modal'
                    );

                if (
                    modalId &&
                    modalId !== 'dashboard'
                ) {

                    openModal(modalId);

                } else {

                    closeAllModals();
                }

                navItems.forEach(
                    (el) =>
                        el.classList.remove(
                            'active'
                        )
                );

                this.classList.add(
                    'active'
                );
            }
        );
    });

    document
        .querySelectorAll('.modal-close')
        .forEach(
            (btn) =>
                btn.addEventListener(
                    'click',
                    closeAllModals
                )
        );

    const ledgerSearch =
        document.getElementById(
            'ledger-detail-search'
        );

    if (ledgerSearch) {

        ledgerSearch.addEventListener(
            'input',
            (event) =>
                filterLedgerDetails(
                    event.target.value
                )
        );
    }

    const ledgerKpi =
        document.getElementById(
            'kpi-blockchain-card'
        );

    if (ledgerKpi) {

        ledgerKpi.addEventListener(
            'click',
            openLedgerChooser
        );

        ledgerKpi.addEventListener(
            'keydown',
            (e) => {

                if (
                    e.key === 'Enter' ||
                    e.key === ' '
                ) {

                    e.preventDefault();

                    openLedgerChooser();
                }
            }
        );
    }

    const singleChoice =
        document.getElementById(
            'ledger-single-choice'
        );

    if (singleChoice) {

        singleChoice.addEventListener(
            'click',
            () => {

                const form =
                    document.getElementById(
                        'ledger-single-form'
                    );

                if (form)
                    form.hidden = false;

                const select =
                    document.getElementById(
                        'ledger-product-id'
                    );

                if (select)
                    select.focus();
            }
        );
    }

    const multipleChoice =
        document.getElementById(
            'ledger-multiple-choice'
        );

    if (multipleChoice) {

        multipleChoice.addEventListener(
            'click',
            () =>
                loadExpandedLedger('')
        );
    }

    const singleSubmit =
        document.getElementById(
            'ledger-single-submit'
        );

    if (singleSubmit) {

        singleSubmit.addEventListener(
            'click',
            () => {

                const input =
                    document.getElementById(
                        'ledger-product-id'
                    );

                const value =
                    input
                        ? input.value.trim()
                        : '';

                if (value) {

                    loadExpandedLedger(
                        value
                    );

                } else {

                    const message =
                        document.getElementById(
                            'ledger-choice-message'
                        );

                    if (message) {

                        message.textContent =
                            'Enter a product or batch ID first.';
                    }
                }
            }
        );
    }

    if (overlay) {

        overlay.addEventListener(
            'click',
            closeAllModals
        );
    }

    document.addEventListener(
        'keydown',
        (e) => {

            if (e.key === 'Escape') {
                closeAllModals();
            }
        }
    );
}


// ============================================================
// QR CODE
// ============================================================

function renderQrCode() {

    const canvas =
        document.getElementById(
            'qr-canvas'
        );

    if (
        !canvas ||
        typeof QRCode === 'undefined'
    ) {
        return;
    }

    const batchId =
        sensorRecords.find(
            (record) =>
                record.batch_id
        )?.batch_id;

    const title =
        document.getElementById(
            'qr-batch-title'
        );

    const description =
        document.getElementById(
            'qr-encoded-link'
        );

    if (!batchId) {

        if (title)
            title.textContent =
                'No live batch available';

        if (description)
            description.textContent =
                'A QR code can be generated when live batch data is available.';

        return;
    }

    if (title)
        title.textContent =
            `Batch: ${batchId}`;

    if (description)
        description.textContent =
            'Verification link for this live batch.';

    const verifyUrl =
        `${FoodChainAPI.getApiBaseUrl()}/verify/${batchId}`;

    QRCode.toCanvas(
        canvas,
        verifyUrl,
        {
            width: 180,
            margin: 1,
            color: {
                dark: '#0b1220',
                light: '#ffffff'
            }
        },
        function (error) {

            if (error)
                console.error(
                    'QR Render Error:',
                    error
                );
        }
    );
}


// ============================================================
// ANALYTICS
// ============================================================

function renderAnalyticsChart() {

    const canvas =
        document.getElementById(
            'admin-analytics-chart'
        );

    if (
        !canvas ||
        typeof Chart === 'undefined'
    ) {
        return;
    }

    if (adminCharts.analyticsChart)
        return;

    const ctx =
        canvas.getContext('2d');

    adminCharts.analyticsChart =
        new Chart(ctx, {

            type: 'bar',

            data: {

                labels: [
                    'Batch 001',
                    'Batch 002',
                    'Batch 003',
                    'Batch 004',
                    'Batch 005',
                    'Batch 006'
                ],

                datasets: [

                    {
                        label:
                            'Cold-Chain Compliance (%)',

                        data: [
                            100,
                            98,
                            99,
                            97,
                            100,
                            100
                        ],

                        backgroundColor:
                            'rgba(45, 212, 191, 0.7)',

                        borderRadius: 6
                    },

                    {
                        label:
                            'Blockchain Commit Time (ms)',

                        data: [
                            42,
                            38,
                            45,
                            50,
                            39,
                            41
                        ],

                        backgroundColor:
                            'rgba(78, 195, 255, 0.7)',

                        borderRadius: 6
                    }

                ]
            },

            options: {

                responsive: true,

                maintainAspectRatio: false,

                plugins: {

                    legend: {

                        labels: {
                            color: '#9bb2c8',

                            font: {
                                family: 'Inter',
                                size: 11
                            }
                        }
                    }
                },

                scales: {

                    x: {

                        ticks: {
                            color: '#9bb2c8',

                            font: {
                                size: 10
                            }
                        },

                        grid: {
                            display: false
                        }
                    },

                    y: {

                        ticks: {
                            color: '#9bb2c8',

                            font: {
                                size: 10
                            }
                        },

                        grid: {
                            color:
                                'rgba(255,255,255,0.06)'
                        }
                    }
                }
            }
        });
}


// ============================================================
// OPEN / CLOSE MODALS
// ============================================================

function openModal(modalId) {

    closeAllModals();

    const modal =
        document.getElementById(
            `modal-${modalId}`
        );

    const overlay =
        document.getElementById(
            'modal-overlay'
        );

    if (modal)
        modal.classList.add('active');

    if (overlay)
        overlay.classList.add('active');

    if (modalId === 'qr') {

        setTimeout(
            renderQrCode,
            50
        );

    } else if (
        modalId === 'analytics'
    ) {

        setTimeout(
            renderAnalyticsChart,
            50
        );
    }
}

function closeAllModals() {

    document
        .querySelectorAll('.modal')
        .forEach(
            (modal) =>
                modal.classList.remove(
                    'active'
                )
        );

    const overlay =
        document.getElementById(
            'modal-overlay'
        );

    if (overlay)
        overlay.classList.remove(
            'active'
        );
}


// ============================================================
// IoT MONITOR
// ============================================================

function updateIotMonitor(
    latest,
    records
) {

    const setText =
        (id, value) => {

            const el =
                document.getElementById(id);

            if (el)
                el.textContent = value;
        };

    if (!latest) {

        setText(
            'iot-temp',
            '--'
        );

        setText(
            'iot-humidity',
            '--'
        );

        setText(
            'iot-gps',
            '--'
        );

        setText(
            'iot-last-reading',
            'No data'
        );

        const status =
            document.getElementById(
                'iot-device-status'
            );

        if (status) {

            status.textContent =
                'Offline';

            status.className =
                'device-status offline';
        }

        return;
    }

    setText(
        'iot-device-name',
        latest.sensor_id ||
        latest.device_id ||
        'ESP32-01'
    );

    setText(
        'iot-temp',
        latest.temperature != null
            ? `${Number(
                latest.temperature
            ).toFixed(1)}°C`
            : '--'
    );

    setText(
        'iot-humidity',
        latest.humidity != null
            ? `${Number(
                latest.humidity
            ).toFixed(1)}%`
            : '--'
    );

    setText(
    'iot-gps',
    (
        latest.latitude != null &&
latest.longitude != null
)
    ? `${Number(latest.latitude).toFixed(6)}, ${Number(latest.longitude).toFixed(6)}`
    : '--'
);

updateFoodChainMapLocation(
    latest.latitude,
    latest.longitude,
    latest.timestamp
);

setText(
    'iot-last-reading',
    latest.timestamp || '--'
);

    // Online only if last reading is under 5 minutes old
    const readingTime =
        latest.timestamp
            ? new Date(
                String(
                    latest.timestamp
                ).replace(' ', 'T')
            )
            : null;

    const ageMinutes =
        readingTime &&
        !isNaN(readingTime)
            ? (
                Date.now() -
                readingTime.getTime()
            ) / 60000
            : Infinity;

    const status =
        document.getElementById(
            'iot-device-status'
        );

    if (status) {

        const isOnline =
            ageMinutes < 5;

        status.textContent =
            isOnline
                ? 'Online'
                : 'Offline';

        status.className =
            'device-status ' +
            (
                isOnline
                    ? 'online'
                    : 'offline'
            );
    }

    // Update chart using latest 7 readings
    const chart =
        adminCharts.sensorHistory;

    if (
        chart &&
        Array.isArray(records) &&
        records.length
    ) {

        const recent =
            records
                .slice(0, 7)
                .reverse()
                .filter(
                    (r) =>
                        r.temperature != null
                );

        if (recent.length) {

            const temps =
                recent.map(
                    (r) =>
                        Number(
                            r.temperature
                        )
                );

            chart.data.labels =
                recent.map(
                    (r) =>
                        String(
                            r.timestamp || ''
                        ).slice(11, 16)
                );

            chart.data.datasets[0].data =
                temps;

            chart.options.scales.y.min =
                Math.floor(
                    Math.min(...temps)
                ) - 1;

            chart.options.scales.y.max =
                Math.ceil(
                    Math.max(...temps)
                ) + 1;

            chart.update();
        }
    }
}


// ============================================================
// LOAD ADMIN DASHBOARD DATA
// ============================================================

async function loadAdminDashboardData() {

    if (adminRefreshInFlight)
        return;

    adminRefreshInFlight = true;

    try {

        const [
            adminStats,
            kpiRes,
            fabricRes,
            dataRes,
            alertRes
        ] = await Promise.all([

            FoodChainAPI.fetchJson(
                '/api/admin/stats'
            ),

            fetch(
                FoodChainAPI.resolveApiUrl(
                    '/api/kpis'
                )
            )
                .then(
                    (r) =>
                        r.ok
                            ? r.json()
                            : null
                )
                .catch(
                    () => null
                ),

            fetch(
                FoodChainAPI.resolveApiUrl(
                    '/api/fabric-status'
                )
            )
                .then(
                    (r) =>
                        r.ok
                            ? r.json()
                            : null
                )
                .catch(
                    () => null
                ),

            fetch(
                FoodChainAPI.resolveApiUrl(
                    '/data'
                )
            )
                .then(
                    (r) =>
                        r.ok
                            ? r.json()
                            : null
                )
                .catch(
                    () => null
                ),

            fetch(
                FoodChainAPI.resolveApiUrl(
                    '/api/agent-alerts'
                )
            )
                .then(
                    (r) =>
                        r.ok
                            ? r.json()
                            : null
                )
                .catch(
                    () => null
                )

        ]);

        const liveAlerts =
            Array.isArray(
                alertRes?.alerts
            )
                ? alertRes.alerts
                : [];

        populateAlerts(
            liveAlerts
                .slice(0, 50)
                .map((alert) => ({

                    kind:
                        alert.severity ===
                        'critical'
                            ? 'critical'
                            : alert.severity ===
                              'warning'
                                ? 'warning'
                                : 'info',

                    title:
                        alert.message ||
                        alert.title ||
                        'System alert',

                    meta: [

                        alert.batch_id &&
                            `Batch ${alert.batch_id}`,

                        alert.source ||
                            alert.alert_source,

                        alert.timestamp

                    ]
                        .filter(Boolean)
                        .join(' · ') ||
                        'Live monitoring'

                }))
        );

        // /data returns newest first
        const latestReading =
            dataRes?.latest ||
            (
                dataRes &&
                Array.isArray(
                    dataRes.data
                ) &&
                dataRes.data.length > 0
            )
                ? dataRes.data[0]
                : null;

        sensorRecords =
            Array.isArray(
                dataRes?.data
            )
                ? dataRes.data
                : [];

        const kpis = {

            batches:
                adminStats?.batches?.total ??
                kpiRes?.total_batches ??
                0,

            shipments:
                kpiRes?.active_shipments ??
                0,

            blockchainTx:
                adminStats?.batches?.transfers ??
                kpiRes?.blockchain_transactions ??
                0,

            sensorReadings:
                adminStats?.iot?.sensor_readings ??
                kpiRes?.total_sensors ??
                0,

            health:
                fabricRes?.fabric_available
                    ? 100
                    : 95,

            devices:
                new Set(
                    sensorRecords
                        .map(
                            (record) =>
                                record.device_id ||
                                record.sensor_id
                        )
                        .filter(Boolean)
                ).size,

            temp:
                latestReading?.temperature != null
                    ? `${Number(
                        latestReading.temperature
                    ).toFixed(1)}°C`
                    : '--',

            humidity:
                latestReading?.humidity != null
                    ? `${Number(
                        latestReading.humidity
                    ).toFixed(1)}%`
                    : '--',

            alerts:
                liveAlerts.length
        };

        populateKpis(kpis);

        const latestFabricTx =
            latestReading?.fabric_tx_id ||
            latestReading?.sensor_fabric_tx_id ||
            latestReading?.blockchain_tx_id;

        updateFabricIndicator(
            Boolean(latestFabricTx)
        );

        updateIotMonitor(
            latestReading,
            sensorRecords
        );

        FoodChainAPI.setApiStatus(
            'Live Backend & ESP32 Connected',
            'success'
        );

        const liveSensorRows =
            Array.isArray(sensorRecords)
                ? sensorRecords.slice(0, 10)
                : [];

        populateTransactions(
            liveSensorRows
        );

        // Refresh open ledger modal
        const ledgerModalEl =
            document.getElementById(
                'modal-ledger'
            );

        if (
            ledgerModalEl &&
            ledgerModalEl.classList.contains(
                'active'
            )
        ) {

            loadExpandedLedger(
                ledgerSelection.batchId,
                {
                    silent: true
                }
            );
        }

    } catch (err) {

        console.warn(
            'Dashboard live update issue:',
            err
        );

        populateKpis({});

        populateTransactions([]);

        populateAlerts([]);

        FoodChainAPI.setApiStatus(
            'Admin data unavailable',
            'warning'
        );

    } finally {

        adminRefreshInFlight =
            false;
    }
}


// ============================================================
// INITIALIZE ADMIN DASHBOARD
// ============================================================

document.addEventListener(
    'DOMContentLoaded',
    () => {

        // Role protection
        FoodChainAPI.initRoleGuard(
            'admin'
        );

        updateCurrentDate();

        initMap();

        initSensorChart();

        initModals();

        populateKpis({});

        populateTransactions([]);

        populateAlerts([]);

        const printButton =
            document.getElementById(
                'admin-print-report'
            );

        if (printButton) {

            printButton.addEventListener(
                'click',
                () => window.print()
            );
        }

        // Initial fetch
        loadAdminDashboardData();

        // Live polling every 5 seconds
        setInterval(
            loadAdminDashboardData,
            5000
        );
    }
);


// ============================================================
// RESIZE CHARTS
// ============================================================

window.addEventListener(
    'resize',
    function () {

        if (
            adminCharts.sensorHistory
        ) {
            adminCharts.sensorHistory.resize();
        }

        if (
            adminCharts.analyticsChart
        ) {
            adminCharts.analyticsChart.resize();
        }
    }
);


// ============================================================
// CREATE USER MODAL
// ============================================================

(function () {

    function showCreateUserModal() {

        const modal =
            document.getElementById(
                'modal-create-user'
            );

        if (!modal) return;

        const form =
            document.getElementById(
                'create-user-form'
            );

        if (form)
            form.reset();

        const fb =
            document.getElementById(
                'create-user-feedback'
            );

        if (fb) {

            fb.style.display =
                'none';

            fb.textContent =
                '';
        }

        modal.style.display =
            'flex';

        modal.style.alignItems =
            'center';

        modal.style.justifyContent =
            'center';

        setTimeout(
            () => {

                const inp =
                    document.getElementById(
                        'cu-username'
                    );

                if (inp)
                    inp.focus();

            },
            80
        );
    }


    function hideCreateUserModal() {

        const modal =
            document.getElementById(
                'modal-create-user'
            );

        if (modal)
            modal.style.display =
                'none';
    }


    function setFeedback(
        msg,
        isSuccess
    ) {

        const fb =
            document.getElementById(
                'create-user-feedback'
            );

        if (!fb) return;

        fb.textContent =
            msg;

        fb.style.display =
            'block';

        if (isSuccess) {

            fb.style.background =
                'rgba(34,197,94,0.15)';

            fb.style.border =
                '1px solid rgba(34,197,94,0.4)';

            fb.style.color =
                '#86efac';

        } else {

            fb.style.background =
                'rgba(239,68,68,0.15)';

            fb.style.border =
                '1px solid rgba(239,68,68,0.4)';

            fb.style.color =
                '#fca5a5';
        }
    }


    document.addEventListener(
        'DOMContentLoaded',
        () => {

            const sidebarBtn =
                document.getElementById(
                    'sidebar-create-user-btn'
                );

            if (sidebarBtn) {

                sidebarBtn.addEventListener(
                    'click',
                    (e) => {

                        e.preventDefault();

                        showCreateUserModal();
                    }
                );
            }


            document
                .getElementById(
                    'create-user-modal-close'
                )
                ?.addEventListener(
                    'click',
                    hideCreateUserModal
                );


            document
                .getElementById(
                    'cu-cancel-btn'
                )
                ?.addEventListener(
                    'click',
                    hideCreateUserModal
                );


            document
                .getElementById(
                    'modal-create-user'
                )
                ?.addEventListener(
                    'click',
                    (e) => {

                        if (
                            e.target ===
                            document.getElementById(
                                'modal-create-user'
                            )
                        ) {

                            hideCreateUserModal();
                        }
                    }
                );


            document
                .getElementById(
                    'create-user-form'
                )
                ?.addEventListener(
                    'submit',
                    async (e) => {

                        e.preventDefault();

                        const username =
                            (
                                document.getElementById(
                                    'cu-username'
                                )?.value ||
                                ''
                            ).trim();

                        const password =
                            document.getElementById(
                                'cu-password'
                            )?.value ||
                            '';

                        const role =
                            document.getElementById(
                                'cu-role'
                            )?.value ||
                            '';

                        if (
                            !username ||
                            !password ||
                            !role
                        ) {

                            setFeedback(
                                'Please fill in all fields.',
                                false
                            );

                            return;
                        }

                        if (
                            password.length < 6
                        ) {

                            setFeedback(
                                'Password must be at least 6 characters.',
                                false
                            );

                            return;
                        }

                        const btn =
                            document.getElementById(
                                'cu-submit-btn'
                            );

                        if (btn) {

                            btn.disabled =
                                true;

                            btn.innerHTML =
                                '<i class="fa-solid fa-spinner fa-spin"></i>&nbsp; Creating...';
                        }

                        try {

                            const result =
                                await FoodChainAPI.postJson(
                                    '/api/admin/users',
                                    {
                                        username,
                                        password,
                                        role
                                    }
                                );

                            setFeedback(
                                `✅ User "${result.username}" created successfully with role: ${result.role}`,
                                true
                            );

                            document
                                .getElementById(
                                    'create-user-form'
                                )
                                ?.reset();

                            setTimeout(
                                hideCreateUserModal,
                                2500
                            );

                        } catch (err) {

                            setFeedback(
                                `❌ ${
                                    err.message ||
                                    'Failed to create user. Check credentials.'
                                }`,
                                false
                            );

                        } finally {

                            if (btn) {

                                btn.disabled =
                                    false;

                                btn.innerHTML =
                                    '<i class="fa-solid fa-user-plus"></i>&nbsp; Create User';
                            }
                        }
                    }
                );
        }
    );

})();