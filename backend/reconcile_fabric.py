#!/usr/bin/env python3
"""
Reconcile missing fabric_tx_id by invoking Fabric CLI via WSL test_invoke.sh
Writes temp_ctor.json to blockchain folder and runs test_invoke.sh for each row.
"""
import sqlite3
import json
import subprocess
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'food_chain.db'
BLOCKCHAIN_DIR = BASE_DIR.parent / 'blockchain'
CTOR_PATH = BLOCKCHAIN_DIR / 'temp_ctor.json'

TXID_RE = re.compile(r"([a-f0-9]{64})")

MAX_ROWS = 20

def rows_to_reconcile(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, batch_id, temperature, humidity, latitude, longitude, gas_value, product_id, current_stage, timestamp
        FROM sensor_data
        WHERE (fabric_tx_id IS NULL OR fabric_tx_id='')
          AND block_hash IS NOT NULL AND block_hash != ''
        ORDER BY id ASC
        LIMIT ?
    """, (MAX_ROWS,))
    return cur.fetchall()


def build_sensor_json(row):
    (id_, batch_id, temp, hum, lat, lon, gas, product, stage, ts) = row
    data = {
        'temperature': float(temp) if temp is not None else 0.0,
        'humidity': float(hum) if hum is not None else 0.0,
        'current_stage': stage if stage else 'transport',
        'latitude': float(lat) if lat is not None else None,
        'longitude': float(lon) if lon is not None else None,
        'gas_value': float(gas) if gas is not None else None,
        'product_id': product,
        'timestamp': ts,
        'record_id': id_
    }
    return data


def run_invoke_via_wsl():
    # run test_invoke.sh inside WSL Ubuntu
    script_path = BLOCKCHAIN_DIR / 'test_invoke.sh'
    # craft wsl command: compute correct /mnt/c/... path from Windows path
    win_path = str(BLOCKCHAIN_DIR)
    # win_path looks like 'C:\Users\raj vikash\Desktop\food_chain\blockchain'
    drive = win_path[0].lower()
    rest = win_path[2:].replace('\\', '/')
    wsl_dir = f"/mnt/{drive}{rest}"
    cmd = [
        'wsl', '-d', 'Ubuntu', 'bash', '-lc', f"cd '{wsl_dir}' && ./test_invoke.sh"
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8', timeout=120)
    return proc.returncode, proc.stdout


def main():
    if not DB_PATH.exists():
        print(f"DB not found at {DB_PATH}")
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = None

    rows = rows_to_reconcile(conn)
    if not rows:
        print('No rows to reconcile (fabric_tx_id missing + block_hash present)')
        return 0

    print(f'Reconciling up to {len(rows)} rows...')
    cur = conn.cursor()
    for r in rows:
        sid = r[0]
        batch_id = r[1] or f'BATCH_{sid}'
        sensor_json = build_sensor_json(r)
        json_text = json.dumps(sensor_json, separators=(',', ':'))

        # Write temp ctor JSON expected by test_invoke.sh
        ctor = { 'Args': ['RecordSensorData', batch_id, json_text] }
        CTOR_PATH.write_text(json.dumps(ctor), encoding='utf-8')
        print(f'Wrote CTOR for id={sid} batch={batch_id} -> {CTOR_PATH}')

        try:
            rc, out = run_invoke_via_wsl()
        except subprocess.TimeoutExpired:
            print(f'id={sid} invoke timed out')
            continue

        print(f'Invoke output (rc={rc}):')
        print(out[:1000])

        m = TXID_RE.search(out)
        txid = m.group(1) if m else None
        if txid:
            print(f'Found txId {txid} for id={sid} — updating DB')
            cur.execute('UPDATE sensor_data SET fabric_tx_id = ? WHERE id = ?', (txid, sid))
            conn.commit()
        else:
            print(f'No txId found for id={sid}; leaving row unchanged')

    conn.close()
    print('Reconciliation complete')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
