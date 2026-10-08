"""
Hyperledger Fabric Gateway client for FoodChain SCM.

When Fabric network is running (Docker containers up via WSL):
  - Submits sensor data as real ledger transactions via Node.js gateway script
  - Returns actual Fabric txId stored in SQLite field: fabric_tx_id

When Fabric is offline:
  - Falls back silently to SHA-256 hash chain only mode
  - No crash, full application still works
"""

import json
import logging
import socket
import subprocess
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
_BACKEND_DIR    = Path(__file__).resolve().parent.parent
_PROJECT_DIR    = _BACKEND_DIR.parent
_SUBMIT_SCRIPT  = _PROJECT_DIR / "blockchain" / "submit_transaction.js"
_BLOCKCHAIN_DIR = _PROJECT_DIR / "blockchain"
# TLS cert is created by network.sh up — its presence means the network has been initialized
_TLS_CERT_PATH  = (
    _PROJECT_DIR
    / "blockchain" / "fabric-samples" / "test-network"
    / "organizations" / "peerOrganizations" / "org1.example.com"
    / "peers" / "peer0.org1.example.com" / "tls" / "ca.crt"
)

# ── State ────────────────────────────────────────────────────────────────────
FABRIC_AVAILABLE = False
_STATUS_LOCK     = threading.Lock()


# ── Connection check ─────────────────────────────────────────────────────────
def _get_wsl_ip() -> str | None:
    """Resolve the WSL2 distro's IP address for port-forwarding fallback."""
    try:
        result = subprocess.run(
            ["wsl", "-d", "Ubuntu", "bash", "-c", "hostname -I"],
            capture_output=True, text=True, timeout=3
        )
        ip = result.stdout.strip().split()[0] if result.stdout.strip() else None
        return ip
    except Exception:
        return None


def _tcp_check(host: str, port: int = 7051, timeout: float = 1.5) -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        res = sock.connect_ex((host, port))
        sock.close()
        return res == 0
    except Exception:
        return False


def check_fabric_connection(retries: int = 2, retry_delay: float = 1.0) -> bool:
    """Check if Hyperledger Fabric peer0 is reachable — Docker CLI first (most
    reliable in WSL2), then TCP checks against localhost AND the WSL IP."""
    global FABRIC_AVAILABLE
    available = False

    for attempt in range(retries + 1):
        # Check 1: Windows Docker CLI (fastest, most reliable if docker desktop
        # integrates with WSL2 — usually accurate regardless of port-forwarding)
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=peer0", "--format", "{{.Names}}"],
                capture_output=True, text=True, timeout=4
            )
            if "peer0" in result.stdout:
                available = True
        except Exception:
            pass

        # Check 2: WSL Ubuntu Docker CLI (in case Windows docker CLI isn't wired up)
        if not available:
            try:
                result = subprocess.run(
                    ["wsl", "-d", "Ubuntu", "bash", "-c",
                     "docker ps --filter name=peer0 --format '{{.Names}}'"],
                    capture_output=True, text=True, timeout=4
                )
                if "peer0" in result.stdout:
                    available = True
            except Exception:
                pass

        # Check 3: TCP to localhost:7051 (works if WSL2 port-forwarding is up)
        if not available:
            available = _tcp_check("localhost", 7051)

        # Check 4: TCP directly to WSL IP:7051 (bypasses Windows port-forwarding
        # entirely — catches the case where forwarding hasn't propagated yet)
        if not available:
            wsl_ip = _get_wsl_ip()
            if wsl_ip:
                available = _tcp_check(wsl_ip, 7051)

        if available:
            break

        if attempt < retries:
            time.sleep(retry_delay)  # give WSL2 networking a moment to catch up

    with _STATUS_LOCK:
        FABRIC_AVAILABLE = available

    if available:
        logger.info("Hyperledger Fabric network detected — dual-layer mode active")
    else:
        logger.info("Fabric peer0 not running — hash-chain-only mode")

    return FABRIC_AVAILABLE


def start_background_checker(interval_seconds: int = 30):
    """Background thread that re-checks Fabric availability every N seconds.
    This allows the dashboard to automatically detect when Fabric comes online
    after start.bat launches the WSL network."""
    def _loop():
        # Quick re-check 10s after startup (Fabric may still be coming up)
        time.sleep(10)
        check_fabric_connection()
        while True:
            time.sleep(interval_seconds)
            check_fabric_connection()
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    logger.info(f"Fabric availability checker running every {interval_seconds}s")


# ── Transaction submission ────────────────────────────────────────────────────
def submit_to_fabric(batch_id: str, sensor_data: dict) -> str | None:
    """Submit sensor data to Hyperledger Fabric via Node.js gateway script.
    
    Returns:
        str  — Fabric transaction ID (txId) if successful
        None — if Fabric is offline or submission failed
    """
    with _STATUS_LOCK:
        available = FABRIC_AVAILABLE

    if not available:
        return None

    if not _SUBMIT_SCRIPT.exists():
        logger.warning(f"Fabric submit script not found: {_SUBMIT_SCRIPT}")
        return None

    # Check node_modules installed
    node_modules = _BLOCKCHAIN_DIR / "node_modules"
    if not node_modules.exists():
        logger.warning("Fabric Gateway npm deps not installed. Run: npm install in blockchain/")
        return None

    try:
        sensor_json = json.dumps({
            "temperature":   sensor_data.get("temperature"),
            "humidity":      sensor_data.get("humidity"),
            "current_stage": sensor_data.get("current_stage"),
            "product_name":  sensor_data.get("product_name"),
            "sensor_id":     sensor_data.get("sensor_id"),
            "timestamp":     sensor_data.get("timestamp"),
            "latitude":      sensor_data.get("latitude"),
            "longitude":     sensor_data.get("longitude"),
            "gas_value":     sensor_data.get("gas_value"),
            "alert_status":  sensor_data.get("alert_status"),
            "transportation_status": sensor_data.get("transportation_status"),
            "telemetry_mode": sensor_data.get("telemetry_mode"),
        })

        result = subprocess.run(
            ["node", str(_SUBMIT_SCRIPT), batch_id, sensor_json],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(_BLOCKCHAIN_DIR),
        )

        if result.returncode != 0:
            err = result.stderr.strip() or result.stdout.strip()
            logger.warning(f"Fabric submit failed for {batch_id}: {err}")
            return None

        output = json.loads(result.stdout.strip())

        if output.get("success"):
            tx_id = output.get("txId")
            if not tx_id:
                logger.warning("Fabric submit succeeded but no real txId was returned")
                return None
            logger.info(f"Fabric TX committed — batch: {batch_id} | txId: {tx_id[:16]}...")
            return tx_id
        else:
            logger.warning(f"Fabric submit error: {output.get('error')}")
            return None

    except subprocess.TimeoutExpired:
        logger.warning(f"Fabric submit timed out for batch {batch_id}")
        return None
    except Exception as e:
        logger.warning(f"Fabric submit exception: {e}")
        return None


def submit_to_fabric_async(batch_id: str, sensor_data: dict, callback=None):
    """Fire-and-forget Fabric submission on a background thread.
    Does NOT block the MQTT/replay ingestion path.
    
    callback(tx_id: str | None) is called when submission completes.
    """
    def _submit():
        tx_id = submit_to_fabric(batch_id, sensor_data)
        if callback:
            try:
                callback(tx_id)
            except Exception as cb_err:
                logger.warning(f"Fabric async callback error: {cb_err}")

    t = threading.Thread(target=_submit, daemon=True)
    t.start()


# ── Status ───────────────────────────────────────────────────────────────────
def get_fabric_status() -> dict:
    with _STATUS_LOCK:
        available = FABRIC_AVAILABLE
    return {
        "fabric_available": available,
        "mode": "hyperledger-fabric" if available else "hash-chain-only",
        "description": (
            "Hyperledger Fabric — foodchainchannel | foodchain chaincode"
            if available else
            "SHA-256 Hash Chain (Fabric offline — start Docker + run start.bat)"
        ),
        "peer_endpoint": "localhost:7051" if available else None,
        "channel": "foodchainchannel" if available else None,
        "chaincode": "foodchain" if available else None,
    }
