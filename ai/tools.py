import json
import sys
from pathlib import Path
from crewai.tools import tool

# Make sure the project root is in sys.path to access the backend module
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from backend.utils import safe_query
from backend.services.fabric_client import get_fabric_status

@tool("Get Shipment Data")
def get_shipment_data(batch_id: str) -> str:
    """Fetch all sensor readings and current status for a given batch ID or shipment ID."""
    query = "SELECT * FROM sensor_data WHERE batch_id = ? OR product_id = ? ORDER BY timestamp DESC"
    try:
        rows = safe_query(query, (batch_id, batch_id))
        if not rows:
            return json.dumps({"error": f"No records found for shipment {batch_id}."})
        
        # Convert sqlite3.Row objects to dicts
        data = [dict(row) for row in rows]
        # Return only the most important fields to save LLM context
        filtered_data = [
            {
                "timestamp": d.get("timestamp"),
                "temperature": d.get("temperature"),
                "humidity": d.get("humidity"),
                "status": d.get("status"),
                "current_stage": d.get("current_stage")
            } for d in data
        ]
        return json.dumps(filtered_data, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Database error: {str(e)}"})

@tool("Check Blockchain Status")
def check_blockchain_status(batch_id: str) -> str:
    """Check the Hyperledger Fabric connection status and whether transactions exist for the batch."""
    try:
        status = get_fabric_status()
        query = "SELECT block_hash, fabric_tx_id FROM sensor_data WHERE batch_id = ? OR product_id = ?"
        rows = safe_query(query, (batch_id, batch_id))
        
        if not rows:
            return json.dumps({"error": f"No records found in database to verify against blockchain for {batch_id}."})

        has_tx = any(row["fabric_tx_id"] for row in rows)
        has_hash = any(row["block_hash"] for row in rows)
        
        result = {
            "fabric_connected": status.get("fabric_available"),
            "has_fabric_transactions": has_tx,
            "has_local_hashes": has_hash,
            "mode": status.get("mode")
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Database error: {str(e)}"})
