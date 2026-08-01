# FoodChain: Supply Chain Management in Food Sector with IoT & Blockchain

FoodChain is an enterprise-grade food supply chain traceability and compliance monitoring system. It leverages real-time IoT sensor telemetry, edge processing thresholds, and a dual-layer ledger design utilizing a local cryptographic SHA-256 Hash Chain combined with **Hyperledger Fabric** smart contract capabilities.

---

## 🏗️ System Architecture

```mermaid
graph TD
    subgraph IoT_Layer [IoT Telemetry Layer]
        ESP32[ESP32 Hardware Node] -->|MQTT| Mosquitto[Mosquitto Broker]
        Simulator[Sensor Simulation Tool] -->|MQTT| Mosquitto
    end

    subgraph Edge_Layer [Edge Processing Layer]
        Mosquitto -->|Raw Feed| EdgeProc[Edge Processor]
        EdgeProc -->|Compliance Checked| MosquittoProcessed[Processed Topics]
    end

    subgraph Core_Layer [Core Backend & Database]
        FastAPI[FastAPI Backend] <--Subscribe--> MosquittoProcessed
        FastAPI <--Read/Write--> SQLite[SQLite DB with WAL]
    end

    subgraph Ledger_Layer [Blockchain Integrity Layer]
        FastAPI -->|Compute block hash| HashChain[SHA-256 Hash Chain]
        FastAPI <-->|Fabric Gateway SDK| Hyperledger[Hyperledger Fabric Node]
    end

    subgraph User_Interface [Frontend Layer]
        Dashboard[Admin Dashboard Page] <--> FastAPI
        ConsumerPortal[Consumer Tracker Page] <--> FastAPI
        QRPage[QR Manager / Scanner] --> ConsumerPortal
    end
```

---

## 🛠️ Technology Stack

| Component | Technology | Role |
|---|---|---|
| **IoT Telemetry** | NodeMCU / ESP32, DHT22 (Temp & Hum), NEO-6M (GPS) | Sensor readings and geo-location tracking |
| **Messaging** | Eclipse Mosquitto (MQTT) | Lightweight pub/sub telemetry transport |
| **Edge Layer** | Python / local script (Raspberry Pi compatible) | Stream preprocessing and alert extraction |
| **Backend API** | FastAPI (Python 3.13), Uvicorn | High-performance async REST API and MQTT client |
| **Security** | JWT (JSON Web Tokens), CryptContext (Bcrypt) | Secure login routes and role-based route guards |
| **Database** | SQLite3 (configured with WAL mode) | Off-chain structured data storage and indexing |
| **Blockchain** | Hyperledger Fabric (WSL2 Ubuntu) + SHA-256 | Immutable distributed ledger and smart contracts (`foodchain`) |
| **Frontend** | Vanilla JS, HTML5, CSS Variables, Leaflet.js, Chart.js | Interactive charts, live mapping, and QR camera scanner |
| **AI Agents** | CrewAI, LangChain, Ollama (Llama 3) | Autonomous multi-agent shipment analysis and anomaly detection |

---

---

## 🤖 AI Multi-Agent System (CrewAI)

FoodChain includes an autonomous, local multi-agent system powered by **CrewAI** and **Ollama**. When anomalies are detected, a specialized team of AI agents analyzes the incident:

1. **Orion (Supply Chain Orchestrator):** Supervises the workflow and compiles the final incident report.
2. **Data-Tron (Database Records Specialist):** Retrieves raw sensor readings and shipment metadata.
3. **Aero (IoT Safety Analyst):** Evaluates sensor telemetry for safety threshold breaches (e.g., temperature spikes).
4. **Ledger-Guard (Blockchain Verifier):** Audits Hyperledger Fabric to ensure data integrity hasn't been compromised.

You can trigger a full agent analysis for any shipment by sending a `POST` request to `/api/agents/analyze` with the `batch_id`.

---

## ⚡ Quick Start (One-Click Launch & Shutdown)

### 1. Start All Services
Simply double-click or run:
```cmd
start.bat
```
*This automatically launches Hyperledger Fabric (WSL2), Mosquitto MQTT Broker, FastAPI Backend (`http://127.0.0.1:8001`), IoT Simulation, and opens the Web Portal in your browser.*

### 2. Stop All Services
Simply double-click or run:
```cmd
stop.bat
```
*This cleanly stops all Docker containers, terminates background Python/MQTT processes, and closes terminal windows.*

---

## 📂 Project Directory Structure

```
food_chain/
├── start.bat                 # One-click startup script
├── stop.bat                  # One-click shutdown script
├── backend/                  # FastAPI Application
│   ├── main.py               # Slim entry point & app configuration
│   ├── config.py             # Global constants & thresholds
│   ├── database.py           # DB connection, init, migrations, & indexing
│   ├── auth.py               # JWT authentication logic & route guards
│   ├── schemas.py            # Pydantic input/output schemas
│   ├── mqtt_handler.py       # MQTT connection & message routing
│   ├── agents/               # CrewAI Multi-Agent System
│   │   ├── crew.py           # Orchestrator & Task definitions
│   │   ├── orchestrator.py   # Orion - Supply Chain Supervisor
│   │   ├── iot_analyst.py    # Aero - IoT Safety Analyst
│   │   ├── database_checker.py # Data-Tron - Database Specialist
│   │   ├── blockchain_verifier.py # Ledger-Guard - Blockchain Auditor
│   │   ├── tools.py          # Tools for SQLite & Fabric lookups
│   │   └── llm.py            # Local Ollama connection setup
│   ├── routes/               # API Router endpoints
│   │   ├── agents.py         # CrewAI trigger endpoint (/api/agents/analyze)
│   │   ├── sensor.py         # Telemetry, batches, alerts, and uids
│   │   ├── tracking.py       # Detailed batch & UID history tracking
│   │   └── blockchain.py     # Hash chain verification & Fabric status
│   └── services/             # Core business service logic
│       ├── hash_chain.py     # SHA-256 block hash computation
│       ├── edge_health.py    # Multi-stage threshold checks
│       └── fabric_client.py  # Hyperledger Fabric Client gateway
├── blockchain/               # Hyperledger Fabric Workspace
│   ├── start_fabric.sh       # Clean WSL Fabric network launch & chaincode deploy
│   ├── stop_fabric.sh        # WSL Fabric network teardown
│   ├── test_invoke.sh        # Transaction invocation script
│   ├── submit_transaction.js # Node.js Fabric Gateway client
│   └── chaincode/            # JavaScript chaincode (smart contracts)
│       └── foodchain/
│           ├── foodchain.js  # ABAC, compliance, and event contracts
│           └── package.json  # Chaincode node packaging
├── frontend/                 # Client Web Pages
│   ├── css/                  # Shared stylesheet system
│   ├── js/                   # Shared scripts (Auth & Theme)
│   ├── login.html            # JWT login page
│   ├── home.html             # Landing portal page
│   ├── dashboard.html        # Admin control console
│   ├── qr.html               # QR manager and html5-qrcode camera scanner
│   └── track.html            # Consumer timeline & Leaflet map tracking
├── iot_simulation/           # Physical Node & Simulation scripts
│   ├── esp32_firmware/
│   │   └── foodchain_sensor.ino  # ESP32 C++ microcontroller code
│   ├── edge_processor.py     # Python edge analytics service
│   └── sensor_simulation.py  # Standalone mock telemetry generator
└── README.md                 # Primary system documentation
```

---

## 🔍 Verification & Inspection

| Goal | Location / Endpoint | Details |
|---|---|---|
| **Product Trace UI** | `http://127.0.0.1:8001/track.html` | Search any Batch ID (e.g. `BATCH_001`) or Product UID (`UID-353581A7AE3B`) |
| **Fabric Status API** | `http://127.0.0.1:8001/api/blockchain/status` | Check Fabric peer status, channel (`mychannel`), and chaincode (`foodchain`) |
| **Hash Chain Verify** | `http://127.0.0.1:8001/verify/BATCH_001` | SHA-256 cryptographic chain tamper verification |
| **Off-Chain DB** | `backend/food_chain.db` | Open with DB Browser for SQLite -> inspect `sensor_data` table |
| **On-Chain Logs** | Docker Desktop / WSL | `wsl -d Ubuntu docker logs peer0.org1.example.com` |

---

## 🔒 Security & Default Credentials

Authentication is fully protected using **JWT token authorization**. The database seeds default roles on initial startup.

| Role | Username | Password | Access Level |
|---|---|---|---|
| **Admin** | `admin` | `admin123` | Master dashboard overview, blockchain verification |
| **Farmer** | `farmer` | `farmer123` | Update collection/harvest details |
| **Retailer** | `retailer` | `retail123` | Log retailer check-ins and handoffs |
| **Consumer** | *Guest* | *N/A* | Read-only scan access to track product timeline |

---

## 📄 License
This project is licensed under the Apache License 2.0.
