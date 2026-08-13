from crewai import Task, Crew, Process

import sys
from pathlib import Path
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from ai.agents.database_checker import db_agent
from ai.agents.iot_analyst import iot_agent
from ai.agents.blockchain_verifier import blockchain_agent
from ai.agents.anomaly_detector import anomaly_agent
from ai.agents.risk_assessor import risk_agent
from ai.agents.recommendation_agent import recommendation_agent
from ai.agents.orchestrator import orchestrator_agent

def run_shipment_analysis(batch_id: str):
    # 1. Fetch DB Records
    fetch_db_records = Task(
        description=f"Fetch the sensor data for shipment '{batch_id}' using your database tool. Output the raw data clearly.",
        expected_output="A list of raw sensor records (temperature, humidity, status) for the shipment.",
        agent=db_agent
    )

    # 2. Analyze IoT & Blockchain (Independent of each other, dependent on DB)
    analyze_sensors = Task(
        description=(
            "Review the raw sensor data provided by the Database Agent. Analyze the temperature and humidity. "
            "Note any spikes or drops outside normal ranges (e.g. Temp > 30C or < 0C is High Risk)."
        ),
        expected_output="A brief summary of the sensor data, identifying any anomalies and assigning a Risk Level (Low/Medium/High).",
        agent=iot_agent,
        context=[fetch_db_records]
    )

    verify_ledger = Task(
        description=f"Check the blockchain status for shipment '{batch_id}' using your tool. Determine if it has valid Fabric transactions or local hashes.",
        expected_output="A statement on whether the shipment records are securely verified on the blockchain.",
        agent=blockchain_agent,
        context=[fetch_db_records]
    )

    # 3. Anomaly / Fraud Detection (Dependent on DB, IoT, Blockchain)
    detect_anomalies = Task(
        description=(
            "Review the database records, IoT analysis, and Blockchain verification results. "
            "Detect suspicious or inconsistent data (e.g., impossible sensor values, large time gaps, duplicate records, missing blockchain hashes). "
            "Explain WHY something is suspicious using terms like Normal, Suspicious, High Suspicion, or Possible Data Manipulation."
        ),
        expected_output="An analysis report detailing any detected anomalies, fraud risks, or inconsistencies.",
        agent=anomaly_agent,
        context=[fetch_db_records, analyze_sensors, verify_ledger]
    )

    # 4. Risk Assessment (Dependent on everything above)
    assess_risk = Task(
        description=(
            "Calculate the overall shipment risk based on the Database, IoT, Blockchain, and Anomaly findings. "
            "Define a clear score (0-100) and risk level (LOW/MEDIUM/HIGH/CRITICAL). Explain the reasons and affected factors."
        ),
        expected_output="A structured risk result containing risk_level, risk_score, reasons, affected factors, and confidence.",
        agent=risk_agent,
        context=[fetch_db_records, analyze_sensors, verify_ledger, detect_anomalies]
    )

    # 5. Recommendation (Dependent on Risk Assessment)
    generate_recommendations = Task(
        description=(
            "Based on the Risk Assessment and all previous findings, formulate practical, prioritized next steps "
            "(e.g., 'Safe to continue', 'Hold shipment', 'Verify blockchain record')."
        ),
        expected_output="A prioritized list of actionable recommendations.",
        agent=recommendation_agent,
        context=[assess_risk]
    )

    # 6. Final Report Orchestration
    compile_report = Task(
        description=(
            "Compile all findings into a final Markdown report with the exact format:\n\n"
            "### Shipment AI Analysis\n"
            "### Summary\n"
            "### Findings\n"
            "#### Sensor Status\n"
            "#### Shipment Status\n"
            "#### Blockchain Verification\n"
            "#### Anomaly / Fraud Analysis\n"
            "#### Risk Assessment\n"
            "### Recommendations\n"
            "### Evidence\n\n"
            "Ensure the content fits neatly into these sections without making up any data."
        ),
        expected_output="A polished Markdown report matching the requested format perfectly.",
        agent=orchestrator_agent,
        context=[fetch_db_records, analyze_sensors, verify_ledger, detect_anomalies, assess_risk, generate_recommendations]
    )

    # The CrewAI process can be sequential, but context allows agents to use parallel outputs
    crew = Crew(
        agents=[
            db_agent, iot_agent, blockchain_agent, anomaly_agent, 
            risk_agent, recommendation_agent, orchestrator_agent
        ],
        tasks=[
            fetch_db_records, analyze_sensors, verify_ledger, 
            detect_anomalies, assess_risk, generate_recommendations, compile_report
        ],
        process=Process.sequential,
        verbose=True
    )

    result = crew.kickoff()
    return result
