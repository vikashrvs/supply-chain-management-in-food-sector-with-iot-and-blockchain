from crewai import Task, Crew, Process

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from agents.orchestrator import orchestrator_agent
from agents.database_checker import db_agent
from agents.iot_analyst import iot_agent
from agents.blockchain_verifier import blockchain_agent

def run_shipment_analysis(batch_id: str):
    fetch_db_records = Task(
        description=f"Fetch the sensor data for shipment '{batch_id}' using your database tool. Output the raw data clearly.",
        expected_output="A list of raw sensor records (temperature, humidity, status) for the shipment.",
        agent=db_agent
    )

    analyze_sensors = Task(
        description="Review the raw sensor data provided by the Database Agent. Analyze the temperature and humidity. Note any spikes or drops outside normal ranges (e.g. Temp > 30C or < 0C is High Risk).",
        expected_output="A brief summary of the sensor data, identifying any anomalies and assigning a Risk Level (Low/Medium/High).",
        agent=iot_agent,
        context=[fetch_db_records]
    )

    verify_ledger = Task(
        description=f"Check the blockchain status for shipment '{batch_id}' using your tool. Determine if it has valid Fabric transactions or local hashes.",
        expected_output="A statement on whether the shipment records are securely verified on the blockchain.",
        agent=blockchain_agent
    )

    compile_report = Task(
        description="Compile the findings from the IoT, Blockchain, and Database agents into a final report using the required format: Summary, Findings (Sensor status, Shipment status, Blockchain verification, Database consistency, Risk level), and Recommendations. Ensure the format matches exactly.",
        expected_output="A markdown formatted report with '### Summary', '### Findings', and '### Recommendations' sections.",
        agent=orchestrator_agent,
        context=[analyze_sensors, verify_ledger, fetch_db_records]
    )

    crew = Crew(
        agents=[db_agent, iot_agent, blockchain_agent, orchestrator_agent],
        tasks=[fetch_db_records, analyze_sensors, verify_ledger, compile_report],
        process=Process.sequential,
        verbose=True
    )

    result = crew.kickoff()
    return result
