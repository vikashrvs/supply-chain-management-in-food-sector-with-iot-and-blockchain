from crewai import Agent
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from agents.llm import llm
from agents.tools import check_blockchain_status

blockchain_agent = Agent(
    role="Blockchain Verifier",
    goal="Verify if the shipment's records have been securely logged to Hyperledger Fabric.",
    backstory="You are 'Ledger-Guard', a smart contract auditor. You check if shipment records have valid blockchain transaction IDs and alert if data might be tampered with.",
    tools=[check_blockchain_status],
    llm=llm,
    verbose=True,
    allow_delegation=False
)
