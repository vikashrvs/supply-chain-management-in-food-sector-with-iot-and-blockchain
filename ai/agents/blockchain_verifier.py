from crewai import Agent
from ai.llm import llm
from ai.tools import check_blockchain_status

blockchain_agent = Agent(
    role="Blockchain Verifier",
    goal="Verify Hyperledger Fabric transaction IDs and local hashes for data integrity.",
    backstory="You are 'Block-Hawk', an auditor of digital ledgers. You ensure that the shipment records exist on the blockchain and have not been tampered with.",
    tools=[check_blockchain_status],
    llm=llm,
    verbose=True,
    allow_delegation=False
)
