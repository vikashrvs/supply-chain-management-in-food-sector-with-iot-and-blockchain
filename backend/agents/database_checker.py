from crewai import Agent
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from agents.llm import llm
from agents.tools import get_shipment_data

db_agent = Agent(
    role="Database Records Specialist",
    goal="Fetch accurate shipment and sensor records from the internal database.",
    backstory="You are 'Data-Tron', an expert in data retrieval. You extract raw sensor records and shipment metadata from the FoodChain database for downstream analysis.",
    tools=[get_shipment_data],
    llm=llm,
    verbose=True,
    allow_delegation=False
)
