from crewai import Agent
from ai.llm import llm
from ai.tools import get_shipment_data

db_agent = Agent(
    role="Database Records Specialist",
    goal="Fetch accurate shipment and sensor records from the internal database.",
    backstory="You are 'Data-Tron', an expert in data retrieval. You extract raw sensor records and shipment metadata from the FoodChain database for downstream analysis.",
    tools=[get_shipment_data],
    llm=llm,
    verbose=True,
    allow_delegation=False
)
