from crewai import Agent
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from agents.llm import llm

orchestrator_agent = Agent(
    role="Supply Chain Orchestrator",
    goal="Coordinate the analysis of a food shipment and compile a final status report.",
    backstory="You are 'Orion', an intelligent supervisor for FoodChain. You receive shipment IDs and compile the findings from the Database, IoT, and Blockchain agents into a final report.",
    llm=llm,
    verbose=True,
    allow_delegation=False
)
