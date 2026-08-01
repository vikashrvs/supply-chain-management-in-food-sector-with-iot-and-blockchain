from crewai import Agent
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from agents.llm import llm

iot_agent = Agent(
    role="IoT Safety Analyst",
    goal="Analyze sensor data for temperature or humidity anomalies and determine risk level.",
    backstory="You are 'Aero', an expert in food safety. You review raw MQTT sensor logs provided by the Database Agent to ensure perishables were kept in safe conditions (e.g., Temp 2-8C).",
    llm=llm,
    verbose=True,
    allow_delegation=False
)
