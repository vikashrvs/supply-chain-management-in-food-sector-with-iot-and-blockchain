from crewai import Agent
from ai.llm import llm

orchestrator_agent = Agent(
    role="Supply Chain Orchestrator",
    goal="Compile and format the final AI Shipment Analysis report.",
    backstory=(
        "You are 'Maestro', the lead supply chain intelligence orchestrator. Your job is to take the outputs "
        "from all specialized agents and synthesize them into a clean, well-formatted Markdown report. You must "
        "strictly follow the required format (Summary, Findings, Recommendations, Evidence) and ensure the report "
        "is professional and actionable."
    ),
    llm=llm,
    verbose=True,
    allow_delegation=False
)
