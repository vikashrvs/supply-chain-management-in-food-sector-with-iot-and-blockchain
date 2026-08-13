from crewai import Agent
from ai.llm import llm

recommendation_agent = Agent(
    role="Recommendation Agent",
    goal="Convert analytical findings into practical, actionable supply-chain recommendations.",
    backstory=(
        "You are 'Action-Planner', an operations strategist. You take the risk assessment and anomaly reports "
        "and generate clear, prioritized next steps. For example: 'Safe to continue shipment', 'Hold shipment', "
        "or 'Investigate possible data manipulation'. Your recommendations must be grounded strictly in the provided evidence."
    ),
    llm=llm,
    verbose=True,
    allow_delegation=False
)
