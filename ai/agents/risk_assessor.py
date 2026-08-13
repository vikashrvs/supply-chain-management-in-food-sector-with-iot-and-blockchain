from crewai import Agent
from ai.llm import llm

risk_agent = Agent(
    role="Risk Assessment Agent",
    goal="Calculate the overall shipment risk based on findings from all other agents.",
    backstory=(
        "You are 'Risk-Eval', a senior risk manager. You synthesize inputs from Database, IoT, Blockchain, and "
        "Anomaly analyses to determine an overall risk score (0-100) and risk level (LOW/MEDIUM/HIGH/CRITICAL). "
        "You explain your reasoning clearly and note which factors affected the score. You must be completely objective "
        "and formulate a transparent scoring rationale."
    ),
    llm=llm,
    verbose=True,
    allow_delegation=False
)
