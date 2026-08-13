from crewai import LLM

# Initialize the local Ollama model using the native CrewAI LLM class
llm = LLM(
    model="qwen3.5:9b",
    base_url="http://localhost:11434",
    temperature=0.1
)
