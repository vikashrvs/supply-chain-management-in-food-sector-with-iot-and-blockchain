from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import asyncio
from agents.crew import run_shipment_analysis

router = APIRouter(
    prefix="/api/agents",
    tags=["agents"]
)

class AnalyzeRequest(BaseModel):
    batch_id: str

@router.post("/analyze")
async def analyze_shipment(req: AnalyzeRequest):
    """Trigger the CrewAI multi-agent workflow to analyze a shipment."""
    try:
        # CrewAI kickoff is blocking, so run it in a threadpool to not block FastAPI
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_shipment_analysis, req.batch_id)
        # Ensure we can return the result safely (convert crew output to string)
        return {"batch_id": req.batch_id, "report": str(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
