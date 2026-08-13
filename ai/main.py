import sys
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ AI_SERVICE │ %(levelname)-7s │ %(message)s")
logger = logging.getLogger("ai_service")

# Make sure we can import from backend and ai
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from ai.crew import run_shipment_analysis

app = FastAPI(title="FoodChain AI Service", version="1.0.0")

class AnalyzeRequest(BaseModel):
    batch_id: str

@app.post("/analyze")
async def analyze_shipment(req: AnalyzeRequest):
    """Trigger the CrewAI multi-agent workflow to analyze a shipment."""
    try:
        logger.info(f"Starting AI analysis for batch_id: {req.batch_id}")
        loop = asyncio.get_event_loop()
        # CrewAI kickoff is blocking, run it in a threadpool
        result = await loop.run_in_executor(None, run_shipment_analysis, req.batch_id)
        logger.info(f"Completed AI analysis for batch_id: {req.batch_id}")
        return {"batch_id": req.batch_id, "report": str(result)}
    except Exception as e:
        logger.error(f"Error analyzing shipment {req.batch_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8002, reload=True)
