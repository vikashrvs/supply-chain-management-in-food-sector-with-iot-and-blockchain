from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx

router = APIRouter(
    prefix="/api/agents",
    tags=["agents"]
)

class AnalyzeRequest(BaseModel):
    batch_id: str

@router.post("/analyze")
async def analyze_shipment(req: AnalyzeRequest):
    """Trigger the CrewAI multi-agent workflow by proxying to the AI Service."""
    try:
        # Forward request to the dedicated AI Service running on port 8002
        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(
                "http://127.0.0.1:8002/analyze",
                json={"batch_id": req.batch_id}
            )
            response.raise_for_status()
            return response.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"AI Service unavailable: {str(e)}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"AI Service error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
