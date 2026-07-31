
from pydantic import BaseModel, Field
from typing import Optional


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=50)
    password: str = Field(..., min_length=1)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str


class KPIs(BaseModel):
    total_batches: int
    total_sensors: int
    active_shipments: int
    blockchain_transactions: int
    alerts_today: int
    healthy_shipments: int

class SensorReading(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=50)
    product_uid: Optional[str] = None
    product: Optional[str] = None
    product_name: Optional[str] = None
    product_id: Optional[str] = None
    sensor_id: Optional[str] = "UNKNOWN_SENSOR"
    current_stage: str = Field("transport", pattern="^(field|warehouse|transport|retailer|consumer)$")
    temperature: float = Field(..., ge=-50, le=80)
    humidity: float = Field(..., ge=0, le=100)
    location: Optional[dict] = None
    status: Optional[str] = None
    timestamp: Optional[str] = None

