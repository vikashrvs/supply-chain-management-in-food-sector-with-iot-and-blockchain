
from pydantic import BaseModel, Field, ConfigDict
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
    model_config = ConfigDict(extra="ignore")

    record_id: Optional[int] = None
    batch_id: str = Field(..., min_length=1, max_length=50)
    product_uid: Optional[str] = None
    product: Optional[str] = None
    product_name: Optional[str] = None
    product_id: Optional[str] = None
    sensor_id: Optional[str] = "UNKNOWN_SENSOR"
    current_stage: str = Field("transport", pattern="^(field|warehouse|transport|retailer|consumer)$")
    temperature: float = Field(..., ge=-50, le=80)
    humidity: float = Field(..., ge=0, le=100)
    gas_value: Optional[float] = Field(default=None, ge=0)
    environmental_value: Optional[float] = Field(default=None, ge=0)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    location: Optional[dict] = None
    status: Optional[str] = None
    transportation_status: Optional[str] = None
    alert_status: Optional[str] = None
    telemetry_mode: Optional[str] = "Demo Telemetry / Replay Mode"
    origin_name: Optional[str] = None
    destination_name: Optional[str] = None
    timestamp: Optional[str] = None

