
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List


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
    telemetry_mode: Optional[str] = "Physical ESP32 Telemetry"
    origin_name: Optional[str] = None
    destination_name: Optional[str] = None
    timestamp: Optional[str] = None


# ── Producer Schemas ──────────────────────────────────────────────────────────

class BatchCreate(BaseModel):
    """Schema for creating a new batch (Producer only)."""
    product_name: str = Field(..., min_length=1, max_length=100)
    product_type: Optional[str] = Field(default=None, max_length=100)
    origin: Optional[str] = Field(default=None, max_length=200)
    destination: Optional[str] = Field(default=None, max_length=200)
    quantity: Optional[str] = Field(default=None, max_length=50)
    description: Optional[str] = Field(default=None, max_length=500)
    harvest_date: Optional[str] = None


class BatchResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    batch_id: str
    product_name: str
    product_type: Optional[str] = None
    origin: Optional[str] = None
    destination: Optional[str] = None
    quantity: Optional[str] = None
    description: Optional[str] = None
    harvest_date: Optional[str] = None
    status: str
    created_by: str
    created_at: str
    blockchain_tx_id: Optional[str] = None
    device_id: Optional[str] = None
    last_reading_at: Optional[str] = None


# ── Distributor Schemas ───────────────────────────────────────────────────────

class TransferEventCreate(BaseModel):
    """Schema for recording a transfer event (Distributor only)."""
    batch_id: str = Field(..., min_length=1, max_length=50)
    event_type: str = Field(..., pattern="^(received|transferred|checkpoint|anomaly)$")
    location_name: Optional[str] = Field(default=None, max_length=200)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    temperature: Optional[float] = Field(default=None, ge=-50, le=80)
    humidity: Optional[float] = Field(default=None, ge=0, le=100)
    notes: Optional[str] = Field(default=None, max_length=500)
    anomaly_description: Optional[str] = Field(default=None, max_length=500)


class TransferEventResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    batch_id: str
    event_type: str
    location_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    notes: Optional[str] = None
    anomaly_description: Optional[str] = None
    created_by: str
    created_at: str
    blockchain_tx_id: Optional[str] = None
    status: Optional[str] = None
    device_id: Optional[str] = None
    latest_iot: Optional[dict] = None
    block_hash: Optional[str] = None
    field_hash: Optional[str] = None
    fabric_tx_id: Optional[str] = None


# ── Admin Schemas ─────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    """Schema for creating a user (Admin only)."""
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)
    role: str = Field(..., pattern="^(admin|manager|producer|distributor|consumer)$")


class UserUpdate(BaseModel):
    """Schema for updating a user (Admin only)."""
    role: Optional[str] = Field(default=None, pattern="^(admin|manager|producer|distributor|consumer)$")
    is_active: Optional[bool] = None


class UserResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    username: str
    role: str
    is_active: bool
    created_at: str


class AuditLogEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    timestamp: str
    event_type: str
    result: str
    username: Optional[str] = None
    role: Optional[str] = None
    batch_id: Optional[str] = None
    detail: Optional[str] = None
    ip_address: Optional[str] = None
