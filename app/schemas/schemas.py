#app\schemas\schemas.py
from pydantic import BaseModel
from typing import Optional, List
from uuid import UUID
from datetime import datetime
from app.models.models import UserRole, ServiceCategory, TicketStatus


# ── Auth ──────────────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    role: UserRole
    agent_id: Optional[UUID] = None
    username: str


# ── Agent ─────────────────────────────────────────────────────────────────────
class AgentCreate(BaseModel):
    name: str
    last_name: Optional[str] = None
    password: str = "cnas1234"   # default password agents can change later

class AgentUpdate(BaseModel):
    name: Optional[str] = None
    last_name: Optional[str] = None
    category: Optional[ServiceCategory] = None
    assigned_service: Optional[str] = None
    sub_service: Optional[str] = None
    queue_number: Optional[str] = None
    structure: Optional[str] = None
    is_paused: Optional[bool] = None

class AgentPasswordChange(BaseModel):
    current_password: str
    new_password: str

class AgentOut(BaseModel):
    id: UUID
    name: str
    last_name: Optional[str]
    structure: Optional[str]
    queue_number: Optional[str]
    category: Optional[ServiceCategory]
    assigned_service: Optional[str]
    sub_service: Optional[str]
    is_active: bool
    is_paused: bool

    model_config = {"from_attributes": True}


# ── Service ───────────────────────────────────────────────────────────────────
class ServiceCreate(BaseModel):
    name: str
    category: ServiceCategory
    avg_time_min: int = 10

class ServiceOut(BaseModel):
    id: int
    name: str
    category: ServiceCategory
    avg_time_min: int
    is_active: bool
    waiting_count: int = 0    # populated from Redis queue length

    model_config = {"from_attributes": True}


# ── Ticket ────────────────────────────────────────────────────────────────────
class TicketIssue(BaseModel):
    service_id: int
    sub_service: Optional[str] = None
    priority: bool = False   # ✅ ADD THIS

class TicketAction(BaseModel):
    action: str   # "call_next" | "skip" | "recall" | "done"
    agent_id: UUID

class TicketOut(BaseModel):
    id: UUID
    number: str
    status: TicketStatus
    wait_minutes: int
    created_at: datetime
    called_at: Optional[datetime]
    service_name: Optional[str] = None

    model_config = {"from_attributes": True}


# ── Stats (AdminPage cards) ───────────────────────────────────────────────────
class StatsOut(BaseModel):
    total_agents: int
    active_windows: int
    citizens_waiting: int
    avg_wait_minutes: float
    tickets_today: int
    served_today: int
