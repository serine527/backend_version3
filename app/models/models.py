"""
Database models — match the frontend data structures exactly:

  User      → login credentials (admin or agent)
  Agent     → agent profile with queue/service assignment
  Service   → a service category (prestation or medical) with sub-services
  Queue     → one queue per service
  Ticket    → a citizen's ticket in a queue
"""

import uuid
import enum
from sqlalchemy import (
    Column, String, Integer, Boolean, DateTime, Enum,
    ForeignKey, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class UserRole(str, enum.Enum):
    admin = "admin"
    agent = "agent"


class ServiceCategory(str, enum.Enum):
    prestation = "prestation"
    medical = "medical"


class TicketStatus(str, enum.Enum):
    waiting   = "waiting"
    serving   = "serving"
    paused    = "paused"
    done      = "done"
    skipped   = "skipped"


# ── User (login) ──────────────────────────────────────────────────────────────
class User(Base):
    __tablename__ = "users"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username   = Column(String(100), unique=True, nullable=False, index=True)
    password   = Column(String(255), nullable=False)
    role       = Column(Enum(UserRole), nullable=False, default=UserRole.agent)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    agent = relationship("Agent", back_populates="user", uselist=False)


# ── Agent ─────────────────────────────────────────────────────────────────────
class Agent(Base):
    __tablename__ = "agents"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id          = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=True)
    name             = Column(String(100), nullable=False)
    last_name        = Column(String(100), nullable=True)
    structure        = Column(String(150), nullable=True)   # AgentPage: agentRecord.structure
    queue_number     = Column(String(50),  nullable=True)   # AgentPage: agentRecord.queue (guichet)
    category         = Column(Enum(ServiceCategory), nullable=True)
    assigned_service = Column(String(200), nullable=True)   # matches agent.assignedService in frontend
    sub_service      = Column(String(200), nullable=True)   # AgentPage: agentRecord.subService
    is_active        = Column(Boolean, default=True)
    is_paused        = Column(Boolean, default=False)
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), onupdate=func.now())

    user    = relationship("User", back_populates="agent")
    tickets = relationship("Ticket", back_populates="agent")


# ── Service ───────────────────────────────────────────────────────────────────
class Service(Base):
    __tablename__ = "services"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    name         = Column(String(200), nullable=False)
    category     = Column(Enum(ServiceCategory), nullable=False)
    avg_time_min = Column(Integer, default=10)   # average service time in minutes
    is_active    = Column(Boolean, default=True)

    queues = relationship("Queue", back_populates="service", cascade="all, delete-orphan")


# ── Queue ─────────────────────────────────────────────────────────────────────
class Queue(Base):
    __tablename__ = "queues"
    __table_args__ = (UniqueConstraint("service_id", name="uq_queue_service"),)

    id         = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(Integer, ForeignKey("services.id", ondelete="CASCADE"), nullable=False)
    prefix     = Column(String(5), default="A")  # ticket prefix, e.g. A001
    counter    = Column(Integer, default=0)       # last issued ticket number

    service = relationship("Service", back_populates="queues")
    tickets = relationship("Ticket", back_populates="queue", order_by="Ticket.created_at")


# ── Ticket ────────────────────────────────────────────────────────────────────
class Ticket(Base):
    __tablename__ = "tickets"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    number       = Column(String(20), nullable=False)   # e.g. "A001"
    queue_id     = Column(Integer, ForeignKey("queues.id", ondelete="CASCADE"), nullable=False)
    agent_id     = Column(UUID(as_uuid=True), ForeignKey("agents.id"), nullable=True)
    status       = Column(Enum(TicketStatus), nullable=False, default=TicketStatus.waiting)
    wait_minutes = Column(Integer, default=0)
    created_at   = Column(DateTime(timezone=True), server_default=func.now())
    called_at    = Column(DateTime(timezone=True), nullable=True)
    done_at      = Column(DateTime(timezone=True), nullable=True)
    service_id = Column(Integer, ForeignKey("services.id"), nullable=False)
    sub_service = Column(String(200), nullable=True)
    priority = Column(Boolean, default=False)
    
    queue = relationship("Queue", back_populates="tickets")
    agent = relationship("Agent", back_populates="tickets")
    service = relationship("Service")

    # ── System Config ────────────────────────────────────────────────────────────
class SystemConfig(Base):
    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    mode = Column(String(10), nullable=False, default="single")