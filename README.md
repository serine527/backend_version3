# CNAS Queue Management — Backend v2

FastAPI + PostgreSQL + Redis + WebSocket

---

## Architecture

```
React Frontend
  ├── LoginPage      → POST /api/v1/auth/login
  ├── AdminPage      → GET  /api/v1/agents, /tickets/stats
  │                  → WS  /ws/admin  (live stats, agent events)
  └── AgentPage      → WS  /ws/agent/{agent_id}  (live queue)
                     → POST /api/v1/tickets/action  (call_next, skip…)
```

---

## Setup

### Prerequisites
- Python 3.11+
- PostgreSQL running locally
- Redis running locally (`redis-server`)

### Install
```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env          # then edit .env with your DB credentials
```

### Run
```bash
uvicorn app.main:app --reload --port 8000
```

On first start, tables are created and an admin account is seeded:
- Username: `admin`   Password: `admin1234`

Interactive API docs: **http://localhost:8000/docs**

---

## REST API

### Auth
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/auth/login` | Login → returns `{ access_token, role, agent_id, username }` |

### Agents (AdminPage)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/agents/` | List all active agents |
| POST | `/api/v1/agents/` | Add agent (creates login too) |
| PATCH | `/api/v1/agents/{id}` | Update assignment: category, assigned_service |
| DELETE | `/api/v1/agents/{id}` | Deactivate agent |
| POST | `/api/v1/agents/{id}/change-password` | Agent password change (AgentPage) |

### Queue & Tickets
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/tickets/issue` | Issue a ticket to a citizen |
| POST | `/api/v1/tickets/action` | Agent action: call_next, skip, recall, done, pause, resume |
| GET | `/api/v1/tickets/queue/{agent_id}` | Current queue for an agent |
| GET | `/api/v1/tickets/stats` | Admin dashboard stats |

---

## WebSocket

### Connect
```javascript
// AdminPage
const ws = new WebSocket("ws://localhost:8000/ws/admin");

// AgentPage
const ws = new WebSocket(`ws://localhost:8000/ws/agent/${agentId}`);
```

### Events received (server → client)

| Event type | Received by | Payload |
|------------|-------------|---------|
| `queue_snapshot` | agent room | `{ tickets: [...] }` — sent on connect |
| `stats_updated` | admin room | `{ total_agents, citizens_waiting, … }` |
| `ticket_called` | admin + agent + display | `{ ticket_number, agent_name, waiting_count }` |
| `ticket_skipped` | admin + agent | `{ ticket_number }` |
| `ticket_done` | admin + agent | `{ ticket_number }` |
| `ticket_recalled` | admin + agent + display | `{ ticket_number }` |
| `queue_updated` | admin + agent | `{ queue_id, waiting_count }` |
| `agent_paused` | admin + agent | `{ agent_id, agent_name }` |
| `agent_resumed` | admin + agent | `{ agent_id, agent_name }` |

### Keepalive ping
```javascript
setInterval(() => ws.send(JSON.stringify({ type: "ping" })), 30000);
```

---

## Frontend integration snippet

```typescript
// In AgentPage, replace the mock generateQueue() with:
useEffect(() => {
  const ws = new WebSocket(`ws://localhost:8000/ws/agent/${agentId}`);
  
  ws.onmessage = (e) => {
    const event = JSON.parse(e.data);
    
    if (event.type === "queue_snapshot") {
      setQueue(event.tickets);
    } else if (event.type === "ticket_called") {
      setQueue(prev => prev.map(t =>
        t.number === event.ticket_number ? { ...t, status: "serving" } : t
      ));
      notify(`تم استدعاء التذكرة ${event.ticket_number}`);
      playDing();
    } else if (event.type === "ticket_skipped") {
      setQueue(prev => prev.map(t =>
        t.number === event.ticket_number ? { ...t, status: "skipped" } : t
      ));
    }
  };
  
  // Keepalive
  const ping = setInterval(() => ws.send(JSON.stringify({ type: "ping" })), 30000);
  
  return () => {
    clearInterval(ping);
    ws.close();
  };
}, [agentId]);

// Replace callNext / skipCurrent buttons:
const callNext = async () => {
  await fetch("/api/v1/tickets/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "call_next", agent_id: agentId }),
  });
  // No need to update state — the WebSocket event will do it automatically
};
```

---

## Project structure

```
cnas-backend/
├── app/
│   ├── main.py                   # FastAPI app, startup, Redis subscriber task
│   ├── config.py                 # Settings from .env
│   ├── database.py               # Async SQLAlchemy
│   ├── core/
│   │   ├── redis.py              # Redis client init/close
│   │   ├── websocket_manager.py  # Room manager + Redis → WS bridge
│   │   └── auth.py               # JWT + password hashing
│   ├── models/
│   │   └── models.py             # User, Agent, Service, Queue, Ticket
│   ├── schemas/
│   │   └── schemas.py            # Pydantic in/out models
│   ├── services/
│   │   ├── auth_service.py       # Login logic
│   │   ├── agent_service.py      # Agent CRUD
│   │   └── ticket_service.py     # Queue logic + Redis pub/sub events
│   └── routers/
│       ├── auth.py
│       ├── agents.py
│       ├── tickets.py
│       └── websockets.py         # /ws/admin, /ws/agent/{id}, /ws/display
└── requirements.txt
```
