from __future__ import annotations

import json
from enum import Enum
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

from app.db import (
    init_db,
    upsert_employee,
    set_employee_status,
    log_hr_event,
    list_employees,
    list_hr_events,
)

class EventType(str, Enum):
    joiner = "joiner"
    mover = "mover"
    leaver = "leaver"

class HREvent(BaseModel):
    event_type: EventType
    employee_id: str
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    role: Optional[str] = None
    manager: Optional[str] = None

app = FastAPI(title="JML Hybrid IT Infrastructure (Demo)")

@app.on_event("startup")
def _startup():
    init_db()

@app.get("/")
def root():
    return {
        "service": "jml-hybrid",
        "status": "ok",
        "endpoints": ["/docs", "/hr/event", "/employees", "/events"],
    }

@app.post("/hr/event")
def handle_hr_event(evt: HREvent):
    emp_id = evt.employee_id.strip()

    # Always ensure employee exists before logging event (prevents FK failure)
    upsert_employee(
        employee_id=emp_id,
        given_name=(evt.given_name or "").strip(),
        family_name=(evt.family_name or "").strip(),
        role=(evt.role or "").strip(),
        manager=(evt.manager or "").strip(),
        status="active",
    )

    # Simple JML logic
    if evt.event_type == EventType.leaver:
        set_employee_status(emp_id, "inactive")

    payload_json = json.dumps(evt.dict(), ensure_ascii=False)
    log_hr_event(evt.event_type.value, emp_id, payload_json)

    return {"status": "processed", "employee_id": emp_id, "message": "ok"}

@app.get("/employees")
def get_employees():
    return list_employees()

@app.get("/events")
def get_events():
    return list_hr_events(limit=200)
