from __future__ import annotations

from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from app import db

app = FastAPI(title="JML Hybrid", version="1.0.0")

ALLOWED_EVENT_TYPES = {"joiner", "mover", "leaver"}
ADMIN_ROLES = {"HR_ADMIN", "IT_ADMIN", "SEC_ADMIN"}


def require_admin(x_role: Optional[str]) -> str:
    role = (x_role or "WORKER").strip().upper()
    if role not in ADMIN_ROLES:
        raise HTTPException(
            status_code=403,
            detail=f"Forbidden: requires one of {sorted(list(ADMIN_ROLES))}, got {role}",
        )
    return role


class HREvent(BaseModel):
    event_type: str
    employee_id: str
    given_name: Optional[str] = ""
    family_name: Optional[str] = ""
    role: Optional[str] = "WORKER"
    manager: Optional[str] = ""


@app.on_event("startup")
def _startup():
    db.init_db()


@app.get("/")
def health():
    return {
        "service": "jml-hybrid",
        "status": "ok",
        "endpoints": ["/docs", "/hr/event", "/employees", "/events"],
    }


@app.post("/hr/event")
def handle_hr_event(evt: HREvent, x_role: Optional[str] = Header(default=None, alias="X-Role")):
    # only admins can submit HR events
    require_admin(x_role)

    event_type = (evt.event_type or "").strip().lower()
    if event_type not in ALLOWED_EVENT_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid event_type. Use one of {sorted(list(ALLOWED_EVENT_TYPES))}")

    emp_id = (evt.employee_id or "").strip()
    if not emp_id:
        raise HTTPException(status_code=400, detail="employee_id is required")

    # Ensure employee exists FIRST (prevents FK failures)
    db.upsert_employee(
        employee_id=emp_id,
        given_name=(evt.given_name or "").strip(),
        family_name=(evt.family_name or "").strip(),
        role=(evt.role or "WORKER").strip().upper(),
        manager=(evt.manager or "").strip(),
        status="active",
    )

    payload_json = evt.model_dump_json()
    db.log_hr_event(event_type, emp_id, payload_json)

    return {"status": "processed", "employee_id": emp_id, "event_type": event_type}


@app.get("/employees")
def employees(x_role: Optional[str] = Header(default=None, alias="X-Role")) -> List[Dict[str, Any]]:
    require_admin(x_role)
    return db.list_employees()


@app.get("/events")
def events(x_role: Optional[str] = Header(default=None, alias="X-Role")) -> List[Dict[str, Any]]:
    require_admin(x_role)
    return db.list_events(limit=200)
