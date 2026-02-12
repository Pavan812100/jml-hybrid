from typing import Optional, Literal
from pydantic import BaseModel

EventType = Literal["joiner", "mover", "leaver"]

class HREvent(BaseModel):
    event_type: EventType
    employee_id: str
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    role: Optional[str] = None
    manager: Optional[str] = None

class APIResponse(BaseModel):
    status: str
    employee_id: str
    message: str
