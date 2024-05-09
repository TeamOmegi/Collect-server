from typing import Optional
from pydantic import BaseModel


class Work(BaseModel):
    trace_id: str
    project_id: str
    service_id: str
    count: int
    error_trace: Optional[dict]