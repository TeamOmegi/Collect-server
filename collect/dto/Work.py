from typing import Optional
from pydantic import BaseModel


class Work(BaseModel):
    trace_id: str
    project_id: int
    service_id: int
    count: int
    error_trace: Optional[dict]