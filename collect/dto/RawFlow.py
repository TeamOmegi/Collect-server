from pydantic import BaseModel
from datetime import datetime


class RawFlow(BaseModel):
    trace_id: str
    project_id: int
    service_id: int
    service_name: str
    span_id: str
    parent_span_id: str
    span_enter_time: datetime
    span_exit_time: datetime
