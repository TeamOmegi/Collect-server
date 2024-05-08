from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class TraceSpan(BaseModel):
    span_id: str
    service_name: str
    name: str
    parent_span_id: str
    kind: str
    arguments: List[str]
    enter_time: datetime
    exit_time: datetime
