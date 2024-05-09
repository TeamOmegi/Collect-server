from bson import datetime
from pydantic import BaseModel


class TraceSpan(BaseModel):
    span_id: str
    service_name: str
    name: str
    parent_span_id: str
    kind: str
    attributes: dict
    enter_time: datetime
    exit_time: datetime
