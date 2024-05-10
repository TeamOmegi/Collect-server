from typing import List

from pydantic import BaseModel


class Flow(BaseModel):
    trace_id: str
    project_id: int
    service_flow_asc: List
