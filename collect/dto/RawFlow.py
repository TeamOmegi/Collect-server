from pydantic import BaseModel
from datetime import datetime


class RawFlow(BaseModel):
    trace_id: str
    project_id: int
    service_id: int
    span_id: str
    parent_span_id: str
    span_enter_time: datetime
    span_exit_time: datetime

    def dict(self, *args, **kwargs):
        raw_flow_dict = super().dict(*args, **kwargs)
        raw_flow_dict['span_enter_time'] = self.span_enter_time.isoformat()
        raw_flow_dict['span_exit_time'] = self.span_exit_time.isoformat()
        return raw_flow_dict
