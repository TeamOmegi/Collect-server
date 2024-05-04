from typing import Optional
from pydantic import BaseModel


class ErrorLog(BaseModel):
    project: str
    service: str
    log: str

    def to_dict(self):
        return self.dict(exclude_unset=True) # 값이 설정되지 않은 필드를 제외하는 옵션
