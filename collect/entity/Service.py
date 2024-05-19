from sqlalchemy import Column, Boolean, Integer, String, TIMESTAMP, Text, Date
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from entity.Base import Base


class Service(Base):
    __tablename__ = 'service'
    project_id = Column(Integer, nullable=False)
    service_id = Column(Integer, primary_key=True, autoincrement=True)
    service_token_id = Column(Integer, nullable=False)
    service_type_id = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, default=func.now())
    updated_at = Column(TIMESTAMP, onupdate=func.now())