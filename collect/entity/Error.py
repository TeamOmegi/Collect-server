from sqlalchemy import Column, Boolean, Integer, String, TIMESTAMP, Text, Date
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from entity.Base import Base


class Error(Base):
    __tablename__ = 'error'
    error_id = Column(Integer, primary_key=True)
    note_id = Column(Integer, nullable=True)
    service_id = Column(Integer, nullable=False)
    type = Column(String(255), nullable=False)
    summary = Column(Text, nullable=False)
    time = Column(TIMESTAMP, nullable=False)
    solved = Column(Boolean, nullable=False, default=False)
    created_at = Column(TIMESTAMP, nullable=False, default=func.now())
    updated_at = Column(TIMESTAMP, onupdate=func.now())
