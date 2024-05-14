from sqlalchemy import Column, Integer, Boolean, TIMESTAMP, func

from entity.Base import Base


class ServiceLink(Base):
    __tablename__ = 'service_link'
    service_link_id = Column(Integer, primary_key=True, autoincrement=True)
    service_id = Column(Integer, nullable=True)
    linked_service_id = Column(Integer, nullable=False)
    enabled = Column(Boolean, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())
