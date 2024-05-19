from typing import List

from sqlmodel import Session
from entity.Error import Error
from entity.Service import Service
from entity.Project import Project
from entity.ServiceLink import ServiceLink


def insert(error: Error, session: Session) -> int:
    session.add(error)
    session.flush()
    session.refresh(error)
    error_id = error.error_id
    session.commit()
    return error_id


def insert_service_link(service_link: ServiceLink, session: Session) -> int:
    session.add(service_link)
    session.flush()
    session.refresh(service_link)
    service_link_id = service_link.service_link_id
    session.commit()
    return service_link_id


def check_service_link_exists(service_id, linked_service_id, session: Session) -> bool:
    return session.query(ServiceLink).filter(ServiceLink.linked_service_id == linked_service_id). \
        filter(ServiceLink.service_id == service_id). \
        first() is not None


def find_all(session: Session) -> List[Error]:
    return session.query(Error).all()


def check_service_exists(service_id, session: Session) -> bool:
    return session.query(Service).filter(Service.service_id == service_id).exists()


def check_project_exists(project_id, session: Session) -> bool:
    return session.query(Project).filter(Project.project_id == project_id).exists()