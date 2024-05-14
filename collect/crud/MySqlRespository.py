from typing import List
from sqlmodel import Session
from entity.Error import Error
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
    service_link = session.query(ServiceLink).\
        filter(ServiceLink.service_id == service_id).\
        filter(ServiceLink.linked_service_id == linked_service_id).\
        first()

    if service_link:
        return True
    else:
        return False


def find_all(session: Session) -> List[Error]:
    return session.query(Error).all()
