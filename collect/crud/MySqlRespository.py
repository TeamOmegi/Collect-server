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

def insertServiceLink(serviceLink: ServiceLink, session: Session) -> int:
    session.add(serviceLink)
    session.flush()
    session.refresh(serviceLink)
    service_link_id=serviceLink.service_link_id
    session.commit()
    return service_link_id


def find_all(session: Session) -> List[Error]:
    return session.query(Error).all()
