from typing import List
from sqlmodel import Session
from entity.Error import Error


def insert(error: Error, session: Session) -> int:
    session.add(error)
    session.flush()
    session.refresh(error)
    error_id = error.error_id
    session.commit()
    return error_id


def find_all(session: Session) -> List[Error]:
    return session.query(Error).all()
