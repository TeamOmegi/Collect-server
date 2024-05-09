from typing import List
from sqlmodel import Session
from entity.Error import Error


def insert(error: Error, session: Session) -> Error:
    session.add(error)
    session.flush()
    session.refresh(error)
    session.commit()
    return error


def find_all(session: Session) -> List[Error]:
    return session.query(Error).all()
