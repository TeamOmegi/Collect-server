from typing import List

from sqlmodel import Session

from database.MySqlClient import get_database
from entity.Error import Error


def insert(error: Error, session: Session) -> Error:
    session.add(error)
    session.flush()
    session.refresh(error)
    return error


def find_all(session: Session) -> List[Error]:
    return session.query(Error).all()


print(find_all(get_database()))
