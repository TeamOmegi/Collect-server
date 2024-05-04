from sqlmodel import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote
from dotenv import load_dotenv
import os

load_dotenv()


def get_database():
    db_url = f'mysql+pymysql://{os.getenv("DATABASE_USER")}:{quote(os.getenv("DATABASE_PASSWORD"))}@{os.getenv("DATABASE_HOST")}:{os.getenv("DATABASE_PORT")}/{os.getenv("DATABASE_NAME")}'
    print("Db "+db_url)
    db_engine = create_engine(db_url, echo=True)
    session = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    return session()
