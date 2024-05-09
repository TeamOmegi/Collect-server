import pymongo
from dotenv import load_dotenv
import os

load_dotenv()


def get_mongo_client() -> pymongo.collection.Collection:
   client = __connect_to_mongodb__()
   db = __get_database__(client, os.getenv("MONGO_DB_NAME"))
   return __get_collection__(db, os.getenv("MONGO_COLLECTION_NAME"))


def __connect_to_mongodb__() -> pymongo.MongoClient:
   uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{str(os.getenv('MONGO_PORT'))}/"
   print(uri)
   return pymongo.MongoClient(uri)


def __get_database__(client: pymongo.MongoClient, database_name: str) -> pymongo.database.Database:
   return client[database_name]


def __get_collection__(database: pymongo.database.Database, collection_name: str) -> pymongo.collection.Collection:
   return database[collection_name]
