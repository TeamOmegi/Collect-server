from database import MongoDbClient
from entity import ErrorLog

mongo_client = MongoDbClient.get_mongo_client()


def insert(log: ErrorLog):
    mongo_client.insert_one(log.to_dict())
    