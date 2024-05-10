from bson import ObjectId

from database import MongoDbClient
from dto import ErrorLog

mongo_client = MongoDbClient.get_mongo_client()


def insert(log: ErrorLog):
    result = mongo_client.insert_one(log.to_dict())
    return str(result.inserted_id)


def find_by_id(log_id: str):
    result = mongo_client.find_one({"_id": ObjectId(log_id)})
    if result:
        return result
    return None
