from pymongo import MongoClient
from db_plugins.db.mongo._connection import MongoConnection
from bson.raw_bson import RawBSONDocument
import os


# import models so that they can be created by MongoConnection
from db_plugins.db.mongo.models import Object, Detection, NonDetection, ForcedPhotometry


def create_indexes():
    db = MongoConnection(
        {
            "host": "localhost",
            "serverSelectionTimeoutMS": 3000,  # 3 second timeout
            "port": 27018,
            "database": "new_db",
        }
    )
    db.create_db()


def create_mongo_connections():
    print("connecting to source database")
    url = os.getenv("SOURCE_MONGODB_URL", "mongodb://localhost:27017/")
    source_client = MongoClient(url, document_class=RawBSONDocument)
    source_database = url.split("/")[-1]
    source_db = source_client[source_database]
    print("connecting to target database")
    url = os.getenv("TARGET_MONGODB_URL", "mongodb://localhost:27018/")
    target_client = MongoClient(url, document_class=RawBSONDocument)
    target_database = url.split("/")[-1]
    target_db = target_client[target_database]
    return source_db, target_db
