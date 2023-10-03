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
    print("connecting to target database")
    url = os.getenv("TARGET_MONGODB_URL", "mongodb://localhost:27018/")
    target_client = MongoClient(url, document_class=RawBSONDocument)
    source_db = source_client.old_db
    target_db = target_client.new_db
    return source_db, target_db
