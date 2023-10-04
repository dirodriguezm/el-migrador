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


def read_env_variables(which="SOURCE"):
    host = os.getenv(f"MONGODB_HOST_{which}", "localhost")
    port = int(os.getenv(f"MONGODB_PORT_{which}", 27017))
    database = os.getenv(f"MONGODB_DATABASE_{which}", "")
    username = os.getenv(f"MONGODB_USERNAME_{which}", "")
    password = os.getenv(f"MONGODB_PASSWORD_{which}", "")
    auth_source = os.getenv(f"MONGODB_AUTH_SOURCE_{which}", "admin")
    return host, port, database, username, password, auth_source


def create_mongo_connections():
    print("connecting to source database")
    host, port, database, username, password, auth_source = read_env_variables("SOURCE")
    source_client = MongoClient(
        host=host,
        port=port,
        username=username,
        password=password,
        authSource=auth_source,
        document_class=RawBSONDocument,
    )
    source_db = source_client[database]
    print("connecting to target database")
    host, port, database, username, password, auth_source = read_env_variables("TARGET")
    target_client = MongoClient(
        host=host,
        port=port,
        username=username,
        password=password,
        authSource=auth_source,
        document_class=RawBSONDocument,
    )
    target_db = target_client[database]
    return source_db, target_db
