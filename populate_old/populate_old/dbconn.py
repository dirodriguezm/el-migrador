from db_plugins.db.mongo import MongoConnection
from pymongo.database import Database

# Create a connection to the database
db_conn = MongoConnection()
db_settings = {
    "HOST": "localhost",
    "USER": "",
    "PASSWORD": "",
    "PORT": 27017,
    "DATABASE": "old_db",
}

db_conn.connect(db_settings)
db_conn.create_db()

db: Database = db_conn.database
