from typing import Callable, Dict
from pymongo.change_stream import CollectionChangeStream
from pymongo.errors import BulkWriteError
from pymongo.cursor import Cursor
from dbconn import create_mongo_connections
from pymongo import InsertOne
from pymongo.database import Database
from transform_object import transform_object
from transform_detection import transform_detection
from transform_non_detection import transform_non_detection
from multiprocessing import Process
import bson
import time
import sys


def write_bulk_operations(
    db: Database, collection: str, operations: Dict[str, InsertOne], dry_run=True
):
    # start session to avoid timeout
    # see https://www.mongodb.com/docs/v4.4/reference/method/cursor.noCursorTimeout/#session-idle-timeout-overrides-nocursortimeout
    if dry_run:
        print(f"Writing {len(operations)} objects")
    else:
        try:
            db[collection].bulk_write(list(operations.values()), ordered=False)
        except BulkWriteError as bwe:
            for err in bwe.details["writeErrors"]:
                repeated_id = err["keyValue"]["_id"]
                operations.pop(repeated_id)
            db[collection].bulk_write(list(operations.values()), ordered=False)
        finally:
            return


def get_cursor(
    db: Database,
    collection: str,
    session,
    batch_size,
    cursor_type: str = "find",
) -> Cursor | CollectionChangeStream:
    if cursor_type == "find":
        cursor = db[collection].find(
            batch_size=batch_size,
            session=session,
            no_cursor_timeout=True,
        )
    elif cursor_type == "watch":
        cursor = db[collection].watch(batch_size=batch_size, session=session)
    else:
        raise ValueError(f"cursor_type must be 'find' or 'watch', not {cursor_type}")
    return cursor


def eval_performance(times: list, counter: int, collection: str):
    print(f"Processed {counter} {collection}")
    print(f"Average time: {sum(times)/len(times)}")
    print(f"Partial time: {sum(times)}")


def migrate(
    collection: str,
    transform_operation: Callable[[dict], dict],
    read_batch_size: int,
    write_batch_size: int,
    eval_every: int,
    dry_run: bool,
    cursor_type: str = "find",
):
    source_db, target_db = create_mongo_connections()
    # start session to avoid timeout
    # see https://www.mongodb.com/docs/v4.4/reference/method/cursor.noCursorTimeout/#session-idle-timeout-overrides-nocursortimeout
    with source_db.client.start_session() as session:
        cursor = get_cursor(
            source_db,
            collection,
            session,
            read_batch_size,
            cursor_type,
        )
        operations = {}
        times = []
        time0 = time.time()
        counter = 0
        for document in cursor:
            counter += 1
            decoded_document = bson.decode(document.raw)
            transformed_document = transform_operation(decoded_document)
            if transformed_document["_id"] not in operations:
                op = InsertOne(transformed_document)
                operations[transformed_document["_id"]] = op
            if len(operations.keys()) == write_batch_size:
                write_bulk_operations(
                    target_db, collection, operations, dry_run=dry_run
                )
                time1 = time.time()
                times.append(time1 - time0)
                operations = {}
                time0 = time.time()
            if counter % eval_every == 0:
                eval_performance(times, counter, collection)

        if len(operations.keys()):
            write_bulk_operations(target_db, collection, operations, dry_run=dry_run)
            time1 = time.time()
            times.append(time1 - time0)
            operations = {}

    assert len(operations) == 0
    session.end_session()
    print(f"Average time for {collection}: {sum(times)/len(times)}")
    print(f"Total time for {collection}: {sum(times)}")


if __name__ == "__main__":
    if len(sys.argv) == 6:
        dry_run = sys.argv[5] == "--dry-run"
    else:
        dry_run = False

    read_batch_size = int(sys.argv[1])
    write_batch_size = int(sys.argv[2])
    eval_every = int(sys.argv[3])
    valid_collections = {
        "object": transform_object,
        "detection": transform_detection,
        "non_detection": transform_non_detection,
    }
    collections = sys.argv[4].split(",")
    for collection in collections:
        if collection not in valid_collections:
            raise ValueError(
                f"Invalid collection {collection}. Valid collections are: object, detection, non_detection"
            )
        p = Process(
            target=migrate,
            args=(
                collection,
                valid_collections[collection],
                read_batch_size,
                write_batch_size,
                eval_every,
                dry_run,
            ),
        )
        p.start()
