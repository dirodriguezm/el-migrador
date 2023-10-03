from typing import Callable, List
from pymongo.change_stream import CollectionChangeStream

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
    db: Database, collection: str, operations: List[InsertOne], dry_run=True
):
    # start session to avoid timeout
    # see https://www.mongodb.com/docs/v4.4/reference/method/cursor.noCursorTimeout/#session-idle-timeout-overrides-nocursortimeout
    if dry_run:
        print(f"Writing {len(operations)} objects")
    else:
        db[collection].bulk_write(operations, ordered=False)


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


def migrate(
    collection: str,
    transform_operation: Callable[[dict], dict],
    read_batch_size: int,
    write_batch_size: int,
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
        operations = []
        times = []
        time0 = time.time()
        for document in cursor:
            decoded_document = bson.decode(document.raw)
            op = InsertOne(transform_operation(decoded_document))
            operations.append(op)
            if len(operations) == write_batch_size:
                write_bulk_operations(
                    target_db, collection, operations, dry_run=dry_run
                )
                time1 = time.time()
                times.append(time1 - time0)
                operations = []
                time0 = time.time()

        if len(operations):
            write_bulk_operations(target_db, collection, operations, dry_run=dry_run)
            time1 = time.time()
            times.append(time1 - time0)
            operations = []

    assert len(operations) == 0
    session.end_session()
    print(f"Average time for {collection}: {sum(times)/len(times)}")
    print(f"Total time for {collection}: {sum(times)}")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        dry_run = sys.argv[1] == "--dry-run"
    else:
        dry_run = False

    read_batch_size = 10000
    write_batch_size = 10000

    p_object = Process(
        target=migrate,
        args=(
            "object",
            transform_object,
            read_batch_size,
            write_batch_size,
            dry_run,
        ),
    )
    p_detection = Process(
        target=migrate,
        args=(
            "detection",
            transform_detection,
            read_batch_size,
            write_batch_size,
            dry_run,
        ),
    )
    p_non_detection = Process(
        target=migrate,
        args=(
            "non_detection",
            transform_non_detection,
            read_batch_size,
            write_batch_size,
            dry_run,
        ),
    )

    p_object.start()
    p_detection.start()
    p_non_detection.start()
