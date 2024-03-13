import pykka
import pykka.debug
from group_detection import GroupDetectionActor
from mongo_detection import MongoDetectionWriterActor
from mongo_writer import MongoWriterActor
from mongo_object import MongoObjectWriterActor
from sorting_hat import SortingHatActor
from transform_detection import TransformDetectionActor
from dbconn import create_mongo_connections
import bson
import logging
import signal


logging.basicConfig(level=logging.INFO)
signal.signal(signal.SIGUSR1, pykka.debug.log_thread_tracebacks)


def get_detections_cursor(session, db, batch_size):
    cursor = db["detection"].find(
        batch_size=batch_size,
        session=session,
        no_cursor_timeout=True,
    )
    return cursor


def get_batch_from_db(cursor):
    batch = []
    for document in cursor:
        decoded_document = bson.decode(document.raw)
        batch.append(decoded_document)
        if len(batch) == 2000:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


def start_actors(target_db, write_batch_size, dry_run):
    writer_actor = MongoWriterActor.start(target_db, write_batch_size, dry_run)
    detection_operation_actor = MongoDetectionWriterActor.start(
        writer_actor, write_batch_size
    )
    object_operation_actor = MongoObjectWriterActor.start()
    sorting_hat_actor = SortingHatActor.start(
        detection_operation_actor, object_operation_actor, target_db
    )
    grouper_actor = GroupDetectionActor.start(sorting_hat_actor, write_batch_size)
    transform_actor = TransformDetectionActor.start(grouper_actor, num_transformers=5)
    return transform_actor


def migrate_detection(read_batch_size: int, write_batch_size: int, dry_run=True):
    source_db, target_db = create_mongo_connections()
    with source_db.client.start_session() as session:
        detections_cursor = get_detections_cursor(session, source_db, read_batch_size)
        transform_actor = start_actors(target_db, write_batch_size, dry_run)
        for batch in get_batch_from_db(detections_cursor):
            transform_actor.tell(batch)

    pykka.ActorRegistry.stop_all()
