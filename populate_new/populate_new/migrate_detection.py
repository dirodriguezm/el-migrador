import pykka
from group_detection import GroupDetectionActor
from mongo_detection import MongoDetectionOperationActor
from mongo_writer import MongoWriterActor
from mongo_object import MongoObjectOperationActor
from sorting_hat import SortingHatActor
from transform_detection import TransformDetectionActor
from dbconn import create_mongo_connections
import bson
import logging

logging.basicConfig(level=logging.INFO)


def get_detections_cursor(session, db, batch_size):
    cursor = db["detection"].find(
        batch_size=batch_size,
        session=session,
        no_cursor_timeout=True,
    )
    return cursor


def documents(cursor):
    for document in cursor:
        decoded_document = bson.decode(document.raw)
        yield decoded_document


def migrate_detection(read_batch_size: int, write_batch_size: int, dry_run=True):
    source_db, target_db = create_mongo_connections()
    with source_db.client.start_session() as session:
        detections_cursor = get_detections_cursor(session, source_db, read_batch_size)
        writer_actor = MongoWriterActor.start(target_db, write_batch_size, dry_run)
        detection_operation_actor = MongoDetectionOperationActor.start(
            writer_actor, write_batch_size
        )
        object_operation_actor = MongoObjectOperationActor.start()
        sorting_hat_actor = SortingHatActor.start(
            detection_operation_actor, object_operation_actor
        )
        grouper_actor = GroupDetectionActor.start(sorting_hat_actor, write_batch_size)
        transform_actor = TransformDetectionActor.start(grouper_actor)
        for detection in documents(detections_cursor):
            transform_actor.tell(detection)

    pykka.ActorRegistry.stop_all()
