import logging
import pykka
from typing import List

from pymongo import InsertOne


class MongoDetectionOperationActor(pykka.ThreadingActor):
    def __init__(self, mongo_writer_actor: pykka.ActorRef, write_batch_size: int):
        super().__init__()
        self.mongo_writer_actor = mongo_writer_actor
        self.operations = {}
        self.logger = logging.getLogger("MongoDetectionOperationActor")
        self.write_batch_size = write_batch_size

    def on_receive(self, message: List[dict]) -> None:
        self.logger.debug(f"Creating operations for {len(message)} detections")
        for detection in message:
            operation = InsertOne(detection)
            self.operations[detection["_id"]] = operation
            if len(self.operations.keys()) == self.write_batch_size:
                self.send_operations()

    def send_operations(self):
        self.mongo_writer_actor.tell(self.operations)
        self.operations = {}

    def on_stop(self) -> None:
        self.logger.debug("Sending last operations")
        self.send_operations()
