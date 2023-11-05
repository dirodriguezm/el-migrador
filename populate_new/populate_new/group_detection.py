import logging
from typing import List
import pykka


class GroupDetectionActor(pykka.ThreadingActor):
    def __init__(self, sorting_hat_actor: pykka.ActorRef, max_size: int):
        super().__init__()
        self.sorting_hat_actor = sorting_hat_actor
        self.logger = logging.getLogger("GroupDetectionActor")
        self.groups = {}
        self.size = 0
        self.max_size = max_size

    def on_receive(self, message: List[dict]) -> None:
        self.logger.debug(f"Grouping {len(message)} detections")
        self.size += len(message)
        for detection in message:
            if detection["oid"] in self.groups:
                self.groups[detection["oid"]].append(detection)
            else:
                self.groups[detection["oid"]] = [detection]
            if self.size == self.max_size:
                self.sorting_hat_actor.tell(self.groups)
                self.groups = {}
                self.size = 0

    def on_stop(self) -> None:
        self.logger.debug("Grouping last detection")
        self.sorting_hat_actor.tell(self.groups)
        self.groups = {}
        self.size = 0
