import logging
import pykka


class GroupDetectionActor(pykka.ThreadingActor):
    def __init__(self, sorting_hat_actor: pykka.ActorRef, max_size: int):
        super().__init__()
        self.sorting_hat_actor = sorting_hat_actor
        self.logger = logging.getLogger("GroupDetectionActor")
        self.groups = {}
        self.size = 0
        self.max_size = max_size

    def on_receive(self, message: dict) -> None:
        self.logger.debug(f"Grouping detection {message['_id']}")
        self.size += 1
        if message["oid"] in self.groups:
            self.groups[message["oid"]].append(message)
        else:
            self.groups[message["oid"]] = [message]
        if self.size == self.max_size:
            self.sorting_hat_actor.tell(self.groups)
            self.groups = {}
            self.size = 0

    def on_stop(self) -> None:
        self.logger.debug("Grouping last detection")
        self.sorting_hat_actor.tell(self.groups)
        self.groups = {}
        self.size = 0
