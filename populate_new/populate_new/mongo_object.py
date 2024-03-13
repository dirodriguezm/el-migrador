import pykka
import logging


class MongoObjectWriterActor(pykka.ThreadingActor):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("MongoObjectOperationActor")
