import logging
from typing import Dict
import pykka
from pymongo import InsertOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError
import time


class MongoWriterActor(pykka.ThreadingActor):
    """Actor to write documents to the database.

    This actor receives a dictionary of operations and writes them to the database.

    Parameters
    ----------
    db : Database
        The database to write to
    batch_size : int
        Number of documents to write in a batch
    dry_run : bool
        If True, the actor will not write to the database
    """
    def __init__(
        self,
        db: Database,
        batch_size: int,
        dry_run: bool = False,
    ):
        super().__init__()
        self.db = db
        self.dry_run = dry_run
        self.logger = logging.getLogger("MongoWriterActor")
        self.time_logger = TimeLogger(batch_size, batch_size * 10)

    def on_receive(self, operations: Dict[str, InsertOne]) -> None:
        self.logger.debug(f"Writing {len(operations)} objects")
        if self.dry_run:
            self.logger.debug(f"Writing {len(operations)} objects")
            return
        try:
            # TODO: handle different collections
            operation_values = list(operations.values()) 
            self.db["detection"].bulk_write(operation_values, ordered=False)
            self.time_logger.tell(
                {"type": "increase_counter", "counter": len(operations)}
            )
            operations.clear()
        except BulkWriteError as bwe:
            for err in bwe.details["writeErrors"]:
                repeated_op = operation_values[err["index"]]
                repeated_key = (repeated_op.document["candid"], repeated_op.document["oid"])
                operations.pop(repeated_key)
            self.db["detection"].bulk_write(list(operations.values()), ordered=False)
            operations.clear()
        finally:
            self.time_logger.tell({"type": "log_times"})
            return

    def on_stop(self) -> None:
        self.time_logger.tell({"type": "summary"})


class TimeLogger:
    """Class to log the time it takes to write to the database.

    This class logs the time it takes to write to the database and the average time it takes to write
    to the database.

    Parameters
    ----------
    write_batch_size : int
        Number of documents to write
    eval_every : int
        Number of documents to write before logging the average time
    """
    def __init__(self, write_batch_size: int, eval_every: int):
        super().__init__()
        self.t0 = time.time()
        self.times = []
        self.counter = 0
        self.write_batch_size = write_batch_size
        self.eval_every = eval_every
        self.logger = logging.getLogger("TimeLogger")

    def on_receive(self, message: dict) -> None:
        if message["type"] == "log_times":
            self.times.append(time.time() - self.t0)
            self.t0 = time.time()
            if self.counter % self.eval_every == 0:
                self.logger.info(f"Processed {self.counter} detections")
                self.logger.info(f"Average time: {sum(self.times)/len(self.times)}")
                self.logger.info(f"Elapsed time: {sum(self.times)}")
        elif message["type"] == "increase_counter":
            self.counter += message["counter"]
        elif message["type"] == "summary":
            average = sum(self.times) / len(self.times)
            total = sum(self.times)
            persec = self.write_batch_size / average
            self.logger.info("")
            self.logger.info(f"Average time per batch: {average} seconds")
            self.logger.info(f"Total time for detection: {total} seconds")
            self.logger.info(f"Detections per second: {persec}")
        else:
            raise ValueError(f"Unknown message {message}")

    def tell(self, what: dict):
        self.on_receive(what)
