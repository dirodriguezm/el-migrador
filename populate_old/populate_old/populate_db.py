from multiprocessing.managers import ListProxy
from typing import List

from pymongo.database import Database
from dbconn import db
from pymongo import InsertOne
from generate_object import generate_object
from generate_detection import generate_detection
from generate_non_detection import generate_non_detection
from multiprocessing import Manager, Pool


def bulk_write_commands(db: Database, collection: str, commands):
    db[collection].bulk_write(commands, ordered=False)


def multi_bulk_write(db: Database, collection: str, commands):
    nprocess = 8
    size = int(len(commands) / nprocess)
    with Pool(processes=nprocess) as pool:
        pool_args = []
        for i in range(nprocess):
            start = i * size
            end = (i + 1) * size
            if i == nprocess - 1:
                end = len(commands)
            pool_args.append(
                (
                    db,
                    collection,
                    commands[start:end],
                )
            )
        pool.starmap(bulk_write_commands, pool_args)
        pool.close()
        pool.join()


if __name__ == "__main__":
    ## Objects
    print("Generating objects")
    objects, commands = generate_object(10000)
    print(f"Generated {len(objects)} objects")
    print("Writing objects")
    bulk_write_commands(db, "object", commands)
    print(f"Wrote {len(objects)} objects")

    ## Detections
    print("Generating detections")
    detections = generate_detection(objects)
    print(f"Generated {len(detections)} detections")
    print("Writing detections")
    bulk_write_commands(db, "detection", detections)
    print(f"Wrote {len(detections)} detections")

    ## Non-detections
    print("Generating non-detections")
    non_detections = generate_non_detection(objects)
    print(f"Generated {len(non_detections)} non-detections")
    print("Writing non-detections")
    bulk_write_commands(db, "non_detection", non_detections)
    print(f"Wrote {len(non_detections)} non-detections")
