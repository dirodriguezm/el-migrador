from random import randint, random
from typing import List
from uuid import uuid4
from multiprocessing import Manager, Pool
from multiprocessing.managers import ListProxy

from pymongo import InsertOne


def _generate_single_non_detection(object: dict) -> InsertOne:
    _id = str(uuid4())
    return InsertOne(
        {
            "_id": _id,
            "aid": object["aid"],
            "oid": object["oid"][randint(0, len(object["oid"]) - 1)],
            "tid": object["extra_fields"]["tid"][
                randint(0, len(object["extra_fields"]["tid"]) - 1)
            ],
            "mjd": object["firstmjd"]
            + random() * (object["lastmjd"] - object["firstmjd"]),
            "diffmaglim": 20 + random() * 5,
            "fid": randint(1, 4),
            "extra_fields": {},
        }
    )


def append_non_detection(objects: list, non_detections: ListProxy):
    for obj in objects:
        ndet = obj["ndet"]
        for i in range(ndet):
            non_detections.append(_generate_single_non_detection(obj))


def generate_non_detection(objects: List[dict]) -> List[InsertOne]:
    non_detections = Manager().list()

    nprocess = 8
    size = int(len(objects) / nprocess)

    with Pool(processes=nprocess) as pool:
        pool_args = []
        for i in range(nprocess):
            start = i * size
            end = (i + 1) * size
            if i == nprocess - 1:
                end = len(objects)
            pool_args.append((objects[start:end], non_detections))
        pool.starmap(append_non_detection, pool_args)
        pool.close()
        pool.join()
    return list(non_detections)
