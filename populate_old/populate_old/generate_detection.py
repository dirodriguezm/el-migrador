from multiprocessing.managers import ListProxy, ValueProxy
from typing import List
from random import randint, random
from multiprocessing import Pool, Manager
from uuid import uuid4
from pymongo import InsertOne


def _generate_single_detection(object: dict) -> InsertOne:
    candid = str(uuid4())
    return InsertOne(
        {
            "_id": candid,
            "candid": candid,
            "tid": object["extra_fields"]["tid"][
                randint(0, len(object["extra_fields"]["tid"]) - 1)
            ],
            "aid": object["aid"],
            "oid": object["oid"][randint(0, len(object["oid"]) - 1)],
            "mjd": object["firstmjd"]
            + random() * (object["lastmjd"] - object["firstmjd"]),
            "fid": randint(1, 4),
            "ra": object["meanra"] + random() * object["extra_fields"]["e_ra"],
            "dec": object["meandec"] + random() * object["extra_fields"]["e_dec"],
            "rb": None,
            "mag": 20 + random() * 5,
            "e_mag": random(),
            "rfid": None,
            "e_ra": object["extra_fields"]["e_ra"],
            "e_dec": object["extra_fields"]["e_dec"],
            "isdiffpos": randint(0, 1),
            "corrected": random() > 0.5,
            "parent_candid": None,
            "has_stamp": True,
            "step_id_corr": None,
            "rbversion": None,
            "extra_fields": {},
        }
    )


def append_detection(objects: list, detections: ListProxy):
    for obj in objects:
        ndet = obj["ndet"]
        for i in range(ndet):
            detections.append(_generate_single_detection(obj))


def generate_detection(objects: List[dict]) -> List[InsertOne]:
    detections = Manager().list()

    nprocess = 8
    size = int(len(objects) / nprocess)

    with Pool(processes=nprocess) as pool:
        pool_args = []
        for i in range(nprocess):
            start = i * size
            end = (i + 1) * size
            if i == nprocess - 1:
                end = len(objects)
            pool_args.append((objects[start:end], detections))
        pool.starmap(append_detection, pool_args)
        pool.close()
        pool.join()
    return list(detections)
