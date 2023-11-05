from random import random, randint
from typing import List
from uuid import uuid4


def generate_oid() -> List[str]:
    random_number = randint(1, 10)
    return [f"oid{i}" for i in range(random_number)]


def generate_tid() -> List[str]:
    tids = []
    for _ in range(2):
        random_number = randint(1, 2)
        tid = "ZTF" if random_number % 2 else "ATLAS"
        if tid not in tids:
            tids.append(tid)
    return tids


def generate_object(idx: int) -> dict:
    ra = random()
    dec = random()
    obj = {
        "_id": f"aid{idx}",
        "aid": f"aid{idx}",
        "oid": generate_oid(),
        "lastmjd": random(),
        "firstmjd": random(),
        "ndet": randint(1, 5),
        "meanra": ra,
        "meandec": dec,
        "loc": {"type": "Point", "coordinates": [ra - 180, dec]},
        "extra_fields": {
            "e_ra": random(),
            "e_dec": random(),
            "tid": generate_tid(),
        },
    }
    return obj


def generate_objects(n: int):
    for i in range(n):
        yield generate_object(i)


def _generate_single_detection(obj: dict) -> dict:
    candid = str(uuid4())
    return {
        "_id": candid,
        "candid": candid,
        "tid": obj["extra_fields"]["tid"][
            randint(0, len(obj["extra_fields"]["tid"]) - 1)
        ],
        "aid": obj["aid"],
        "oid": obj["oid"][randint(0, len(obj["oid"]) - 1)],
        "mjd": obj["firstmjd"] + random() * (obj["lastmjd"] - obj["firstmjd"]),
        "fid": randint(1, 4),
        "ra": obj["meanra"] + random() * obj["extra_fields"]["e_ra"],
        "dec": obj["meandec"] + random() * obj["extra_fields"]["e_dec"],
        "rb": None,
        "mag": 20 + random() * 5,
        "e_mag": random(),
        "rfid": None,
        "e_ra": obj["extra_fields"]["e_ra"],
        "e_dec": obj["extra_fields"]["e_dec"],
        "isdiffpos": randint(0, 1),
        "corrected": random() > 0.5,
        "parent_candid": None,
        "has_stamp": True,
        "step_id_corr": None,
        "rbversion": None,
        "extra_fields": {},
    }


def generate_detections(objects: List[dict]):
    for obj in objects:
        for _ in range(obj["ndet"]):
            yield _generate_single_detection(obj)
