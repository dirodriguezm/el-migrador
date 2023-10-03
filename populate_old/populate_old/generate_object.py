#!/usr/bin/env python3
from typing import List, Tuple
from random import randint, random
from multiprocessing import Pool, Manager
from multiprocessing.managers import ListProxy
from pymongo import InsertOne


def append_object(start, end, objects: ListProxy, commands: ListProxy):
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

    for j in range(start, end):
        ra = random()
        dec = random()
        object = {
            "_id": f"aid{j}",
            "aid": f"aid{j}",
            "oid": generate_oid(),
            "lastmjd": random(),
            "firstmjd": random(),
            "ndet": randint(1, 100),
            "meanra": ra,
            "meandec": dec,
            "loc": {"type": "Point", "coordinates": [ra - 180, dec]},
            "extra_fields": {
                "e_ra": random(),
                "e_dec": random(),
                "tid": generate_tid(),
            },
        }
        objects.append(object)
        commands.append(InsertOne(object))


def generate_object(number: int) -> Tuple[List[dict], List[InsertOne]]:
    objects = Manager().list()
    commands = Manager().list()

    nprocess = 8
    size = int(number / nprocess)

    with Pool(processes=nprocess) as pool:
        pool_args = []
        for i in range(nprocess):
            start = i * size
            end = (i + 1) * size
            if i == nprocess - 1:
                end = number
            pool_args.append((start, end, objects, commands))
        pool.starmap(append_object, pool_args)
        pool.close()
        pool.join()
    return list(objects), list(commands)
