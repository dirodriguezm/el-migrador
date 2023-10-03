from typing import List


def transform_object(document: dict):
    def get_sid(tids: List[str]):
        sid = []
        for tid in tids:
            if "ZTF" in tid:
                sid.append("ZTF")
            elif "ATLAS" in tid:
                sid.append("ATLAS")
            elif "LSST" in tid:
                sid.append("LSST")

        return sid

    transformed_object = {
        "_id": document["aid"],
        "oid": document["oid"],
        "lastmjd": document["lastmjd"],
        "firstmjd": document["firstmjd"],
        "ndet": document["ndet"],
        "meanra": document["meanra"],
        "meandec": document["meandec"],
        "sigmara": document["extra_fields"]["e_ra"],
        "sigmadec": document["extra_fields"]["e_dec"],
        "loc": document["loc"],
        "tid": document["extra_fields"]["tid"],
        "sid": get_sid(document["extra_fields"]["tid"]),
    }

    return transformed_object
