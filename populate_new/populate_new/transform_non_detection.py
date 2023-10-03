from typing import List


def transform_non_detection(document: dict) -> dict:
    def get_sid(tid: str):
        sid = ""
        if "ZTF" in tid or "ztf" in tid:
            sid = "ZTF"
        elif "ATLAS" in tid or "atlas" in tid:
            sid = "ATLAS"
        elif "LSST" in tid or "lsst" in tid:
            sid = "LSST"
        return sid

    new_non_detection = {
        "_id": document["_id"],
        "aid": document["aid"],
        "tid": document["tid"],
        "sid": get_sid(document["tid"]),
        "oid": document["oid"],
        "mjd": document["mjd"],
        "fid": document["fid"],
        "diffmaglim": document["diffmaglim"],
        "extra_fields": document["extra_fields"],
    }
    return new_non_detection
