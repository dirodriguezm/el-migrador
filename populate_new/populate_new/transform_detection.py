from typing import List


def transform_detection(document: dict) -> dict:
    def get_sid(tid: str):
        sid = ""
        if "ZTF" in tid or "ztf" in tid:
            sid = "ZTF"
        elif "ATLAS" in tid or "atlas" in tid:
            sid = "ATLAS"
        elif "LSST" in tid or "lsst" in tid:
            sid = "LSST"
        return sid

    new_detection = {
        "_id": document["candid"],
        "tid": document["tid"],
        "sid": get_sid(document["tid"]),
        "aid": document["aid"],
        "oid": document["oid"],
        "mjd": document["mjd"],
        "fid": document["fid"],
        "ra": document["ra"],
        "dec": document["dec"],
        "e_ra": document["e_ra"],
        "e_dec": document["e_dec"],
        "mag": document["mag"],
        "e_mag": document["e_mag"],
        "mag_corr": document["mag"] if document["corrected"] else None,
        "e_mag_corr": document["e_mag"] if document["corrected"] else None,
        "e_mag_corr_ext": None,
        "isdiffpos": document["isdiffpos"],
        "corrected": document["corrected"],
        "dubious": None,
        "parent_candid": document["parent_candid"],
        "has_stamp": document["has_stamp"],
    }

    new_extra_fields = document.pop("extra_fields")
    new_extra_fields.update(
        {k: v for k, v in document.items() if k not in new_detection}
    )
    new_detection["extra_fields"] = new_extra_fields

    return new_detection
