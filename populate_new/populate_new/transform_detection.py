import logging
import pykka
from typing import List


class TransformSingleDetectionActor(pykka.ThreadingActor):
    def on_receive(self, message: List[dict]) -> List[dict]:
        return [self.transform_detection(detection) for detection in message]

    def transform_detection(self, document: dict) -> dict:
        new_detection = {
            "candid": document["_id"],
            "tid": document["tid"],
            "sid": self.get_sid(document["tid"]),
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
        if type(new_extra_fields) is not dict:
            new_extra_fields = {}
        new_extra_fields.update(
            {k: v for k, v in document.items() if k not in new_detection}
        )
        # delete candid from extra fields if it exists
        new_extra_fields.pop("candid", None)
        new_detection["extra_fields"] = new_extra_fields
        return new_detection

    def get_sid(self, tid: str):
        sid = ""
        if "ZTF" in tid or "ztf" in tid:
            sid = "ZTF"
        elif "ATLAS" in tid or "atlas" in tid:
            sid = "ATLAS"
        elif "LSST" in tid or "lsst" in tid:
            sid = "LSST"
        return sid


class TransformDetectionActor(pykka.ThreadingActor):
    """Actor to transform detections before sending them to the grouper actor.

    This actor will parse the detections into the new schema and send them to the grouper actor.
    It uses a pool of TransformSingleDetectionActor to parallelize the transformation process.
    """
    def __init__(self, grouper_actor: pykka.ActorRef, num_transformers: int):
        super().__init__()
        self.grouper_actor = grouper_actor
        self.logger = logging.getLogger("TransformDetectionActor")
        self.transform_actors = [
            TransformSingleDetectionActor.start()
        ] * num_transformers

    def on_receive(self, message: List[dict]) -> None:
        self.logger.debug(f"Transforming {len(message)} detections")
        batch = []
        n_coroutines = len(self.transform_actors)
        start = 0
        end = int(len(message) / n_coroutines)
        for ta in self.transform_actors:
            future = ta.ask(message[start:end], block=False)
            batch.append(future)
            start = end
            end = (
                end + int(len(message) / n_coroutines)
                if end + int(len(message) / n_coroutines) < len(message)
                else len(message)
            )
        result = batch[0].join(*batch[1:])
        for res in result.get():
            self.grouper_actor.tell(res)

    def on_stop(self):
        for ta in self.transform_actors:
            ta.stop()
