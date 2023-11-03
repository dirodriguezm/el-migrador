import logging
from typing import List, Tuple
import pykka


class SortingHatActor(pykka.ThreadingActor):
    def __init__(
        self,
        detection_operation_actor: pykka.ActorRef,
        object_operation_actor: pykka.ActorRef,
    ):
        super().__init__()
        self.detection_operation_actor = detection_operation_actor
        self.object_operation_actor = object_operation_actor
        self.logger = logging.getLogger("SortingHatActor")

    def on_receive(self, message: dict) -> None:
        self.logger.debug(f"Sorting {len(message)} detections")
        self.assign_aid(message)

    def assign_aid(self, detections):
        """Receives detections and assigns aid
        Parameters
        ----------
        message : dict
            The detections message where each key is the oid and the value is a list of detections

        Examples
        --------
        >>> message = {"oid1": [detection1, detection2], "oid2": [detection3, detection4]}
        """
        detections, found = self.get_aid_by_oid(detections)
        if found:
            self.detection_operation_actor.tell(detections)
            return
        detections, found = self.get_aid_by_conesearch(detections)
        if found:
            self.detection_operation_actor.tell(detections)
            return
        detections, objects = self.new_aid(detections)
        self.detection_operation_actor.tell(detections)
        self.object_operation_actor.tell(objects)

    def get_aid_by_oid(self, detections: dict) -> Tuple[List[dict], bool]:
        """Gets alerce id from database and assigns each detection that id

        If an aid is found in the database, it is assigned to each detection in the message.
        If an aid is not found in the database, the aid is removed from the message.

        Parameters
        ----------
        detections : dict
            Dictionary where each key is the oid and value is a list of detections

        Returns
        -------
        List[dict]
            List of detections with aid assigned

        Examples
        --------
        >>> detections = {"oid1": [detection1, detection2], "oid2": [detection3, detection4]}
        >>> get_aid_by_oid(detections)
        [{"oid": "oid1", "aid": "ALabc123", "mjd": "59000", "mag": 20, ...}]
        """
        dets = []
        found = False
        ## TODO delete this for
        for det_group in detections.values():
            dets.extend(det_group)
        found = True  # TODO delete this line
        return dets, found

    def get_aid_by_conesearch(self, detections: List[dict]) -> Tuple[List[dict], bool]:
        found = False
        return detections, found

    def new_aid(self, detections: List[dict]) -> List[dict]:
        return detections
