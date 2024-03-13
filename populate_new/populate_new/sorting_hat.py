import logging
from typing import List, Tuple, Union
import pykka
from functools import reduce
from pymongo.database import Database
import math


class SortingHatActor(pykka.ThreadingActor):
    """Actor to assign aid to detections before sending them to the detection operation actor.

    This actor assigns aid to detections and sends them to the detection operation actor.

    The actor first tries to get the aid from the database. If the aid is found, it is assigned to
    each detection in the message. If the aid is not found, the actor tries to find the aid by
    cone search. If the aid is found, it is assigned to each detection in the message. If the aid
    is not found, the actor assigns a new aid to each detection in the message.

    Parameters
    ----------
    detection_operation_actor : pykka.ActorRef
        Actor to send detections
    object_operation_actor : pykka.ActorRef
        Actor to send objects
    """

    def __init__(
        self,
        detection_writer_actor: pykka.ActorRef,
        object_writer_actor: pykka.ActorRef,
        db: Database,
    ):
        super().__init__()
        self.detection_writter_actor = detection_writer_actor
        self.object_writer_actor = object_writer_actor
        self.logger = logging.getLogger("SortingHatActor")
        self.db = db

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
        >>> assign_aid(message)
        """
        detections, found = self.get_aid_by_oid(detections)
        if found:
            self.detection_writter_actor.tell(
                list(reduce(lambda x, y: x + y, detections.values(), []))
            )
            return
        detections, found = self.get_aid_by_conesearch(detections)
        if found:
            self.detection_writter_actor.tell(
                list(reduce(lambda x, y: x + y, detections.values(), []))
            )
            return
        detections, objects = self.new_aid(detections)
        self.detection_writter_actor.tell(
            list(reduce(lambda x, y: x + y, detections.values(), []))
        )
        self.object_writer_actor.tell(objects)

    def get_aid_by_oid(self, detections: dict) -> Tuple[dict, bool]:
        """Gets alerce id from database and assigns each detection that id

        If an aid is found in the database, it is assigned to each detection in the message.

        Note: This method modifies the input dictionary in place.

        Parameters
        ----------
        detections : dict
            Dictionary where each key is the oid and value is a list of detections

        Returns
        -------
        Tuple[dict, bool]
            The modified dictionary and a boolean indicating if the aid was found

        Examples
        --------
        >>> detections = {"oid1": [detection1, detection2], "oid2": [detection3, detection4]}

        >>> get_aid_by_oid(detections)
        ({"oid1": [detection1, detection2], "oid2": [detection3, detection4]}, True)
        """
        for oid in detections:
            # TODO: improve this query
            obj = self.db["object"].find_one({"_id": oid})
            if not obj:
                return detections, False
            detections[oid] = list(
                map(lambda x: {**x, "aid": obj["aid"]}, detections[oid])
            )
        return detections

    def get_aid_by_conesearch(self, detections: dict) -> Tuple[List[dict], bool]:
        """Gets alerce id by cone search and assigns each detection that id.

        If an aid is found by cone search, it is assigned to each detection in the message.

        Note: This method modifies the input dictionary in place.

        Parameters
        ----------
        detections : dict
            Dictionary where each key is the oid and value is a list of detections

        Returns
        -------
        Tuple[List[dict], bool]
            The modified list and a boolean indicating if the aid was found

        Examples
        --------
        >>> detections = {"oid1": [detection1, detection2], "oid2": [detection3, detection4]}
        >>> get_aid_by_conesearch(detections)
        ({"oid1": [detection1, detection2], "oid2": [detection3, detection4]}, True)
        """
        for oid in detections:
            aid = self.conesearch_query(
                self.db, detections[oid][0]["ra"], detections[oid][0]["dec"], 1
            )
            if not aid:
                return detections, False
            detections[oid] = list(map(lambda x: {**x, "aid": aid}, detections[oid]))
        return detections, True

    def conesearch_query(
        self, db: Database, ra: float, dec: float, radius: float
    ) -> Union[str, None]:
        """Query the database and check if there is an alerce_id for the specified coordinates and search radius

        Parameters
        ----------
        db : Database
            The database to query
        ra : float
            Right ascension in degrees
        dec : float
            Declination in degrees
        radius : float
            Search radius in arcseconds

        Returns
        -------
        Union[str, None]
            The alerce_id if found, None otherwise

        Examples
        --------
        >>> conesearch_query(db, 0, 0, 1)
        1234567890123456789

        >>> conesearch_query(db, 10, 20, 1)
        None
        """
        found = db["object"].find_one(
            {
                "loc": {
                    "$nearSphere": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": [ra - 180, dec],
                        },
                        "$maxDistance": math.radians(radius / 3600) * 6.3781e6,
                    },
                },
            },
            {"aid": 1},
        )
        if found:
            return found["aid"]
        return None

    def new_aid(self, detections: dict) -> Tuple[List[dict], List[dict]]:
        """Assigns a new alerce id to each detection

        Parameters
        ----------
        detections : dict
            Dictionary where each key is the oid and value is a list of detections

        Returns
        -------
        Tuple[List[dict], List[dict]]
            The modified list and a list of new objects

        Examples
        --------
        >>> detections = {"oid1": [detection1, detection2], "oid2": [detection3, detection4]}
        >>> new_aid(detections)
        ({"oid1": [detection1, detection2], "oid2": [detection3, detection4]}, [{"_id": oid1, "aid": 1234567890123456789}, {"_id": oid2, "aid": 1234567890123456789}])
        """
        new_objects = []
        for oid in detections:
            aid = self.id_generator(detections[oid][0]["ra"], detections[oid][0]["dec"])
            detections[oid] = list(map(lambda x: {**x, "aid": aid}, detections[oid]))
            new_objects.append(
                {
                    "_id": oid,
                    "aid": aid,
                }
            )
        return detections, new_objects

    def id_generator(self, ra: float, dec: float) -> int:
        """Method that create an identifier of 19 digits given its ra, dec.

        Parameters
        ----------
        ra : float
            Right ascension in degrees
        dec : float
            Declination in degrees

        Returns
        -------
        int
            Identifier of 19 digits

        Examples
        --------
        >>> id_generator(0, 0)
        1000000000000000000
        """
        # 19-Digit ID - two spare at the end for up to 100 duplicates
        aid = 1000000000000000000

        # 2013-11-15 KWS Altered code to fix the negative RA problem
        if ra < 0.0:
            ra += 360.0

        if ra > 360.0:
            ra -= 360.0

        # Calculation assumes Decimal Degrees:
        ra_hh = int(ra / 15)
        ra_mm = int((ra / 15 - ra_hh) * 60)
        ra_ss = int(((ra / 15 - ra_hh) * 60 - ra_mm) * 60)
        ra_ff = int((((ra / 15 - ra_hh) * 60 - ra_mm) * 60 - ra_ss) * 100)

        if dec >= 0:
            h = 1
        else:
            h = 0
            dec = dec * -1

        dec_deg = int(dec)
        dec_mm = int((dec - dec_deg) * 60)
        dec_ss = int(((dec - dec_deg) * 60 - dec_mm) * 60)
        dec_f = int(((((dec - dec_deg) * 60 - dec_mm) * 60) - dec_ss) * 10)

        aid += ra_hh * 10000000000000000
        aid += ra_mm * 100000000000000
        aid += ra_ss * 1000000000000
        aid += ra_ff * 10000000000

        aid += h * 1000000000
        aid += dec_deg * 10000000
        aid += dec_mm * 100000
        aid += dec_ss * 1000
        aid += dec_f * 100
        # transform to str
        return aid
