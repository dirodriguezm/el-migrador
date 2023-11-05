from populate_new.transform_detection import TransformDetectionActor
from .utils import generate_detections, generate_objects
from unittest import mock
import pytest


@pytest.fixture
def grouper_actor():
    return mock.Mock()


@pytest.fixture
def transform_actor(grouper_actor):
    transform_actor = TransformDetectionActor.start(grouper_actor, 5)
    yield transform_actor
    transform_actor.stop()


def test_transform_detection_actor(transform_actor, grouper_actor):
    objects = [obj for obj in generate_objects(5)]
    detections = [d for d in generate_detections(objects)]
    transform_actor.ask(detections)
    assert grouper_actor.tell.call_count == 5
