import datetime
import logging
import multiprocessing
import pytest
import uuid

from c4.messaging import Envelope, MessageTracker, MessageTrackerDB

log = logging.getLogger(__name__)

@pytest.fixture(scope="function", params=[MessageTracker(), MessageTrackerDB()])
def tracker(request):
    return request.param

def test_tracking(tracker):

    a = Envelope("a", "b", "test")
    aRelatedA = str(uuid.uuid4())
    aRelatedB = str(uuid.uuid4())

    b = Envelope("a", "b", "test")
    bRelatedA = str(uuid.uuid4())

    tracker.add(a)
    tracker.addRelatedMessage(a.MessageID, aRelatedA)
    tracker.addRelatedMessage(a.MessageID, aRelatedB)
    tracker.add(b)
    tracker.addRelatedMessage(b.MessageID, bRelatedA)

    assert tracker.hasMoreRelatedMessages(a.MessageID)
    assert tracker.hasMoreRelatedMessages(b.MessageID)
    assert len(tracker.getRelatedMessageIds(a.MessageID)) == 2

    tracker.removeRelatedMessage(aRelatedA)
    tracker.removeRelatedMessage(aRelatedB)

    assert tracker.hasMoreRelatedMessages(a.MessageID) == False
    assert tracker.hasMoreRelatedMessages(b.MessageID)
    assert len(tracker.getRelatedMessageIds(a.MessageID)) == 0

    aCopy = tracker.remove(a.MessageID)

    assert aCopy.Action == "test"

def test_sharedTracking(tracker):

    numberOfProcesses = 10
    ids = [str(uuid.uuid4()) for _ in range(numberOfProcesses)]
    start = datetime.datetime.utcnow()
    a = Envelope("a", "b", "test")

    def add(d, relatedId):
        tracker.addRelatedMessage(a.MessageID, relatedId)

    processes = []
    for number in ids:
        process = multiprocessing.Process(target=add, args=([tracker, number]))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
    end = datetime.datetime.utcnow()
    assert len(tracker.getRelatedMessageIds(a.MessageID)) == numberOfProcesses
    logging.debug("testing %s backend took %s", tracker.__class__, end-start)

def test_updating():

    tracker = MessageTracker()

    a = Envelope("a", "b", "test")
    a.Message = {"same": 1,
           "valueChange1": 1,
           "valueChange2": {"test": "test"},
           "valueChange3": {"test": {"test2": "test"}},
           "onlyInOne": 1
           }
    tracker.add(a)

    content = {"same": 1,
           "valueChange1": 2,
           "valueChange2": {"test": "newValue"},
           "valueChange3": {"test": {"test2": "newValue"}},
           "onlyInTwo1": 1,
           "onlyInTwo2": {"test": "test"}
           }

    tracker.updateMessageContent(a.MessageID, content)
    merged = tracker.messages[a.MessageID].Message

    assert merged["same"] == 1
    assert merged["onlyInOne"] == 1
    assert merged["onlyInTwo1"] == 1
    assert merged["onlyInTwo2"] == {"test": "test"}
    assert merged["valueChange1"] == 2
    assert merged["valueChange2"] == {"test": "newValue"}
    assert merged["valueChange3"] == {"test": {"test2": "newValue"}}
