import datetime
import logging
import multiprocessing
import pytest
import random
import time
import zmq

from multiprocessing.managers import SyncManager

from c4.messaging import ClusterInfo, Dealer, DealerRouter, Peer, PeerRouter, Envelope, Router, sendMessage, MessagingException

log = logging.getLogger(__name__)

class ClusterInfoClient(multiprocessing.Process):

    def __init__(self, clusterInfo):
        super(ClusterInfoClient, self).__init__()
        self.clusterInfo = clusterInfo

    def run(self):
        r = random.randint(1, 10000)
        self.clusterInfo.nodes["test"] = r

        self.clusterInfo.nodes.lock.acquire()
        self.clusterInfo.nodes["test"] = r + 1
        assert self.clusterInfo.nodes["test"] == r + 1
        self.clusterInfo.nodes.lock.release()

@pytest.fixture
def clusterInfo(cleandir):
    """
    Set up a basic cluster information
    """
    clusterInformation = ClusterInfo()
    clusterInformation.nodes["peer1"] = "ipc://peer1-peerAddress.ipc"
    clusterInformation.nodes["peer2"] = "ipc://peer2-peerAddress.ipc"
    clusterInformation.aliases["system-manager"] = "peer1"

    return clusterInformation

def test_sharedclusterInfo(clusterInfo):

    ps = []
    for i in range(1000):
        p = ClusterInfoClient(clusterInfo)
        p.start()
        ps.append(p)

    for p in ps:
        p.join()

    log.debug(clusterInfo.nodes)
    assert "peer1" in clusterInfo.nodes
    assert "peer2" in clusterInfo.nodes
    assert "system-manager" in clusterInfo.aliases
    assert len(clusterInfo.nodes) == 3
    assert sorted(clusterInfo.nodeNames) == ['peer1', 'peer2', 'test']
    assert "ipc://peer1-peerAddress.ipc" in clusterInfo.nodeAddresses
    assert "ipc://peer2-peerAddress.ipc" in clusterInfo.nodeAddresses

    del clusterInfo.nodes["test"]

    assert "peer1" in clusterInfo.nodes
    assert "peer2" in clusterInfo.nodes
    assert "system-manager" in clusterInfo.aliases
    assert len(clusterInfo.nodes) == 2
    assert sorted(clusterInfo.nodeNames) == ['peer1', 'peer2']
    assert "ipc://peer1-peerAddress.ipc" in clusterInfo.nodeAddresses
    assert "ipc://peer2-peerAddress.ipc" in clusterInfo.nodeAddresses

    with pytest.raises(NotImplementedError):
        for node in clusterInfo.nodes:
            log.fatal(node)

def test_peer_performance(clusterInfo):

    messages = 1000

    manager = SyncManager()
    manager.start()
    sharedDict = manager.dict()
    sharedDict["peer1"] = False
    sharedDict["peer2"] = False
    sharedDict["response"] = False
    directTCPTimes = manager.list()
    oneHopTimes = manager.list()

    peer1 = PeerRouter("peer1", clusterInfo, "ipc://peer1.ipc")
    def test1(message):
        log.debug("peer1 received %s", message)
        sent = datetime.datetime.strptime(message, "%Y-%m-%d %H:%M:%S.%f")
        utcTime = datetime.datetime.utcnow()
        directTCPTimes.append(utcTime - sent)
    peer1.addHandler(test1)
    peer1.start(timeout=1)

    peer2 = PeerRouter("peer2", clusterInfo, "ipc://peer2.ipc")
    def test2(message):
        log.debug("peer2 received %s", message)
        sent = datetime.datetime.strptime(message, "%Y-%m-%d %H:%M:%S.%f")
        utcTime = datetime.datetime.utcnow()
        oneHopTimes.append(utcTime - sent)
    peer2.addHandler(test2)
    peer2.start(timeout=1)

    messageEnvelope = Envelope("system-manager", "peer1", "test")
    for i in range(messages):
        utcTime = datetime.datetime.utcnow()
        messageEnvelope.Message = utcTime.strftime("%Y-%m-%d %H:%M:%S.%f")
        sendMessage(clusterInfo.getNodeAddress("peer1"), messageEnvelope)

    while (len(directTCPTimes) < messages):
        log.debug("direct: %10d %2.2f", len(directTCPTimes), len(directTCPTimes) * 100.0 / messages)
        time.sleep(0.01)

    total = sum(directTCPTimes, datetime.timedelta(0))
    log.debug("direct\t\tmin: %s, max: %s, total: %s, average: %s", min(directTCPTimes), max(directTCPTimes), total, total / len(directTCPTimes))

    messageEnvelope = Envelope("system-manager", "peer2", "test")
    for i in range(messages):
        utcTime = datetime.datetime.utcnow()
        messageEnvelope.Message = utcTime.strftime("%Y-%m-%d %H:%M:%S.%f")
        sendMessage(clusterInfo.getNodeAddress("peer1"), messageEnvelope)

    while (len(oneHopTimes) < messages):
        log.debug("one hop: %10d %2.2f", len(oneHopTimes), len(oneHopTimes) * 100.0 / messages)
        time.sleep(0.01)

    total = sum(oneHopTimes, datetime.timedelta(0))
    log.debug("one hop\t\tmin: %s, max: %s, total: %s, average: %s", min(oneHopTimes), max(oneHopTimes), total, total / len(oneHopTimes))

    peer1.stop(timeout=1)
    peer2.stop(timeout=1)

#     # 10000 MESSAGES
#     # direct        min: 0:00:00.000813, max: 0:00:00.343308, total: 0:18:13.495131, average: 0:00:00.109349
#     # direct        min: 0:00:00.000994, max: 0:00:00.267798, total: 0:13:58.260871, average: 0:00:00.083826
#     # direct        min: 0:00:00.001583, max: 0:00:00.192484, total: 0:12:27.643058, average: 0:00:00.074764
#
#     # PERSISTENT
#     # one hop        min: 0:00:00.016444, max: 0:00:01.400288, total: 1:45:38.572968, average: 0:00:00.633857
#     # one hop        min: 0:00:00.009620, max: 0:00:01.324690, total: 1:43:37.707239, average: 0:00:00.621770
#     # one hop        min: 0:00:00.009235, max: 0:00:02.053092, total: 2:05:14.748340, average: 0:00:00.751474
#
#     # NON_PERSISTENT
#     # one hop        min: 0:00:00.020513, max: 0:00:07.717862, total: 8:59:26.204896, average: 0:00:03.236620
#     # one hop        min: 0:00:00.012712, max: 0:00:07.601768, total: 9:13:17.822126, average: 0:00:03.319782
#     # one hop        min: 0:00:00.009152, max: 0:00:07.592734, total: 9:28:34.304328, average: 0:00:03.411430
#
#     # TODO: investigate this error: "Bad file descriptor (epoll.cpp:81)"


def test_peers(clusterInfo):

    manager = SyncManager()
    manager.start()
    sharedDict = manager.dict()
    sharedDict["peer1"] = False
    sharedDict["peer2"] = False
    sharedDict["response"] = False

    peer1 = PeerRouter("peer1", clusterInfo, "ipc://peer1.ipc")
    def test1(message):
        log.debug("peer1 received %s", message)
        sharedDict["peer1"] = True
        if message == "response":
            sharedDict["response"] = True
    peer1.addHandler(test1)
    peer1.start(timeout=1)

    peer2 = PeerRouter("peer2", clusterInfo, "ipc://peer2.ipc")
    def test2(message):
        log.debug("peer2 received %s", message)
        sharedDict["peer2"] = True
        return "response"
    peer2.addHandler(test2)
    peer2.start(timeout=1)

    messageEnvelope = Envelope("system-manager", "peer2", "test")
    sendMessage(clusterInfo.getNodeAddress("peer1"), messageEnvelope)

    time.sleep(0.1)

    peer1.stop(timeout=1)
    peer2.stop(timeout=1)

    assert sharedDict["peer1"]
    assert sharedDict["peer2"]
    assert sharedDict["response"]

def test_peerRouterDealer(clusterInfo):

    manager = SyncManager()
    manager.start()
    sharedDict = manager.dict()
    sharedDict["peer1/one"] = False
    sharedDict["peer1/one/one"] = False
    sharedDict["peer1/one/one/one"] = False
    sharedDict["peer2Response"] = False

    peer1 = PeerRouter("peer1", clusterInfo, "ipc://peer1.ipc")
    def peer1Handler(message):
        log.debug("peer1 received %s", message)
    peer1.addHandler(peer1Handler)
    peer1.start(timeout=1)

    peer2 = PeerRouter("peer2", clusterInfo, "ipc://peer2.ipc")
    def peer2Handler(message):
        log.debug("peer2 received %s", message)
        if message == "response":
            sharedDict["peer2Response"] = True
    peer2.addHandler(peer2Handler)
    peer2.start(timeout=1)

    one = DealerRouter("ipc://peer1.ipc", "peer1/one", "ipc://one.ipc")
    def oneHandler(message):
        log.debug("peer1/one received %s", message)
        sharedDict["peer1/one"] = True
    one.addHandler(oneHandler)
    one.start(timeout=1)

    one_one = DealerRouter("ipc://one.ipc", "peer1/one/one", "ipc://one|one.ipc")
    def oneOneHandler(message):
        log.debug("peer1/one/one received %s", message)
        sharedDict["peer1/one/one"] = True
    one_one.addHandler(oneOneHandler)
    one_one.start(timeout=1)

    one_one_one = DealerRouter("ipc://one|one.ipc", "peer1/one/one/one", "ipc://one|one|one.ipc")
    def oneOneOneHandler(message):
        log.debug("peer1/one/one/one received %s", message)
        sharedDict["peer1/one/one/one"] = True
        return "response"
    one_one_one.addHandler(oneOneOneHandler)
    one_one_one.start(timeout=1)

    messageEnvelope = Envelope("peer2", "peer1/one/one/one", "test")
    sendMessage(clusterInfo.getNodeAddress("peer2"), messageEnvelope)

    time.sleep(0.1)

    peer1.stop(timeout=1)
    peer2.stop(timeout=1)
    one.stop(timeout=1)
    one_one.stop(timeout=1)
    one_one_one.stop(timeout=1)

    assert sharedDict["peer1/one/one/one"]
    assert sharedDict["peer2Response"]

def test_routerDealers(cleandir):

    manager = SyncManager()
    manager.start()
    sharedDict = manager.dict()
    sharedDict["peer1/one"] = False
    sharedDict["peer1/one/one"] = False
    sharedDict["peer1/one/one/one"] = False
    sharedDict["peer1/one/two/one"] = False
    sharedDict["peer1/one/one/oneResponse"] = False
    sharedDict["peer1/one/two/oneResponse"] = False

    context = zmq.Context()
    peer1 = context.socket(zmq.ROUTER)  # @UndefinedVariable
    peer1.bind("ipc://peer1.ipc")

    one = DealerRouter("ipc://peer1.ipc", "peer1/one", "ipc://one.ipc")
    def oneHandler(message):
        log.debug("peer1/one received %s", message)
        sharedDict["peer1/one"] = True
        if message == "peer1/one/one/oneResponse":
            sharedDict["peer1/one/one/oneResponse"] = True
        if message == "peer1/one/two/oneResponse":
            sharedDict["peer1/one/two/oneResponse"] = True
    one.addHandler(oneHandler)
    one.start(timeout=1)

    one_one = DealerRouter("ipc://one.ipc", "peer1/one/one", "ipc://one|one.ipc")
    def oneOneHandler(message):
        log.debug("peer1/one/one received %s", message)
        sharedDict["peer1/one/one"] = True
    one_one.addHandler(oneOneHandler)
    one_one.start(timeout=1)

    one_one_one = DealerRouter("ipc://one|one.ipc", "peer1/one/one/one", "ipc://one|one|one.ipc")
    def oneOneOneHandler(message):
        log.debug("peer1/one/one/one received %s", message)
        sharedDict["peer1/one/one/one"] = True
        return "peer1/one/one/oneResponse"
    one_one_one.addHandler(oneOneOneHandler)
    one_one_one.start(timeout=1)

    one_two = DealerRouter("ipc://one.ipc", "peer1/one/two", "ipc://one|two.ipc")
    def oneTwoHandler(message):
        log.debug("peer1/two/one received %s", message)
        sharedDict["peer1/two/one"] = True
    one_two.addHandler(oneTwoHandler)
    one_two.start(timeout=1)

    one_two_one = DealerRouter("ipc://one|two.ipc", "peer1/one/two/one", "ipc://one|two|one.ipc")
    def oneTwoOneHandler(message):
        log.debug("peer1/one/two/one received %s", message)
        sharedDict["peer1/one/two/one"] = True
        return "peer1/one/two/oneResponse"
    one_two_one.addHandler(oneTwoOneHandler)
    one_two_one.start(timeout=1)

    messageEnvelope = Envelope("peer1/one", "peer1/one/*/one", "test")
    peer1.send_multipart(["one", "*", "one", bytes(messageEnvelope.toJSON(True))])

    time.sleep(0.1)

    peer1.unbind("ipc://peer1.ipc")
    one.stop(timeout=1)
    one_one.stop(timeout=1)
    one_one_one.stop(timeout=1)
    one_two.stop(timeout=1)
    one_two_one.stop(timeout=1)

    assert sharedDict["peer1/one"]
    # this one should not be covered by the wildcard
    assert sharedDict["peer1/one/one"] == False
    assert sharedDict["peer1/one/one/one"]
    assert sharedDict["peer1/one/two/one"]
    assert sharedDict["peer1/one/one/oneResponse"]
    assert sharedDict["peer1/one/two/oneResponse"]

def test_messageComponentsStartup(cleandir):

    clusterInfo = ClusterInfo()
    clusterInfo.nodes["peerRouter"] = "ipc://peerRouter-peerAddress.ipc"

    peer = Peer("ipc://testPeerRouter.ipc", clusterInfo)
    with pytest.raises(MessagingException):
        peerPrime = Peer("ipc://testPeerRouter.ipc", clusterInfo)

    # this should fail because the upstream is not set up
    with pytest.raises(MessagingException):
        dealer = Dealer("ipc://testRouter.ipc", "testDealer", "testDealer")

    router = Router("ipc://testRouter.ipc")
    with pytest.raises(MessagingException):
        routerPrime = Router("ipc://testRouter.ipc")

    peerRouter = PeerRouter("peerRouter", clusterInfo, "ipc://peerRouter.ipc")
    peerRouter.start(timeout=1)
    # this should fail because the downstream address is the same as an already existing dealer
    with pytest.raises(MessagingException):
        peerRouterPrime = PeerRouter("peerRouter", clusterInfo, "ipc://peerRouter.ipc")
        peerRouterPrime.start(timeout=1)
    peerRouter.stop(timeout=1)

    # this should fail because the upstream is not set up
    with pytest.raises(MessagingException):
        one = DealerRouter("ipc://peer1.ipc", "peer1/one", "ipc://one.ipc")
        one.start(timeout=1)

    context = zmq.Context()
    peer1 = context.socket(zmq.ROUTER)  # @UndefinedVariable
    peer1.bind("ipc://peer1.ipc")

    one = DealerRouter("ipc://peer1.ipc", "peer1/one", "ipc://one.ipc")
    one.start(timeout=1)
    # this should fail because the downstream address is the same as an already existing dealer
    with pytest.raises(MessagingException):
        onePrime = DealerRouter("ipc://peer1.ipc", "peer1/one", "ipc://one.ipc")
        onePrime.start(timeout=1)
    one.stop(timeout=1)

    peer1.unbind("ipc://peer1.ipc")

