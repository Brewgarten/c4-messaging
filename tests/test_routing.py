import datetime
import logging
import multiprocessing
import pytest
import random
import time

from multiprocessing.managers import SyncManager

from c4.messaging import (Dealer, DealerRouter,
                          Envelope,
                          MessagingException,
                          Peer, PeerClient, PeerRouter, Poller,
                          Router, RouterClient)

log = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures("temporaryIPCPath")

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

class TestDealerRouter(object):

    def test_handleMessage(self):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["router/dealerRouter"] = False

        router = Router("router")  # @UnusedVariable

        dealerRouter = DealerRouter("router", "router/dealerRouter")
        def testHandler(message):
            sharedDict["router/dealerRouter"] = True
        dealerRouter.addHandler(testHandler)
        dealerRouter.start(timeout=1)

        client = RouterClient("router/dealerRouter")
        client.sendMessage(Envelope("client", "router/dealerRouter", "test", isRequest=False))

        # Give worker threads a chance to finish
        time.sleep(.5)

        dealerRouter.stop(timeout=1)

        assert sharedDict["router/dealerRouter"]

    def test_handleMessage_withException(self):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["router/dealerRouter"] = False

        router = Router("router")  # @UnusedVariable

        dealerRouter = DealerRouter("router", "router/dealerRouter")
        def testHandler(message):
            sharedDict["router/dealerRouter"] = True
            # This exception should be handled in HandlerThread
            raise ValueError("Error!")
        dealerRouter.addHandler(testHandler)
        dealerRouter.start(timeout=1)

        client = RouterClient("router/dealerRouter")
        client.sendMessage(Envelope("client", "router/dealerRouter", "test", isRequest=False))

        # Give worker threads a chance to finish
        time.sleep(.5)

        dealerRouter.stop(timeout=1)

        assert sharedDict["router/dealerRouter"]

    def test_handleMessage_exceedMaxThreads(self):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedLock = manager.Lock()
        sharedDict["callCount"] = 0

        router = Router("router")  # @UnusedVariable

        dealerRouter = DealerRouter("router", "router/dealerRouter")
        def testHandler(message):
            with sharedLock:
                sharedDict["callCount"] += 1
            time.sleep(1)
        dealerRouter.addHandler(testHandler)
        dealerRouter.start(timeout=1)

        client = RouterClient("router/dealerRouter")

        for i in range(0, 5): # @UnusedVariable
            client.sendMessage(Envelope("client", "router/dealerRouter", "test", isRequest=False))

        # Give worker threads a chance to finish
        time.sleep(5)

        dealerRouter.stop(timeout=1)

        assert sharedDict["callCount"] == 5

    def test_routeMessageDownstream(self):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["router/one/one"] = False

        router = Router("router")  # @UnusedVariable

        one = DealerRouter("router", "router/one")
        one.start(timeout=1)

        one_one = DealerRouter("router/one", "router/one/one")
        def testHandler(message):
            sharedDict["router/one/one"] = True
        one_one.addHandler(testHandler)
        one_one.start(timeout=1)

        client = RouterClient("router/one")
        client.sendMessage(Envelope("client", "router/one/one", "test", isRequest=False))

        # Give worker threads a chance to finish
        time.sleep(.5)

        one.stop(timeout=1)
        one_one.stop(timeout=1)

        assert sharedDict["router/one/one"]

    def test_routeMessageUpstream(self):

        router = Router("router")

        dealerRouter = DealerRouter("router", "router/dealerRouter")
        dealerRouter.start(timeout=1)

        client = RouterClient("router/dealerRouter")
        client.sendMessage(Envelope("client", "router", "test", isRequest=False))

        dealerRouter.stop(timeout=1)

        poller1 = Poller()
        poller1.register(router.socket)
        # handle dealer registration
        assert router.hasNewMessage(dict(poller1.poll())) == False
        assert router.hasNewMessage(dict(poller1.poll()))
        assert len(router.newMessage) == 2
        assert router.newMessage[0] == "router"

    def test_routeWildcardMessage(self):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["peer1/one"] = False
        sharedDict["peer1/one/one"] = False
        sharedDict["peer1/one/one/one"] = False
        sharedDict["peer1/one/two/one"] = False
        sharedDict["peer1/one/one/oneResponse"] = False
        sharedDict["peer1/one/two/oneResponse"] = False

        router = Router("peer1")  # @UnusedVariable

        one = DealerRouter("peer1", "peer1/one")
        def oneHandler(message):
            log.debug("peer1/one received %s", message)
            sharedDict["peer1/one"] = True
            if message == "peer1/one/one/oneResponse":
                sharedDict["peer1/one/one/oneResponse"] = True
            if message == "peer1/one/two/oneResponse":
                sharedDict["peer1/one/two/oneResponse"] = True
        one.addHandler(oneHandler)
        one.start(timeout=1)

        one_one = DealerRouter("peer1/one", "peer1/one/one")
        def oneOneHandler(message):
            log.debug("peer1/one/one received %s", message)
            sharedDict["peer1/one/one"] = True
        one_one.addHandler(oneOneHandler)
        one_one.start(timeout=1)

        one_one_one = DealerRouter("peer1/one/one", "peer1/one/one/one")
        def oneOneOneHandler(message):
            log.debug("peer1/one/one/one received %s", message)
            sharedDict["peer1/one/one/one"] = True
            return "peer1/one/one/oneResponse"
        one_one_one.addHandler(oneOneOneHandler)
        one_one_one.start(timeout=1)

        one_two = DealerRouter("peer1/one", "peer1/one/two")
        def oneTwoHandler(message):
            log.debug("peer1/two/one received %s", message)
            sharedDict["peer1/two/one"] = True
        one_two.addHandler(oneTwoHandler)
        one_two.start(timeout=1)

        one_two_one = DealerRouter("peer1/one/two", "peer1/one/two/one")
        def oneTwoOneHandler(message):
            log.debug("peer1/one/two/one received %s", message)
            sharedDict["peer1/one/two/one"] = True
            return "peer1/one/two/oneResponse"
        one_two_one.addHandler(oneTwoOneHandler)
        one_two_one.start(timeout=1)

        client = RouterClient("peer1/one")
        client.forwardMessage(Envelope("peer1/one", "peer1/one/*/one", "test"))

        # Give worker threads a chance to finish
        time.sleep(.5)

        one_one_one.stop(timeout=1)
        one_one.stop(timeout=1)
        one_two_one.stop(timeout=1)
        one_two.stop(timeout=1)
        one.stop(timeout=1)

        assert sharedDict["peer1/one"]
        # this one should not be covered by the wildcard
        assert sharedDict["peer1/one/one"] == False
        assert sharedDict["peer1/one/one/one"]
        assert sharedDict["peer1/one/two/one"]
        assert sharedDict["peer1/one/one/oneResponse"]
        assert sharedDict["peer1/one/two/oneResponse"]

    def test_startup(self):

        # this should fail because the upstream is not set up
        with pytest.raises(MessagingException):
            one = DealerRouter("peer1", "peer1/one")  # @UnusedVariable

        peer1 = Router("peer1")  # @UnusedVariable

        one = DealerRouter("peer1", "peer1/one")
        one.start(timeout=1)
        # this should fail because the downstream address is the same as an already existing dealer
        with pytest.raises(MessagingException):
            onePrime = DealerRouter("peer1", "peer1/one")
            onePrime.start(timeout=1)
        one.stop(timeout=1)

class TestDealer(object):

    def test_dealerRegistration(self):

        router = Router("router")
        dealer = Dealer("router", "router/dealer")  # @UnusedVariable

        poller = Poller()
        poller.register(router.socket)

        assert router.hasNewMessage(dict(poller.poll())) == False
        assert router.dealers == set(["router/dealer"])

    def test_newMessage(self):

        router = Router("router")  # @UnusedVariable
        dealer = Dealer("router", "router/dealer")

        client = RouterClient("router/dealer")
        client.sendMessage(Envelope("client", "router/dealer", "test", isRequest=False))

        poller = Poller()
        poller.register(dealer.socket)

        assert dealer.hasNewMessage(dict(poller.poll()))
        assert len(dealer.newMessage) == 2
        assert dealer.newMessage[0] == "router/dealer"
        envelope = Envelope.fromJSON(dealer.newMessage[1])
        assert envelope is not None

    def test_routeMessage(self):

        router = Router("router")

        dealer = Dealer("router", "router/dealer")

        poller1 = Poller()
        poller1.register(router.socket)
        # handle dealer registration
        assert router.hasNewMessage(dict(poller1.poll())) == False

        client = RouterClient("router/dealer")
        client.sendMessage(Envelope("client", "router", "test", isRequest=False))

        poller2 = Poller()
        poller2.register(dealer.socket)

        assert dealer.hasNewMessage(dict(poller2.poll()))
        assert len(dealer.newMessage) == 2
        assert dealer.newMessage[0] == "router"
        dealer.routeMessage(*dealer.newMessage)

        assert router.hasNewMessage(dict(poller1.poll()))
        assert len(router.newMessage) == 2
        assert router.newMessage[0] == "router"
        envelope = Envelope.fromJSON(router.newMessage[1])
        assert envelope is not None

    def test_startup(self):

        # this should fail because the upstream is not set up
        with pytest.raises(MessagingException):
            dealer = Dealer("router", "router/dealer")  # @UnusedVariable

        router = Router("router")  # @UnusedVariable
        dealer = Dealer("router", "router/dealer")  # @UnusedVariable
        with pytest.raises(MessagingException):
            dealer = Dealer("router", "router/dealer")  # @UnusedVariable

class TestPeer(object):

    def test_newMessage(self, clusterInfo):

        peer1 = Peer("peer1", clusterInfo)

        client = PeerClient("peer1", clusterInfo)
        client.forwardMessage(Envelope("client", "peer1", "test"))

        poller = Poller()
        poller.register(peer1.socket)

        assert peer1.hasNewMessage(dict(poller.poll()))
        assert len(peer1.newMessage) == 2
        assert peer1.newMessage[0] == "peer1"
        envelope = Envelope.fromJSON(peer1.newMessage[1])
        assert envelope is not None

    def test_routeMessage(self, clusterInfo):

        peer1 = Peer("peer1", clusterInfo)
        peer2 = Peer("peer2", clusterInfo)

        client = PeerClient("peer1", clusterInfo)
        client.forwardMessage(Envelope("client", "peer2", "test"))

        poller1 = Poller()
        poller1.register(peer1.socket)

        assert peer1.hasNewMessage(dict(poller1.poll()))
        assert len(peer1.newMessage) == 2
        assert peer1.newMessage[0] == "peer2"
        peer1.routeMessage(*peer1.newMessage)

        poller2 = Poller()
        poller2.register(peer2.socket)

        assert peer2.hasNewMessage(dict(poller2.poll(timeout=1000)))
        assert len(peer2.newMessage) == 2
        assert peer2.newMessage[0] == "peer2"
        envelope = Envelope.fromJSON(peer2.newMessage[1])
        assert envelope is not None

    def test_startup(self, clusterInfo):

        # this should fail because the peer address is not in cluster info
        with pytest.raises(MessagingException):
            missingPeer = Peer("missingPeer", clusterInfo)  # @UnusedVariable

        peer = Peer("peer1", clusterInfo)  # @UnusedVariable
        with pytest.raises(MessagingException):
            peerPrime = Peer("peer1", clusterInfo)  # @UnusedVariable


class TestPeerRouter(object):

    def test_performance(self, clusterInfo):

        messages = 1000

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["peer1"] = False
        sharedDict["peer2"] = False
        sharedDict["response"] = False
        directTCPTimes = manager.list()
        oneHopTimes = manager.list()

        peer1 = PeerRouter("peer1", clusterInfo)
        def test1(message):
            log.debug("peer1 received %s", message)
            sent = datetime.datetime.strptime(message, "%Y-%m-%d %H:%M:%S.%f")
            utcTime = datetime.datetime.utcnow()
            directTCPTimes.append(utcTime - sent)
        peer1.addHandler(test1)
        peer1.start(timeout=1)

        peer2 = PeerRouter("peer2", clusterInfo)
        def test2(message):
            log.debug("peer2 received %s", message)
            sent = datetime.datetime.strptime(message, "%Y-%m-%d %H:%M:%S.%f")
            utcTime = datetime.datetime.utcnow()
            oneHopTimes.append(utcTime - sent)
        peer2.addHandler(test2)
        peer2.start(timeout=1)

        client = RouterClient("peer1")

        messageEnvelope = Envelope("system-manager", "peer1", "test")
        for _ in range(messages):
            utcTime = datetime.datetime.utcnow()
            messageEnvelope.Message = utcTime.strftime("%Y-%m-%d %H:%M:%S.%f")
            client.forwardMessage(messageEnvelope)

        while (len(directTCPTimes) < messages):
            log.debug("direct: %10d %2.2f", len(directTCPTimes), len(directTCPTimes) * 100.0 / messages)
            time.sleep(0.01)

        total = sum(directTCPTimes, datetime.timedelta(0))
        log.debug("direct\t\tmin: %s, max: %s, total: %s, average: %s", min(directTCPTimes), max(directTCPTimes), total, total / len(directTCPTimes))

        messageEnvelope = Envelope("system-manager", "peer2", "test")
        for _ in range(messages):
            utcTime = datetime.datetime.utcnow()
            messageEnvelope.Message = utcTime.strftime("%Y-%m-%d %H:%M:%S.%f")
            client.forwardMessage(messageEnvelope)
#             send(clusterInfo.getNodeAddress("peer1"), messageEnvelope)

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

    def test_routeMessageDownstream(self, clusterInfo):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["peer1/one"] = False
        sharedDict["peer1/one/one"] = False
        sharedDict["peer1/one/one/one"] = False
        sharedDict["peer2Response"] = False

        peer1 = PeerRouter("peer1", clusterInfo)
        def peer1Handler(message):
            log.debug("peer1 received %s", message)
        peer1.addHandler(peer1Handler)
        peer1.start(timeout=1)

        peer2 = PeerRouter("peer2", clusterInfo)
        def peer2Handler(message):
            log.debug("peer2 received %s", message)
            if message == "response":
                sharedDict["peer2Response"] = True
        peer2.addHandler(peer2Handler)
        peer2.start(timeout=1)

        one = DealerRouter("peer1", "peer1/one")
        def oneHandler(message):
            log.debug("peer1/one received %s", message)
            sharedDict["peer1/one"] = True
        one.addHandler(oneHandler)
        one.start(timeout=1)

        one_one = DealerRouter("peer1/one", "peer1/one/one")
        def oneOneHandler(message):
            log.debug("peer1/one/one received %s", message)
            sharedDict["peer1/one/one"] = True
        one_one.addHandler(oneOneHandler)
        one_one.start(timeout=1)

        one_one_one = DealerRouter("peer1/one/one", "peer1/one/one/one")
        def oneOneOneHandler(message):
            log.debug("peer1/one/one/one received %s", message)
            sharedDict["peer1/one/one/one"] = True
            return "response"
        one_one_one.addHandler(oneOneOneHandler)
        one_one_one.start(timeout=1)

        client = RouterClient("peer2")
        client.forwardMessage(Envelope("peer2", "peer1/one/one/one", "test"))

        time.sleep(0.1)

        one_one_one.stop(timeout=1)
        one_one.stop(timeout=1)
        one.stop(timeout=1)
        peer2.stop(timeout=1)
        peer1.stop(timeout=1)

        assert sharedDict["peer1/one/one/one"]
        assert sharedDict["peer2Response"]

    def test_routeMessagePeerAlias(self, clusterInfo):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["system-manager/one"] = False

        peer1 = PeerRouter("peer1", clusterInfo)
        peer1.start(timeout=1)

        peer2 = PeerRouter("peer2", clusterInfo)
        peer2.start(timeout=1)

        one = DealerRouter("peer1", "peer1/one")
        def oneHandler(message):
            log.debug("system-manager/one received %s", message)
            sharedDict["system-manager/one"] = True
            return message
        one.addHandler(oneHandler)
        one.start(timeout=1)

        client = RouterClient("peer2")
        client.sendRequest(Envelope("client", "system-manager/one", "test"))

        one.stop(timeout=1)
        peer2.stop(timeout=1)
        peer1.stop(timeout=1)

        assert sharedDict["system-manager/one"]

    def test_routeMessagePeers(self, clusterInfo):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedDict["peer1"] = False
        sharedDict["peer2"] = False
        sharedDict["response"] = False

        peer1 = PeerRouter("peer1", clusterInfo)
        def test1(message):
            log.debug("peer1 received %s", message)
            sharedDict["peer1"] = True
            if message == "response":
                sharedDict["response"] = True
        peer1.addHandler(test1)
        peer1.start(timeout=1)

        peer2 = PeerRouter("peer2", clusterInfo)
        def test2(message):
            log.debug("peer2 received %s", message)
            sharedDict["peer2"] = True
            return "response"
        peer2.addHandler(test2)
        peer2.start(timeout=1)

        client = RouterClient("peer1")
        client.forwardMessage(Envelope("system-manager", "peer2", "test"))

        time.sleep(0.5)

        peer1.stop(timeout=1)
        peer2.stop(timeout=1)

        assert sharedDict["peer1"]
        assert sharedDict["peer2"]
        assert sharedDict["response"]

    def test_routeMessagePeers_exceedMaxThreads(self, clusterInfo):

        manager = SyncManager()
        manager.start()
        sharedDict = manager.dict()
        sharedLock = manager.Lock()
        sharedDict["peer1Count"] = 0
        sharedDict["peer2Count"] = 0
        sharedDict["responseCount"] = 0

        peer1 = PeerRouter("peer1", clusterInfo)
        def test1(message):
            log.debug("peer1 received %s", message)
            with sharedLock:
                sharedDict["peer1Count"] += 1
            if message == "response":
                with sharedLock:
                    sharedDict["responseCount"] += 1
            time.sleep(1)
        peer1.addHandler(test1)
        peer1.start(timeout=1)

        peer2 = PeerRouter("peer2", clusterInfo)
        def test2(message):
            log.debug("peer2 received %s", message)
            with sharedLock:
                sharedDict["peer2Count"] += 1
            time.sleep(1)
            return "response"
        peer2.addHandler(test2)
        peer2.start(timeout=1)

        client = RouterClient("peer1")
        for i in range(0, 5): # @UnusedVariable
            client.forwardMessage(Envelope("system-manager", "peer2", "test"))

        # Give worker threads a chance to finish
        time.sleep(5)

        peer1.stop(timeout=1)
        peer2.stop(timeout=1)

        assert sharedDict["peer1Count"] == 5
        assert sharedDict["peer2Count"] == 5
        assert sharedDict["responseCount"] == 5

    def test_startup(self, clusterInfo):

        # this should fail because the peer address is not in cluster info
        with pytest.raises(MessagingException):
            missingPeer = PeerRouter("missingPeer", clusterInfo)  # @UnusedVariable

        peerRouter = PeerRouter("peer1", clusterInfo)
        peerRouter.start(timeout=1)
        # this should fail because the downstream address is the same as an already existing dealer
        with pytest.raises(MessagingException):
            peerRouterPrime = PeerRouter("peer1", clusterInfo)  # @UnusedVariable
        peerRouter.stop(timeout=1)

class TestRouter(object):

    def test_dealerRegistration(self):

        router = Router("router")

        client = RouterClient("router")
        client.forwardMessage(Envelope("dealer", "router", "DealerRegistration"))

        poller = Poller()
        poller.register(router.socket)

        assert router.hasNewMessage(dict(poller.poll())) == False
        assert router.dealers == set(["dealer"])

    def test_newMessage(self):

        router = Router("router")

        client = RouterClient("router")
        client.sendMessage(Envelope("client", "router", "test", isRequest=False))

        poller = Poller()
        poller.register(router.socket)

        assert router.hasNewMessage(dict(poller.poll()))
        assert len(router.newMessage) == 2
        assert router.newMessage[0] == "router"
        envelope = Envelope.fromJSON(router.newMessage[1])
        assert envelope is not None

    def test_routeMessage(self):

        router = Router("router")
        dealer = Dealer("router", "router/dealer")

        poller1 = Poller()
        poller1.register(router.socket)
        # handle dealer registration
        assert router.hasNewMessage(dict(poller1.poll())) == False

        client = RouterClient("router")
        client.sendMessage(Envelope("client", "router/dealer", "test", isRequest=False))

        assert router.hasNewMessage(dict(poller1.poll()))
        assert len(router.newMessage) == 2
        assert router.newMessage[0] == "router/dealer"
        router.routeMessage(*router.newMessage)

        poller2 = Poller()
        poller2.register(dealer.socket)
        assert dealer.hasNewMessage(dict(poller2.poll()))
        assert len(dealer.newMessage) == 2
        assert dealer.newMessage[0] == "router/dealer"
        envelope = Envelope.fromJSON(dealer.newMessage[1])
        assert envelope is not None

    def test_routeWildcardMessage(self):

        router = Router("router")
        dealer = Dealer("router", "router/dealer")

        poller1 = Poller()
        poller1.register(router.socket)
        # handle dealer registration
        assert router.hasNewMessage(dict(poller1.poll())) == False

        client = RouterClient("router")
        client.sendMessage(Envelope("client", "router/*", "test", isRequest=False))

        assert router.hasNewMessage(dict(poller1.poll()))
        assert len(router.newMessage) == 2
        assert router.newMessage[0] == "router/*"
        router.routeMessage(*router.newMessage)

        poller2 = Poller()
        poller2.register(dealer.socket)
        assert dealer.hasNewMessage(dict(poller2.poll()))
        assert len(dealer.newMessage) == 2
        assert dealer.newMessage[0] == "router/dealer"
        envelope = Envelope.fromJSON(dealer.newMessage[1])
        assert envelope is not None

    def test_startup(self):

        router = Router("testRouter")  # @UnusedVariable
        with pytest.raises(MessagingException):
            routerPrime = Router("testRouter")  # @UnusedVariable

def test_sharedclusterInfo(clusterInfo):

    ps = []
    for _ in range(100):
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
