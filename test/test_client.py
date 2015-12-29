import logging
import multiprocessing
import time

from c4.messaging import (Envelope,
                          PeerRouter,
                          RouterClient)


log = logging.getLogger(__name__)

class TestRouterClient(object):

    MESSAGES = 100

    def test_sendMessage(self, clusterInfo):

        counter = multiprocessing.Value("i", 0)

        peer1 = PeerRouter("peer1", clusterInfo, "ipc://peer1.ipc")
        def peer1Handler(message, envelope):
            with counter.get_lock():
                counter.value += 1
        peer1.addHandler(peer1Handler)
        peer1.start(timeout=1)

        client = RouterClient("peer1")
        for _ in range(self.MESSAGES):
            testEnvelope = Envelope("client", "peer1", Action="test", isRequest=False, includeTime=True)
            client.sendMessage(testEnvelope)

        # wait with timeout for messages
        end = time.clock() + 10
        while counter.value < self.MESSAGES and time.clock() < end:
            time.sleep(0.01)

        peer1.stop(timeout=1)

        assert counter.value == self.MESSAGES

    def test_sendRequest(self, clusterInfo):

        counter = multiprocessing.Value("i", 0)

        peer1 = PeerRouter("peer1", clusterInfo, "ipc://peer1.ipc")
        def peer1Handler(message, envelope):
            with counter.get_lock():
                counter.value += 1
            return message
        peer1.addHandler(peer1Handler)
        peer1.start(timeout=1)

        client = RouterClient("peer1")
        for _ in range(self.MESSAGES):
            testEnvelope = Envelope("client", "peer1", Action="test", includeTime=True)
            message = client.sendRequest(testEnvelope)
            assert message is not None

        peer1.stop(timeout=1)

        assert counter.value == self.MESSAGES