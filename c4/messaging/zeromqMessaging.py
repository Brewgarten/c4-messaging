"""
This library contains process functionality to handle and route messages
using ZMQ.

Message Routing
---------------

Peer
^^^^
A peer-to-peer message server that connects to other peers that
are part of the same cluster. In particular, we can set up a cluster
with two peers as follows:

.. code-block:: python

    clusterInfo = ClusterInfo()
    clusterInfo.nodes["peer1"] = "tcp://1.2.3.4:5000"
    clusterInfo.nodes["peer2"] = "tcp://5.6.7.8:5000"
    clusterInfo.aliases["peer1Alias"] = "peer1"

    peer1 = Peer("peer1", clusterInfo)
    peer2 = Peer("peer2", clusterInfo)

Router
^^^^^^
An abstraction similar to the ZMQ ``ROUTER`` that provides messaging to
`downstream` :class:`Dealer`s. E.g.,

.. code-block:: python

    router = Router("router")

Dealer
^^^^^^
An abstraction similar to the ZMQ ``DEALER`` that connects to a :class:`Router`
using the specified address. In the following example, we set up a dealer
with ``router/dealer`` address that also registers with the :class:`Router`
automatically.

.. code-block:: python

    dealer = Dealer("router", "router/dealer")

DealerRouter
^^^^^^^^^^^^
A combination of a :class:`Dealer` that connects to an `upstream` :class:`Router`
and a :class:`Router` that provides messaging to `downstream` :class:`Dealer`s. We can
use this approach to build hierarchies or message routing chains. For example after
we set up the following three components ``peer1/one``, ``peer1/one/one``, and
``peer1/one/one/one/`` we can send a message to ``peer1`` for ``peer1/one/one/one/``
which gets routed automatically.

.. code-block:: python

    peer1 = Peer("peer1")

    one = DealerRouter("peer1", "peer1/one")
    def oneHandler(message):
        logging.debug("peer1/one received %s", message)
    one.addHandler(oneHandler)
    one.start()

    one_one = DealerRouter("peer1/one", "peer1/one/one")
    def oneOneHandler(message):
        logging.debug("peer1/one/one received %s", message)
    one_one.addHandler(oneOneHandler)
    one_one.start()

    one_one_one = DealerRouter("peer1/one/one", "peer1/one/one/one")
    def oneOneOneHandler(message):
        logging.debug("peer1/one/one/one received %s", message)
        return "peer1/one/one/oneResponse"
    one_one_one.addHandler(oneOneOneHandler)
    one_one_one.start()

PeerRouter
^^^^^^^^^^
A combination of a :class:`Peer` that connects to other :class:`Peer`s
and a :class:`Router` that provides messaging to `downstream` :class:`Dealer`s.
This functionality can be used to set up several distinct peers on
nodes of a cluster which then perform `local` routing to connected
components.

.. code-block:: python

    clusterInfo = ClusterInfo()
    clusterInfo.nodes["peer1"] = "tcp://1.2.3.4:5000"
    clusterInfo.nodes["peer2"] = "tcp://5.6.7.8:5000"
    clusterInfo.aliases["peer1Alias"] = "peer1"

    peer1 = PeerRouter("peer1", clusterInfo)
    def test1(message):
        logging.debug("peer1 received %s", message)
    peer1.addHandler(test1)
    peer1.start()

    peer2 = PeerRouter("peer2", clusterInfo)
    def test2(message):
        logging.debug("peer2 received %s", message)
    peer2.addHandler(test2)
    peer2.start()

Message Client
--------------

RouterClient
^^^^^^^^^^^^
A client that can be used to connect to any :class:`Router` in the system
and is able to perform message sending::

    client = RouterClient("router")
    client.sendMessage(Envelope("client", "router", "test", isRequest=False))

requests::

    response = client.sendRequest(Envelope("client", "router", "test", isRequest=False))

and injection::

    client.forwardMessage(Envelope("client", "router", "test", isRequest=False))

Functionality
-------------
"""

from abc import ABCMeta, abstractmethod
import errno
import logging
import os
import time
import traceback
import uuid

import zmq

from c4.utils.logutil import ClassLogger
from c4.utils.util import addressesMatch

from .base import (Envelope,
                   MessagingException,
                   RoutingProcess)


DEFAULT_IPC_PATH = "/tmp"
DEFAULT_POLL_TIMEOUT = 100
DEFAULT_ZMQ_HWM = 0 # The default ZMQ_HWM value of zero means "no limit".
DEFAULT_ZMQ_LINGER = 250

log = logging.getLogger(__name__)

class BaseComponent(object):
    """
    A base component that receives from and sends messages to a
    :class:`Router` that it connects to.

    :param address: address of this component
    :type address: str
    :param socketAddress: optional socket address
    :type socketAddress: str
    :raises MessagingException: if own address is unavailable
    """
    __metaclass__ = ABCMeta

    def __init__(self, address, socketAddress=None):

        self.address = address
        if socketAddress:
            self.socketAddress = socketAddress
        else:
            self.socketAddress = getIPCSocketAddress(self.address)

        # check component address
        if isAddressInUse(self.socketAddress):
            raise MessagingException("{0} '{1}' already in use".format(self, self.socketAddress))

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)  # @UndefinedVariable
        self.socket.set_hwm(DEFAULT_ZMQ_HWM)
        self.socket.bind(self.socketAddress)
        self.newMessage = None

    def __del__(self):
        """
        Unbind socket
        """
        if hasattr(self, "socket"):
            self.socket.unbind(self.socketAddress)
            self.socket.close()
            if self.socketAddress.startswith("ipc"):
                # make sure that the ipc socket file gets cleaned up before we continue
                socketFile = self.socketAddress.split("//")[1]
                end = time.time() + 1
                while time.time() < end:
                    if os.path.exists(socketFile):
                        time.sleep(0.001)
                    else:
                        break
                else:
                    self.log.error("removing socket file '%s' timed out", socketFile)

    def hasNewMessage(self, sockets):
        """
        Check sockets dictionary if there is a new message
        and set ``newMessage`` if it is.

        :param sockets: sockets dictionary resulting from ``dict(poller.poll())``
        :type sockets: dict
        :returns: bool
        """
        if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:  # @UndefinedVariable
            self.newMessage = self.socket.recv_multipart()
            return True
        else:
            return False

    @abstractmethod
    def routeMessage(self, To, envelopeString):
        """
        Route message accordingly

        :param To: to field
        :type To: str
        :param envelopeString: envelope string
        :type envelopeString: str
        """

@ClassLogger
class Dealer(BaseComponent):
    """
    An implementation of a component that receives from and sends messages to a
    :class:`Router` that it connects to.

    :param routerAddress: address of a router
    :type routerAddress: str
    :param address: address of this dealer
    :type address: str
    :param register: whether to automatically register this dealer with the router
    :type register: bool
    :raises MessagingException: if router address is not set up or own address is
        unavailable
    """
    def __init__(self, routerAddress, address, register=True):

        self.routerAddress = routerAddress
        self.routerSocketAddress = getIPCSocketAddress(self.routerAddress)

        # make sure upstream address is already set up
        if not isAddressInUse(self.routerSocketAddress):
            raise MessagingException("{0} upstream '{1}' not set up".format(address, self.routerSocketAddress))

        super(Dealer, self).__init__(address)

        self.routerSocket = self.context.socket(zmq.PUSH)  # @UndefinedVariable
        self.routerSocket.set(zmq.LINGER, DEFAULT_ZMQ_LINGER) # @UndefinedVariable
        self.routerSocket.set_hwm(DEFAULT_ZMQ_HWM)
        self.routerSocket.connect(self.routerSocketAddress)

        if register:
            self.register()

    def register(self):
        """
        Send ``DealerRegistration`` message to the ``ROUTER``
        """
        self.routerSocket.send_multipart([
            bytes(self.routerAddress),
            bytes(Envelope(self.address, self.routerAddress, "DealerRegistration").toJSON(includeClassInfo=True))
        ])

    def routeMessage(self, To, envelopeString):
        """
        Route message to the router

        :param To: to field
        :type To: str
        :param envelopeString: envelope string
        :type envelopeString: str
        """
        self.log.debug("'%s' routing '%s' to '%s' via '%s'", self.address, envelopeString, To, self.routerAddress)
        self.routerSocket.send_multipart([bytes(To), bytes(envelopeString)])

@ClassLogger
class DealerRouter(RoutingProcess):
    """
    A combination of a :class:`Dealer` that connects to an `upstream` :class:`Router`
    and a :class:`Router` that provides messaging to `downstream` :class:`Dealer`s

    :param routerAddress: address of a router
    :type routerAddress: str
    :param address: address of this dealer
    :type address: str
    :param register: whether to automatically register this dealer with the router
    :type register: bool
    :param name: optional process name
    :type name: str
    :raises MessagingException: if router address is not set up or own address is
        unavailable
    """
    def __init__(self, routerAddress, address, register=True, name=None):

        self.address = address
        self.routerAddress = routerAddress
        self.register = register

        # make sure upstream address is already set up
        upstreamAddress = getIPCSocketAddress(self.routerAddress)
        if not isAddressInUse(upstreamAddress):
            raise MessagingException("{0} upstream '{1}' not set up".format(self.address, upstreamAddress))

        # check downstream address
        downstreamAddress = getIPCSocketAddress(self.address)
        if isAddressInUse(downstreamAddress):
            raise MessagingException("{0} '{1}' already in use".format(self.address, downstreamAddress))

        if name:
            processName = name + " " + address
        else:
            processName = address
        super(DealerRouter, self).__init__(upstreamAddress, downstreamAddress, name=processName)

    def areComponentsReady(self):
        """
        Check if the routing components are set up and ready to use

        :returns: ``True`` if ready, ``False`` otherwise
        :rtype: bool
        """
        return isAddressInUse(self.upstreamAddress) and isAddressInUse(self.downstreamAddress)

    def routeMessage(self, To, envelopeString, upstreamComponent, downstreamComponent):
        """
        Route message to upstream or downstream component using the To address.

        :param To: to field
        :type To: str
        :param envelopeString: envelope string
        :type envelopeString: str
        :param upstreamComponent: upstream component
        :param downstreamComponent: downstream component
        """
        receiverHierarchy = To.split("/")

        if To == self.address:
            # pass envelope to message handler and collect responses
            responseEnvelopes = []
            try:
                envelope = Envelope.fromJSON(envelopeString)
                responseEnvelopes = self.handleMessage(envelope)
            except Exception as e:
                self.log.error("Invalid message envelope %s", envelopeString)
                self.log.exception(e)
            for responseEnvelope in responseEnvelopes:
                self.routeMessage(responseEnvelope.To, responseEnvelope.toJSON(includeClassInfo=True), upstreamComponent, downstreamComponent)
        else:
            dealerRouterHierarchy = self.address.split("/")

            # find longest match in the hierarchy
            matchingHierarchy = []
            for index, part in enumerate(receiverHierarchy):
                if index < len(dealerRouterHierarchy) and part == dealerRouterHierarchy[index]:
                    matchingHierarchy.append(part)
                else:
                    break

            if len(matchingHierarchy) < len(dealerRouterHierarchy):
                self.log.debug("'%s' routing '%s' to '%s' upstream", self.address, envelopeString, To)
                upstreamComponent.routeMessage(To, envelopeString)
            else:
                self.log.debug("'%s' routing '%s' to '%s' downstream", self.address, envelopeString, To)
                downstreamComponent.routeMessage(To, envelopeString)


    def run(self):
        """
        The implementation of the `Dealer`-`Router` routing process
        """
        downstreamRouter = Router(self.address)
        upstreamDealer = ThinDealer(self.routerAddress, self.address, downstreamRouter.socket, register=self.register)

        poller = Poller()
        poller.register(downstreamRouter.socket, zmq.POLLIN)  # @UndefinedVariable
        try:
            while not self.stopFlag.is_set():
                sockets = dict(poller.poll(timeout=DEFAULT_POLL_TIMEOUT))

                if downstreamRouter.hasNewMessage(sockets):

                    To, envelopeString = downstreamRouter.newMessage
                    self.log.debug("'%s' received '%s' for '%s'", self.downstreamAddress, envelopeString, To)
                    self.routeMessage(To, envelopeString, upstreamDealer, downstreamRouter)

        except KeyboardInterrupt:
            self.log.debug("terminating %s", self.name)
        except:
            self.log.debug("stopping %s", self.name)
            self.log.error(traceback.format_exc())
        finally:
            self.stopFlag.set()
            poller.unregister(downstreamRouter.socket)

@ClassLogger
class Peer(BaseComponent):
    """
    A peer-to-peer message server that connects to other peers that
    are part of the same cluster

    :param address: address
    :type address: str
    :param clusterInfoImplementation: cluster information implementation
    :type clusterInfoImplementation: :class:`ClusterInfo`
    :raises MessagingException: if peer address is already in use
    """
    def __init__(self, address, clusterInfoImplementation):

        self.clusterInfo = clusterInfoImplementation
        self.peerSockets = {}
        self.stats = {}

        # check peer address
        socketAddress = self.clusterInfo.getNodeAddress(address)
        if not socketAddress:
            raise MessagingException("{0} is missing address in cluster info".format(address))

        super(Peer, self).__init__(address, socketAddress=socketAddress)

        for node in self.clusterInfo.nodeNames:
            self.setupPeerSocket(node)
            self.stats[node] = 0

    def routeMessage(self, To, envelopeString):
        """
        Route message to other peers

        :param To: to field
        :type To: str
        :param envelope: envelope
        :type envelope: :class:`Envelope`
        """
        receiverHierarchy = To.split("/")
        node = receiverHierarchy[0]
        newNodes = [newNode for newNode in self.clusterInfo.nodeNames if newNode not in self.peerSockets]
        for newNode in newNodes:
            self.setupPeerSocket(newNode)
            self.stats[newNode] = 0

        # check for node wildcard
        if node == "*":
            peers = self.clusterInfo.nodeNames
            envelope = Envelope.fromJSON(envelopeString)
            for peer in peers:
                # adjust To field of the envelope
                receiverHierarchy[0] = peer
                envelope.To = "/".join(receiverHierarchy)
                self.routeMessage(envelope.To, envelope.toJSON(includeClassInfo=True))
        else:
            address = self.clusterInfo.getNodeAddress(node)
            if address:
                self.log.debug("routing '%s' to '%s' at '%s'", envelopeString, node, address)
                # resolve aliases
                if node in self.clusterInfo.aliases:
                    node = self.clusterInfo.aliases[node]
                if self.peerSockets[node].closed:
                    self.setupPeerSocket(node)
                try:
                    self.peerSockets[node].send_multipart([bytes(To), bytes(envelopeString)])
                    self.stats[node] += 1
                except zmq.ZMQError as e:
                    self.log.debug("socket for '%s' was invalid", node)
                    self.log.debug(traceback.format_exc())
                    self.setupPeerSocket(node)
                    # retry sending message
                    try:
                        self.peerSockets[node].send_multipart([bytes(To), bytes(envelopeString)])
                        self.stats[node] += 1
                    except Exception as e:
                        self.log.error(e)
                        self.log.error(traceback.format_exc())
            else:
                self.log.error("%s could not be resolved to a valid address", node)

    def setupPeerSocket(self, node):
        """
        Setup persistent socket connection to peer

        :param node: node
        :type node: str
        """
        peerAddress = self.clusterInfo.getNodeAddress(node)
        if not peerAddress:
            self.log.error("could not find peer address for '%s'", node)
            return
        peerSocket = self.context.socket(zmq.PUSH)  # @UndefinedVariable
        # TODO: what about LINGER option for sockets in case we cannot send?
        peerSocket.set(zmq.LINGER, DEFAULT_ZMQ_LINGER)  # @UndefinedVariable
        peerSocket.set_hwm(DEFAULT_ZMQ_HWM)
        # FIXME: check replacement logic since this could cause problems
        peerSocket.connect(peerAddress.replace('*','127.0.0.1'))
        if node in self.peerSockets:
            self.peerSockets[node].close()
        self.peerSockets[node] = peerSocket

@ClassLogger
class PeerClient(object):
    """
    A client that connects to a :class:`Peer` using the
    specified address

    :param peerAddress: peer address
    :type peerAddress: str
    :param clusterInfoImplementation: cluster information implementation
    :type clusterInfoImplementation: :class:`ClusterInfo`
    :raises MessagingException: if peer is not available
    """
    def __init__(self, peerAddress, clusterInfo):
        self.peerAddress = peerAddress
        self.identifier = str(uuid.uuid4())
        self.address = "{name}/client-{identifier}".format(
            name=self.peerAddress,
            identifier=self.identifier
        )
        self.socketAddress = clusterInfo.getNodeAddress(self.peerAddress)

        # check peer address
        if not self.socketAddress:
            raise MessagingException("{0} is missing address in cluster info".format(self.peerAddress))
        if not isAddressInUse(self.socketAddress):
            raise MessagingException("Could not connect to '{peerAddress}'".format(peerAddress=self.peerAddress))

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)  # @UndefinedVariable
        self.socket.set(zmq.LINGER, DEFAULT_ZMQ_LINGER) # @UndefinedVariable
        self.socket.connect(self.socketAddress)

    def forwardMessage(self, envelope):
        """
        Forward a message into the system without waiting for the response.
        This represents a ``fire-and-forget`` message pattern

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        :returns: response
        """
        if not isinstance(envelope, Envelope):
            self.log.error("'%s' needs to be of type '%s'", envelope, Envelope)
            return

        # send off message
        self.socket.send_multipart([bytes(envelope.To), bytes(envelope.toJSON(includeClassInfo=True))])

@ClassLogger
class PeerRouter(RoutingProcess):
    """
    A combination of a :class:`Peer` that connects to other :class:`Peer`s
    and a :class:`Router` that provides messaging to `downstream` :class:`Dealer`s

    :param address: specific address of this component, e.g., ``peer1``
    :type address: str
    :param clusterInfoImplementation: cluster information implementation
    :type clusterInfoImplementation: :class:`ClusterInfo`
    :param name: optional process name
    :type name: str
    :raises MessagingException: if router address is not set up or own address is
        unavailable
    """
    def __init__(self, address, clusterInfoImplementation, name=None):
        self.address = address
        self.clusterInfo = clusterInfoImplementation

        # check upstream address
        upstreamAddress = self.clusterInfo.getNodeAddress(self.address)
        if not upstreamAddress:
            raise MessagingException("{0} is missing address in cluster info".format(self.address))
        if isAddressInUse(upstreamAddress):
            raise MessagingException("{0} '{1}' already in use".format(self.address, upstreamAddress))

        # check downstream address
        downstreamAddress = getIPCSocketAddress(self.address)
        if isAddressInUse(downstreamAddress):
            raise MessagingException("{0} '{1}' already in use".format(self.address, downstreamAddress))

        if name:
            processName = name + " " + address
        else:
            processName = address
        super(PeerRouter, self).__init__(upstreamAddress, downstreamAddress, name=processName)

    def areComponentsReady(self):
        """
        Check if the routing components are set up and ready to use

        :returns: ``True`` if ready, ``False`` otherwise
        :rtype: bool
        """
        return isAddressInUse(self.upstreamAddress) and isAddressInUse(self.downstreamAddress)

    def routeMessage(self, To, envelopeString, upstreamComponent, downstreamComponent):
        """
        Route message to upstream or downstream component using the To address.

        :param To: to field
        :type To: str
        :param envelopeString: envelope string
        :type envelopeString: str
        :param upstreamComponent: upstream component
        :param downstreamComponent: downstream component
        """
        receiverHierarchy = To.split("/")
        node = receiverHierarchy[0]

        # check if the address matches but ignore general wildcard
        if node != "*" and addressesMatch(self.address, node, upstreamComponent.clusterInfo.aliases.get(node)):

            # check if node is directly addressed
            if len(receiverHierarchy) == 1:
                # pass envelope to message handler and collect responses
                responseEnvelopes = []
                try:
                    envelope = Envelope.fromJSON(envelopeString)
                    responseEnvelopes = self.handleMessage(envelope)
                except Exception as e:
                    self.log.error("Invalid message envelope %s", envelopeString)
                    self.log.exception(e)
                for responseEnvelope in responseEnvelopes:
                    self.routeMessage(responseEnvelope.To, responseEnvelope.toJSON(includeClassInfo=True), upstreamComponent, downstreamComponent)
            else:
                # send downstream
                downstreamComponent.routeMessage(To, envelopeString)
        else:
            # route wild cards and other nodes through peer
            upstreamComponent.routeMessage(To, envelopeString)

    def run(self):
        """
        The implementation of the `Peer`-`Router` routing process
        """
        peer = Peer(self.address, self.clusterInfo)
        downstreamRouter = Router(self.address)

        poller = Poller()
        poller.register(peer.socket, zmq.POLLIN)  # @UndefinedVariable
        poller.register(downstreamRouter.socket, zmq.POLLIN)  # @UndefinedVariable
        try:
            while not self.stopFlag.is_set():
                sockets = dict(poller.poll(timeout=DEFAULT_POLL_TIMEOUT))

                if peer.hasNewMessage(sockets):

                    To, envelopeString = peer.newMessage
                    self.log.debug("'%s' received '%s' from peer for '%s'", self.upstreamAddress, envelopeString, To)
                    self.routeMessage(To, envelopeString, peer, downstreamRouter)

                if downstreamRouter.hasNewMessage(sockets):

                    To, envelopeString = downstreamRouter.newMessage
                    self.log.debug("'%s' received '%s' from downstream for '%s'", self.downstreamAddress, envelopeString, To)
                    self.routeMessage(To, envelopeString, peer, downstreamRouter)

        except KeyboardInterrupt:
            self.log.debug("terminating %s", self.name)
        except:
            self.log.debug("stopping %s", self.name)
            self.log.error(traceback.format_exc())
        finally:
            self.stopFlag.set()
            poller.unregister(peer.socket)
            poller.unregister(downstreamRouter.socket)

# TODO: documentation and check why we are overwriting
@ClassLogger
class Poller(zmq.Poller):

    def poll(self, timeout=DEFAULT_POLL_TIMEOUT):
        sockets = None
        while sockets is None:
            try:
                sockets = super(Poller, self).poll(timeout=timeout)
            except zmq.ZMQError, err:
                from pprint import pformat
                self.log.error("Caught error {0} {1}".format(err.errno, pformat(err)))
                if err.errno != errno.EINTR:
                    self.log.error("Raising error {0}".format(zmq.ZMQError.errno))
                    raise
                sockets = None  # restart the system call
        return sockets

    def register(self, socket, flags=zmq.POLLIN):  # @UndefinedVariable
        super(Poller, self).register(socket, flags=flags)

@ClassLogger
class Router(BaseComponent):
    """
    An implementation of a component that receives from and sends messages to
    `downstream` :class:`Dealer`s that connect to it.

    :param address: address of this router
    :type address: str
    :raises MessagingException: if router address is already in use
    """
    def __init__(self, address):
        super(Router, self).__init__(address)
        self.dealers = set()

    def hasNewMessage(self, sockets):
        """
        Check sockets dictionary if there is a new message
        and set ``newMessage`` if it is.

        :param sockets: sockets dictionary resulting from ``dict(poller.poll())``
        :type sockets: dict
        :returns: bool
        """
        if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:  # @UndefinedVariable
            self.newMessage = self.socket.recv_multipart()

            # TODO: check if there is a better/faster way to do this check since we want to avoid unneccessary deserialization
            if '"Action":"DealerRegistration"' in self.newMessage[1]:
                envelope = Envelope.fromJSON(self.newMessage[1])
                self.dealers.add(envelope.From)
                self.log.debug("'%s' received dealer '%s' registration and now knows dealers '%s'",
                               self.address, envelope.From, ",".join(self.dealers))
                self.newMessage = None
                return False
            return True
        else:
            return False

    def routeMessage(self, To, envelopeString):
        """
        Route message to the dealer/dealers

        :param To: to field
        :type To: str
        :param envelopeString: envelope string
        :type envelopeString: str
        """
        routerHierarchy = self.address.split("/")
        receiverHierarchy = To.split("/")
        self.log.debug("'%s' routing '%s' to '%s'", self.address, envelopeString, To)

        if len(receiverHierarchy) > len(routerHierarchy):

            directDealerPart = receiverHierarchy[len(routerHierarchy)]

            # TODO: check for other dealer wildcards
            if directDealerPart == "*":
                self.log.debug("routing message to dealers '%s'", ",".join(self.dealers))
                envelope = Envelope.fromJSON(envelopeString)
                for dealer in self.dealers:
                    # adjust To field of the envelope
                    receiverHierarchy[len(routerHierarchy)] = dealer.split("/")[len(routerHierarchy)]
                    envelope.To = "/".join(receiverHierarchy)
                    self.routeMessage(envelope.To, envelope.toJSON(includeClassInfo=True))
            else:
                dealerAddress = getIPCSocketAddress("|".join(receiverHierarchy[:len(routerHierarchy)+1]))
                if isAddressInUse(dealerAddress):
                    # TODO: decide whether to make this persistent like the Peer sockets
                    dealerSocket = self.context.socket(zmq.PUSH)  # @UndefinedVariable
                    # TODO: what about LINGER option for sockets in case we cannot send?
                    dealerSocket.set(zmq.LINGER, DEFAULT_ZMQ_LINGER)  # @UndefinedVariable
                    dealerSocket.set_hwm(DEFAULT_ZMQ_HWM)
                    dealerSocket.connect(dealerAddress)
                    dealerSocket.send_multipart([bytes(To), bytes(envelopeString)])
                else:
                    self.log.error("router '%s' does not have dealer '%s' with address '%s' available", self.address, directDealerPart, dealerAddress)

        else:
            log.error("'%s' is an upstream address for '%s'", To, self.address)

@ClassLogger
class RouterClient(object):
    """
    A client that connects to a :class:`Router` using the
    specified address

    :param routerAddress: router address
    :type routerAddress: str
    :raises MessagingException: if router is not available
    """
    def __init__(self, routerAddress):
        self.routerAddress = routerAddress
        self.identifier = str(uuid.uuid4())
        self.address = "{name}/client-{identifier}".format(
            name=self.routerAddress,
            identifier=self.identifier
        )
        try:
            self.upstreamDealer = Dealer(self.routerAddress, self.address, register=False)
        except MessagingException as e:
            self.log.error(e)
            raise MessagingException("Could not connect to '{routerAddress}'".format(routerAddress=self.routerAddress))

    def forwardMessage(self, envelope):
        """
        Forward a message into the system without waiting for the response.
        This represents a ``fire-and-forget`` message pattern

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        :returns: response
        """
        if not isinstance(envelope, Envelope):
            self.log.error("'%s' needs to be of type '%s'", envelope, Envelope)
            return

        # send off message
        self.upstreamDealer.routeMessage(envelope.To, envelope.toJSON(includeClassInfo=True))

    def sendMessage(self, envelope):
        """
        Send a message into the system without waiting for the response.
        This represents a ``fire-and-forget`` message pattern

        Note that this *WILL* automatically adjust the From field of the envelope
        to use the client address.

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        :returns: response
        """
        if not isinstance(envelope, Envelope):
            self.log.error("'%s' needs to be of type '%s'", envelope, Envelope)
            return

        # adjust from field
        envelope.From = self.address
        if hasattr(envelope, "ReplyTo"):
            self.log.warn("removing 'ReplyTo' field '%s' of '%s' envelope to '%s'", envelope.ReplyTo, envelope.Action, envelope.To)
            delattr(envelope, "ReplyTo")

        # send off message
        self.upstreamDealer.routeMessage(envelope.To, envelope.toJSON(includeClassInfo=True))

    def sendRequest(self, envelope, timeout=60):
        """
        Send a message into the system and wait for the response.
        This represents a ``synchronous request-reply`` message pattern

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        :param timeout: timeout
        :type timeout: int
        :returns: response
        """
        if not isinstance(envelope, Envelope):
            self.log.error("'%s' needs to be of type '%s'", envelope, Envelope)
            return None

        # adjust from and reply to fields
        envelope.From = self.address
        envelope.ReplyTo = self.address

        # send off message
        self.upstreamDealer.routeMessage(envelope.To, envelope.toJSON(includeClassInfo=True))

        # wait for response
        poller = Poller()
        poller.register(self.upstreamDealer.socket)
        response = None
        try:
            if timeout:
                end = time.time() + timeout
                while time.time() < end:
                    sockets = dict(poller.poll(timeout=DEFAULT_POLL_TIMEOUT))

                    if self.upstreamDealer.hasNewMessage(sockets):
                        envelope = Envelope.fromJSON(self.upstreamDealer.newMessage[1])
                        response = envelope.Message
                        break
                    else:
                        time.sleep(0.1)
                else:
                    self.log.error("timed out waiting for response for '%s' to '%s', took more than '%s' seconds", envelope.Action, envelope.To, timeout)

            else:
                sockets = dict(poller.poll(timeout=DEFAULT_POLL_TIMEOUT))

                if self.upstreamDealer.hasNewMessage(sockets):
                    envelope = Envelope.fromJSON(self.upstreamDealer.newMessage[1])
                    response = envelope.Message

        except KeyboardInterrupt:
            self.log.debug("terminating request")
        except:
            self.log.debug("stopping request")
            self.log.error(traceback.format_exc())
        finally:
            poller.unregister(self.upstreamDealer.socket)

        return response

@ClassLogger
class ThinDealer(object):
    """
    An implementation of a component that receives from and sends messages to a
    :class:`Router` that it connects to.

    Note that this uses an existing socket as its own instead of setting up a new
    one like the :class:`Dealer`

    :param routerAddress: address of a router
    :type routerAddress: str
    :param address: address of this dealer
    :type address: str
    :param socket: existing socket
    :type socket: :class:`zmq.Socket`
    :param register: whether to automatically register this dealer with the router
    :type register: bool
    :raises MessagingException: if router address is not set up
    """
    def __init__(self, routerAddress, address, socket, register=True):

        self.address = address
        self.routerAddress = routerAddress
        self.routerSocketAddress = getIPCSocketAddress(self.routerAddress)

        # make sure upstream address is already set up
        if not isAddressInUse(self.routerSocketAddress):
            raise MessagingException("{0} upstream '{1}' not set up".format(self.address, self.routerSocketAddress))

        self.context = zmq.Context()
        self.routerSocket = self.context.socket(zmq.PUSH)  # @UndefinedVariable
        self.routerSocket.set_hwm(DEFAULT_ZMQ_HWM)
        self.routerSocket.connect(self.routerSocketAddress)
        self.socket = socket
        self.newMessage = None

        if register:
            self.register()

    def hasNewMessage(self, sockets):
        """
        Check sockets dictionary if there is a new message
        and set ``newMessage`` if it is.

        :param sockets: sockets dictionary resulting from ``dict(poller.poll())``
        :type sockets: dict
        :returns: bool
        """
        if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:  # @UndefinedVariable
            self.newMessage = self.socket.recv_multipart()
            return True
        else:
            return False

    def register(self):
        """
        Send ``DealerRegistration`` message to the ``ROUTER``
        """
        self.routerSocket.send_multipart([
            bytes(self.routerAddress),
            bytes(Envelope(self.address, self.routerAddress, "DealerRegistration").toJSON(includeClassInfo=True))
        ])

    def routeMessage(self, To, envelopeString):
        """
        Route message to the ``ROUTER``

        :param messageParts: multipart message parts
        :type messageParts: [str]
        """
        self.log.debug("'%s' routing '%s' to '%s' via '%s'", self.address, envelopeString, To, self.routerAddress)
        self.routerSocket.send_multipart([bytes(To), bytes(envelopeString)])

def getIPCSocketAddress(address):
    """
    Retrieve matching ipc socket address based on the specifed address

    :param address: address
    :type address: str
    :returns: socket address
    :rtype: str
    """
    return "ipc://{prefix}/{address}.ipc".format(
                prefix=DEFAULT_IPC_PATH.rstrip("/"),
                address=address.replace("/", "|")
            )

def isAddressInUse(fullAddress):
    """
    Check if the specified address is already in use

    :param fullAddress: full ZeroMQ address including its protocol
    :type fullAddress: str
    :returns: ``True`` if address alreay in use, ``False`` otherwise
    :rtype: bool
    """
    if not fullAddress:
        return False
    [protocol, address] = fullAddress.split("://")
    if protocol == "ipc":
        if os.path.exists(address):
            log.debug("{0} exists".format(fullAddress))
            return True
    elif protocol == "tcp":
        try:
            context = zmq.Context()
            socket = context.socket(zmq.PULL)  # @UndefinedVariable
            socket.bind(fullAddress)
            socket.unbind(fullAddress)
        except zmq.ZMQError:
            log.debug("{0} exists".format(fullAddress))
            return True
    else:
        raise NotImplementedError
    return False
