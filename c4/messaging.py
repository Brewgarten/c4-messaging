"""
This library contains process and threading functionality to handle
ZMQ messages synchronously and asynchronously with both a functional
as well as object-oriented approach.

.. note::

    Asynchronous request-reply with a callback requires a client object.

Message Routing
---------------

Peers
^^^^^
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
A ZMQ ``ROUTER`` that provides messaging to `downstream` ``DEALERs``. E.g.,

.. code-block:: python

    router = Router("ipc://router.ipc")

Dealer
^^^^^^
A ZMQ ``DEALER`` that connects to a ``ROUTER`` using the
specified address and id. In the following example, we set up
a dealer with ``dealer`` address and ``1`` as the identity it presents
itself to the ``ROUTER``. It also registers with the ``ROUTER``.

.. code-block:: python

    dealer = Dealer("ipc://router.ipc", "dealer", "1")
    dealer.register()

Router-Dealer
^^^^^^^^^^^^^^^^^^^^^
A combination of a ZMQ ``DEALER`` that connects to an `upstream` ``ROUTER``
and a ``ROUTER`` that provides messaging to `downstream` ``DEALERs``. We can
use this approach to build hierarchies or message chains. For example after
we set up the following three components ``peer1/one``, ``peer1/one/one``, and
``peer1/one/one/one/`` we can send a message to ``peer1`` for ``peer1/one/one/one/``
which gets routed automatically.

.. code-block:: python

    context = zmq.Context()
    peer1 = context.socket(zmq.ROUTER)
    peer1.bind("ipc://peer1.ipc")

    one = DealerRouter("ipc://peer1.ipc", "peer1/one", "ipc://one.ipc")
    def oneHandler(message):
        logging.debug("peer1/one received %s", message)
    one.addHandler(oneHandler)
    one.start()

    one_one = DealerRouter("ipc://one.ipc", "peer1/one/one", "ipc://one|one.ipc")
    def oneOneHandler(message):
        logging.debug("peer1/one/one received %s", message)
    one_one.addHandler(oneOneHandler)
    one_one.start()

    one_one_one = DealerRouter("ipc://one|one.ipc", "peer1/one/one/one", "ipc://one|one|one.ipc")
    def oneOneOneHandler(message):
        logging.debug("peer1/one/one/one received %s", message)
        return "peer1/one/one/oneResponse"
    one_one_one.addHandler(oneOneOneHandler)
    one_one_one.start()

    peer1.send_multipart(["one", "one", "one", "testMessage"])

Peer-Router
^^^^^^^^^^^
A combination of a `peer` that connects to other `peers`
and a ``ROUTER`` that provides messaging to `downstream` ``DEALERs``.
This functionality can be used to set up several distinct peers on
nodes of a cluster which then perform `local` routing to connected
components.

.. code-block:: python

    clusterInfo = ClusterInfo()
    clusterInfo.nodes["peer1"] = "tcp://1.2.3.4:5000"
    clusterInfo.nodes["peer2"] = "tcp://5.6.7.8:5000"
    clusterInfo.aliases["peer1Alias"] = "peer1"

    peer1 = PeerRouter("peer1", clusterInfo, "ipc://peer1.ipc")
    def test1(message):
        logging.debug("peer1 received %s", message)
    peer1.addHandler(test1)
    peer1.start()

    peer2 = PeerRouter("peer2", clusterInfo, "ipc://peer2.ipc")
    def test2(message):
        logging.debug("peer2 received %s", message)
    peer2.addHandler(test2)
    peer2.start()

Message Servers
---------------

Client
^^^^^^

.. code-block:: python

    messageClient = MessageClient("tcp://1.2.3.4:5000")
    messageClient.sendMessage(message)
    response = messageClient.sendRequest(message)
    messageClient.sendRequest(message, callbackFunction)

Functional
^^^^^^^^^^

.. code-block:: python

    sendMessage("tcp://1.2.3.4:5000", message)
    response = sendRequest("tcp://1.2.3.4:5000", message)

Functionality
-------------
"""

import datetime
import inspect
import logging
import multiprocessing
import multiprocessing.managers
import os
import re
import sqlite3
import time
import traceback
import uuid

import zmq
import errno

import c4.utils.jsonutil
import c4.utils.util

from c4.utils.logutil import ClassLogger

DEFAULT_ZMQ_HWM = 0 # The default ZMQ_HWM value of zero means "no limit".
DEFAULT_ZMQ_LINGER = 250

DEFAULT_POLL_TIMEOUT = 100

IPC_PATH = "/tmp"

log = logging.getLogger(__name__)

__version__ = "1.0.dev"

@ClassLogger
class MessageHandlerProcess(multiprocessing.Process):
    """
    A message server handler process that contains a stop method and message handlers

    :param name: name
    :type name: str
    """
    def __init__(self, name=None):
        super(MessageHandlerProcess, self).__init__(name=name)
        self.handlers = []

        # connect stop flag to allow other processes to stop the message server
        self.stopFlagProcess = c4.utils.util.StopFlagProcess(self)
        self.stopFlag = self.stopFlagProcess.stopFlag

    def addHandler(self, function):
        """
        Add a message handler to the server

        .. note::

            The handler function should take exactly one argument: the message

        :param function: function
        :type function: func
        """
        self.handlers.append(function)

    def start(self):
        """
        Start message server
        """
        super(MessageHandlerProcess, self).start()
        self.stopFlagProcess.start()

    def stop(self):
        """
        Stop message server
        """
        self.stopFlag.set()

@ClassLogger
class SyncMessageServer(MessageHandlerProcess):
    """
    A ZMQ message server that handles messages synchronously.

    :param address: the address to listen on, e.g., `tcp://127.0.0.1:5000`
    :type address: str
    :param name: name of the server process
    :type name: str
    :param asJSON: encode/decode messages as JSON
    :type asJSON: bool
    """
    def __init__(self, address, name=None, asJSON=True):
        super(SyncMessageServer, self).__init__(name=name)
        self.address = address
        self.asJSON = asJSON

    def run(self):
        """
        The implementation of the asynchronous message server
        """
        context = zmq.Context()
        socket = context.socket(zmq.PULL)  # @UndefinedVariable
        socket.bind(self.address)

        try:
            while True:
                if self.asJSON:
                    message = socket.recv_json()
                else:
                    message = socket.recv_string()

                for handler in self.handlers:
                    handler(message)

        except KeyboardInterrupt:
            self.log.debug("terminating %s", self.name)
        except:
            self.log.debug("stopping %s", self.name)
            self.log.error(traceback.format_exc())
        finally:
            socket.unbind(self.address)
            socket.close()

    def terminate(self):
        """
        Terminate message server and clean up sockets accordingly
        """
        super(SyncMessageServer, self).terminate()
        if self.address.startswith("ipc"):
            socketFile = self.address.split("//")[1]
            if os.path.exists(socketFile):
                os.remove(socketFile)

@ClassLogger
class AsyncMessageServer(SyncMessageServer):
    """
    A ZMQ message server that handles messages asynchronously by using a
    process pool.

    :param address: the address to listen on, e.g., `tcp://127.0.0.1:5000`
    :type address: str
    :param name: name of the server process
    :type name: str
    :param asJSON: encode/decode messages as JSON
    :type asJSON: bool
    :param workers: number of workers in the process pool
    :type workers: int
    """
    def __init__(self, address, name=None, asJSON=True, workers=10):
        super(AsyncMessageServer, self).__init__(address, name=name, asJSON=asJSON)
        self.workers = workers

        self.tasks = multiprocessing.JoinableQueue()
        self.workerProcesses = [
                c4.utils.util.Worker(self.tasks, "{}-Worker-{}".format(name, i+1)).start()
                for i in range(self.workers)
            ]

    def run(self):
        """
        The implementation of the asynchronous message server
        """
        context = zmq.Context()
        socket = context.socket(zmq.PULL)  # @UndefinedVariable
        socket.bind(self.address)

        try:
            while True:
                if self.asJSON:
                    message = socket.recv_json()
                else:
                    message = socket.recv_string()

                for handler in self.handlers:
                    # check if class method or just regular function
                    if inspect.ismethod(handler):
                        # since class methods are not picklable we need to use a wrapper/proxy
                        self.tasks.put((methodProxyHandler, [handler.im_self, handler.im_func.__name__, message]))
                    else:
                        self.tasks.put((handler, [message]))

        except KeyboardInterrupt:
            self.log.debug("terminating %s", self.name)
        except:
            self.log.debug("stopping %s", self.name)
            self.log.error(traceback.format_exc())
        finally:
            socket.unbind(self.address)
            socket.close()

    def terminate(self):
        """
        Terminate message server and stop process pool accordingly
        """
        super(AsyncMessageServer, self).terminate()
        # close task queue
        for i in range(self.workers):
            self.tasks.put(None)
        # terminate worker processes
        for w in self.workerProcesses:
            w.terminate()

    def join(self, timeout=None):
        """
        Join message server and process pool accordingly
        """
        super(AsyncMessageServer, self).join(timeout=timeout)
        # join worker processes
        for w in self.workerProcesses:
            w.join(timeout=timeout)

@ClassLogger
class ClusterInfo(object):
    """
    A basic cluster information object
    """
    def __init__(self):
        self.nodes = c4.utils.util.SharedDictWithLock()
        self.aliases = c4.utils.util.SharedDictWithLock()

    def getNodeAddress(self, node):
        """
        Get address for specified node

        :param node: node
        :type node: str
        :returns: str or ``None`` if not found
        """
        if node in self.aliases:
            node = self.aliases[node]
        return self.nodes.get(node)

    @property
    def nodeNames(self):
        """
        Names of the nodes in the cluster
        """
        return self.nodes.keys()

    @property
    def nodeAddresses(self):
        """
        Addressess of the nodes in the cluster
        """
        return self.nodes.values()

@ClassLogger
class Dealer(object):
    """
    A ZMQ ``DEALER`` that connects to a ``ROUTER`` using the
    specified address and id

    :param routerAddress: address of a ``ROUTER``
    :type routerAddress: str
    :param address: address of this ``DEALER``
    :type address: str
    :param identity: identity used by the ``ROUTER`` to send messages
        to this ``DEALER``
    :type identity: str
    :raises MessagingException: if router address is not set up
    """
    def __init__(self, routerAddress, address, identity):
        self.routerAddress = routerAddress

        # make sure upstream address is already set up
        if not isAddressInUse(self.routerAddress):
            raise MessagingException("{0} upstream '{1}' not set up".format(self, self.routerAddress))

        self.address = address
        self.identity = identity
        context = zmq.Context()
        self.socket = context.socket(zmq.DEALER)  # @UndefinedVariable
        self.socket.set_hwm(DEFAULT_ZMQ_HWM)
        self.socket.identity = self.identity.encode("ascii")
        self.socket.connect(self.routerAddress)
        self.newMessage = None

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
        Send ``dealerRegistration`` message to the ``ROUTER``
        """
        self.socket.send_string(Envelope(self.address, self.routerAddress, "dealerRegistration").toJSON(True))

    def routeMessage(self, messageParts):
        """
        Route message to the ``ROUTER``

        :param messageParts: multipart message parts
        :type messageParts: [str]
        """
        destination = messageParts[:-1]
        envelopeString = messageParts[-1]
        self.log.debug("'%s' routing '%s' to '%s'", self.address, envelopeString, destination)
        self.socket.send_multipart(messageParts)

    def __del__(self):
        """
        Disconnect from ``ROUTER``
        """
        try:
            self.socket.disconnect(self.address)
            self.socket.close()
        except:
            pass

@ClassLogger
class Poller(zmq.Poller):
    def poll(self, timeout=None):
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

@ClassLogger
class DealerRouter(multiprocessing.Process):
    """
    A combination of a ZMQ ``DEALER`` that connects to an `upstream` ``ROUTER``
    and a ``ROUTER`` that provides messaging to `downstream` ``DEALERs``

    :param upstreamAddress: address of an `upstream` ``ROUTER``
    :type upstreamAddress: str
    :param address: address of this component
    :type address: str
    :param downstreamAddress: address for `downstream` ``DEALERs``
    :type downstreamAddress: str
    :raises MessagingException: if either upstream address is not set up or downstream address is already in use
    """
    def __init__(self, upstreamAddress, address, downstreamAddress, name=None):
        if name:
            processName = name + " " + address
        else:
            processName = address
        super(DealerRouter, self).__init__(name=processName)
        self.handlers = []
        self.upstreamAddress = upstreamAddress
        self.address = address
        self.downstreamAddress = downstreamAddress

        # make sure upstream address is already set up
        if not isAddressInUse(self.upstreamAddress):
            raise MessagingException("{0} upstream '{1}' not set up".format(self, self.upstreamAddress))

        # check downstream address
        if isAddressInUse(self.downstreamAddress):
            raise MessagingException("{0} '{1}' already in use".format(self, self.downstreamAddress))

        self.stopFlag = multiprocessing.Event()

    def addHandler(self, function):
        """
        Add a message handler to the server

        .. note::

            The handler function should take exactly one argument: the message

        :param function: function
        :type function: func
        """
        self.handlers.append(function)

    def handleMessage(self, envelopeString, upstreamDealer):
        """
        Pass message to message handlers and send out responses accordingly.

        :param envelopeString: envelope string
        :type envelopeString: str
        :param upstreamDealer: upstream dealer
        :type upstreamDealer: :class:`Dealer`
        """
        try:
            envelope = Envelope.fromJSON(envelopeString)

            for handler in self.handlers:
                response = c4.utils.util.callWithVariableArguments(handler, envelope.Message, envelope)
                if response is not None:
                    try:
                        envelope.toResponse(response)
                        envelope.From = self.address
                        upstreamDealer.routeMessage([bytes(envelope.To), bytes(envelope.toJSON(True))])
                    except Exception as e:
                        self.log.error(e)

        except:
            self.log.error("Invalid message envelope %s", envelopeString)
            self.log.error(traceback.format_exc())

    def run(self):
        """
        The implementation of the ``DEALER``-``ROUTER`` message server
        """
        upstreamDealer = Dealer(self.upstreamAddress, self.address, self.address.split("/")[-1])
        upstreamDealer.register()

        downstreamRouter = Router(self.downstreamAddress)

        poller = Poller()
        poller.register(upstreamDealer.socket, zmq.POLLIN)  # @UndefinedVariable
        poller.register(downstreamRouter.socket, zmq.POLLIN)  # @UndefinedVariable
        try:

            while not self.stopFlag.is_set():
                sockets = dict(poller.poll(timeout=DEFAULT_POLL_TIMEOUT))

                if upstreamDealer.hasNewMessage(sockets):

                    self.log.debug("upstream '%s' received '%s'", self.upstreamAddress, upstreamDealer.newMessage)

                    # check if for me else pass downstream
                    if len(upstreamDealer.newMessage) == 1:
                        self.handleMessage(upstreamDealer.newMessage[0], upstreamDealer)
                    else:
                        downstreamRouter.routeMessage(upstreamDealer.newMessage)

                if downstreamRouter.hasNewMessage(sockets):

                    From, To, envelopeString = downstreamRouter.newMessage
                    if any(ord(c) >= 128 for c in From):
                        From = "dealerClient"
                    self.log.debug("'%s' received '%s' from downstream '%s' for '%s'", self.downstreamAddress, envelopeString, From, To)

                    # check if for me else pass upstream
                    if c4.utils.util.addressesMatch(self.address, To):
                        self.handleMessage(envelopeString, upstreamDealer)
                    else:
                        upstreamDealer.routeMessage([To, envelopeString])

        except KeyboardInterrupt:
            self.log.debug("terminating %s", self.name)
        except:
            self.log.debug("stopping %s", self.name)
            self.log.error(traceback.format_exc())
        finally:
            self.stopFlag.set()
            poller.unregister(upstreamDealer.socket)
            poller.unregister(downstreamRouter.socket)
            del upstreamDealer
            del downstreamRouter

    def start(self, timeout=None):
        """
        Start the message server

        :param timeout: timeout in seconds, if set stop is blocking else non-blocking
        :type timeout: int
        :returns: whether start was successful
        :rtype: bool
        """
        super(DealerRouter, self).start()
        if timeout:
            end = time.time() + timeout
            while time.time() < end:
                if not self.is_alive() or not isAddressInUse(self.downstreamAddress):
                    time.sleep(0.1)
                else:
                    self.log.debug("Started '%s'", self.name)
                    break
            else:
                self.log.error("Starting '%s' timed out", self.name)
                return False
        return True

    def stop(self, timeout=None):
        """
        Stop the message server

        :param timeout: timeout in seconds, if set stop is blocking else non-blocking
        :type timeout: int
        :returns: whether stop was successful
        :rtype: bool
        """
        self.stopFlag.set()
        if timeout:
            end = time.time() + timeout
            while time.time() < end:
                if self.is_alive():
                    time.sleep(0.1)
                else:
                    self.log.debug("Stopped '%s'", self.name)
                    break
            else:
                self.log.error("Stopping '%s' timed out", self.name)
                return False
        return True

    def terminate(self):
        """
        Terminate message server and clean up sockets accordingly
        """
        super(DealerRouter, self).terminate()
        if self.downstreamAddress.startswith("ipc"):
            socketFile = self.downstreamAddress.split("//")[1]
            if os.path.exists(socketFile):
                os.remove(socketFile)

class Envelope(c4.utils.jsonutil.JSONSerializable):
    """
    A message packaged in an WS-Addressing like envelope

    :param From: from field
    :type From: str
    :param To: to field
    :type To: str
    :param Action: action field, corresponding to an operation
    :type Action: str
    :param isRequest: is the new message is a request
    :type isRequest: bool
    :param includeTime: include a time stamp in the message
    :type includeTime: bool
    """
    def __init__(self, From, To, Action=None, isRequest=True, includeTime=False):
        super(Envelope, self).__init__()
        self.MessageID = str(uuid.uuid4())
        self.From = From
        if isRequest:
            self.ReplyTo = From
        self.To = To
        if Action:
            self.Action = Action
        else:
            self.Action = self.__class__.__name__
        self.Message = {}

        if includeTime:
            utcTime = datetime.datetime.utcnow()
            self.Message["time"] = "{:%Y-%m-%d %H:%M:%S}.{:06d}".format(utcTime, utcTime.microsecond)

    # TODO: implement check function to make sure we have all properties (From, To, Action, MessageID, etc...)

    def isResponse(self):
        """
        Check if the message is a response
        """
        return hasattr(self, "RelatesTo")

    def toResponse(self, message=None):
        """
        Convert envelope to a response and include the optional message

        :param message: message
        """
        if hasattr(self, "RelatesTo"):
            raise Exception("{} already a response".format(self.toJSON(True)))

        self.RelatesTo = self.MessageID
        self.MessageID = str(uuid.uuid4())
        self.From = self.To
        self.To = self.ReplyTo
        del self.ReplyTo
        self.Message = {}
        if message is not None:
            self.Message = message
        return self

@ClassLogger
class MessageClient(object):
    """
    A ZMQ message client that allows several message patterns

    * ``fire-and-forget``
    * ``synchronous request-reply``
    * ``asynchronous request-reply`` using a process pool

    :param address: the address to send to, e.g., `tcp://1.2.3.4:5000`
    :type address: str
    :param asJSON: encode/decode messages as JSON
    :type asJSON: bool
    :param workers: number of workers in the process pool
    :type workers: int
    """
    def __init__(self, address, asJSON=True, workers=10):
        self.address = address
        self.asJSON = asJSON
        self.workers = workers
        self.pool = multiprocessing.Pool(
            self.workers, initializer=poolWorkerInitializer, initargs=["MessageClient"])

    def sendMessage(self, message):
        """
        Send a message. This represents a
        ``fire-and-forget`` message pattern

        :param message: message
        :type message: dict
        """
        sendMessage(self.address, message)

    def sendRequest(self, message, callback=None):
        """
        Send a message and wait for the response or if a callback is
        specified return immediately. This represents either a
        ``synchronous request-reply`` or an ``asynchronous request-reply``
        message pattern

        :param message: message
        :type message: dict
        :param callback: optional callback function which takes one argument, the response
        :type callback: func
        :returns: response or :class:`~multiprocessing.pool.ApplyResult`
        """
        try:
            if callback:
                return self.pool.apply_async(sendRequest, [self.address, message, self.asJSON], callback=callback)
            else:
                return sendRequest(self.address, message, self.asJSON)
        except:
            self.log.error("Could not send message %s", message)
            self.log.error(traceback.format_exc())

@ClassLogger
class MessageTracker(object):
    """
    Messaging utility class that allows the tracking of related messages.

    This implementation is backed by :class:`~multiprocessing.managers.SyncManager`
    """
    def __init__(self):
        super(MessageTracker, self).__init__()

        self.manager = multiprocessing.managers.SyncManager()
        self.manager.start(c4.utils.util.disableInterruptSignal)

        self.messages = self.manager.dict()
        self.related = self.manager.dict()
        self.relatedReverse = self.manager.dict()
        self.lock = multiprocessing.RLock()
        self.acquire = self.lock.acquire
        self.release = self.lock.release

    def add(self, message):
        """
        Add message

        :param message: message
        :type message: :class:`Envelope`
        """
        self.acquire()
        try:
            self.messages[message.MessageID] = message
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def addRelatedMessage(self, messageId, relatedId):
        """
        Add related message

        :param messageId: message id
        :type messageId: str
        :param relatedId: related message id
        :type relatedId: str
        """
        self.acquire()
        try:
            if messageId not in self.related:
                self.related[messageId] = set()
            relatedSet = self.related[messageId]
            relatedSet.add(relatedId)
            self.related[messageId] = relatedSet
            self.relatedReverse[relatedId] = messageId
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def getRelatedMessageIds(self, messageId):
        """
        Get message ids that are related to the specified message id

        :param messageId: message id
        :type messageId: str
        :returns: [str]
        """
        self.acquire()
        try:
            if messageId in self.related:
                return self.related[messageId]
            else:
                return []
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def hasMoreRelatedMessages(self, messageId):
        """
        Check if there are more messages related to the specified one

        :param messageId: message id
        :type messageId: str
        :returns: bool
        """
        return messageId in self.related

    def isARelatedMessage(self, messageId):
        """
        Check if this message is related to another message

        :param messageId: message id
        :type messageId: str
        :returns: bool
        """
        return messageId in self.relatedReverse

    def isInMessages(self, messageId):
        """
        Check if messageId corresponds to a message in self.messages

        :param messageId: message id
        :type messageId: str
        :returns: bool
        """
        return messageId in self.messages

    def remove(self, messageId):
        """
        Remove message

        :param messageId: message id
        :type messageId: str
        :returns: message
        :rtype: :class:`Envelope`
        """
        self.acquire()
        message = self.messages.pop(messageId, None)
        # also remove all related message ids
        relatedSet = self.related.pop(messageId, set())
        if relatedSet:
            self.log.warn("'%s' still has related messages: %s", messageId, relatedSet)
        for relatedId in relatedSet:
            self.relatedReverse.pop(relatedId, None)
        self.release()
        return message

    def removeRelatedMessage(self, relatedId):
        """
        Remove related message

        :param relatedId: related message id
        :type relatedId: str
        :returns: message id that the related id is connected to
        :rtype: str
        """
        self.acquire()
        messageId = None
        if relatedId in self.relatedReverse:
            messageId = self.relatedReverse.pop(relatedId)
            relatedSet = self.related[messageId]
            relatedSet.remove(relatedId)
            if relatedSet:
                self.related[messageId] = relatedSet
            else:
                self.related.pop(messageId)
        self.release()
        return messageId

    def updateMessageContent(self, messageId, content):
        """
        Update message content of the stored message with.

        This merges existing message content with the specified new one.

        :param messageId: message id
        :type messageId: str
        :param content: new message content
        """
        self.acquire()
        if messageId in self.messages:
            storedMessageEnvelope = self.messages[messageId]
            storedMessageEnvelope.Message = c4.utils.util.mergeDictionaries(
                                                storedMessageEnvelope.Message,
                                                content)
            self.messages[messageId] = storedMessageEnvelope
        self.release()

@ClassLogger
class MessageTrackerDB(object):
    """
    Messaging utility class that allows the tracking of related messages.

    This implementation is backed by ``sqlite``
    """
    def __init__(self):
        super(MessageTrackerDB, self).__init__()
        self.filename = "{0}.mt".format(str(uuid.uuid4()))

        self.connection = sqlite3.connect(self.filename)
        cursor = self.connection.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS messages (
                id text PRIMARY KEY NOT NULL,
                message text)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS related (
                id text NOT NULL,
                relatedId text NOT NULL)""")
        self.connection.commit()
        self.lock = multiprocessing.RLock()
        self.acquire = self.lock.acquire
        self.release = self.lock.release

    def __del__(self):
        self.connection.close()
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def add(self, message):
        """
        Add message

        :param message: message
        :type message: :class:`Envelope`
        """
        self.acquire()
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO messages (id, message) VALUES (?, ?)", (message.MessageID, message.toJSON(includeClassInfo=True),))
            self.connection.commit()
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def addRelatedMessage(self, messageId, relatedId):
        """
        Add related message

        :param messageId: message id
        :type messageId: str
        :param relatedId: related message id
        :type relatedId: str
        """
        self.acquire()
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO related (id, relatedId) VALUES (?, ?)", (messageId, relatedId,))
            self.connection.commit()
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def getRelatedMessageIds(self, messageId):
        """
        Get message ids that are related to the specified message id

        :param messageId: message id
        :type messageId: str
        :returns: [str]
        """
        self.acquire()
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT relatedId FROM related where id is ?", (messageId,))
            rows = cursor.fetchall()
            if rows is None:
                return []
            else:
                return [row[0] for row in rows]
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def hasMoreRelatedMessages(self, messageId):
        """
        Check if there are more messages related to the specified one

        :param messageId: message id
        :type messageId: str
        :returns: bool
        """
        self.acquire()
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM related where id is ?", (messageId,))
            rows = cursor.fetchone()
            if rows is None:
                return False
            else:
                if rows[0] > 0:
                    return True
                else:
                    return False
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def remove(self, messageId):
        """
        Remove message

        :param messageId: message id
        :type messageId: str
        """
        self.acquire()
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT message FROM messages where id is ?", (messageId,))
            row = cursor.fetchone()
            messageString = row[0]
            message = Envelope.fromJSON(messageString)
            cursor.execute("DELETE FROM messages WHERE id is ?", (messageId,))
            self.connection.commit()
            return message
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

    def removeRelatedMessage(self, relatedId):
        """
        Remove related message

        :param relatedId: related message id
        :type relatedId: str
        """
        self.acquire()
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM related where relatedId is ?", (relatedId,))
            row = cursor.fetchone()
            messageId = row[0][0]
            cursor.execute("DELETE FROM related WHERE relatedId is ?", (relatedId,))
            self.connection.commit()
            return messageId
        except Exception as e:
            self.log.error(e)
        finally:
            self.release()

class MessagingException(Exception):
    """
    A generic messaging exception
    :param message: message
    :type message: str
    """
    def __init__(self, message):
        super(MessagingException, self).__init__()
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return "MessagingException('%s')"%self.message

@ClassLogger
class Peer(object):
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
        self.address = address
        self.clusterInfo = clusterInfoImplementation
        self.peerSockets = {}

        # check peer address
        if isAddressInUse(self.address):
            raise MessagingException("{0} '{1}' already in use".format(self, self.address))

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)  # @UndefinedVariable
        self.socket.set_hwm(DEFAULT_ZMQ_HWM)
        self.socket.bind(self.address)

        self.stats = {}

        for node in self.clusterInfo.nodeNames:
            self.setupPeerSocket(node)
            self.stats[node] = 0

        self.newMessage = None

    def hasNewMessage(self, sockets):
        """
        Check sockets dictionary if there is a new message
        and set ``newMessage`` if it is.

        :param sockets: sockets dictionary resulting from ``dict(poller.poll())``
        :type sockets: dict
        :returns: bool
        """
        if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:  # @UndefinedVariable

            envelopeString = self.socket.recv_string()
            try:
                # convert into envelope
                self.newMessage = Envelope.fromJSON(envelopeString)
            except:
                self.log.error("Invalid message envelope %s", self.newMessage)
                self.log.error(traceback.format_exc())
                return False

            return True
        else:
            return False

    def routeMessage(self, envelope):
        """
        Route message to other peers

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        """
        receiverHierarchy = envelope.To.split("/")
        node = receiverHierarchy[0]
        newNodes = [newNode for newNode in self.clusterInfo.nodeNames if newNode not in self.peerSockets]
        for newNode in newNodes:
            self.setupPeerSocket(newNode)
            self.stats[newNode] = 0

        if node == "*":
            # resolve node wildcard
            peers = self.clusterInfo.nodeNames
        else:
            peers = [node]

        for node in peers:
            # adjust To field of the envelope
            receiverHierarchy[0] = node
            envelope.To = "/".join(receiverHierarchy)

            # TODO: handle special case of node being ourself, maybe take a short-cut and don't route to external socket
            address = self.clusterInfo.getNodeAddress(node)
            if address:
                log.debug("routing '%s' to '%s' at '%s'", envelope, node, address)
                # resolve aliases
                if node in self.clusterInfo.aliases:
                    node = self.clusterInfo.aliases[node]
                if self.peerSockets[node].closed:
                    self.setupPeerSocket(node)
                try:
                    self.peerSockets[node].send_string(envelope.toJSON(True))
                    self.stats[node] += 1
                except zmq.ZMQError as e:
                    self.log.debug("socket for '%s' was invalid", node)
                    self.log.debug(traceback.format_exc())
                    self.setupPeerSocket(node)
                    # retry sending message
                    try:
                        self.peerSockets[node].send_string(envelope.toJSON(True))
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
        peerSocket = self.context.socket(zmq.PUSH)  # @UndefinedVariable
        # TODO: what about LINGER option for sockets in case we cannot send?
        peerSocket.set(zmq.LINGER, DEFAULT_ZMQ_LINGER)  # @UndefinedVariable
        peerSocket.set_hwm(DEFAULT_ZMQ_HWM)
        if peerAddress is not None:
            peerSocket.connect(peerAddress.replace('*','127.0.0.1'))
        else:
            peerSocket.connect(peerAddress)
        if node in self.peerSockets:
            self.peerSockets[node].close()
        self.peerSockets[node] = peerSocket

    def __del__(self):
        """
        Unbind address
        """
        try:
            self.socket.unbind(self.address)
            self.socket.close()
        except:
            pass
        for node, peerSocket in self.peerSockets.items():
            try:
                peerSocket.disconnect(self.clusterInfo.getNodeAddress(node))
            except:
                pass

@ClassLogger
class PeerRouter(multiprocessing.Process):
    """
    A combination of a `peer` that connects to other `peers`
    and a ``ROUTER`` that provides messaging to `downstream` ``DEALERs``

    :param address: specific address of this component, e.g., ``peer1``
    :type address: str
    :param clusterInfoImplementation: cluster information implementation
    :type clusterInfoImplementation: :class:`ClusterInfo`
    :param downstreamAddress: address for `downstream` ``DEALERs``
    :type downstreamAddress: str
    :raises MessagingException: if either peer or downstream address is already in use
    """
    def __init__(self, address, clusterInfoImplementation, downstreamAddress, name=None):
        if name:
            processName = name + " " + address
        else:
            processName = address
        super(PeerRouter, self).__init__(name=processName)
        self.handlers = []
        self.address = address
        self.clusterInfo = clusterInfoImplementation
        self.downstreamAddress = downstreamAddress
        self.peerAddress = self.clusterInfo.getNodeAddress(self.address)

        # check peer address
        if isAddressInUse(self.peerAddress):
            raise MessagingException("{0} '{1}' already in use".format(self, self.peerAddress))

        # check downstream address
        if isAddressInUse(self.downstreamAddress):
            raise MessagingException("{0} '{1}' already in use".format(self, self.downstreamAddress))

        self.stopFlag = multiprocessing.Event()

    def addHandler(self, function):
        """
        Add a message handler to the server

        .. note::

            The handler function should take exactly one argument: the message

        :param function: function
        :type function: func
        """
        self.handlers.append(function)

    def handleMessage(self, envelope, peer):
        """
        Pass message to message handlers and send out responses accordingly.

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        :param peer: peer
        :type peer: :class:`Peer`
        """
        for handler in self.handlers:
            response = c4.utils.util.callWithVariableArguments(handler, envelope.Message, envelope)
            if response is not None:
                try:
                    envelope.toResponse(response)
                    envelope.From = self.address
                    peer.routeMessage(envelope)
                except Exception as e:
                    self.log.error(e)

    def run(self):
        """
        The implementation of the `peer`-``ROUTER`` message server
        """
        peer = Peer(self.peerAddress, self.clusterInfo)

        downstreamRouter = Router(self.downstreamAddress)

        poller = Poller()
        poller.register(peer.socket, zmq.POLLIN)  # @UndefinedVariable
        poller.register(downstreamRouter.socket, zmq.POLLIN)  # @UndefinedVariable
        try:
            while not self.stopFlag.is_set():
                sockets = dict(poller.poll(timeout=DEFAULT_POLL_TIMEOUT))

                if peer.hasNewMessage(sockets):

                    self.log.debug("peer '%s' received '%s'", self.peerAddress, peer.newMessage)

                    envelope = peer.newMessage
                    receiverHierarchy = envelope.To.split("/")
                    node = receiverHierarchy[0]

                    # check if the address matches but ignore general wildcard
                    if node != "*" and c4.utils.util.addressesMatch(self.address, node, peer.clusterInfo.aliases.get(node)):

                        # check if node is directly addressed
                        if len(receiverHierarchy) == 1:
                            self.handleMessage(envelope, peer)
                        else:
                            # remove node from routing hierarchy and send downstream
                            messageParts = receiverHierarchy[1:]
                            messageParts.append(envelope.toJSON(True))
                            downstreamRouter.routeMessage(messageParts)
                    else:
                        # route to peer
                        peer.routeMessage(envelope)

                if downstreamRouter.hasNewMessage(sockets):

                    From, To, envelopeString = downstreamRouter.newMessage
                    if any(ord(c) >= 128 for c in From):
                        From = "dealerClient"
                    self.log.debug("'%s' received '%s' from downstream '%s' for '%s'", self.downstreamAddress, envelopeString, From, To)

                    try:
                        envelope = Envelope.fromJSON(envelopeString)

                        # check if for me else route to peer
                        if c4.utils.util.addressesMatch(self.address, To, peer.clusterInfo.aliases.get(To)):
                            self.handleMessage(envelope, peer)
                        else:
                            # route to peer
                            peer.routeMessage(Envelope.fromJSON(envelopeString))
                    except KeyboardInterrupt:
                        raise
                    except:
                        self.log.error("Invalid message envelope %s", envelopeString)
                        self.log.error(traceback.format_exc())

        except KeyboardInterrupt:
            self.log.debug("terminating %s", self.name)
        except:
            self.log.debug("stopping %s", self.name)
            self.log.error(traceback.format_exc())
        finally:
            self.stopFlag.set()
            poller.unregister(peer.socket)
            poller.unregister(downstreamRouter.socket)
            del peer
            del downstreamRouter

    def start(self, timeout=None):
        """
        Start the message server

        :param timeout: timeout in seconds, if set stop is blocking else non-blocking
        :type timeout: int
        :returns: whether start was successful
        :rtype: bool
        """
        super(PeerRouter, self).start()
        if timeout:
            end = time.time() + timeout
            while time.time() < end:
                if not self.is_alive() or not isAddressInUse(self.peerAddress) or not isAddressInUse(self.downstreamAddress):
                    time.sleep(0.1)
                else:
                    self.log.debug("Started '%s'", self.name)
                    break
            else:
                self.log.error("Starting '%s' timed out", self.name)
                return False
        return True

    def stop(self, timeout=None):
        """
        Stop the message server

        :param timeout: timeout in seconds, if set stop is blocking else non-blocking
        :type timeout: int
        :returns: whether stop was successful
        :rtype: bool
        """
        self.stopFlag.set()
        if timeout:
            end = time.time() + timeout
            while time.time() < end:
                if self.is_alive():
                    time.sleep(0.1)
                else:
                    self.log.debug("Stopped '%s'", self.name)
                    break
            else:
                self.log.error("Stopping '%s' timed out", self.name)
                return False
        return True

    def terminate(self):
        """
        Terminate message server and clean up sockets accordingly
        """
        super(PeerRouter, self).terminate()
        if self.downstreamAddress.startswith("ipc"):
            socketFile = self.downstreamAddress.split("//")[1]
            if os.path.exists(socketFile):
                os.remove(socketFile)

@ClassLogger
class Router(object):
    """
    A ZMQ ``ROUTER`` that provides messaging to `downstream` ``DEALERs``

    :param address: address of this ``ROUTER``
    :type address: str
    :raises MessagingException: if router address is already in use
    """
    def __init__(self, address):
        self.address = address

        # check router address
        if isAddressInUse(self.address):
            raise MessagingException("{0} '{1}' already in use".format(self, self.address))

        context = zmq.Context()
        self.socket = context.socket(zmq.ROUTER)  # @UndefinedVariable
        self.socket.set_hwm(DEFAULT_ZMQ_HWM)
        self.socket.bind(self.address)
        self.dealers = set()
        self.newMessage = None

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

            if len(self.newMessage) == 2:

                # register dealer
                self.dealers.add(self.newMessage[0])
                self.log.debug("'%s' received dealer '%s' registration and now knows dealers '%s'",
                              self.address, self.newMessage[0], ",".join(self.dealers))
                self.newMessage = None
                return False

                # TODO: check if necessary, we could assume that even messages with 2 parts are always registration
                # and others always have [From, To, envelopeString]
                # what about device manager registration?
#                 try:
#                     envelope = Envelope.fromJSON(self.newMessage[1])
#
#                     # ignore registration message
#                     if envelope.Action == "dealerRegistration":
#                         self.newMessage = None
#                         return False
#
#                     log.debug("%s received %s for %s", self.socket, envelope.Action, envelope.To)
#                 except:
#                     log.error("Invalid message envelope %s", self.newMessage[1])
#                     log.error(traceback.format_exc())

            return True
        else:
            return False

    def routeMessage(self, messageParts):
        """
        Route message to the ``DEALERs``

        :param messageParts: multipart message parts
        :type messageParts: [str]
        """
        destination = messageParts[:-1]
        envelopeString = messageParts[-1]

        self.log.debug("'%s' routing '%s' to '%s'", self.address, envelopeString, destination)

        # TODO: check for other dealer wildcards
        if destination[0] == "*":

            self.log.debug("routing message to dealers '%s'", ",".join(self.dealers))
            for dealer in self.dealers:
                destination[0] = dealer
                self.log.debug("routing message to '%s'", destination[0])
                messageParts = [bytes(receiver) for receiver in destination]
                messageParts.append(bytes(envelopeString))
                self.log.debug(messageParts)
                self.socket.send_multipart(messageParts)
        else:
            self.log.debug("routing message to '%s'", destination[0])
            messageParts = [bytes(receiver) for receiver in destination]
            messageParts.append(bytes(envelopeString))
            self.log.debug(messageParts)
            self.socket.send_multipart(messageParts)

    def __del__(self):
        """
        Unbind address
        """
        try:
            self.socket.unbind(self.address)
            self.socket.close()
        except:
            pass
        if self.address.startswith("ipc"):
            socketFile = self.address.split("//")[1]
            if os.path.exists(socketFile):
                try:
                    os.remove(socketFile)
                except:
                    pass

@ClassLogger
class RouterClient(object):
    """
    A ZMQ client that connects to a ``ROUTER`` using the
    specified address

    :param fullRouterName: full address/name of the router, e.g., a/b/c/router
    :type fullRouterName: str
    :param routerAddress: optional address of a ``ROUTER``, e.g., ipc://a|b|c|router.ipc
        (uses ipc://<fullRouterName>.ipc by default with / replaced by |
    :type routerAddress: str
    """
    def __init__(self, fullRouterName, routerAddress=None):
        self.fullRouterName = fullRouterName
        self.routerName = fullRouterName.split("/")[-1]
        if routerAddress:
            self.routerAddress = routerAddress
        else:
            self.routerAddress = "ipc://{address}.ipc".format(
                address=self.fullRouterName.replace("/", "|")
            )
        self.identifier = str(uuid.uuid4())
        self.address = "{name}/client-{identifier}".format(
            name=self.fullRouterName,
            identifier=self.identifier
        )
        self.upstreamDealer = Dealer(self.routerAddress, self.address, self.address.split("/")[-1])

    def sendMessage(self, envelope):
        """
        Send a message into the system without waiting for the response.
        This represents a ``fire-and-forget`` message pattern

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        :param timeout: timeout
        :type timeout: int
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
        self.upstreamDealer.routeMessage([bytes(self.routerName), bytes(envelope.toJSON(True))])

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
            return

        # adjust from and reply to fields
        envelope.From = self.address
        envelope.ReplyTo = self.address

        # send off message
        self.upstreamDealer.routeMessage([bytes(self.routerName), bytes(envelope.toJSON(True))])

        # wait for response
        poller = Poller()
        poller.register(self.upstreamDealer.socket, zmq.POLLIN) # @UndefinedVariable
        try:
            sockets = dict(poller.poll(timeout=timeout))

            if self.upstreamDealer.hasNewMessage(sockets):
                envelope = Envelope.fromJSON(self.upstreamDealer.newMessage[0])
                return envelope.Message
            else:
                log.error("timed out waiting for response for '%s' to '%s', took more than '%s' seconds", envelope.Action, envelope.To, timeout)
                return None

        except KeyboardInterrupt:
            self.log.debug("terminating request")
        except:
            self.log.debug("stopping request")
            self.log.error(traceback.format_exc())
        finally:
            poller.unregister(self.upstreamDealer.socket)

def callMessageHandler(object, envelope):
    """
    A utility function to resolve the ``Action`` field of the envelope to a
    handler method in the form of

        ``def handle<Action>(self, message)``
        or
        ``def handle<Action>(self, message, envelope)``

    for the specified object.

    :param object: an object that contains handler implementations
    :type object: :class:`object`
    :param envelope: envelope
    :type envelope: :class:`Envelope`
    :returns: response
    """
    action = envelope.Action.replace("get", "")
    action = action[0].upper() + action[1:]
    if envelope.isResponse():
        action += "Response"
    if hasattr(object, "handle" + action):

        message = {}
        if hasattr(envelope, "Message"):
            message = envelope.Message

        handler = getattr(object, "handle" + action)
        return c4.utils.util.callWithVariableArguments(handler, message, envelope)
    else:
        log.error("%s is missing a 'handle%s' method", object, action)

def isAddressInUse(fullAddress):
    """
    Check if the specified address is already in use

    :param fullAddress: full ZeroMQ address including its protocol
    :type fullAddress: str
    :returns: ``True`` if address alreay in use, ``False`` otherwise
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
            log.debug(traceback.format_exc())
            return True
    else:
        raise NotImplementedError
    return False

def methodProxyHandler(object, methodName, message):
    """
    Handle message using the specified method bound to the object.

    .. note::

        This handler is necessary because object methods cannot be easily pickled.
        We use it to wrap object methods for use in :py:class:`multiprocessing.Pool`

    :param object: object
    :type object: object
    :param methodName: method name
    :type methodName: str
    :param message: message
    :type message: dict
    """
    try:
        f = getattr(object, methodName)
        f(message)
    except:
        log.error(traceback.format_exc())

def poolWorkerInitializer(name):
    """
    Initializer function for worker processes in a :py:class:`multiprocessing.Pool`.

    Here we disable interrupt handling and adjust the worker name to match the
    specified one.

    :param name: name
    :type name: str
    """
    c4.utils.util.disableInterruptSignal()
    workerName = multiprocessing.current_process().name.replace("Pool", name.replace(" ", ""))
    workerName = re.sub("-(\d+):", "-", workerName)
    multiprocessing.current_process().name = workerName

def sendMessage(address, message, asJSON=True, includeClassInfo=True):
    """
    Send a message to the specified address. This represents a
    ``fire-and-forget`` message pattern

    :param address: address
    :type address: str
    :param message: message
    :type message: dict
    :param asJSON: encode/decode messages as JSON
    :type asJSON: bool
    :param includeClassInfo: include class info in JSON, this
        allows deserialization into the respective Python objects
    :type includeClassInfo: bool
    """
    try:
        context = zmq.Context()
        server = context.socket(zmq.PUSH)  # @UndefinedVariable
        server.set(zmq.LINGER, DEFAULT_ZMQ_LINGER)  # @UndefinedVariable
        address = address.replace('*','127.0.0.1')

        server.connect(address)

        if isinstance(message, Envelope):
            server.send_string(message.toJSON(includeClassInfo))
        else:
            if asJSON:
                server.send_json(message)
            else:
                server.send_string(message)
    except:
        log.error("Could not send message %s to %s", message, address)
        log.error(traceback.format_exc())

def sendMessageToRouter(address, message):
    """
    Send a message to the specified router. This represents a
    ``fire-and-forget`` message pattern

    :param address: address
    :type address: str
    :param message: message
    :type message: dict
    """
    if message is None:
        return
    try:
        context = zmq.Context()
        server = context.socket(zmq.DEALER) # @UndefinedVariable
        server.set(zmq.LINGER, DEFAULT_ZMQ_LINGER) # @UndefinedVariable
        server.connect(address)

        if isinstance(message, Envelope):
            server.send_multipart([bytes(message.To), bytes(message.toJSON(includeClassInfo=True))])
        else:
            raise NotImplementedError
    except:
        log.error("Could not send message %s to %s", message, address)
        log.error(traceback.format_exc())

def sendRequest(address, message, includeClassInfo=True):
    """
    Send a message to the specified address and wait for the
    response. This represents a ``synchronous request-reply`` message pattern

    :param address: address
    :type address: str
    :param message: message
    :type message: dict
    :param includeClassInfo: include class info in JSON, this
        allows deserialization into the respective Python objects
    :type includeClassInfo: bool
    :returns: response
    """
    context = zmq.Context()
    server = context.socket(zmq.PUSH)  # @UndefinedVariable
    responseSocket = context.socket(zmq.PULL)  # @UndefinedVariable
    try:
        server.connect(address)

        # TODO: This will not work as expected when sending requests across different systems
        # Need to change it so that responses are sent to requesting node
        responseAddress = address
        if address.startswith("tcp"):
            responseAddress = "tcp://127.0.0.1"

        randomPort = responseSocket.bind_to_random_port(responseAddress)

        if isinstance(message, Envelope):
            message.ReplyTo = "{0}:{1}".format(responseAddress, randomPort)
            server.send_string(message.toJSON(includeClassInfo))
            response = responseSocket.recv_string()
        elif isinstance(message, dict):
            message["ReplyTo"] = "{0}:{1}".format(responseAddress, randomPort)
            server.send_json(message)
            response = responseSocket.recv_json()
        else:
            raise ValueError("{} has unsupported {}".format(message, type(message)))

        return response
    except:
        log.error("Could not send message %s to %s", message, address)
        log.error(traceback.format_exc())
        return None
    finally:
        try:
            server.disconnect(address)
        except:
            pass
        try:
            responseSocket.unbind(responseAddress)
            responseSocket.close()
        except:
            pass
        if responseAddress.startswith("ipc"):
            socketFile = responseAddress.split("//")[1]
            if os.path.exists(socketFile):
                try:
                    os.remove(socketFile)
                except:
                    pass
