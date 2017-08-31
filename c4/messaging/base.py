from abc import ABCMeta, abstractmethod
import copy
import datetime
import logging
import multiprocessing
import os
import sqlite3
import time
import uuid

from threading import Thread

from c4.utils.jsonutil import JSONSerializable
from c4.utils.logutil import ClassLogger
from c4.utils.util import (SharedDictWithLock,
                           callWithVariableArguments,
                           disableInterruptSignal,
                           mergeDictionaries)


log = logging.getLogger(__name__)

@ClassLogger
class ClusterInfo(object):
    """
    A basic cluster information object
    """
    def __init__(self):
        self.nodes = SharedDictWithLock()
        self.aliases = SharedDictWithLock()

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

class Envelope(JSONSerializable):
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

    def isResponse(self):
        """
        Check if the message is a response

        :returns: ``True`` if it is, ``False`` otherwise
        :rtype: bool
        """
        return hasattr(self, "RelatesTo")

    def toResponse(self, message=None):
        """
        Convert envelope to a response and include the optional message

        :param message: message
        :returns: response envelope
        :rtype: :class:`Envelope`
        """
        if hasattr(self, "RelatesTo"):
            raise MessagingException("{0} already a response".format(self.toJSON(includeClassInfo=True)))

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
class MessageTracker(object):
    """
    Messaging utility class that allows the tracking of related messages.

    This implementation is backed by :class:`~multiprocessing.managers.SyncManager`
    """
    def __init__(self):
        super(MessageTracker, self).__init__()

        self.manager = multiprocessing.managers.SyncManager()
        self.manager.start(disableInterruptSignal)

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
            storedMessageEnvelope.Message = mergeDictionaries(
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
        self.filename = "/dev/shm/{0}.mt".format(str(uuid.uuid4()))

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
class HandlerThread(Thread):
    """
    Thread to do the work of message handling.
    :param handlers: handlers to pass the message to
    :type handlers: list
    :param envelope: Envelope containing the message
    :type envelope: Envelope
    """
    def __init__(self, name, handlers, envelope):
        super(HandlerThread, self).__init__(name=name)
        self.envelope = envelope
        self.handlers = handlers
        self.responses = []

    """
    Do the work on passing the message to handlers and accumulating responses.
    """
    def run(self):
        for handler in self.handlers:
            try:
                response = callWithVariableArguments(handler, self.envelope.Message, self.envelope)
                if response is not None:
                    self.responses.append(response)
            except Exception as e:
                self.log.exception("Error handling message %s: %s", self.envelope.Message, e)

@ClassLogger
class RoutingProcess(multiprocessing.Process):
    """
    An abstract generic routing process that combines an `upstream` and `downstream`
    component, provides routing capabilities and support for message handlers.

    :param upstreamAddress: upstream address, e.g., socket or tcp address
    :type upstreamAddress: str
    :param downstreamAddress: downstream address, e.g., socket or tcp address
    :type downstreamAddress: str
    :param maxThreads: maximum number of threads to use for message handling
    :type maxThreads: int
    :param name: process name
    :type name: str
    """
    __metaclass__ = ABCMeta

    def __init__(self, upstreamAddress, downstreamAddress, name=None, maxThreads=2):
        super(RoutingProcess, self).__init__(name=name)
        self.upstreamAddress = upstreamAddress
        self.downstreamAddress = downstreamAddress
        self.handlers = []
        self.stopFlag = multiprocessing.Event()
        self.maxThreads = maxThreads
        if self.maxThreads < 2:
            raise ValueError("The minimum recommended number of threads is 2. Supplied value is: {}".format(self.maxThreads))
        self.handlerThreads = []

    def addHandler(self, function):
        """
        Add a message handler to the server

        .. note::

            The handler function should take exactly one argument: the message

        :param function: function
        :type function: func
        """
        self.handlers.append(function)

    @abstractmethod
    def areComponentsReady(self):
        """
        Check if the routing components are set up and ready to use

        :returns: ``True`` if ready, ``False`` otherwise
        :rtype: bool
        """

    def handleMessage(self, envelope):
        """
        Pass message to message handlers

        :param envelope: envelope
        :type envelope: :class:`Envelope`
        """
        if len(self.handlerThreads) >= self.maxThreads:
            self.log.error("Maximum number of message handler threads exceeded.")

        thread = HandlerThread(self.name + " " + envelope.Action, self.handlers, envelope)
        self.handlerThreads.append(thread)
        thread.start()

    def handleResponses(self):
        responseEnvelopes = []
        for thread in self.handlerThreads:
            if not thread.isAlive():
                for response in thread.responses:
                    try:
                        # we need to make copies of the envelope such that unique response can be generated
                        envelopeCopy = copy.deepcopy(thread.envelope)
                        envelopeCopy.toResponse(response)
                        envelopeCopy.From = self.address
                        responseEnvelopes.append(envelopeCopy)
                    except Exception as e:
                        self.log.error(e)
                self.handlerThreads.remove(thread)

        return responseEnvelopes

    @abstractmethod
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

    @abstractmethod
    def run(self):
        """
        The implementation of the `upstream`-`downstream` routing process
        """

    def start(self, timeout=None):
        """
        Start the routing process

        :param timeout: timeout in seconds, if set stop is blocking else non-blocking
        :type timeout: int
        :returns: whether start was successful
        :rtype: bool
        """
        super(RoutingProcess, self).start()
        if timeout:
            end = time.time() + timeout
            while time.time() < end:
                if not self.is_alive() or not self.areComponentsReady():
                    time.sleep(0.5)
                else:
                    self.log.debug("Started '%s'", self.name)
                    break
            else:
                self.log.error("Starting '%s' timed out", self.name)
                return False
        return True

    def stop(self, timeout=None):
        """
        Stop the routing process

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

def callMessageHandler(implementation, envelope):
    """
    A utility function to resolve the ``Action`` field of the envelope to a
    handler method in the form of

        ``def handle<Action>(self, message)``
        or
        ``def handle<Action>(self, message, envelope)``

    for the specified implementation.

    :param implementation: an object that contains handler implementations
    :type implementation: :class:`object`
    :param envelope: envelope
    :type envelope: :class:`Envelope`
    :returns: response
    """
    action = envelope.Action.replace("get", "")
    action = action[0].upper() + action[1:]
    if envelope.isResponse():
        action += "Response"
    if hasattr(implementation, "handle" + action):

        message = {}
        if hasattr(envelope, "Message"):
            message = envelope.Message

        handler = getattr(implementation, "handle" + action)
        return callWithVariableArguments(handler, message, envelope)
    else:
        log.error("'%s' is missing a 'handle%s' method", implementation, action)

