"""
This library contains process and threading functionality to handle
ZMQ messages synchronously and asynchronously with both a functional
as well as object-oriented approach.

.. note::

    Asynchronous request-reply with a callback requires a client object.

Message Client
--------------

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
import inspect
import logging
import multiprocessing
import os
import re
import traceback

import zmq

from c4.utils.logutil import ClassLogger
from c4.utils.util import StopFlagProcess, Worker, disableInterruptSignal

from .base import Envelope


log = logging.getLogger(__name__)

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
        self.stopFlagProcess = StopFlagProcess(self)
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
                Worker(self.tasks, "{}-Worker-{}".format(name, i+1)).start()
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
        for _ in range(self.workers):
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

def methodProxyHandler(instance, methodName, message):
    """
    Handle message using the specified method bound to the object.

    .. note::

        This handler is necessary because object methods cannot be easily pickled.
        We use it to wrap object methods for use in :py:class:`multiprocessing.Pool`

    :param instance: object
    :type instance: object
    :param methodName: method name
    :type methodName: str
    :param message: message
    :type message: dict
    """
    try:
        f = getattr(instance, methodName)
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
    disableInterruptSignal()
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
