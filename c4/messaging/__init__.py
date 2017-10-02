"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-messaging
This project is licensed under the MIT License, see LICENSE
"""
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

from .base import (ClusterInfo,
                   Envelope,
                   MessageTracker, MessageTrackerDB, MessagingException,
                   RoutingProcess,
                   callMessageHandler)

from .zeromqMessaging import (Dealer, DealerRouter,
                              Peer, PeerClient, PeerRouter, Poller,
                              Router, RouterClient,
                              isAddressInUse, DEFAULT_IPC_PATH)

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
