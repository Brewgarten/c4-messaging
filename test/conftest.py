import logging
import os
import shutil
import tempfile

import pytest

from c4.messaging import ClusterInfo


logging.basicConfig(format='%(asctime)s [%(levelname)s] <%(processName)s> [%(name)s(%(filename)s:%(lineno)d)] - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

@pytest.fixture()
def cleandir(request):
    """
    Create a new temporary directory and change the current working directory to it
    """
    oldCurrentWorkingDirectory = os.getcwd()
    newCurrentWorkingDirectory = tempfile.mkdtemp(dir="/dev/shm")
#     newCurrentWorkingDirectory = tempfile.mkdtemp(dir="/tmp")
    os.chdir(newCurrentWorkingDirectory)

    def removeTemporaryDirectory():
        os.chdir(oldCurrentWorkingDirectory)
        shutil.rmtree(newCurrentWorkingDirectory)
    request.addfinalizer(removeTemporaryDirectory)
    return newCurrentWorkingDirectory

@pytest.fixture
def clusterInfo(cleandir, temporaryIPCPath):
    """
    Set up a basic cluster information
    """
    clusterInformation = ClusterInfo()
    clusterInformation.nodes["peer1"] = "ipc://peer1-peerAddress.ipc"
    clusterInformation.nodes["peer2"] = "ipc://peer2-peerAddress.ipc"
    clusterInformation.aliases["system-manager"] = "peer1"
    return clusterInformation

@pytest.fixture
def temporaryIPCPath(request, monkeypatch):
    """
    Create a new temporary directory and set c4.messaging.zeromqMessaging.DEFAULT_IPC_PATH to it
    """
    newpath = tempfile.mkdtemp(dir="/dev/shm")
#     newpath = tempfile.mkdtemp(dir="/tmp")
    monkeypatch.setattr("c4.messaging.zeromqMessaging.DEFAULT_IPC_PATH", newpath)

    def removeTemporaryDirectory():
        shutil.rmtree(newpath)
    request.addfinalizer(removeTemporaryDirectory)
    return newpath
