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
def clusterInfo(cleandir):
    """
    Set up a basic cluster information
    """
    clusterInformation = ClusterInfo()
    clusterInformation.nodes["peer1"] = "ipc://peer1-peerAddress.ipc"
    clusterInformation.nodes["peer2"] = "ipc://peer2-peerAddress.ipc"
    clusterInformation.aliases["system-manager"] = "peer1"
    return clusterInformation

