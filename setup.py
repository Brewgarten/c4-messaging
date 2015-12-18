from setuptools import setup, find_packages
from setuptools.command.test import test
import sys

class pytestcmd(test):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        test.initialize_options(self)
        self.pytest_args = None

    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

# get version without importing
versionFileName = "c4/messaging.py"
packageVersion = None

try:
    import imp
    import os
    currentDirectory = os.path.dirname(os.path.abspath(__file__))
    baseDirectory = os.path.dirname(currentDirectory)
    versioningModulePath = os.path.join(baseDirectory, "versioning.py")
    setup = imp.load_source("versioning", versioningModulePath).versionedSetup
except:
    import re
    with open(versionFileName) as f:
        match = re.search("__version__\s*=\s*['\"](.*)['\"]", f.read())
        packageVersion = match.group(1)

setup(
    name = "c4-messaging",
    version = packageVersion,
    versionFileName = versionFileName,
    packages = find_packages(),
    install_requires = ['c4-utils'],
    author = 'IBM',
    author_email = '',
    description = '''This library provides the messaging infrastructure for C4''',
    license = 'IBM',
    keywords = 'python c4 messaging',
    url = '',
    cmdclass = {'test': pytestcmd},
)
