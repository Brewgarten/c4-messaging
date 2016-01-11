import sys

from setuptools import setup, find_packages

import versioneer


needs_pytest = {"pytest", "test", "ptr"}.intersection(sys.argv)
pytest_runner = ["pytest-runner"] if needs_pytest else []

setup(
    name = "c4-messaging",
    version = versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages = find_packages(),
    install_requires = ["c4-utils", "pyzmq"],
    setup_requires=[] + pytest_runner,
    tests_require=["pytest", "pytest-cov"],
    author = 'IBM',
    author_email = "",
    description = "This library provides the messaging infrastructure for C4",
    license = "IBM",
    keywords = "python c4 messaging",
    url = '',
)
