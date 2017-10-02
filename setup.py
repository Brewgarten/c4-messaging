"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-messaging
This project is licensed under the MIT License, see LICENSE
"""
import sys

from setuptools import setup, find_packages

import versioneer


needs_pytest = {"pytest", "test", "ptr", "coverage"}.intersection(sys.argv)
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
    license = "MIT",
    keywords = "python c4 messaging",
    url = '',
)
