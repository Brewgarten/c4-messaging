from setuptools import setup, find_packages

import versioneer


setup(
    name = "c4-messaging",
    version = versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages = find_packages(),
    install_requires = ["c4-utils", "pyzmq"],
    author = 'IBM',
    author_email = "",
    description = "This library provides the messaging infrastructure for C4",
    license = "IBM",
    keywords = "python c4 messaging",
    url = '',
)
