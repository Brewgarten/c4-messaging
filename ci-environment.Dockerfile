FROM jfloff/alpine-python:2.7

RUN pip install --disable-pip-version-check --no-cache-dir \
    coverage==4.3 \
    pylint==1.6 \
    pytest==3.0 \
    pytest-cov==2.4 \
    virtualenv==15.1