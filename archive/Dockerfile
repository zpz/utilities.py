ARG PYTHON_VERSION=3.8

#################################################
FROM python:${PYTHON_VERSION}-slim AS build
USER root

COPY install-deps /opt/zpz/
RUN cd /opt/zpz && ./install-deps


#################################################
FROM build AS test

COPY requirements-test.txt /opt/zpz/
RUN cd /opt/zpz \
    && python -m pip install --no-cache-dir -r requirements-test.txt

COPY ./src /opt/zpz/src
COPY setup.py setup.cfg README.md /opt/zpz/

RUN cd /opt/zpz && python -m pip install --no-cache-dir -q .

COPY tests /opt/zpz/tests


#################################################
FROM build AS release-prep

COPY ./src /opt/zpz/src
COPY setup.py setup.cfg README.md /opt/zpz/

RUN mkdir /zpz-dist \
        && cd /opt/zpz \
        && cp install-deps /zpz-dist/ \
        && python setup.py sdist -d /zpz-dist \
        && python setup.py bdist_wheel -d /zpz-dist


#################################################
FROM busybox:1 AS release

COPY --from=release-prep /zpz-dist /zpz-dist
RUN echo '#!/bin/sh' > /zpz-dist/INSTALL \
        && echo './install-deps && python -m pip install --no-cache-dir --no-index --find-links ./ zpz' >> /zpz-dist/INSTALL \
        && chmod +x /zpz-dist/INSTALL


#################################################
FROM python:${PYTHON_VERSION}-slim AS release-test

COPY --from=release /zpz-dist /tmp/zpz-dist
RUN cd /tmp/zpz-dist && ./INSTALL

# prep for tests
COPY tests /tmp/zpz-dist/tests
COPY requirements-test.txt /tmp/zpz-dist/
RUN cd /tmp/zpz-dist \
        && python -m pip install --no-cache-dir -r requirements-test.txt
