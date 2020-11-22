ARG PYTHON_VERSION=3.8

#################################################
FROM python:${PYTHON_VERSION}-slim AS build
USER root

# ENV DEBIAN_FRONTEND=noninteractive

#         'readme-renderer[md]==26.0' \
# RUN pip-install \
#         'twine==3.2.0' \
#         'wheel==0.35.1'

# Use `pympler` to check total memory size of a Python object.

# Use `yapf` to format Python code in-place:
#   yapf -ir -vv --no-local-style .
#
# Also check out `black`.

# `pip install line_profiler` fails on Python 3.7.
# See https://github.com/rkern/line_profiler/issues/132
# Hopefully the fix will be integrated soon.
#
# Check out alternatives `pprofile` and `py-spy`.
#
# For other profilers, refer to
#  https://devopedia.org/profiling-python-code


# Installing `line_profiler` needs gcc.

# Use `snakeviz` to view profiling stats.
# `snakeviz` is not installed in this Docker image as it's better
# installed on the hosting machine 'natively'.

# Other useful packages:
#    flake8
#    pyflakes
#    radon

# To generate some graphs such as class hierarchy diagrams with `Sphinx`,
# one needs to install the system package `graphviz` and Python package `graphviz`.

RUN apt-get update \
        && apt-get install -y --no-install-recommends --no-upgrade \
                gcc libc6-dev g++ \
                unixodbc-dev \
                default-libmysqlclient-dev \
        && apt-get autoremove -y \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

COPY requirements-*.txt /tmp/
RUN python -m pip install \
        -r /tmp/requirements-1.txt \
        -r /tmp/requirements-2.txt \
        -r /tmp/requirements-test.txt

COPY ./ /zpz-src
RUN cd /zpz-src && python -m pip install -q .


#################################################
FROM build AS release-prep

RUN cd /zpz-src \
        && mkdir /zpz-dist \
        && cp requirements-*.txt /zpz-dist/ \
        && mv tests /zpz-dist/ \
        && python setup.py sdist -d /zpz-dist \
        && python setup.py bdist_wheel -d /zpz-dist \
        && python -m pip wheel -r requirements-2.txt -w /zpz-dist


#################################################
FROM python:${PYTHON_VERSION}-slim AS release-test

COPY --from=release-prep /zpz-dist /zpz-dist

RUN cd /zpz-dist \
    && python -m pip install -r /zpz-dist/requirements-1.txt \
    && python -m pip install --no-index --find-links=/zpz-dist/ -r /zpz-dist/requirements-2.txt \
    && python -m pip install --no-index --find-links=/zpz-dist/ zpz

# Install test dependencies in order to run tests.
RUN python -m pip install -r /zpz-dist/requirements-test.txt


#################################################
FROM alpine:3 AS release

COPY --from=zpz-release-test:latest /zpz-dist /zpz-dist