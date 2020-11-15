ARG PYTHON_VERSION=3.8

FROM python:${PYTHON_VERSION}-slim
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

COPY requirements.txt /zpz-dist/
COPY requirements-test.txt /zpz-dist/
RUN cd /zpz-dist \
        && python -m pip install --no-cache-dir -r requirements.txt \
        && python -m pip install --no-cache-dir -r requirements-test.txt

COPY ./ /tmp/zpz-src
RUN cd /tmp/zpz-src \
        && mv tests /zpz-dist/ \
        && python setup.py sdist -d /zpz-dist bdist_wheel -d /zpz-dist \
        && cd \
        && rm -rf /tmp/zpz-src \
        && python -m pip install /zpz-dist/zpz-*.whl
