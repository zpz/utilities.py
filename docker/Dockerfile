ARG PARENT

FROM ${PARENT}
USER root

ENV DEBIAN_FRONTEND=noninteractive

# Install `daemontools` for the `multilog` program.
RUN apt-update \
    && apt-install daemontools \
    && apt-clean


# Packages for code development.
RUN apt-update \
    && apt-install gcc \
    && pip-install \
        'line-profiler==3.0.2' \
        'psutil==5.7.2' \
        'readme-renderer[md]==26.0' \
    && apt-remove gcc \
    && apt-clean

RUN pip-install \
        'twine==3.2.0' \
        'wheel==0.35.1'

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
#    black
#    coverage
#    flake8
#    pyflakes
#    radon
#    boltons

# Decided to not install Sphinx; install it in specific images where needed.
# To generate some graphs such as class hierarchy diagrams with `Sphinx`,
# one needs to install the system package `graphviz` and Python package `graphviz`.


RUN pip-install \
        'anyio==1.4.0' \
        'arrow==0.15.8' \
        'httpx==0.14.1' \
        'orjson==3.3.0' \
        'pretty-errors==1.2.17' \
        'python-multipart==0.0.5' \
        'pytz==2020.1' \
        'starlette==0.13.8' \
        'toolz==0.10.0' \
        'uvicorn==0.11.8'

RUN pip-install \
        'avro-python3==1.10.0' \
        'joblib==0.16.0' \
        'numpy==1.19.1' \
        'pandas==1.1.1' \
        'scikit-learn==0.23.2'

# Hive
#   sasl, thrift, thrift-sasl are required by impyla.
# `impyla` has some issue with `thrift_sasl`.
# Don't upgrade the versions of the following block w/o checking.
#
# Since I have no way to test Hive related functions in this personal project,
# it is not really needed to install the following packages.
#
# RUN pip-install 'retrying' 'sqlparse' \
#     && apt-update \
#     && apt-install libsasl2-dev libsasl2-modules \
#     && ldconfig \
#     && apt-clean \
#     && pip-install \
#         'sasl==0.2.1' \
#         'thrift==0.11.0' \
#         'thrift_sasl==0.2.1' \
#         'impyla==0.15.0'
