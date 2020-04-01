ARG PARENT

FROM ${PARENT}
USER root


# Install `daemontools` for the `multilog` program.
RUN apt-update \
    && DEBIAN_FRONTEND=noninteractive apt-install daemontools \
    && apt-clean


RUN apt-update \
    && apt-install gcc \
    && DEBIAN_FRONTEND=noninteractive pip-install \
        'line-profiler==3.0.2' \
        'memory-profiler==0.57.0' \
        'mypy==0.770' \
        'pretty-errors==1.2.16' \
        'pympler==0.8' \
        'pytest-runner==5.2' \
        'readme-renderer[md]==25.0' \
        'twine==3.1.1' \
    && apt-remove gcc \
    && apt-clean



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
#    pudb
#    pyflakes
#    radon

# Decided to not install Sphinx; install it in specific images where needed.
# To generate some graphs such as class hierarchy diagrams with `Sphinx`,
# one needs to install the system package `graphviz` and Python package `graphviz`.


RUN pip-install \
        'anyio==1.3.0' \
        'arrow==0.15.5' \
        'avro-python3==1.9.2.1' \
        'boltons==20.1.0' \
        'orjson==2.6.2' \
        'pytz==2019.3' \
        'toolz==0.10.0'



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



