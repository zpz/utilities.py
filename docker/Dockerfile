ARG PARENT

FROM ${PARENT}
USER root

RUN pip-install \
    'arrow' \
    'avro-python3' \
    'pytz' \
    'twine' \
    'readme-renderer[md]'

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

# Install `daemontools` for the `multilog` program.
RUN apt-update && apt-install daemontools && apt-clean

RUN pip-install pympler