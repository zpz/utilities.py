FROM zppz/py3:22.11.25
ENV PARENT_IMAGE zppz/py3:22.11.25
USER root

COPY . /tmp/src/

RUN python -m pip install /tmp/src/[avro,lineprofiler,test] \
    && python -m pip uninstall -y zpz \
    && rm -rf /tmp/src
