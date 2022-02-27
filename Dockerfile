FROM zppz/py3:22.01.26
USER root

ARG PROJ

COPY . /src/

RUN apt-update && \
    apt-install python3.8-venv && \
    apt-clean

RUN cd /src \
    && python -m pip install -e . \
    && python -m pip uninstall -y ${PROJ}  \
    && python -m pip install -r /src/requirements-test.txt \
    && python -m pip install build

USER docker-user
