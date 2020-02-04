FROM quay.io/battelleecology/python_parent:3

ENV APP_DIR="egress/egress"
ENV LIB_DIR="lib"
ENV CONTAINER_ROOT="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:/usr/src/app"

RUN pip3.6 install pyyaml

ADD ${APP_DIR} ${CONTAINER_ROOT}/${APP_DIR}
ADD ${LIB_DIR} ${CONTAINER_ROOT}/${LIB_DIR}

WORKDIR ${CONTAINER_ROOT}