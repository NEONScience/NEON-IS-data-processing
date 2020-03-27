FROM quay.io/battelleecology/python_parent:3

ENV APP_DIR="data_gap_filler"
ENV LIB_DIR="lib"

ENV CONTAINER_ROOT="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:/usr/src/app"

WORKDIR ${CONTAINER_ROOT}

ADD ${APP_DIR} ${CONTAINER_ROOT}/${APP_DIR}
ADD ${LIB_DIR} ${CONTAINER_ROOT}/${LIB_DIR}