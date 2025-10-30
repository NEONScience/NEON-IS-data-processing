####
#
# This dockerfile will build the combined trino converter, parquet linkmerge, and array parser modules.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t neon-is-data-src-trino:latest -f ./modules_combined/data_source_trino/Dockerfile .
#
###
FROM us-central1-docker.pkg.dev/neon-shared-service/bei/neon-avro-genscript:v4.13.0

ARG MODULE_DIR="modules"
ARG APP_DIR="parquet_linkmerge"
ARG APP_DIR_2="array_parser"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt

RUN python3 -m ensurepip --default-pip && \
    python3 -m pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    python3 -m pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}



USER appuser
