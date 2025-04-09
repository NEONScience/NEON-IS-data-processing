####
#
# This dockerfile will build the combined level 0 data parser and data padder
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t neon-is-data-parse-pad:latest -f ./modules_combined/parse_and_pad/Dockerfile .
#
###
FROM us-central1-docker.pkg.dev/neon-shared-service/bei/neon-raw-data-parser:sha-aca0a49

ARG MODULE_DIR="modules"
ARG APP_DIR="timeseries_padder"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt 

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}



USER appuser
