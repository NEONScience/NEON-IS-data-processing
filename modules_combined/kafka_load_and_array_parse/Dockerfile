####
#
# This dockerfile will build the combined kafka loader and array parser module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t neon-is-kfka-load-arry-pars:latest -f ./modules_combined/kafka_load_and_array_parse/Dockerfile .
#
###
FROM us-central1-docker.pkg.dev/neon-shared-service/bei/neon-avro-kafka-loader:v4.8.3

ARG MODULE_DIR="modules"
ARG APP_DIR="array_parser"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src"
# Commenting PYTHONPATH because adding it conflicts with the common folder/module in the kafka loader image. 
# If common modules are needed from NEON-IS-data-processing, need to resolve that conflict. For now it works.
# ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt 

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}



USER appuser
