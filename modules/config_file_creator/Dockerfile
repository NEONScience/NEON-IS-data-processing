####
#
# This dockerfile will build an image to run the config_file_creator module.
# Example command (run from root repo directory in Docker context):
# docker build --no-cache -t neon-is-cfg-env-file-crea -f ./modules/config_file_creator/Dockerfile .
#
###
FROM python:3.11-slim

ARG MODULE_DIR="./modules"
ARG APP_DIR="config_file_creator"
ARG CONTAINER_APP_DIR="/usr/src/app"

WORKDIR ${CONTAINER_APP_DIR}

# Install dependencies
RUN pip install --no-cache-dir PyYAML==6.0

RUN groupadd -g 990 appuser && \
    useradd -r -u 990 -g appuser appuser 

# Copy the normalizer scripts
COPY ${MODULE_DIR}/${APP_DIR}/config-env-file-creator.py ${CONTAINER_APP_DIR}/${APP_DIR}/config-env-file-creator.py
COPY ${MODULE_DIR}/${APP_DIR}/processing-instructions-file-creator.py ${CONTAINER_APP_DIR}/${APP_DIR}/processing-instructions-file-creator.py

# Make scripts executable
RUN chmod +x ${CONTAINER_APP_DIR}/${APP_DIR}/config-env-file-creator.py \
    && chmod +x ${CONTAINER_APP_DIR}/${APP_DIR}/processing-instructions-file-creator.py

USER appuser
