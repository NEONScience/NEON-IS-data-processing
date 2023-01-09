####
#
# This dockerfile will build the combined trino converter and parquet linkmerge module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t data_source_trino:latest -f ./modules_combined/data_source_trino/Dockerfile .
#
###
FROM quay.io/battelleecology/genavro:v1.0.74

ARG MODULE_DIR="modules"
ARG APP_DIR="parquet_linkmerge"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN update-ca-trust && \
    microdnf update -y --disableplugin=subscription-manager && \
    microdnf install -y --disableplugin=subscription-manager \
            shadow-utils \
            gcc \
            libzstd  \
            python38 \
            python38-pip \
            python38-wheel \
            python38-devel \
            python38-setuptools && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

RUN chown appuser:appuser -R ${CONTAINER_APP_DIR}

USER appuser
