####
#
# This dockerfile will build the combined module for level1_consolidate and filter_joiner.
# Example command (must be run from project root directory to include modules and modules_combined path in Docker context):
# docker build -t level1_group_and_consolidate -f ./modules_combined/level1_group_and_consolidate/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG MODULE_DIR="modules"
ARG APP_DIR="level1_consolidate"
ARG APP_DIR_2="filter_joiner"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_DIR="/usr/interimData"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements2.txt
COPY ${MODULE_DIR}/${COMMON_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/common_requirements.txt

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
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements2.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/common_requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser && \
    mkdir ${DATA_DIR} && \
    chown appuser:appuser -R ${CONTAINER_APP_DIR} && \
    chown appuser:appuser -R ${DATA_DIR}

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser