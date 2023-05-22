####
#
# This dockerfile will build the level1 consolidation module. It also includes the pub_workbook_loader module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t level1_consolidate:latest -f level1_consolidate/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG APP_DIR_1="level1_consolidate"
ARG APP_DIR_2="pub_workbook_loader"
ARG COMMON_DIR="common"
ARG DATA_ACCESS_DIR="data_access"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_DIR="/data"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR_1}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt
COPY ${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt
COPY ${COMMON_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${COMMON_DIR}/requirements.txt
COPY ${DATA_ACCESS_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}/requirements.txt

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
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${COMMON_DIR}/requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser && \
    mkdir ${DATA_DIR} && \
    chown appuser:appuser -R ${DATA_DIR}

COPY ${APP_DIR_1} ${CONTAINER_APP_DIR}/${APP_DIR_1}
COPY ${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}
COPY ${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}
COPY ${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser