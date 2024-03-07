####
#
# This dockerfile will build the OS table loader module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t os_table_loader:latest -f os_table_loader/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG APP_DIR="os_table_loader"
ARG COMMON_DIR="common"
ARG DATA_ACCESS_DIR="data_access"
ARG PUB_FILES_DIR="pub_files"
ARG PUB_WORKBOOK_LOADER_DIR="pub_workbook_loader"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN update-ca-trust && \
    microdnf update -y --disableplugin=subscription-manager && \
    microdnf install -y --disableplugin=subscription-manager \
            shadow-utils \
            gcc \
            libzstd  \
            python39 \
            python39-pip \
            python39-wheel \
            python39-devel \
            python39-setuptools && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}
COPY ${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}
COPY ${PUB_FILES_DIR} ${CONTAINER_APP_DIR}/${PUB_FILES_DIR}
COPY ${PUB_WORKBOOK_LOADER_DIR} ${CONTAINER_APP_DIR}/${PUB_WORKBOOK_LOADER_DIR}

USER appuser