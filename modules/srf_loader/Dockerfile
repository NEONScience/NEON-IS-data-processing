####
# This dockerfile will build the srf loader module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build --no-cache -t srf_loader:latest -f srf_loader/Dockerfile .
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG MODULE_DIR="./modules"
ARG APP_DIR="srf_loader"
ARG COMMON_DIR="common"
ARG DATA_ACCESS_DIR="data_access"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/app-requirements.txt
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/data-access-requirements.txt

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
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/app-requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/data-access-requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}

USER appuser
