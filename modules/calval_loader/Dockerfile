####
# This dockerfile will build the caliration repo loader module.
# Example command (must be run from project root directory(from the modules folder) to include common path in Docker context):
# docker build --no-cache -t neon-cval-loader:latest:latest -f calval_loader/Dockerfile .
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG APP_DIR="calval_loader"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_ACCESS_DIR="data_access"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"
ENV CVAL_INGEST_BUCKET="neon-cval"


WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/app-requirements.txt
COPY ${DATA_ACCESS_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/data-access-requirements.txt


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
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}

USER appuser
