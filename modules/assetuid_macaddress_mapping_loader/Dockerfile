####
#
# This dockerfile will build the assetuid_macaddresss_loader module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build --no-cache -t assetuid_macaddresss_loader:latest -f assetuid_macaddresss_loader/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG MODULE_DIR="./modules"
ARG APP_DIR="assetuid_macaddress_mapping_loader"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
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

USER appuser
