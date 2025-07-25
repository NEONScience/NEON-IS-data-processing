####
#
# This dockerfile will build the flow_sae_trst_dp0p module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t flow_sae_trst_dp0p:latest -f flow_sae_trst_dp0p/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi9/ubi-minimal

ARG FLOW_DIR="./flow"
#ARG MODULE_DIR="./modules"
ARG APP_DIR="flow_sae_trst_dp0p"
#ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${CONTAINER_APP_DIR}:${PYTHONPATH}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${FLOW_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN update-ca-trust && \
    microdnf update -y --disableplugin=subscription-manager && \
    microdnf install -y --disableplugin=subscription-manager \
            shadow-utils \
            gcc \
            libzstd  \
            python3 \
            python3-pip \
            python3-wheel \
            python3-devel \
            python3-setuptools && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${FLOW_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
#COPY ${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser
