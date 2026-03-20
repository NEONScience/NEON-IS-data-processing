####
# This dockerfile will build the l0_gcs_loader repo loader module.
# Example command (must be run from project root directory(from the modules folder) to include common path in Docker context):
# docker build --no-cache -t l0_gcs_loader:latest -f l0_gcs_loader/Dockerfile .
###
FROM registry.access.redhat.com/ubi9/ubi-minimal

ARG MODULE_DIR="./modules"
ARG APP_DIR="l0_gcs_loader"
ARG CONTAINER_APP_DIR="/usr/src"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

# For rclone
ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.62.2

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN update-ca-trust && \
    microdnf update -y --disableplugin=subscription-manager && \
    microdnf install -y --disableplugin=subscription-manager \
            shadow-utils \
            findutils \
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
    
# Multi-platform package install for rclone
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; \
    elif [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then ARCHITECTURE=arm-v7; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; \
    else ARCHITECTURE=amd64; fi &&\
    rpm -Uvh "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}.rpm"

    
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}

USER appuser
