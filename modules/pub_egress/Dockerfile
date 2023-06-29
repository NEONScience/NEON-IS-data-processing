####
#
# This dockerfile will build the pub_egress module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t pub_egress:latest -f pub_egress/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi9/ubi-minimal

ARG APP_DIR="pub_egress/pub_egress"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

# For rclone
ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.62.2

WORKDIR ${CONTAINER_APP_DIR}

COPY pub_egress/requirements.txt ${CONTAINER_APP_DIR}

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
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 990 appuser && \
    useradd -r -u 990 -g appuser appuser

# Multi-platform package install for rclone
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; \
    elif [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then ARCHITECTURE=arm-v7; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; \
    else ARCHITECTURE=amd64; fi &&\
    rpm -Uvh "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}.rpm"

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser