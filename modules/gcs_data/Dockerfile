####
#
# This dockerfile will build an image to run the data_source_gcs module (developed for Argo)
# Example command (run from modules/ directory in Docker context):
# docker build -t neon-is-gcs-data -f ./modules/gcs_data/Dockerfile .
#
###
# Use ubi9 as the base image
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.7
ARG MODULE_DIR="./modules"
ARG CONTAINER_APP_DIR="/usr/src/app"

# For rclone
ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.72.1

# Install system dependencies
RUN microdnf update -y --disableplugin=subscription-manager && \
    microdnf install -y --disableplugin=subscription-manager \
            curl \
            jq \
            findutils \
            gzip \
            libzstd  \
            shadow-utils \
            tar && \
            microdnf clean all --disableplugin=subscription-manager &&\
    groupadd appuser -g 1001 && \
    useradd appuser -d ${CONTAINER_APP_DIR} -g appuser -u 1001 &&\
    rm -rf ${CONTAINER_APP_DIR}/venv 


# Install yq (v4.x example) for manipulating json
ENV YQ_VERSION=v4.44.1
RUN curl -L "https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_amd64" \
      -o /usr/bin/yq && \
    chmod +x /usr/bin/yq


# Multi-platform package install for rclone
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; \
    else ARCHITECTURE=amd64; fi &&\
    rpm -Uvh "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}.rpm"

WORKDIR ${CONTAINER_APP_DIR}


USER appuser