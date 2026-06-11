####
#
# This dockerfile will build an image to run the gcs_data module (developed for Argo)
# Example command (run from modules/ directory in Docker context):
# docker build -t neon-is-gcs-data -f ./modules/gcs_data/Dockerfile .
#
###
# Use Ubuntu as the base image
FROM ubuntu:24.04
ARG MODULE_DIR="./modules"
ARG APP_DIR="gcs_data"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.13

# For rclone
ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.72.1

ENV PYTHON_BIN="python${PYTHON_VERSION}"

# Install system dependencies and Python 3.13 from deadsnakes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        findutils \
        gnupg \
        gzip \
        jq \
        libzstd1 \
        passwd \
        software-properties-common \
        tar \
        unzip && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        ${PYTHON_BIN} \
        ${PYTHON_BIN}-venv && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | ${PYTHON_BIN} && \
    ${PYTHON_BIN} -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    apt-get purge -y --auto-remove software-properties-common gnupg && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd appuser -g 1001 && \
    useradd appuser -d ${CONTAINER_APP_DIR} -g appuser -u 1001 && \
    rm -rf ${CONTAINER_APP_DIR}/venv && \
    ln -s /usr/bin/${PYTHON_BIN} /usr/bin/python3


# Install yq (v4.x example) for manipulating json
ENV YQ_VERSION=v4.44.1
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then YQ_ARCH=amd64; \
        elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then YQ_ARCH=arm64; \
        else YQ_ARCH=amd64; fi && \
        curl -L "https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_${YQ_ARCH}" \
            -o /usr/bin/yq && \
    chmod +x /usr/bin/yq


# Multi-platform package install for rclone
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; \
    else ARCHITECTURE=amd64; fi &&\
        curl -L "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}.zip" \
            -o /tmp/rclone.zip && \
        unzip /tmp/rclone.zip -d /tmp && \
        cp "/tmp/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}/rclone" /usr/bin/rclone && \
        chmod +x /usr/bin/rclone && \
        rm -rf /tmp/rclone* 

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/requirements.txt
RUN ${PYTHON_BIN} -m pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/requirements.txt

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}


USER appuser