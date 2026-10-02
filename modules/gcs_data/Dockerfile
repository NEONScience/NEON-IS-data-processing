####
#
# This dockerfile will build an image to run the gcs_data module (developed for Argo)
# It's base image includes a git sparse clone utility
# Example command (run from modules/ directory in Docker context):
# docker build -t neon-is-gcs-data -f ./modules/gcs_data/Dockerfile .
#
###
FROM us-central1-docker.pkg.dev/neon-shared-service/bei/neon-argo-workflows-data-git-sparse-clone:sha-effa810

ARG MODULE_DIR="./modules"
ARG APP_DIR="gcs_data"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"

ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.72.1

ENV PYTHON_BIN=python3

USER root
RUN apk add --no-cache \
        curl \
        findutils \
        gzip \
        jq \
        tar \
        unzip \
        zstd-libs


ENV YQ_VERSION=v4.44.1
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then YQ_ARCH=amd64; \
        elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then YQ_ARCH=arm64; \
        else YQ_ARCH=amd64; fi && \
    curl --fail --location --silent --show-error \
        "https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_${YQ_ARCH}" \
        -o /usr/bin/yq && \
    chmod +x /usr/bin/yq


# Multi-platform package install for rclone
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; \
    else ARCHITECTURE=amd64; fi &&\
    curl --fail --location --silent --show-error \
        "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}.zip" \
        -o /tmp/rclone.zip && \
    unzip /tmp/rclone.zip -d /tmp && \
    cp "/tmp/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}/rclone" /usr/bin/rclone && \
    chmod +x /usr/bin/rclone && \
    rm -rf /tmp/rclone*

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/requirements.txt
RUN ${PYTHON_BIN} -m pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/requirements.txt

COPY --chown=appuser:appuser ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}
COPY --chown=appuser:appuser ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser
ENTRYPOINT []