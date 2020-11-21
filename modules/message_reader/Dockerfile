####
#
# This dockerfile will build the message reader module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t message_reader:latest -f message_reader/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi8/ubi

ARG APP_DIR="message_reader"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"
WORKDIR ${CONTAINER_APP_DIR}

COPY message_reader/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN update-ca-trust && \
    dnf update -y --disableplugin=subscription-manager && \
    dnf install -y --disableplugin=subscription-manager \
            shadow-utils \
            gcc \
            python36 && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    dnf remove -y --disableplugin=subscription-manager gcc cpp && \
    dnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser