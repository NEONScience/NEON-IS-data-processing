####
#
# This dockerfile will build the combined pub_egress, pub_uploader, and pub_sync modules.
# Example command (must be run from project root directory to include modules and modules_combined path in Docker context):
# docker build -t neon-is-pub-egrs-publ -f ./modules_combined/pub_egress_and_publish/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi8/ubi-minimal

ARG MODULE_DIR="modules"
ARG APP_DIR_1="pub_egress"
ARG APP_DIR_2="pub_uploader"
ARG APP_DIR_3="pub_sync"
ARG COMMON_DIR="common"
ARG DATA_ACCESS_DIR="data_access"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_DIR="/data"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR_1}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_3}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_3}/requirements.txt
COPY ${MODULE_DIR}/${COMMON_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${COMMON_DIR}/requirements.txt
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}/requirements.txt

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
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_3}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${COMMON_DIR}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}/requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser && \
    mkdir ${DATA_DIR} && \
    chown appuser:appuser -R ${DATA_DIR}

COPY ${MODULE_DIR}/${APP_DIR_1} ${CONTAINER_APP_DIR}/${APP_DIR_1}
COPY ${MODULE_DIR}/${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}
COPY ${MODULE_DIR}/${APP_DIR_3} ${CONTAINER_APP_DIR}/${APP_DIR_3}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}

USER appuser