####
#
# This dockerfile will build an image to run the cron_daily_and_date_control module.
# Example command (run from modules/ directory in Docker context):
# docker build --no-cache -t neon-is-date-cntl -f ./modules/cron_daily_and_date_control/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi9/ubi-minimal

ARG MODULE_DIR="./modules"
ARG APP_DIR="cron_daily_and_date_control"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

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
    groupadd -g 990 appuser && \
    useradd -r -u 990 -g appuser appuser

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}

USER appuser
