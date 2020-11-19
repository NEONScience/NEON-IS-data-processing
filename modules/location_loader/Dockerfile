####
#
# This dockerfile will build the location loader module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t location_loader:latest -f location_loader/Dockerfile .
#
###
FROM container-registry.oracle.com/os/oraclelinux:8-slim

ARG APP_DIR="location_loader"
ARG COMMON_DIR="common"
ARG DATA_ACCESS_DIR="data_access"
ARG ORACLE_INSTANT_PKGNAME="oracle-instantclient19.9-basiclite-19.9.0.0.0-1.x86_64.rpm"
ARG ORACLE_INSTANT_URI="https://neon-ci-software.s3.data.neonscience.org/oracle-instant/$ORACLE_INSTANT_PKGNAME"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN update-ca-trust && \
    microdnf update -y && \
    curl -O "${ORACLE_INSTANT_URI}" 2>&1 && \
    microdnf install -y \
            gcc \
            libaio \
            libnsl \
            libxml2 \
            libxml2-devel \
            python38 \
            python38-pip \
            python38-wheel \
            python38-devel \
            python38-setuptools && \
    rpm -Uvh "${ORACLE_INSTANT_PKGNAME}" && \
    ldconfig && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    microdnf remove -y gcc cpp && \
    microdnf clean all && \
    rm -f "/tmp/${ORACLE_INSTANT_PKGNAME}" && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}
COPY ${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}

USER appuser