####
#
# This dockerfile will build the path order module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t path_order:latest -f path_order/Dockerfile .
#
###
FROM centos:7

ARG APP_DIR="path_order"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${CONTAINER_APP_DIR}:${CONTAINER_APP_DIR}/common"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN yum update -y && \
    yum install -y epel-release && \
    yum install -y gcc python36 python36-pip python36-devel python36-setuptools && \
    ldconfig && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools && \
    python3 -mpip install -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    yum remove gcc -y && \
    yum autoremove -y && \
    yum clean all && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser