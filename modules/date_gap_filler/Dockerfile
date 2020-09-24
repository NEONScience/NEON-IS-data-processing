####
#
# This dockerfile will build the date gap filler module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build -t date_gap_filler:latest -f date_gap_filler/Dockerfile .
#
###
FROM centos:7

ARG APP_DIR="data_gap_filler"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

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