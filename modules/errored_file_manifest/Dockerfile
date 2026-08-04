####
#
# This dockerfile will build the errored file manifest module.
# Example command (must be run from project root directory to include common path in Docker context):
# docker build --no-cache -t neon-is-errored-file-manifest:latest -f errored_file_manifest/Dockerfile .
#
###
FROM registry.access.redhat.com/ubi9/ubi-minimal

ARG MODULE_DIR="./modules"
ARG APP_DIR="errored_file_manifest"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

RUN update-ca-trust && \
    microdnf update -y --disableplugin=subscription-manager && \
    microdnf install -y --disableplugin=subscription-manager \
            shadow-utils \
            gcc \
            libzstd  \
        python39 \
        python39-pip \
        python39-devel && \
    python3.9 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3.9 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

USER appuser

ENTRYPOINT ["python3.9", "-m", "errored_file_manifest.errored_file_manifest_main"]
