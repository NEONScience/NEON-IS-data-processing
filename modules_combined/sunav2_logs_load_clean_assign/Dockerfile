# Dockerfile for NEON IS Data Processing - sunav2 logjam loader module combined with sunav2 logfiles clean/assignment module
# Example command (must be run from project parent directory to include modules/ and flow/ paths in Docker context):
# docker build -t neon-is-sunav2-logjam-load-clean-assign -f ./modules_combined/sunav2_logs_group_and_fill/Dockerfile .

FROM registry.access.redhat.com/ubi9/ubi-minimal

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Add in the python-based filter-joiner module
ARG MODULE_DIR="./modules"
ARG APP_DIR="logjam_loader"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_ACCESS_DIR="data_access"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"
ENV LOGJAM_INGEST_BUCKET="neon-nonprod-is-logjam-ingest"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/app-requirements.txt
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/data-access-requirements.txt

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
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/app-requirements.txt && \
    microdnf remove -y --disableplugin=subscription-manager gcc cpp && \
    microdnf clean all --disableplugin=subscription-manager && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}

#base R for R module
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.1

# Build in the module
ARG FLOW_DIR="flow"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./flow/flow.sunav2.logfiles/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in sunav2 log file workflow
COPY ./${FLOW_DIR}/flow.sunav2.logfiles/flow.sunav2.logfiles.R .
COPY ./${FLOW_DIR}/flow.sunav2.logfiles/wrap.sunav2.logfiles.R .

# Run as app user
USER appuser
