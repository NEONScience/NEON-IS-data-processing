# Dockerfile for NEON IS Data Processing - group_path (split by group) combined with
# flow.concH2oSalinity.grp.split (select and rename depth-specific columns for soil salinity data).
# Example command (must be run from project root directory to include modules/ and flow/ paths in Docker context):
# docker build --no-cache -t neon-is-group-split-conc-h2o-salinity -f ./modules_combined/group_split_concH2o/Dockerfile .

# Start with the base R image.
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v2.1.0

# maintainer handle
MAINTAINER "Teresa Burlingame" tburlingame@battelleecology.org

# Build in the python-based group_path module
ARG MODULE_DIR="modules"
ARG APP_DIR="group_path"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

# For rclone
ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.62.2

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt
COPY ${MODULE_DIR}/${COMMON_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/common_requirements.txt

RUN apt update && \
    apt-get install -y --no-install-recommends \
        python3.8 && \
    apt install -y python3-pip && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/common_requirements.txt && \
    apt install -y wget && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

# Multi-platform package install for rclone
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then ARCHITECTURE=amd64; \
    elif [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then ARCHITECTURE=arm-v7; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then ARCHITECTURE=arm64; \
    else ARCHITECTURE=amd64; fi && \
    TEMP_DEB="$(mktemp)" && \
    wget -O "$TEMP_DEB" "https://github.com/rclone/rclone/releases/download/${RCLONE_VERSION}/rclone-${RCLONE_VERSION}-linux-${ARCHITECTURE}.deb" &&\
    dpkg -i "$TEMP_DEB" &&\
    rm -f "$TEMP_DEB"

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the concH2oSalinity group split R flow module
ARG FLOW_DIR="./flow"
ARG FLOW_APP_DIR="flow.concH2oSalinity.grp.split"

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and has all listed dependencies prior to building this image
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in R application code
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/flow.concH2oSalinity.grp.split.R .
COPY ${FLOW_DIR}/${FLOW_APP_DIR}/wrap.concH2oSalinity.grp.split.R .

# Run as app user
USER appuser
