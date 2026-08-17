####
#
# This dockerfile will build the combined module for level1_consolidate, filter_joiner, flow.pub.tabl, and flow.srf.asgn
# It also includes rclone for exporting output to a bucket
# Example command (must be run from project root directory to include modules and modules_combined path in Docker context):
# docker build -t neon-is-levl1-grp-cons-srf -f ./modules_combined/level1_group_consolidate_srf/Dockerfile .
#
###
# Start with the pub package R image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-pub-r:v1.0.10

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

ARG MODULE_DIR="modules"
ARG APP_DIR_1="level1_consolidate"
ARG APP_DIR_2="filter_joiner"
ARG APP_DIR_3="pub_workbook_loader"
ARG COMMON_DIR="common"
ARG DATA_ACCESS_DIR="data_access"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

# For rclone
ARG TARGETPLATFORM
ARG RCLONE_VERSION=v1.62.2

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR_1}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_3}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR_3}/requirements.txt
COPY ${MODULE_DIR}/${COMMON_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${COMMON_DIR}/requirements.txt
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}/requirements.txt

RUN apt update && \
    apt-get install -y --no-install-recommends \
        jq \
        build-essential \
        ca-certificates \
        wget \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        liblzma-dev \
        xz-utils && \
    rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.11.9
RUN wget -q https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j"$(nproc)" && \
    make altinstall && \
    cd / && \
    rm -rf /usr/src/app/Python-${PYTHON_VERSION} /usr/src/app/Python-${PYTHON_VERSION}.tgz

RUN python3.11 -m venv /opt/venv
    
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"


RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_1}/requirements.txt && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_2}/requirements.txt && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR_3}/requirements.txt && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}/requirements.txt && \
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${COMMON_DIR}/requirements.txt && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
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


COPY ${MODULE_DIR}/${APP_DIR_1} ${CONTAINER_APP_DIR}/${APP_DIR_1}
COPY ${MODULE_DIR}/${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}
COPY ${MODULE_DIR}/${APP_DIR_3} ${CONTAINER_APP_DIR}/${APP_DIR_3}
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the flow.pub.tabl module
ARG MODULE_DIR="flow"

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./${MODULE_DIR}/flow.srf.asgn/renv.lock ./renv.lock.srf.asgn
RUN R -e 'renv::restore(lockfile="./renv.lock.srf.asgn")'
COPY ./${MODULE_DIR}/flow.pub.tabl.srf/renv.lock ./renv.lock.pub.tabl.srf
RUN R -e 'renv::restore(lockfile="./renv.lock.pub.tabl.srf")'

# Copy in application code
COPY ./${MODULE_DIR}/flow.pub.tabl.srf/flow.pub.tabl.srf.R .
COPY ./${MODULE_DIR}/flow.pub.tabl.srf/wrap.pub.tabl.srf.R .
COPY ./${MODULE_DIR}/flow.srf.asgn/flow.srf.asgn.R .
COPY ./${MODULE_DIR}/flow.srf.asgn/wrap.srf.asgn.R .

# Run as app user
USER appuser
