# Dockerfile for NEON IS Data Processing - exo2 logs fill module combined with filter-joiner 
# Example command (must be run from project parent directory to include modules/ and flow/ paths in Docker context):
# docker build -t neon-is-exo2-logs-group-fill -f ./modules_combined/exo2_logs_group_and_fill/Dockerfile .

# Start with the base R image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.2

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Add in the python-based filter-joiner module
ARG MODULE_DIR="modules"
ARG APP_DIR="filter_joiner"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

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
    pip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    apt-get autoremove -y && \
    apt-get autoclean -y

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}


# Build in the log file filler module
ARG FLOW_DIR="flow"
ARG APP_DIR_1="flow.exo2.logfiles.fill"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR_1}/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in R code
COPY ${FLOW_DIR}/${APP_DIR_1}/flow.exo2.logfiles.fill.R .
COPY ${FLOW_DIR}/${APP_DIR_1}/wrap.exo2.logfiles.fill.R .

# Run as app user
USER appuser
