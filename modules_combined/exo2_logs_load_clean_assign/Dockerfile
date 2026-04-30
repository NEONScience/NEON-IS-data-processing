# Dockerfile for NEON IS Data Processing - exo2 logjam loader module combined with exo2 logfiles clean/assignment module
# Example command (must be run from project parent directory to include modules/ and flow/ paths in Docker context):
# docker build -t neon-is-exo2-logs-load-clean-assign -f ./modules_combined/exo2_logs_load_clean_assign/Dockerfile .

FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.2

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Add in the python-based logjam loader module
ARG MODULE_DIR="./modules"
ARG APP_DIR="logjam_loader"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_ACCESS_DIR="data_access"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"
ENV LOGJAM_INGEST_BUCKET="neon-nonprod-is-logjam-ingest"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/data-access-requirements.txt
    
RUN apt update && \
    apt-get install -y --no-install-recommends \
        software-properties-common \
        gnupg && \
    rm -rf /var/lib/apt/lists/* 
    
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt update
RUN apt-get install -y python3.9 python3.9-venv python3-pip
RUN python3.9 -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${DATA_ACCESS_DIR} ${CONTAINER_APP_DIR}/${DATA_ACCESS_DIR}


# Build in the module
ARG FLOW_DIR="flow"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./flow/flow.exo2.logfiles/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in exo2 log file workflow
COPY ./${FLOW_DIR}/flow.exo2.logfiles/flow.exo2.logfiles.R .
COPY ./${FLOW_DIR}/flow.exo2.logfiles/wrap.exo2.logfiles.R .

# Run as app user
USER appuser
