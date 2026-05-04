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
        python3.8 && \
    apt install -y python3-pip && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
   apt-get autoremove -y && \
	 apt-get autoclean -y && \
	 rm -rf /var/lib/apt/lists/* 

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}


# Build in the flow module
ARG FLOW_DIR="flow"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./flow/flow.exo2.logfiles/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in exo2 log file workflow
COPY ./${FLOW_DIR}/flow.exo2.logfiles/flow.exo2.logfiles.R .
COPY ./${FLOW_DIR}/flow.exo2.logfiles/wrap.exo2.logfiles.R .

# Run as app user
USER appuser
