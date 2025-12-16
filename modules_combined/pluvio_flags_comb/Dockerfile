# Dockerfile for NEON IS Data Processing - Combined padded_timeseries_analyzer, Basic QA/QC module (plausibility) and Pluvio 200L Custom Flag module
# Example command (must be run from project root directory to include both modules in Docker context):
# Build with the following command
# docker build --no-cache -t neon-is-pluvio-flgs-f ./modules_combined/pluvio_flags_comb/Dockerfile .

# Start with the neon-is-pack-qaqc-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.9

ARG MODULE_DIR="modules"
ARG APP_DIR="padded_timeseries_analyzer"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt

 
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

# Build in the threshold selection module
ARG MODULE_DIR="flow"
ARG APP_DIR="flow.qaqc.plau"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./${MODULE_DIR}/${APP_DIR}/renv.lock ./renv.lock
RUN R -e 'renv::restore(lockfile="./renv.lock")'

#copy lock file from pluvio flags
ARG APP_DIR2="flow.precip.pluvio.flags"

COPY ./${MODULE_DIR}/${APP_DIR2}/renv.lock ./renv.lock2
RUN R -e 'renv::restore(lockfile="./renv.lock2")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in application code
COPY ./${MODULE_DIR}/${APP_DIR}/wrap.qaqc.plau.R .
COPY ./${MODULE_DIR}/${APP_DIR}/flow.qaqc.plau.R .

# Copy in workflow
COPY ./${MODULE_DIR}/${APP_DIR2}/flow.precip.pluvio.flags.R .
COPY ./${MODULE_DIR}/${APP_DIR2}/wrap.precip.pluvio.flags.R .

# Run as app user
USER appuser
