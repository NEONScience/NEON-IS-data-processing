# Dockerfile for NEON IS Data Processing - Select thresholds by date, source-id, term and context, plus the first stage of the timeseries padder (based on thresholds)
# Example command (must be run from project root directory to include both modules in Docker context):
# Build with the following command
# docker build --no-cache -t neon-is-thsh-slct-ts-pad -f ./modules_combined/threshold_select_and_timeseries_padder/Dockerfile .

# Start with the neon-is-pack-qaqc-r image. 
FROM quay.io/battelleecology/neon-is-pack-qaqc-r:v1.1.5-bed3a052

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Build in the timeseries padder python module
ARG MODULE_DIR="modules"
ARG APP_DIR="timeseries_padder"
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
ARG APP_DIR="flow.thsh.slct"


# Copy the lockfile and restore known working versions of R dependency packages
COPY ./${MODULE_DIR}/${APP_DIR}/renv.lock ./renv.lock
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in application code
COPY ./${MODULE_DIR}/${APP_DIR}/flow.thsh.slct.R .
COPY ./${MODULE_DIR}/${APP_DIR}/wrap.thsh.slct.R .

# Run as app user
USER appuser
