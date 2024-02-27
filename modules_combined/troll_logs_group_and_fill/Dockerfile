# Dockerfile for NEON IS Data Processing - Troll logs fill module combined with filter-joiner 
# Example command (must be run from project parent directory to include modules/ and flow/ paths in Docker context):
# docker build -t neon-is-troll-logs-group-fill -f ./modules_combined/troll_logs_group_and_fill/Dockerfile .

# Start with the R stats package image. 
# Start with the base R image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.4.2

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
        python3.8 && \
    apt install -y python3-pip && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
   apt-get autoremove -y && \
	 apt-get autoclean -y && \
	 rm -rf /var/lib/apt/lists/* 

# Copy in python code
# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}


# Build in the log file filler module
ARG MODULE_DIR="flow"
ARG APP_DIR_1="flow.troll.logfiles.fill"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./flow/flow.troll.logfiles.fill/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in R code
COPY ./flow/flow.troll.logfiles.fill/flow.troll.logfiles.fill.R .
COPY ./flow/flow.troll.logfiles.fill/wrap.troll.logfiles.fill.R .

# Run as app user
USER appuser
