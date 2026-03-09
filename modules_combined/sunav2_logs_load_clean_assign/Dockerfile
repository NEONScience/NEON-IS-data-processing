# Dockerfile for NEON IS Data Processing - sunav2 logjam loader module combined with sunav2 logfiles clean/assignment module
# Example command (must be run from project parent directory to include modules/ and flow/ paths in Docker context):
# docker build -t neon-is-sunav2-logjam-load-clean-assign -f ./modules_combined/sunav2_logs_group_and_fill/Dockerfile .

# Start with the base R image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.1

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Add in the python-based filter-joiner module
ARG MODULE_DIR="modules"
ARG APP_DIR="logjam_loader"
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
	 rm -rf /var/lib/apt/lists/* && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the module
ARG FLOW_DIR="flow"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./flow/flow.sunav2.logfiles.fill/renv.lock .
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in sunav2 log file workflow
COPY ./${FLOW_DIR}/flow.sunav2.logfiles/flow.sunav2.logfiles.R .
COPY ./${FLOW_DIR}/flow.sunav2.logfiles/wrap.sunav2.logfiles.R .

# Run as app user
USER appuser
