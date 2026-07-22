# Dockerfile for NEON IS Data Processing - buoy wind-specific direction statistics and uncertainty calculations 
# combined with filter joiner
# Example command (must be run from project parent directory to include modules/ and flow/ paths in Docker context):
# docker build -t neon-is-wind-buoy-direction-stats-ucrt -f ./modules_combined/windBuoy_direction_stats_ucrt/Dockerfile .

# Start with the neon-is-pack-stat-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-stat-r:v1.1.2

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Build in the timeseries padder python module
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
	 rm -rf /var/lib/apt/lists/* && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser 

# Copy in python code
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the threshold selection module 
ARG MODULE_DIR="flow"
ARG APP_DIR="flow.wind.buoy.direction.stats.ucrt"


# Copy the lockfile and restore known working versions of R dependency packages
COPY ./${MODULE_DIR}/${APP_DIR}/renv.lock ./renv.lock
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in application code
COPY ./${MODULE_DIR}/${APP_DIR}/flow.wind.buoy.direction.stats.ucrt.R .
COPY ./${MODULE_DIR}/${APP_DIR}/wrap.wind.buoy.direction.stats.ucrt.R .

# Run as app user
USER appuser
