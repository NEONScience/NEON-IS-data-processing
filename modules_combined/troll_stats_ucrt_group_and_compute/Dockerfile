# Dockerfile for NEON IS Data Processing - Basic statistics module combined with filter-joiner and troll-specific uncertainty
# Example command (must be run from project parent directory to include modules/ and flow/ paths in Docker context):
# docker build -t neon-is-troll-stat-ucrt-grp -f ./modules_combined/troll_stats_ucrt_group_and_compute/Dockerfile .

# Start with the R stats package image. 
FROM quay.io/battelleecology/neon-is-pack-stat-r:v1.1.0

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

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
COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}


# Build in the basic stats module
ARG MODULE_DIR="flow"
ARG APP_DIR_1="flow.stat.basc"
ARG APP_DIR_2="flow.troll.uncertainty"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./${MODULE_DIR}/${APP_DIR_1}/renv.lock ./renv.lock.1
RUN R -e 'renv::restore(lockfile="./renv.lock.1")'
COPY ./${MODULE_DIR}/${APP_DIR_1}/renv.lock ./renv.lock.2
RUN R -e 'renv::restore(lockfile="./renv.lock.2")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser

# Copy in application code
COPY ./${MODULE_DIR}/${APP_DIR_1}/wrap.stat.basc.R .
COPY ./${MODULE_DIR}/${APP_DIR_1}/flow.stat.basc.R .
COPY ./${MODULE_DIR}/${APP_DIR_2}/flow.troll.uncertainty.R .

# Run as app user
USER appuser
