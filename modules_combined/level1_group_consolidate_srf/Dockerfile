####
#
# This dockerfile will build the combined module for level1_consolidate, filter_joiner, and flow.pub.tabl.
# Example command (must be run from project root directory to include modules and modules_combined path in Docker context):
# docker build -t neon-is-levl1-grp-cons-srf -f ./modules_combined/level1_group_consolidate_srf/Dockerfile .
#
###
# Start with the pub package R image. 
FROM quay.io/battelleecology/neon-is-pack-pub-r:v1.0.0

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

ARG MODULE_DIR="modules"
ARG APP_DIR="level1_consolidate"
ARG APP_DIR_2="filter_joiner"
ARG COMMON_DIR="common"
ARG CONTAINER_APP_DIR="/usr/src/app"
ARG DATA_DIR="/data"
ENV PYTHONPATH="${PYTHONPATH}:${CONTAINER_APP_DIR}"

WORKDIR ${CONTAINER_APP_DIR}

COPY ${MODULE_DIR}/${APP_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt
COPY ${MODULE_DIR}/${APP_DIR_2}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/requirements2.txt
COPY ${MODULE_DIR}/${COMMON_DIR}/requirements.txt ${CONTAINER_APP_DIR}/${APP_DIR}/common_requirements.txt

RUN apt update && \
    apt-get install -y --no-install-recommends \
        python3.8 && \
    apt install -y python3-pip && \
    python3 -mpip install --no-cache-dir --upgrade pip setuptools wheel && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/requirements2.txt && \
    python3 -mpip install --no-cache-dir -r ${CONTAINER_APP_DIR}/${APP_DIR}/common_requirements.txt && \
   apt-get autoremove -y && \
	 apt-get autoclean -y && \
	 rm -rf /var/lib/apt/lists/* && \
    groupadd -g 9999 appuser && \
    useradd -r -u 9999 -g appuser appuser && \
    mkdir ${DATA_DIR} && \
    chown appuser:appuser -R ${CONTAINER_APP_DIR} && \
    chown appuser:appuser -R ${DATA_DIR}

COPY ${MODULE_DIR}/${APP_DIR} ${CONTAINER_APP_DIR}/${APP_DIR}
COPY ${MODULE_DIR}/${APP_DIR_2} ${CONTAINER_APP_DIR}/${APP_DIR_2}
COPY ${MODULE_DIR}/${COMMON_DIR} ${CONTAINER_APP_DIR}/${COMMON_DIR}

# Build in the flow.pub.tabl module
ARG MODULE_DIR="flow"

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./${MODULE_DIR}/flow.pub.tabl.srf/renv.lock ./renv.lock
RUN R -e 'renv::restore(lockfile="./renv.lock")'

# Copy in application code
COPY ./${MODULE_DIR}/flow.pub.tabl.srf/flow.pub.tabl.srf.R .
COPY ./${MODULE_DIR}/flow.pub.tabl.srf/wrap.pub.tabl.srf.R .

# Run as app user
USER appuser