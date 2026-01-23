# Dockerfile for NEON IS Data Processing - insufficient data

# Start with the NEON IS base package image 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.0

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.insufficient.data"

# maintainer handle
MAINTAINER "Bobby Hensley" hensley@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in sunav2 flag workflow
COPY ${FLOW_DIR}/${APP_DIR}/flow.insufficient.data.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.insufficient.data.R .

