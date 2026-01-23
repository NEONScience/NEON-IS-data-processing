# Dockerfile for NEON IS Data Processing - sunav2 sensor-specific quality flags

# Start with the neon-is-pack-qaqc-r image.
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.10

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.sunav2.quality.flags"

# maintainer handle
MAINTAINER "Bobby Hensley" hensley@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in sunav2 flag workflow
COPY ${FLOW_DIR}/${APP_DIR}/flow.sunav2.quality.flags.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.sunav2.quality.flags.R .

