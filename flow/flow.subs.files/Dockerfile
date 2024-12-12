# Dockerfile for NEON IS Data Processing - Subsurface tchain file Processing

# Start with the neon-is-base-r image.
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.4.8

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.subs.files"

# maintainer handle
MAINTAINER "Kaelin Cawley" kcawley@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in subsurface file workflow
COPY ${FLOW_DIR}/${APP_DIR}/flow.subs.files.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.subs.LT400.files.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.subs.HOBOU24.files.R .

