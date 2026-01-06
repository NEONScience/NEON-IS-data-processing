# Dockerfile for NEON IS Data Processing - Troll Logfile Processing

# Start with the neon-is-base-r image.
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.0

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.troll.logfiles.fill"

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in troll flag workflow
COPY ${FLOW_DIR}/${APP_DIR}/flow.troll.logfiles.fill.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.troll.logfiles.fill.R .

