# Dockerfile for NEON IS Data Processing - buoy wind-specific quality flags and SCI statistics

# Start with the neon-is-pack-qaqc-r image.
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.14

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.wind.buoy.sci.stats"

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in buoy wind flag workflow
COPY ${FLOW_DIR}/${APP_DIR}/flow.wind.buoy.sci.stats.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.wind.buoy.sci.stats.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
