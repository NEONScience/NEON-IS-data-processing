# Dockerfile for NEON IS Data Processing - Calibration Conversion

# Start with the calibration package image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-cal-r:v3.0.2

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.cal.conv"

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/flow.cal.conv.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.cal.conv.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
