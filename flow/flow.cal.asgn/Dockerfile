# Dockerfile for NEON IS Data Processing - Calibration assignment

# Start with the calibration package image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-cal-r:v3.1.1
ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.cal.asgn"

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
COPY ${FLOW_DIR}/${APP_DIR}/flow.cal.asgn.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.cal.asgn.R  .

# Run as app user
RUN chown app:app -R /home/app
USER app
