# Dockerfile for NEON IS Data Processing - Threshold selection by date, source-id, term and context

# Start with the neon-is-pack-qaqc-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.9

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.thsh.slct"

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/flow.thsh.slct.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.thsh.slct.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
