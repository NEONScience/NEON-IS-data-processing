# Dockerfile for NEON IS Data Processing - Science Review Flag Assignment Module
# Example command (must be run from parent directory (flow/flow.srf.asgn) in Docker context:
# docker build -t neon-is-srf-asgn-r .

# Start with the base image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-pub-r:v1.0.6

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.srf.asgn"

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
COPY ${FLOW_DIR}/${APP_DIR}/flow.srf.asgn.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.srf.asgn.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
