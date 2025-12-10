# Dockerfile for NEON IS Data Processing - Basic QA/QC module (plausibility)
# Build with the following command
# docker build --no-cache -t neon-is-qaqc-plau-r <path to this directory>

# Start with the neon-is-pack-qaqc-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.9

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.qaqc.plau"

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in calibration workflow
COPY ${FLOW_DIR}/${APP_DIR}/wrap.qaqc.plau.R .
COPY ${FLOW_DIR}/${APP_DIR}/flow.qaqc.plau.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
