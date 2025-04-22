# Dockerfile for NEON IS Data Processing - Pluvio 200L Custom Flag module
# Build with the following command
# docker build --no-cache -t neon-is-pluvio-flgs <path to this directory>

# Start with the qaqc package image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-ts-pad-anls-qaqc-plau:v1.1.1

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.precip.pluvio.flags"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser
WORKDIR /home/appuser

# Copy in workflow
COPY ${FLOW_DIR}/${APP_DIR}/flow.precip.pluvio.flags.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.precip.pluvio.flags.R .


# Run as app user
RUN chown appuser:appuser -R /home/appuser
USER appuser
