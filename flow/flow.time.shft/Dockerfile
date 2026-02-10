# Dockerfile for NEON IS Data Processing - Shifting time of L0 data
# Example command to build this image: 
# docker build --no-cache -t neon-is-time-shft-r ~/R/NEON-IS-data-processing/flow/flow.time.shft/

# Start with the pub package image. 

FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.0

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.time.shft"

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/wrap.time.shft.R .
COPY ${FLOW_DIR}/${APP_DIR}/flow.time.shft.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
