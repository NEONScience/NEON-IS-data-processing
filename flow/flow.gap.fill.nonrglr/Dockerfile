# Dockerfile for NEON IS Data Processing - flow.gap.fill.nonrglr

# Start with the neon-is-base-r image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.7.0

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.gap.fill.nonrglr"

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/flow.gap.fill.nonrglr.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.gap.fill.nonrglr.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
