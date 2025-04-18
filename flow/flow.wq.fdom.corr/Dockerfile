# Dockerfile for NEON IS Data Processing - fDOM Temperature and Absorbance Corrections for Water Quality

# Start with the neon-is-base-r image. 
FROM quay.io/battelleecology/neon-is-pack-wq-r:v0.0.2-cb658966

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.wq.fdom.corr"

# maintainer handle
MAINTAINER "Kaelin Cawley" kcawley@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/flow.wq.fdom.corr.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
