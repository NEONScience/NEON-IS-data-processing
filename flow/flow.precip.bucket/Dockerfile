# Dockerfile for NEON IS Data Processing - Quality metrics and final quality flag module

# Start with the base image. 

#talk to cove - best image to use here? 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-qaqc-r:v1.1.8

##need def cal meta. 

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.precip.bucket"

# Copy the lockfile and restore known working versions of R dependency packages
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/flow.precip.bucket.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.precip.bucket.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
