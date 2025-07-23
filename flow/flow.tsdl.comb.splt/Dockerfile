# Dockerfile for NEON IS Data Processing - Combine Temperature Specific Depth Lakes data and split by HOR.VER

# Start with the base image. 
FROM us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-pack-base-r:v1.5.2

ARG FLOW_DIR="./flow"
ARG APP_DIR="flow.tsdl.comb.splt"

# maintainer handle
MAINTAINER "Guy Litt" glitt@battelleecology.org, "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ${FLOW_DIR}/${APP_DIR}/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ${FLOW_DIR}/${APP_DIR}/flow.tsdl.comb.splt.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.file.comb.tsdl.splt.R .
COPY ${FLOW_DIR}/${APP_DIR}/wrap.schm.map.char.gsub.R .
COPY ${FLOW_DIR}/${APP_DIR}/def.map.char.gsub.R .
COPY ${FLOW_DIR}/${APP_DIR}/def.schm.avro.pars.map.R .
COPY ${FLOW_DIR}/${APP_DIR}/def.find.mtch.str.best.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
