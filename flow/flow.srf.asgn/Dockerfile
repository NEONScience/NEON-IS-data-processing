# Dockerfile for NEON IS Data Processing - Science Review Flag Assignment Module
# Example command (must be run from parent directory (flow/flow.srf.asgn) in Docker context:
# docker build -t neon-is-srf-asgn-r .

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-pack-pub-r:v1.0.1

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ./flow.srf.asgn.R .
COPY ./wrap.srf.asgn.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
