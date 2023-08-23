# Dockerfile for NEON IS Data Processing - Combine kafka data
# Example command to build this image: 
# docker build --no-cache -t neon-is-kfka-comb-r ~/R/NEON-IS-data-processing/flow/flow.kfka.comb/

# Start with the pub package image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.2.0

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ./wrap.kfka.comb.R .
COPY ./flow.kfka.comb.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
