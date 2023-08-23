# Dockerfile for NEON IS Data Processing - Create pub tables and apply srf

# Start with the pub package image. 
FROM quay.io/battelleecology/neon-is-pack-pub-r:v1.1.0

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
COPY ./wrap.pub.tabl.srf.R .
COPY ./flow.pub.tabl.srf.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
