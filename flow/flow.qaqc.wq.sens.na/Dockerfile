# Dockerfile for NEON IS Data Processing - Water Quality Sensor NA test 

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.1.3

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
COPY ./flow.qaqc.wq.sens.na.R .

# Run as app user
RUN chown app:app -R /home/app
USER app

