# Dockerfile for NEON IS Data Processing - Calibration Conversion

# Start with the calibration package image. 
FROM quay.io/battelleecology/neon-is-pack-cal-r:v1.3.2-bed3a052

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
COPY ./flow.cal.conv.R .
COPY ./wrap.cal.conv.dp0p.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
