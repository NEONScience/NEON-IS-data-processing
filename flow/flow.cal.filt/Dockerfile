# Dockerfile for NEON IS Data Processing - Calibration Filter

# Start with the calibration package image. 
FROM quay.io/battelleecology/neon-is-pack-cal-r:v0.0.38

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration filter workflow
COPY ./flow.cal.filt.R /flow.cal.filt.R
