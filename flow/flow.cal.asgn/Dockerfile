# Dockerfile for NEON IS Data Processing - Calibration assignment

# Start with the calibration package image. 
FROM quay.io/battelleecology/neon-is-pack-cal-r:v0.0.40

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration filter workflow
COPY ./flow.cal.asgn.R /flow.cal.asgn.R
COPY ./wrap.cal.asgn.R /wrap.cal.asgn.R
