# Dockerfile for NEON IS Data Processing - Truncate and merge data files by location

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-base-r:v0.0.64

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration filter workflow
COPY ./flow.loc.data.trnc.comb.R /flow.loc.data.trnc.comb.R
