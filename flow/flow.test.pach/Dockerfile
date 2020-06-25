# Dockerfile for NEON IS Data Processing - Test script

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-base-r:v0.0.52

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org


# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'


# Copy in test workflow
COPY ./flow.test.pach.R /flow.test.pach.R
