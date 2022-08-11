# Dockerfile for NEON IS Data Processing - Troll Elevation Conversion and Uncertainty Calculations

# Start with the neon-is-base-r image. 
FROM quay.io/battelleecology/neon-is-pack-stat-r:v1.0.1

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in troll flag workflow
COPY ./flow.troll.uncertainty.R /flow.troll.uncertainty.R
