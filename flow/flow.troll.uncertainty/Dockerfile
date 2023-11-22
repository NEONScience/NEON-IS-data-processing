# Dockerfile for NEON IS Data Processing - Troll Elevation Conversion and Uncertainty Calculations

# Start with the NEON IS stats package image 
FROM quay.io/battelleecology/neon-is-pack-stat-r:v1.1.5

# maintainer handle
MAINTAINER "Nora Catolico" ncatolico@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in troll flag workflow
COPY ./flow.troll.uncertainty.R .
COPY ./wrap.troll.uncertainty.R .

