# Dockerfile for NEON IS Data Processing - Basic statistics module

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-pack-stat-r:v0.0.3

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration filter workflow
COPY ./flow.stat.basc.R /flow.stat.basc.R
