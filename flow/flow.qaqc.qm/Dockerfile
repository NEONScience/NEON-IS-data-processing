# Dockerfile for NEON IS Data Processing - Quality metrics and final quality flag module

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-pack-qaqc-r:v0.0.22

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration filter workflow
COPY ./flow.qaqc.qm.R /flow.qaqc.qm.R
