# Dockerfile for NEON IS Data Processing - Merge QC-filtered data files

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-base-r:v0.0.64

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration filter workflow
COPY ./flow.qaqc.data.comb.R /flow.qaqc.data.comb.R
