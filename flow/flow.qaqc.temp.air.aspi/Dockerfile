# Dockerfile for NEON IS Data Processing - Sensor-specific QA/QC module for aspirated air temperature

# Start with the neon-is-pack-qaqc-r image. 
FROM quay.io/battelleecology/neon-is-pack-qaqc-r:v0.0.22

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration workflow
COPY ./flow.qaqc.temp.air.aspi.R /flow.qaqc.temp.air.aspi.R