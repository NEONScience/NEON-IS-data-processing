# Dockerfile for NEON IS Data Processing - Calibration Filter

# Start with the calibration package image. 
FROM quay.io/battelleecology/neon-is-pack-cal-r:v0.0.10

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy in calibration filter workflow
COPY ./flow.cal.filt.R /flow.cal.filt.R
COPY ./flow.cal.filt.R /calibration_filter.R
