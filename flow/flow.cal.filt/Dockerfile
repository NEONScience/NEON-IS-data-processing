# Dockerfile for NEON IS Data Processing - Calibration Filter

# Start with the calibration package image. 
FROM quay.io/battelleecology/neon-is-pack-cal-r:v0.0.10

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy in calibration filter workflow
COPY ./flow.cal.filt.R /flow.cal.filt.R
COPY ./calibration_filter.R /calibration_filter.R
#below line is needed for testing in the local docker container for input files.
#COPY  ./tests/test_input /test_input