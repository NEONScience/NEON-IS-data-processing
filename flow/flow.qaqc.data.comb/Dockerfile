# Dockerfile for NEON IS Data Processing - Merge QC-filtered data files

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-base-r:v0.0.31

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy in calibration filter workflow
COPY ./flow.qaqc.data.comb.R /flow.qaqc.data.comb.R
