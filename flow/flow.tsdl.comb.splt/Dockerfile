# Dockerfile for NEON IS Data Processing - Combine Temperature Specific Depth Lakes data and split by HOR.VER

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-base-r:v0.0.0

# maintainer handle
MAINTAINER "Guy Litt" glitt@battelleecology.org, Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in calibration filter workflow
COPY ./flow.tsdl.comb.splt.R /flow.tsdl.comb.splt.R
COPY ./wrap.file.comb.tsdl.splt.R /wrap.file.comb.tsdl.splt.R