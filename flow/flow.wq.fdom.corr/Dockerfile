# Dockerfile for NEON IS Data Processing - fDOM Temperature and Absorbance Corrections for Water Quality

# Start with the neon-is-base-r image. 
FROM quay.io/battelleecology/neon-is-pack-wq-r:v0.0.10

# maintainer handle
MAINTAINER "Kaelin Cawley" kcawley@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Copy in fDOM correction workflow
COPY ./flow.wq.fdom.corr.R /flow.wq.fdom.corr.R
