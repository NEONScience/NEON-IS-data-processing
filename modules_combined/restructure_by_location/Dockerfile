# Dockerfile for NEON IS Data Processing - Restructure and merge data files by location
# This image combines the two workflows flow.loc.repo.strc and flow.loc.data.trnc.comb
# Build with the following command
# docker build --no-cache -t neon-is-loc-strc-comb-r -f <path to this file> <path to flow directory>

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.0.6

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
# ENSURE that the renv.lock file is up-to-date and thus has all listed dependencies prior to creating this docker image
COPY ./flow.loc.repo.strc/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'
COPY ./flow.loc.data.trnc.comb/renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd appuser && \
    useradd appuser -g appuser
WORKDIR /usr/src/app

# Copy in application code
COPY ./flow.loc.repo.strc/wrap.loc.repo.strc.R .
COPY ./flow.loc.repo.strc/flow.loc.repo.strc.R .
COPY ./flow.loc.data.trnc.comb/wrap.loc.data.trnc.comb.R .
COPY ./flow.loc.data.trnc.comb/flow.loc.data.trnc.comb.R .

# Run as app user
RUN chown appuser:appuser -R /usr/src/app
USER appuser
