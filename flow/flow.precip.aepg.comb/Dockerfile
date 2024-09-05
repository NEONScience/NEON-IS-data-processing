# Dockerfile for NEON IS Data Processing - Belfort (aepg600m) Preciptation combine module
# Build with the following command
# docker build --no-cache -t neon-is-precip-aepg-comb-r <path to this directory>

# Start with the neon-is-precip-aepg-comb-r image. 
FROM quay.io/battelleecology/neon-is-base-r:v1.4.7-aa651ed4

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org
MAINTAINER "Teresa Burlingame" tburlingame@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in calibration workflow
COPY ./wrap.precip.aepg.comb.R .
COPY ./flow.precip.aepg.comb.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
