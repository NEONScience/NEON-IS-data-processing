# Dockerfile for NEON IS Data Processing - Basic QA/QC module (plausibility)

# Start with the neon-is-pack-qaqc-r image. 
FROM quay.io/battelleecology/neon-is-pack-qaqc-r:v1.0.5

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in calibration workflow
COPY ./wrap.qaqc.plau.R .
COPY ./flow.qaqc.plau.R .

# Run as app user
RUN chown app:app -R /home/app
USER app
