# Dockerfile for NEON IS Data Processing - Basic statistics module

# Start with the base image. 
FROM quay.io/battelleecology/neon-is-pack-stat-r:v1.1.1

# maintainer handle
MAINTAINER "Cove Sturtevant" csturtevant@battelleecology.org

# Copy the lockfile and restore known working versions of R dependency packages
COPY ./renv.lock /renv.lock
RUN R -e 'renv::restore(lockfile="/renv.lock")'

# Create app user
RUN groupadd app && \
    useradd app -g app
WORKDIR /home/app

# Copy in application code
COPY ./flow.stat.basc.R .
COPY ./wrap.stat.basc.R .

# Run as app user
RUN chown app:app -R /home/app
USER app

